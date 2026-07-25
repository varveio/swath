/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.ParquetReads;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * INT-7 + PERF-1 (smoke): the first time {@link WorkStealingScan} runs against the
 * REAL SDK path — the production {@link S3PageFetcher} over a LocalStack bucket —
 * rather than the in-memory {@code MockPageFetcher}. Proves the parallel engine works
 * end-to-end on real S3 semantics:
 *
 * <ul>
 *   <li><b>INT-7 correctness</b> — a checkpointed {@code WorkStealingScan} run to
 *       Parquet over a modest, badly-skewed bucket COMPLETES, and the union of every
 *       Parquet part equals the uploaded key set exactly (full coverage, no dup/drop),
 *       byte-exact in unsigned order.</li>
 *   <li><b>concurrency reaches {@code T}</b> — an instrumented fetcher records the peak
 *       number of concurrent bulk page fetches; a serialized engine would peak at 1.</li>
 *   <li><b>PERF-1 smoke (stealer balances the skew)</b> — the per-worker bulk-fetch
 *       distribution shows the work was spread across many workers (no single worker did
 *       most of it), i.e. the thief rebalanced the skew concentrated under one prefix.</li>
 *   <li><b>byte-order</b> — a second engine run through the raw pipeline asserts every
 *       emitted page is strictly ascending in unsigned byte order on the real path.</li>
 * </ul>
 *
 * <p><b>Bounded by design</b> (docs/ops/dev/TESTING.md): a modest count of zero-byte
 * objects ({@link #OBJECTS} keys), <b>all under one dominant prefix</b> {@code hot/} so
 * stealing — not top-level prefix fan-out — is the only way to parallelize. No
 * mass-populate, no scale fixture — the heavy ≤50M PERF tier stays out of the default
 * suite. The instrumented fetcher adds a few ms of latency per bulk fetch so the
 * in-flight peak is observable on fast LocalStack; the real ListObjectsV2 call still runs.
 *
 * <p><b>Why high-entropy keys.</b> The suffix under {@code hot/} is spread densely across
 * the hex space, so a thief's split pivot lands in populated content in O(1) probes. A
 * deep-shared-prefix, low-entropy shape ({@code hot/%010d} — keys clustered in a tiny
 * {@code hot/000000…} sub-window) is the band-wider-than-content pathology amplified: at
 * this modest scale, with real per-call latency, the retry-nearer-cursor convergence loses
 * the race to a victim that drains the whole keyspace first, so it would NOT exercise a
 * committed steal. That is a demand-driven-stealing scale property, not a correctness bug.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class Int7EngineLocalStackIT {

    /** Modest, skewed, zero-byte fixture — a few S3 pages, not a benchmark (TESTING.md). */
    private static final int OBJECTS = 2000;
    /** Small page size ⇒ many pages ⇒ the hot range is split across many workers. */
    private static final int MAX_KEYS = 50;
    /** {@code T} > 1: the parallel path. The gauge bounds bulk fetches to this. */
    private static final int WORKERS = 8;
    /** Widen the in-flight window so the concurrency peak is observable on fast LocalStack. */
    private static final long FETCH_DELAY_MILLIS = 4;

    private static final String BUCKET = "int7-engine";

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private static S3Client s3;
    private static List<byte[]> uploaded;

    @BeforeAll
    static void setUp() {
        s3 = LocalStackSupport.client(LOCALSTACK);
        LocalStackSupport.createBucket(s3, BUCKET);
        uploaded = skewedHighEntropy(OBJECTS);
        LocalStackSupport.putObjects(s3, BUCKET, uploaded, 96);
    }

    /**
     * {@link #OBJECTS} keys ALL under one dominant prefix {@code hot/}, with an 8-hex suffix
     * spread densely across the whole {@code 00000000..ffffffff} space so split pivots land in
     * populated content. The single-dominant-prefix skew forces the engine to steal sub-ranges
     * of {@code hot/} (there is no top-level prefix fan-out to fall back on).
     */
    private static List<byte[]> skewedHighEntropy(int n) {
        long step = 0xFFFFFFFFL / n;
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long v = (i * step) & 0xFFFFFFFFL;
            keys.add(String.format("hot/%08x", v).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @AfterAll
    static void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    private static RunKey key(String hash) {
        return new RunKey("s3", null, BUCKET, new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    /**
     * INT-7 correctness + concurrency-reaches-T + PERF-1 stealer-balance, in one run of
     * the production engine→Parquet path against the real {@link S3PageFetcher}.
     *
     * <p>Guards the two live-SDK-only correctness properties INT-7 exposed: a transient, un-started open frontier
     * returns RETRY instead of being mislabeled permanently unsplittable, and split pivots are
     * synthesized as S3-request-safe XML boundaries even when otherwise-valid UTF-8 would be
     * unsafe as an XML request parameter. A failure here is a real regression in the production
     * S3 path, ordering, exactly-once coverage, or steal balance.
     */
    @Test
    @Timeout(120)
    void int7_engineListsCompletelySortedAndBalancedOnRealS3(@TempDir Path dir) throws Exception {
        InstrumentedFetcher fetcher = new InstrumentedFetcher(new S3PageFetcher(s3, BUCKET));

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key("int7-parquet"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            var spec = new ListRunner.ParquetSpec(new byte[0], 64, MAX_KEYS, FilterChain.EMPTY,
                    3, 64 * 1024, 16, "int7-parquet", null, null, 0L, 0L, "");   // small target ⇒ multiple parts
            var stats = new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec, store, run.id(), WORKERS, seeds, List.of());

            assertThat(stats.objects()).isEqualTo(OBJECTS);
        }

        // ---- INT-7: the Parquet union equals the bucket, byte-exact, in unsigned order ----
        assertThat(DatasetLayout.of(dir).manifest()).exists();
        List<String> all = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            all.addAll(ParquetReads.keys(part));
        }
        assertThat(all).as("exactly-once across every Parquet part (no dup, no drop)").hasSize(OBJECTS);
        TreeSet<String> union = new TreeSet<>(all);
        assertThat(union).as("every uploaded key present exactly once").hasSize(OBJECTS);

        // Byte-order: the sorted Parquet output, byte-for-byte, equals the byte-sorted upload.
        List<byte[]> expected = new ArrayList<>(uploaded);
        expected.sort(Arrays::compareUnsigned);
        List<byte[]> actual = all.stream().map(k -> k.getBytes(StandardCharsets.UTF_8))
                .sorted(Arrays::compareUnsigned).toList();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i)).as("byte-exact key #%d round-tripped through the real SDK", i)
                    .isEqualTo(expected.get(i));
        }

        int peak = fetcher.peakInFlight();
        Map<Long, Long> perWorker = fetcher.bulkFetchesByThread();
        long totalBulk = perWorker.values().stream().mapToLong(Long::longValue).sum();
        long maxOneWorker = perWorker.values().stream().mapToLong(Long::longValue).max().orElse(0);
        double maxShare = (double) maxOneWorker / totalBulk;

        // ---- concurrency reaches T: the engine genuinely overlapped bulk fetches ----
        assertThat(peak)
                .as("engine ran bulk fetches concurrently (peak in-flight=%d, T=%d); a serialized "
                        + "engine would peak at 1", peak, WORKERS)
                .isGreaterThan(1);

        // ---- PERF-1 smoke: the stealer balanced the skew across workers ----
        assertThat(perWorker.size())
                .as("work spread across multiple workers (stealing rebalanced the skew); workers=%s",
                        perWorker)
                .isGreaterThanOrEqualTo(3);
        assertThat(maxShare)
                .as("no single worker did most of the work (max share %.2f of %d bulk fetches across %d "
                        + "workers ⇒ balanced steal, not one worker hogging the hot prefix)",
                        maxShare, totalBulk, perWorker.size())
                .isLessThan(0.5);
    }

    /**
     * Byte-order guard on the real path: drives the engine through the raw pipeline (so each
     * emitted {@link PageBatch} is visible) and asserts every page is strictly ascending in
     * unsigned byte order — a real-SDK ordering / encoding bug would surface here even though
     * a set-union over Parquet parts would miss it. Also re-confirms exactly-once coverage.
     *
     * <p>Guards the same live-SDK-only properties as
     * {@link #int7_engineListsCompletelySortedAndBalancedOnRealS3} — thief probes on the
     * LocalStack path stay valid S3 request parameters. A failure here is a current ordering,
     * encoding, or coverage regression, not an expected flake.
     */
    @Test
    @Timeout(120)
    void int7_engineEmitsByteOrderedPagesOnRealS3(@TempDir Path dir) throws Exception {
        S3PageFetcher fetcher = new S3PageFetcher(s3, BUCKET);

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key("int7-order"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = new ArrayList<>();
            PipelineDrain.drain(64, engine, batch -> {
                List<ListEntry> entries = batch.entries();
                // Every emitted page must be strictly ascending in unsigned byte order.
                for (int i = 1; i < entries.size(); i++) {
                    byte[] prev = entries.get(i - 1).key().raw();
                    byte[] cur = entries.get(i).key().raw();
                    assertThat(KeyBytes.compareUnsigned(prev, cur))
                            .as("page keys strictly ascending in unsigned byte order on the real S3 path")
                            .isLessThan(0);
                }
                for (ListEntry e : entries) {
                    emitted.add(e.key().raw());
                }
            });

            TreeSet<byte[]> distinct = new TreeSet<>(Arrays::compareUnsigned);
            distinct.addAll(emitted);
            assertThat(emitted).as("no key emitted twice").hasSize(OBJECTS);
            assertThat(distinct).as("every uploaded key emitted exactly once").hasSize(OBJECTS);
        }
    }

    /**
     * Wraps the real {@link S3PageFetcher} to observe the engine on the live path: tracks the
     * peak number of concurrent <b>bulk</b> page fetches (maxKeys &gt; 1 — the worker listings,
     * not the 1-key thief probes) and the per-worker bulk-fetch count, and adds a small delay so
     * the in-flight peak is observable on fast LocalStack. The real ListObjectsV2 call still runs.
     */
    private static final class InstrumentedFetcher implements PageFetcher {

        private final PageFetcher delegate;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final ConcurrentHashMap<Long, LongAdder> byThread = new ConcurrentHashMap<>();

        InstrumentedFetcher(PageFetcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            boolean bulk = req.maxKeys() > 1;   // worker listing; the thief probes with maxKeys == 1
            if (!bulk) {
                return delegate.fetchPage(req);
            }
            byThread.computeIfAbsent(Thread.currentThread().threadId(), k -> new LongAdder()).increment();
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(FETCH_DELAY_MILLIS);   // hold the slot longer so overlap is observable
                return delegate.fetchPage(req);
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public StoreCapabilities capabilities() {
            return delegate.capabilities();
        }

        int peakInFlight() {
            return peak.get();
        }

        Map<Long, Long> bulkFetchesByThread() {
            Map<Long, Long> out = new HashMap<>();
            byThread.forEach((k, v) -> out.put(k, v.sum()));
            return out;
        }
    }
}
