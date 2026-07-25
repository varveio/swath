/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>PERF-1</b> — the work-stealing payoff on a badly-skewed keyspace (algorithms.md §3/§6/§7). A
 * 100k-key bucket with <b>99% of keys under one narrow
 * prefix</b> ({@link Keyspaces#badlySkewed}) is driven through {@link WorkStealingScan}
 * with small pages and an injected per-page latency so the seed worker genuinely idles
 * its peers — forcing them to <b>steal</b> the dense tail rather than wait. It asserts the
 * three contract guarantees:
 *
 * <ol>
 *   <li><b>The stealer balances</b> (algorithms.md §6): the dense prefix is carved across
 *       many ranges, none bottlenecked on the one worker that seeded it.</li>
 *   <li><b>Probe/steal overhead is bounded</b> {@code O(W·log ρ)}, not {@code O(N)}
 *       (algorithms.md §7): the <b>effective range partition</b> stays within an
 *       {@code O(W·log N)} ceiling even as the keyspace doubles.</li>
 *   <li><b>Exactly-once</b> — every key emitted exactly once: no gap, no overlap, no
 *       duplicate (I1/I2/I3), preserved from the smoke test.</li>
 * </ol>
 *
 * <p><b>Why the assertions are non-flaky.</b> Heavy/opt-in ({@code @Tag("perf")}):
 * <ul>
 *   <li><b>Balance is a work-distribution assertion, not a wall-clock one.</b> This asserts
 *       <i>which {@code nodeId}s emitted the keys</i> — that the dense prefix is split across
 *       {@code ≥ MIN_RANGES} ranges and no single range holds {@code ≥ MAX_RANGE_SHARE} of the
 *       keys. The injected per-page latency guarantees the seed worker idles its peers, so
 *       stealing <b>must</b> occur and the distribution is structural, not timing-dependent.
 *       A broken (no-steal) engine would leave one seed range at ~100% — far above the
 *       threshold — so balanced vs. bottlenecked is an enormous gap, not a marginal one.
 *       (Observed: ~40 emitting ranges, busiest ≈ 10%.)</li>
 *   <li><b>Overhead is the EFFECTIVE-RANGE count, asserted at two keyspace sizes.</b> This runs
 *       the SAME skew at {@code N} and {@code 2N} and asserts the number of distinct ranges that
 *       actually emitted keys (= seeds + non-empty splits = the partition the §7 cost model
 *       bounds by {@code O(W·log ρ)}) stays under one {@code O(W·log N)} ceiling for <b>both</b>
 *       sizes. An {@code O(N)} engine (per-key probing) would produce a partition that grows with
 *       {@code N} and blow past a log-scaled ceiling; an {@code O(W·log ρ)} engine fits it at both
 *       sizes. Observed ≈ 36–73 vs. a ceiling of ~{@value WORKERS}·8·log2(2N) ≈ 1152 — a 16×
 *       margin that absorbs scheduling jitter while staying ~170× below the {@code O(N)} scale
 *       ({@code 2N} = 200 000).</li>
 * </ul>
 *
 * <p><b>Metric choice / limitation.</b> The engine exposes steal <i>outcomes</i>
 * ({@code swath.steals{result}}) and a split counter, not a raw probe counter. This deliberately does
 * <b>not</b> assert on the raw 1-key probe count nor the total {@code CHILD_CREATED} split count:
 * both are dominated by <i>speculative</i> re-stealing (idle workers re-probe every park interval
 * while ranges drain) and are therefore JIT/scheduling-variant — across runs the raw probe count
 * swings ~11k–41k and splits ~1.8k–6.1k, while the <i>effective</i> non-empty range partition stays
 * tight (~36–73). The effective-range count is the stable, store-independent structural signal of
 * the {@code O(W·log ρ)} partition cost; the probe count is measured/reported but not gated.
 *
 * <p><b>Why NOT a wall-clock speedup.</b> The in-memory {@link MockPageFetcher} computes each page by
 * scanning its keyspace, so every call (bulk page <i>and</i> 1-key probe) costs {@code O(keyspace)}
 * CPU — unlike a real store, where a probe is one cheap network round-trip. The parallel path issues
 * far more probes than a {@code T=1} run, so a wall-clock comparison would measure the mock's
 * probe-CPU overhead, not real-store parallelism, and would misleadingly make parallel look slower.
 * On a real store the speedup holds; the mock cannot model that faithfully, so balance is asserted
 * <b>structurally</b>.
 */
@Tag("perf")
final class WorkStealingScanPerf1Test {

    private static final int N = 100_000;
    private static final double HOT_FRACTION = 0.99;   // 99% under "hot/"
    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 500;           // small pages ⇒ many idle/steal windows
    private static final Duration PAGE_LATENCY = Duration.ofMillis(2);   // bulk pages only, not probes

    // Balance thresholds — generous, chosen so a balanced run clears them by a wide margin
    // (observed ~40 ranges, busiest ≈10%) while a no-steal bottleneck (one range ~100%) fails them.
    private static final int MIN_RANGES = 4;
    private static final double MAX_RANGE_SHARE = 0.80;

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "perf1-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** Per-bulk-page latency; probes (maxKeys==1) stay instant so stealing isn't throttled. */
    private static MockPageFetcher fetcherWith(List<byte[]> keyspace, AtomicInteger probeCount) {
        return MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 ? PAGE_LATENCY : Duration.ZERO)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.maxKeys() == 1) {
                        probeCount.incrementAndGet();   // a Thief 1-key probe (Thief#probeNonEmpty)
                    }
                    return page;
                })
                .build();
    }

    /** One full scan: per-nodeId emitted-key counts + the emitted keys (for the exactly-once check). */
    private record Run(Map<Long, Integer> keysByNode, List<byte[]> emitted) {
        long totalKeys() {
            return keysByNode.values().stream().mapToLong(Integer::longValue).sum();
        }

        /** Distinct ranges (nodeIds) that emitted ≥1 key — the effective non-empty partition. */
        long emittingRanges() {
            return keysByNode.values().stream().filter(v -> v > 0).count();
        }
    }

    private Run scan(Path db, List<byte[]> keyspace, int n, AtomicInteger probeCount) throws Exception {
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, new RunMetrics(new SimpleMeterRegistry())),
                    fetcherWith(keyspace, probeCount), store, WORKERS, PAGE_SIZE, seeds, FilterChain.EMPTY);

            Map<Long, Integer> keysByNode = new HashMap<>();
            List<byte[]> emitted = new ArrayList<>(n);
            PipelineDrain.drain(2000, engine, batch -> {
                keysByNode.merge(batch.nodeId(), batch.entries().size(), Integer::sum);
                for (ListEntry e : batch.entries()) {
                    emitted.add(e.key().raw());
                }
            });
            return new Run(keysByNode, emitted);
        }
    }

    private static void assertExactlyOnce(Run run, List<byte[]> keyspace) {
        TreeSet<byte[]> distinct = new TreeSet<>(Arrays::compareUnsigned);
        distinct.addAll(run.emitted());
        assertThat(run.emitted()).as("no duplicates").hasSize(keyspace.size());
        assertThat(distinct).as("every key present (no gap)").hasSize(keyspace.size());
        assertThat(run.totalKeys()).isEqualTo((long) keyspace.size());
    }

    @Test
    @Timeout(120)
    void skewedKeyspaceIsBalancedByStealingWithBoundedProbesAndExactlyOnce(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.badlySkewed(42L, N, HOT_FRACTION);

        AtomicInteger probesN = new AtomicInteger();
        Run parallel = scan(dir.resolve("n.sqlite"), keyspace, N, probesN);

        // (3) exactly-once: emitted set == whole keyspace, no duplicates (no gap/overlap/dup).
        assertExactlyOnce(parallel, keyspace);

        // (1) BALANCE via work distribution: the dense prefix was carved across many ranges and no
        //     single range owns the lion's share. A no-steal bottleneck would leave one range at ~100%.
        long emittingRanges = parallel.emittingRanges();
        assertThat(emittingRanges).as("dense prefix carved across >= %d ranges (stealing happened)", MIN_RANGES)
                .isGreaterThanOrEqualTo(MIN_RANGES);
        int maxRangeKeys = parallel.keysByNode().values().stream().mapToInt(Integer::intValue).max().orElse(0);
        double maxShare = (double) maxRangeKeys / parallel.totalKeys();
        assertThat(maxShare).as("busiest range holds < %.0f%% of keys (not bottlenecked)", MAX_RANGE_SHARE * 100)
                .isLessThan(MAX_RANGE_SHARE);

        // (2) PROBE/STEAL OVERHEAD O(W·log ρ), not O(N): run the SAME skew at 2N and assert the
        //     effective range partition stays under one O(W·log N) ceiling at BOTH sizes. A per-key
        //     O(N) engine would grow the partition with N and exceed a log-scaled ceiling.
        AtomicInteger probes2N = new AtomicInteger();
        List<byte[]> keyspace2N = Keyspaces.badlySkewed(42L, 2 * N, HOT_FRACTION);
        Run doubled = scan(dir.resolve("2n.sqlite"), keyspace2N, 2 * N, probes2N);
        assertExactlyOnce(doubled, keyspace2N);

        long probeCeil = 8L * WORKERS * (long) Math.ceil(Math.log(2.0 * N) / Math.log(2));   // ≈ 8·W·log2(2N)
        assertThat(emittingRanges).as("stealing actually partitioned the keyspace").isGreaterThan(1);
        assertThat(emittingRanges)
                .as("effective ranges at N (%d) within O(W·log N) ceiling %d (not O(N)); raw probes=%d",
                        emittingRanges, probeCeil, probesN.get())
                .isLessThan(probeCeil);
        assertThat(doubled.emittingRanges())
                .as("effective ranges at 2N (%d) still within the SAME O(W·log N) ceiling %d — partition does "
                        + "not scale with N (O(N) would); raw probes=%d", doubled.emittingRanges(), probeCeil, probes2N.get())
                .isLessThan(probeCeil);
    }
}
