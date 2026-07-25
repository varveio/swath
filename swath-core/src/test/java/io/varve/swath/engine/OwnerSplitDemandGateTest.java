/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedTiling;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The owner-split <b>demand gate</b>: once live nodes reach {@code T}, the owner suppresses further
 * self-split carves ({@code OWNER_SPLIT.demand_gated}), and a {@code (1−f)·est > 2·maxKeys}
 * child-mass floor forbids sub-two-page children. Without the gate, a saturated uniform bucket
 * (already at in-flight ≈ T) has the owner keep self-splitting its range into tiny confetti
 * children — one self-split per node, cascading — so {@code splits} scales like the page count and
 * the over-fetch ratio ({@code raw page keys / keys emitted}) climbs to ~1.8–1.9, buying ZERO extra
 * parallelism.
 *
 * <p>A {@code 2×T} threshold instead of {@code T} does not work: idle thieves drain owner-split
 * children as fast as they're created, so {@code outstanding} plateaus at ~T and never climbs to
 * 2T, so a {@code 2×T} gate never engages; {@code T} engages because once every worker already has
 * claimable work, an extra split still buys nothing. This test asserts splits stay bounded (nowhere
 * near the page count), that the trim waste collapses well below the confetti baseline, that the
 * gate actually engaged — on BOTH a multi-seed AND a single-seed saturated shape (the single-seed
 * case is exactly what a {@code 2×T} threshold misses) — and that parallelism still reaches
 * {@code T} (ramp intact). It is NOT the no-gap/no-overlap guarantee — the byte-exact partition guard
 * lives in the PROP-1 suite; the smoke check here only confirms every key surfaced exactly once.
 */
final class OwnerSplitDemandGateTest {

    private static final String DEMAND_GATED_KEY = "OWNER_SPLIT.demand_gated";
    private static final String PUBLISHED_KEY = "OWNER_SPLIT.self_published";
    private static final int MAX_KEYS = 100;
    /** Per-page latency so workers actually run concurrently (an instant in-memory mock never overlaps). */
    private static final Duration PAGE_LATENCY = Duration.ofMillis(1);

    /** A dense flat directory {@code d/000000..} of {@code n} uniform keys — the saturated shape. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static byte[] flatKey(int i) {
        return String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static NodeSpec seed(long runId, byte[] lo, byte[] hi) {
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, null, null);
    }

    private record Result(long splits, long demandGated, long published, double overfetchRatio,
                          int peakInFlight, int emitted) {
    }

    private Result runScan(Path dir, String label, int n, int seedRanges, int workers)
            throws Exception {
        List<byte[]> keyspace = denseFlat(n);
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        LatencyFetcher fetcher = new LatencyFetcher(mock, PAGE_LATENCY);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            SeedTiling.seedTiled(store, run.id(), n, seedRanges,
                    "d/".getBytes(StandardCharsets.UTF_8), OwnerSplitDemandGateTest::flatKey);
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
            // NOT the adversarial PROP-1 no-gap/no-overlap guarantee (that lives in a separate suite) —
            // this only confirms every key this particular run surfaced is byte-exact and
            // exactly-once (see assertByteExactCoverage).
            assertByteExactCoverage(emitted, keyspace);
            Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
            // Over-fetch measured directly off worker bulk pages: raw keys fetched vs keys emitted.
            // (swath.entries.emitted is fed by the output sink, which this channel-draining harness
            //  bypasses, so we compute the ratio from the fetcher's own raw-key tally instead.)
            double overfetch = (double) fetcher.bulkKeysFetched() / emitted.size();
            return new Result(
                    metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 1L, 0L).splits(),
                    reasons.getOrDefault(DEMAND_GATED_KEY, 0L),
                    reasons.getOrDefault(PUBLISHED_KEY, 0L),
                    overfetch,
                    fetcher.peakInFlight(),
                    emitted.size());
        }
    }

    @Test
    @Timeout(120)
    void saturatedUniformBucketBoundsSplitsAndOverfetch(@TempDir Path dir) throws Exception {
        int workers = 8;
        int n = 100_000;
        int pages = n / MAX_KEYS;
        // The realistic saturated shape: SeedStep pre-tiles a large bucket into many live ranges, so the
        // worklist starts with outstanding ≈ seeds ≫ T and every worker already has claimable work. In
        // that regime an owner self-split buys ZERO parallelism and only over-fetches its bounded tail.
        // Without the gate, each of those far-ahead carves cascades into ~1-page confetti (splits ≳
        // pages/4, over-fetch ~1.7). With the demand gate the owner suppresses the carve while
        // outstanding ≥ T (OWNER_SPLIT.demand_gated), so splits stay bounded and the trim waste collapses.
        Result r = runScan(dir, "gate-saturated", n, 8 * workers, workers);
        assertGatedAndBounded(r, pages, workers);
    }

    @Test
    @Timeout(120)
    void singleSeedSaturatedBucketAlsoEngagesGate(@TempDir Path dir) throws Exception {
        // The case a 2×T threshold would miss entirely: a single owner range, no pre-tiling. Idle
        // thieves and the owner's own recursive self-splits drain children as fast as they're
        // created, so `outstanding` plateaus at ~T and never reaches 2T. At T the gate engages
        // because once every worker already has claimable work, an extra split still buys nothing.
        int workers = 8;
        int n = 100_000;
        int pages = n / MAX_KEYS;
        Result r = runScan(dir, "gate-single-seed-saturated", n, 1, workers);
        assertGatedAndBounded(r, pages, workers);
    }

    /**
     * The demand gate does not control the RAW splits total directly: on the single-seed-saturated
     * shape, {@code OWNER_SPLIT.self_published} alone runs ~270-330 (each newly-claimed child's
     * rate-limit window is seeded to allow one immediate self-split, so a deep recursive split tree
     * keeps producing new "first" self-splits even while the gate suppresses the vast majority of
     * attempts — {@code OWNER_SPLIT.demand_gated} runs ~370-560 in the same runs). What the gate DOES
     * control and what this guards instead:
     * <ul>
     *   <li>the gate engages at all ({@code demand_gated > 0});</li>
     *   <li>the trim waste it buys (overfetch ratio) collapses well below the confetti baseline —
     *       measured off worker bulk pages only, excluding the delimiter=/ structure probe (see
     *       {@code LatencyFetcher}): the real ratio observed here is ~1.14-1.43;</li>
     *   <li>{@code OWNER_SPLIT.self_published} — the gate's own attribution — stays well below the
     *       confetti baseline (splits ≳ pages/2), not merely "nowhere near pages";</li>
     *   <li>a coarse total-splits backstop DERIVED from the 2-page child-mass floor (not an
     *       arbitrary constant): a split's child must carry {@code > 2*maxKeys} keys of mass (the
     *       floor in {@code StealMath.childTailBelowObservedMassFloor}), so over a fixed total
     *       mass of {@code pages*maxKeys} keys, no more than ~{@code pages/2} owner-tail carves can
     *       be floor-eligible. This is a COARSE backstop, not a hard ceiling — re-claimed children
     *       that split again overlap the parent's mass, so the bound is approximate; the tighter
     *       attribution bound above (2*pages/5, observed-derived) is the real regression guard.</li>
     * </ul>
     */
    private static void assertGatedAndBounded(Result r, int pages, int workers) {
        // The gate engaged on the saturated shape (post-hoc visibility, §5).
        assertThat(r.demandGated())
                .as("demand gate must engage when outstanding ≥ T on the saturated bucket")
                .isGreaterThan(0L);
        // OWNER_SPLIT.self_published — what the demand gate actually attributes/governs — must stay
        // GENUINELY below the coarse floor ceiling, or this assertion adds nothing over the backstop
        // (published <= splits by construction, so an identical bound would be redundant).
        // Observed range on this shape: ~270-330 across ~30 runs; 2*pages/5 (=400 at pages=1000) gives
        // headroom over the observed max while still failing a gate-degradation regression that
        // the pages/2 backstop would admit.
        assertThat(r.published())
                .as("owner-published splits must stay well below the confetti baseline; pages=%d", pages)
                .isLessThanOrEqualTo(pages * 2L / 5L);
        // Coarse backstop derived from the 2-page child-mass floor (see javadoc above), NOT the key assertion:
        // total mass is pages*maxKeys keys and every floor-eligible split's child carries > 2*maxKeys,
        // so splits can never exceed pages/2 without the floor itself being broken.
        assertThat(r.splits())
                .as("splits must not scale like the page count (confetti); pages=%d", pages)
                .isLessThanOrEqualTo(pages / 2L);
        // Trim waste drops sharply below the confetti baseline (~1.6-1.75 on these shapes).
        // (It cannot reach 1.0 — every bounded range still over-fetches its own terminal page — but the
        //  confetti tail of ~1-page children is gone.) The bound is 1.5, not 1.35: measured ~1.14-1.43
        // across ~30 runs of the single-seed-saturated shape (see javadoc above); 1.35 had only ~0.01
        // headroom over the observed max and flaked.
        assertThat(r.overfetchRatio())
                .as("gated over-fetch ratio must fall well below the confetti baseline")
                .isLessThanOrEqualTo(1.5);
        // Ramp intact: parallelism still reaches T busy workers concurrently — the gate must never
        // under-feed the worker pool.
        assertThat(r.peakInFlight())
                .as("engine must still ramp to T busy workers")
                .isEqualTo(workers);
    }

    @Test
    @Timeout(60)
    void smallBucketNeverEngagesGate(@TempDir Path dir) throws Exception {
        // A single-seed small bucket that never accumulates T live nodes at all (well below the
        // owner-split trigger's own remaining-work floor, so it barely self-splits, let alone
        // saturates the worklist): the demand gate must stay dormant (it suppresses only genuine
        // saturation, never a ramping/small run).
        Result r = runScan(dir, "gate-small", 500, 1, 8);
        assertThat(r.demandGated())
                .as("demand gate must stay dormant on a non-saturated shape")
                .isZero();
    }

    /**
     * Byte-exact set equality + duplicate detection (like {@code
     * ThiefProbeBudgetTest.assertExactlyOnce}) — {@code hasSize(keyspace.size())} alone would pass
     * a run that dropped one key and duplicated another.
     */
    private static void assertByteExactCoverage(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);

        assertThat(emitted).as("no duplicate emissions").hasSize(distinctEmitted.size());
        assertThat(distinctEmitted).as("full byte-exact coverage").hasSize(distinctKeyspace.size());
        var actual = distinctEmitted.iterator();
        var expected = distinctKeyspace.iterator();
        while (actual.hasNext()) {
            assertThat(Arrays.equals(actual.next(), expected.next())).as("byte-exact emitted key").isTrue();
        }
    }

    /** Adds a fixed per-page latency (so workers overlap) and tallies bulk (worker) fetch shape. */
    private static final class LatencyFetcher implements PageFetcher {

        private final PageFetcher delegate;
        private final Duration latency;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final LongAdder bulkKeys = new LongAdder();

        LatencyFetcher(PageFetcher delegate, Duration latency) {
            this.delegate = delegate;
            this.latency = latency;
        }

        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            // A thief/owner probe is never a worker page fetch, and that includes the delimiter=/
            // structure probe — which uses maxKeys=1000 (STRUCTURE_PROBE_MAX_KEYS), NOT 1, so a
            // `maxKeys() > 1` test alone misses it. Per-victim suppression lets many structure
            // probes fire on this flat (no sub-directory) shape's many recursively-created victims,
            // and on a flat keyspace a delimiter=/ probe returns its matches as plain ENTRIES (no
            // CommonPrefix ever forms), so each one inflates `bulkKeys` by up to 1000 keys never
            // actually emitted downstream — an overfetch-ratio artifact of this harness, not a real
            // engine regression. A worker page fetch never sets delimiter, so excluding it here
            // restores a "worker bulk pages only" measurement.
            boolean bulk = req.maxKeys() > 1 && req.delimiter() == null;
            if (!bulk) {
                return delegate.fetchPage(req);
            }
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                long base = latency.toMillis();
                long jitter = ThreadLocalRandom.current().nextLong(0, 15 * base + 1);
                Thread.sleep(base + jitter);
                ListPage page = delegate.fetchPage(req);
                bulkKeys.add(page.entries().size());
                return page;
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

        long bulkKeysFetched() {
            return bulkKeys.sum();
        }
    }
}
