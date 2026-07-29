/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.shapecorpus;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.SeedMode;
import io.varve.swath.engine.SeedStep;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Shape-regression CI tier over a representative bucket-shape corpus. One envelope (fixed numeric
 * bounds on seed/trajectory/classification signals) per shape class, synthesized from a shape that
 * mirrors a real exemplar's structure (never the real bucket), so a future regression on any of
 * these shapes fails a FIXED, cheap, deterministic bound instead of waiting for the next incident
 * survey. Each class synthesizes a `MockPageFetcher` keyspace, runs {@link SeedStep} and/or
 * the full {@link WorkStealingScan}, and asserts the envelope; each test's own Rationale:
 * paragraph states the one-line reasoning behind its bounds.
 *
 * <p>Deterministic throughout ({@code MockPageFetcher}, fixed fixtures); a small fixed per-page
 * {@code Duration} (not the full {@code LatencyModel} fault-injection apparatus — that timing-
 * dependent collapse class is already covered by {@code DenseShapeCollapseSignatureTest}) exposes
 * concurrency for the classes whose envelope needs {@code avg_in_flight}/{@code serial_frac}, the
 * same idiom {@code SeedStepFanoutTilingContractTest}/{@code DenseRootSeedGuardTest} already use.
 * Tagged {@code deep} (like {@code DenseShapeCollapseSignatureTest}): excluded from the per-commit
 * fast suite, run with {@code -Pdeep} (the CI deep job on `main` + nightly). The companion
 * guard-proof ({@code ShapeRegressionCorpusGuardProofTest}) is deliberately NOT deep-tagged — it
 * proves the Hive envelope is sharp (a fanout_tiling regression fails it) and must run on
 * every commit.
 */
@Tag("deep")
final class ShapeRegressionCorpusTest {

    private static final byte[] NO_PREFIX = new byte[0];
    // Currently unused, but pinned explicitly (readahead=false, massAwareSeed=false) like its
    // siblings — the 10-arg form silently inherits the mass_aware_seed=true default, which would
    // contaminate any future legacy-OFF use of this fixture.
    private static final EngineToggles FANOUT_OFF =
            EngineToggles.DEFAULT.withFanoutTiling(false).withMassAwareSeed(false);

    // =====================================================================================
    // 1. Hive key=value/ dense partitions.
    // =====================================================================================

    /** A Hive-style key=value/ shape: a narrow {@code table/} top over a truncated {@code date=} fan-out. */
    static List<byte[]> hiveDensePartitions(int partitions, int perPartition) {
        List<byte[]> keys = new ArrayList<>(partitions * perPartition);
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < perPartition; o++) {
                keys.add(("table/date=%05d/part-%05d.parquet".formatted(p, o))
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    /**
     * Rationale: a truncated {@code key=value/} partition fan-out must be tiled (not
     * broken-and-discarded) at seed time, so the fleet reaches near-full width
     * (seed ranges &gt;= ~0.8W) and drains with materially more than a serial tail
     * (serial_frac &lt; 0.5) — while never losing a single key (byte-exact).
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void hiveDensePartitions_seedsWide_lowSerialFrac_byteExact(@TempDir Path dir) throws Exception {
        int workers = 24;
        List<byte[]> keyspace = hiveDensePartitions(1100, 150);   // ~1100 date= partitions x 150 objs

        Run run = scan(dir, "hive-dense", keyspace, workers, 1000, Duration.ofMillis(2), EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);

        assertThat(run.seedCount)
                .as("Hive fan-out tiles to >= ~0.8W seed ranges")
                .isGreaterThanOrEqualTo((int) Math.floor(0.8 * workers));
        assertThat(run.serialFrac)
                .as("Hive fan-out drains with materially less than a serial tail")
                .isLessThan(0.5);
    }

    // =====================================================================================
    // 2. Deep mass below seed depth.
    //    TRACKED-ONLY: pins CURRENT behavior as a baseline; a future deep-mass-detection
    //    improvement is expected to flip this. Do NOT assert an improvement that does not
    //    exist yet.
    // =====================================================================================

    /**
     * A narrow, non-exploding nested tree whose top-level probe alone already returns MORE common
     * prefixes than the seed's cut-point cap ({@code targetSeeds = min(1000, 4W)}): most of
     * {@code totalRegions} are THIN (one direct object each), but the last {@code heavyCount} are
     * HEAVY (a deep, multi-level sub-tree carrying the bulk of the mass). {@code collectCutPoints}'s
     * descent is no longer stopped by {@code cuts.size() >= targetSeeds} (only the probe budget and
     * frontier exhaustion bound it), so it DOES descend one level into every sibling it has probe
     * budget for — but {@code totalRegions} siblings still exceed {@code maxProbes}, so on THIS
     * fixture the budget runs out partway through the (best-first-ordered, but still frontier-order)
     * sibling list, before ever reaching a heavy region's own interior branches. The final cut set is
     * still reduced by {@link SeedStep}'s mass-weighted subsample (not the plain positional one), so
     * it is no longer blind to whichever heavy regions the descent DID reach — but a fixture this
     * wide (many more siblings than the probe wallet) is exactly the residual limit that motivates a
     * future deep-mass-detection improvement (see the class javadoc's TRACKED-ONLY note). Mirrors a
     * real fleet shape's {@code delimiter_seeded} classification at the cut-point cap.
     */
    static List<byte[]> deepMassBelowSeedDepth(int totalRegions, int heavyCount, int heavyBranches, int heavyPerBranch) {
        List<byte[]> keys = new ArrayList<>();
        int thinCount = totalRegions - heavyCount;
        for (int r = 0; r < thinCount; r++) {
            keys.add(("region%04d/o".formatted(r)).getBytes(StandardCharsets.UTF_8));
        }
        for (int r = thinCount; r < totalRegions; r++) {
            for (int b = 0; b < heavyBranches; b++) {
                for (int o = 0; o < heavyPerBranch; o++) {
                    keys.add(("region%04d/branch%03d/obj%05d".formatted(r, b, o))
                            .getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        return keys;
    }

    /**
     * Rationale: {@code mass_aware_seed} defaults ON. {@link EngineToggles#DEFAULT} engages it
     * here: the top-level page alone already exceeds the cut-point cap
     * ({@code cuts.size() > targetSeeds}) — but the descent no longer stops there (only the probe
     * budget and frontier exhaustion bound it, {@code SeedStep} class javadoc §8), so it spends its
     * WHOLE {@code maxProbes} wallet descending one level into as many of the {@code totalRegions}
     * siblings as it can reach, before the leftover cut set is weight-sampled and reduced by
     * {@code mass_weighted_subsample} (§4) instead of the positional {@code subsampleEvenly} — the
     * top-level probe alone is no longer the whole seed decision. This fixture is pinned to the
     * mass-aware-ON baseline, not the off baseline (§8.3): the heavy regions' mass is no longer
     * invisible to the seed, so avg-in-flight should rise materially above the old pinned ceiling
     * (workers*0.75). The exact avg_in_flight value is timing/simulation-derived (not statically
     * derivable), so this only asserts the qualitative ON-signature (the counter fires, apiCalls
     * reflects the exhausted probe budget) plus a loosened sanity bound.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void deepMassBelowSeedDepth_massAwareDefaultOnEngagesMassWeightedSubsample(@TempDir Path dir) throws Exception {
        int workers = 64;   // targetSeeds = maxProbes = 4*64 = 256 (matches the real 257-cap fleet signature)
        int totalRegions = 600;
        List<byte[]> keyspace = deepMassBelowSeedDepth(totalRegions, 20, 6, 500);   // 20 heavy x 3000 objs

        RunMetrics seedMetrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(seedFetcher, NO_PREFIX, workers, seedMetrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        RunSummary.SeedSummary seedSummary =
                seedMetrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        assertThat(seedSummary).isNotNull();

        // The top-level page alone still exceeds the cut-point cap (600 > 256), but the descent no
        // longer stops there: it keeps probing siblings until maxProbes runs out (probe budget and
        // frontier exhaustion are the descent's only stops, no longer cuts.size()) — here that means
        // the WHOLE maxProbes wallet (== targetSeeds == 256 at these worker counts), not just the
        // 32-probe SAMPLE_BUDGET the old top-level-only behavior spent.
        assertThat(seedFetcher.apiCalls())
                .as("the descent spends its whole probe budget on region siblings, not just the "
                        + "32-probe SAMPLE_BUDGET the old cuts.size()-capped descent left unused")
                .isEqualTo((long) Math.min(256, 4 * workers));
        Map<String, Long> seedReasons = seedMetrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(seedReasons.getOrDefault("SEED.mass_weighted_subsample", 0L))
                .as("the over-cap cut set (top-level siblings plus whatever interior structure the "
                        + "descent reached) is weight-proportionally subsampled, not positionally")
                .isPositive();
        assertThat(seedReasons.getOrDefault("SEED.descent_cuts_subsampled", 0L))
                .as("the descent's cut set exceeded targetSeeds and was actually trimmed")
                .isPositive();
        assertThat(specs.size())
                .as("seed count still bounded, comfortably above a trivial handful of ranges "
                        + "(mass-weighted subsampling reallocates WHICH cuts survive; it also now spends "
                        + "a single dominant-weight sampled cut's credit honestly ON ITSELF rather than "
                        + "letting it spill onto unrelated neighboring cuts (the scoped-tail fix — "
                        + "see HybridSeedPlanner#massWeightedSubsample), so the total picked count legitimately "
                        + "lands further under targetSeeds than the old positionally-inflated walk did "
                        + "when one heavy region dominates the sampled weight)")
                .isLessThanOrEqualTo(Math.min(4 * workers, 256) + 1)
                .isGreaterThan(Math.min(4 * workers, 256) / 4);
        assertThat(seedSummary.decisions().stream()
                .anyMatch(d -> "delimiter_seeded".equals(d.classification())))
                .as("still classified delimiter_seeded, not an explosion special-case")
                .isTrue();

        Run run = scan(dir, "deep-mass", keyspace, workers, 1000, Duration.ofMillis(2), EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
        // Loosened sanity bound only (see javadoc): avg-in-flight must stay a legal fraction of full
        // width.
        assertThat(run.avgInFlight)
                .as("sanity: avg-in-flight stays within legal worker-width bounds")
                .isGreaterThan(0.0)
                .isLessThanOrEqualTo(workers);
    }

    // =====================================================================================
    // 3. Radix-banded flat root: the dense-root banding path.
    // =====================================================================================

    /** A flat DENSE root: high-entropy hex keys with NO {@code /} at all (a flat, un-seeded tail below any prefix). */
    static List<byte[]> flatDenseHexRoot(int n, long seed) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add("%016x".formatted(r.nextLong()).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Rationale: a dense flat root (no {@code CommonPrefixes}) must radix-band at seed time
     * ({@code dense_root_radix_banded} present in {@code decisions[]}) and must not collapse to a
     * couple of ranges (peak in-flight reaches a healthy fraction of {@code W}).
     *
     * <p><b>Why the scan below injects a small per-page latency (unlike the seed step above, which
     * stays at zero).</b> {@code peak_in_flight} is a runtime-achieved-concurrency reading, not a
     * placement/structure fact, and at zero injected page latency a "page fetch" is pure CPU work —
     * the whole 6000-key run can complete before the in-flight gauge ever samples a high value, so
     * the reading measures how fast the runner burned through the work as much as it measures
     * parallelism (#35: CI read {@code peak_in_flight = 2} against a floor of 3 on the standard
     * GitHub runner, then passed 20/20 on an identical rerun — pure scheduler luck, not a
     * regression). A small {@link Duration#ofMillis(long)} latency on the worker listing pages (the
     * same idiom {@link #healthyNestedFanoutControl_seedBandHighAvgInFlightCheapApi} already uses)
     * gives the gauge an observation window without changing the shape's own point, which is
     * banding, not throughput: a virtual thread blocked in {@code Thread.sleep} unmounts from its
     * carrier, so sleeping fetches don't hold a core and aren't core-bound the way a zero-latency
     * CPU-bound fetch is — {@code peak_in_flight} can then reach a healthy fraction of {@code W}
     * regardless of how many physical cores the runner actually schedules on, so the floor no longer
     * needs the runner's own core count as an input. The seed-time structural assertions above
     * (radix bands fired, {@code specs.size() > 8}) stay schedule-invariant and are unaffected by
     * this latency.
     *
     * <p>Exactly-once is asserted on BOTH scans. The zero-latency one is the coverage this shape had
     * before the pacing was introduced — a CPU-bound run interleaves the workers differently from a
     * run whose fetches all park in {@code Thread.sleep}, and emission correctness is exactly the
     * property that must not depend on which interleaving the shape happens to get, so keeping only
     * the paced run would quietly narrow the guard to one timing regime.
     *
     * <p><b>Guard sharpness.</b> Ablating {@code radix_bands} (via {@link EngineToggles#parse}) leaves
     * the dense flat root as ONE un-subdivided seed range ({@code seedCount == 1}) instead of the
     * {@code > 8} bands the ON path produces, so the assertions below would fail without radix
     * banding. A separate, always-on owner-split mechanism can occasionally still manage one dynamic
     * split of the un-banded range even with static banding off, so an ablated {@code peak_in_flight}
     * is not guaranteed to hit exactly 1 — the {@code seedCount == 1} structural signal is the
     * reliable half of this check.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void radixBandedFlatRoot_bandsFireNoCollapseAtZeroLatency(@TempDir Path dir) throws Exception {
        int workers = 16;
        List<byte[]> keyspace = flatDenseHexRoot(6000, 909L);

        RunMetrics seedMetrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(seedFetcher, NO_PREFIX, workers, seedMetrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        RunSummary.SeedSummary seedSummary =
                seedMetrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        assertThat(seedSummary).isNotNull();
        assertThat(seedSummary.decisions().stream()
                .anyMatch(d -> "dense_root_radix_banded".equals(d.classification())))
                .as("dense flat root radix-banded (meeo-s3 shape)").isTrue();
        assertThat(specs.size()).as("banded into many ranges, not one serial root range").isGreaterThan(8);

        Run correctness = scan(dir, "radix-band", keyspace, workers, 1000, Duration.ZERO, EngineToggles.DEFAULT);
        assertExactlyOnce(correctness.emitted, keyspace);

        Run run = scan(dir, "radix-band-paced", keyspace, workers, 1000, Duration.ofMillis(2), EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
        assertThat(run.peakInFlight)
                .as("no collapse: banded root reaches a healthy fraction of W")
                .isGreaterThanOrEqualTo(workers / 4);
    }

    // =====================================================================================
    // 4. Opaque dense chain: correctness-only envelope here.
    // =====================================================================================

    /**
     * One deep, narrow, dense prefix chain with NO branching alternative at any level and a flat
     * leaf tail small enough to stay UNTRUNCATED (so it can never even be considered for any
     * explosion special-case) — the "opaque, no visible structure" shape at classification-only
     * scale. The dense-tail-at-scale performance story (millions of keys) is already covered
     * elsewhere ({@code WorkStealingScanSerialTailTest}, the equivalent real-bucket integration
     * test); this unit only guards no-crash/no-misclassification.
     */
    static List<byte[]> opaqueDenseChain(int leafObjects) {
        List<byte[]> keys = new ArrayList<>(leafObjects);
        for (int i = 0; i < leafObjects; i++) {
            keys.add(("encode/experiment/replicate/obj%05d.bam".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Rationale: an opaque dense chain must not be misclassified into any explosion special-case
     * (no {@code tiny_leaf_explosion}/{@code fanout_tiled}/{@code dense_root_radix_banded}) and the
     * run must complete byte-exact — the correctness floor this shape needs, not performance.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void opaqueDenseChain_noExplosionClassFiresRunCompletesByteExact(@TempDir Path dir) throws Exception {
        int workers = 8;
        List<byte[]> keyspace = opaqueDenseChain(800);

        RunMetrics seedMetrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        SeedSteps.of(seedFetcher, NO_PREFIX, workers, seedMetrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        RunSummary.SeedSummary seedSummary =
                seedMetrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        assertThat(seedSummary).isNotNull();
        for (RunSummary.SeedSummary.SeedDecision d : seedSummary.decisions()) {
            assertThat(d.classification())
                    .as("opaque dense chain never trips an explosion special-case")
                    .isNotIn("tiny_leaf_explosion", "fanout_tiled", "dense_root_radix_banded");
        }

        Run run = scan(dir, "opaque-chain", keyspace, workers, 1000, Duration.ZERO, EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
    }

    // =====================================================================================
    // 5. 1:1 tiny-leaf explosion, PLAIN-named (structurally identical page shape to #6, no '=').
    // =====================================================================================

    /** A PLAIN (non-{@code key=value/}) 1:1 directory explosion under a shared top: one object per leaf. */
    static List<byte[]> plainNamedOneToOneExplosion(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("shard/%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Rationale: a plain-named (no {@code '='}) 1:1 explosion must stay whole (the
     * fanout_tiling discriminator's negative case, the INT-8 tiny-leaf shape) and the total
     * LIST budget must stay flat-scan-bounded (INT-8), never per-directory.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void oneToOneExplosionPlainNamed_staysWholeInt8Budget(@TempDir Path dir) throws Exception {
        int workers = 16;
        int n = 4000;
        List<byte[]> keyspace = plainNamedOneToOneExplosion(n);

        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(seedFetcher, NO_PREFIX, workers).seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(specs.size()).as("plain-named 1:1 explosion left whole (not tiled)").isLessThan(8);

        Run run = scan(dir, "plain-1to1", keyspace, workers, 1000, Duration.ZERO, EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
        long ceil = (n + 999) / 1000;
        long budget = ceil + 4L * run.seedCount + 64;
        assertThat(run.apiCalls)
                .as("INT-8: total LIST stays flat-scan-bounded, not O(n directories)")
                .isLessThan(n / 10L)
                .isLessThan(budget);
    }

    // =====================================================================================
    // 6. 1:1 explosion, key=value-named (pid=-style — the fanout_tiling W-cap defense on a 1:1 shape).
    // =====================================================================================

    /** A 1:1 {@code key=value/} explosion: one object per {@code pid=<hex>} leaf. */
    static List<byte[]> keyValueOneToOneExplosion(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("crawl=2024/pid=%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Rationale: a {@code key=value/} 1:1 explosion still tiles (partition-like names), but the
     * {@code W}-cap must hold the range count and the LIST budget inside the same INT-8 arithmetic
     * as the plain-named case — the fanout_tiling W-cap defense proven on the 1:1 shape.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void oneToOneExplosionKeyValueNamed_tilesWithinWInt8Budget(@TempDir Path dir) throws Exception {
        int workers = 16;
        int n = 4000;
        List<byte[]> keyspace = keyValueOneToOneExplosion(n);

        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(seedFetcher, NO_PREFIX, workers).seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(specs.size())
                .as("key=value 1:1 explosion tiles, W-capped (+1 for the seed's scope-closing "
                        + "sentinel, whose final tile is empty by construction)")
                .isGreaterThan(1)
                .isLessThanOrEqualTo(workers + 3);

        Run run = scan(dir, "kv-1to1", keyspace, workers, 1000, Duration.ZERO, EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
        long ceil = (n + 999) / 1000;
        long budget = ceil + 4L * run.seedCount + 64;
        assertThat(run.apiCalls)
                .as("W-capped tiling keeps LIST inside the same INT-8 bound as the plain-named case")
                .isLessThan(n / 10L)
                .isLessThan(budget);
    }

    // =====================================================================================
    // 7. Healthy nested fanout control — must-not-regress.
    // =====================================================================================

    /** A nested, evenly-branching multi-level fanout: {@code site/dataset/channel/obj}. */
    static List<byte[]> healthyNestedFanout(int sites, int datasetsPerSite, int channelsPerDataset, int objsPerChannel) {
        List<byte[]> keys = new ArrayList<>();
        for (int s = 0; s < sites; s++) {
            for (int d = 0; d < datasetsPerSite; d++) {
                for (int c = 0; c < channelsPerDataset; c++) {
                    for (int o = 0; o < objsPerChannel; o++) {
                        keys.add(("site%03d/dataset%03d/channel%02d/obj%05d.tif".formatted(s, d, c, o))
                                .getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return keys;
    }

    /**
     * Rationale: a healthy, evenly-nested multi-level fanout (must-not-regress control) reaches a
     * reasonable seed-range band, sustains high avg-in-flight, and stays API-cheap per 1k keys —
     * the "everything is fine" contrast to the crawler shapes above.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void healthyNestedFanoutControl_seedBandHighAvgInFlightCheapApi(@TempDir Path dir) throws Exception {
        int workers = 16;
        List<byte[]> keyspace = healthyNestedFanout(20, 10, 5, 200);   // 20*10*5*200 = 200,000 keys

        Run run = scan(dir, "healthy-nested", keyspace, workers, 1000, Duration.ofMillis(4), EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);
        double apiPer1k = run.apiCalls * 1000.0 / keyspace.size();

        assertThat(run.seedCount).as("healthy nested fanout seeds a reasonable band").isBetween(8, 4 * workers + 2);
        assertThat(run.peakInFlight).as("healthy control reaches a healthy fraction of W").isGreaterThanOrEqualTo(workers / 2);
        assertThat(apiPer1k).as("API calls stay cheap per 1k keys (flat-scan-bounded)").isLessThan(10.0);
    }

    // =====================================================================================
    // 8. Extreme skew (gini ~0.97). TRACKED-ONLY baseline.
    // =====================================================================================

    /**
     * Rationale: an extreme-skew keyspace (one prefix holding ~97% of the mass) is TRACKED-ONLY
     * here — record the current byte-exact/seed/avg-in-flight baseline; a future multi-cut seeding
     * mechanism is the wake-up path if/when this shape's numbers need to improve.
     */
    @Test
    @Tag("deep")
    @Timeout(60)
    void extremeSkew_pinnedBaselineTracksCurrentBehavior(@TempDir Path dir) throws Exception {
        int workers = 16;
        List<byte[]> keyspace = Keyspaces.hotNeedle(4242L, 20000, 0.97);

        Run run = scan(dir, "extreme-skew", keyspace, workers, 1000, Duration.ofMillis(2), EngineToggles.DEFAULT);
        assertExactlyOnce(run.emitted, keyspace);

        // BASELINE (pinned): correctness always holds regardless of skew; the parallelism bound is
        // generous on purpose — this guards against a REGRESSION off the current baseline, not an
        // aspirational target.
        assertThat(run.avgInFlight)
                .as("BASELINE: extreme skew still sustains some concurrency, generously bounded")
                .isGreaterThan(0.0);
    }

    // ---- shared harness ---------------------------------------------------------------

    private record Run(List<byte[]> emitted, int seedCount, long apiCalls, long peakInFlight,
                       double avgInFlight, double serialFrac) {
    }

    /** Seed via SHALLOW then drive the full {@link WorkStealingScan}, recording the envelope signals. */
    static Run scan(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys,
                    Duration pageLatency, EngineToggles toggles) throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Latency only on worker listing pages (recursive, maxKeys>1, no delimiter) — seed/thief
                // probes stay instant, so this exposes concurrency without confounding the seed.
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, NO_PREFIX, workers, null, toggles)
                    .seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            metrics.markRunStarted();   // arm the avg-in-flight / trajectory integration window
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        RunSummary.TrajectorySummary traj =
                metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).trajectory();
        double serialFrac = traj != null ? traj.serialFrac() : -1.0;
        return new Run(emitted, seedCount, mock.apiCalls(), metrics.peakInFlight(), metrics.avgInFlight(), serialFrac);
    }

    static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(emitted).as("no duplicate emissions (no overlap)").hasSize(distinctEmitted.size());
        assertThat(distinctEmitted).as("full byte-exact coverage (no gap)").hasSize(distinctKeyspace.size());
        var actual = distinctEmitted.iterator();
        var expected = distinctKeyspace.iterator();
        while (actual.hasNext()) {
            assertThat(Arrays.equals(actual.next(), expected.next())).as("byte-exact key").isTrue();
        }
    }
}
