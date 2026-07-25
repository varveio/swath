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
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Acceptance guards for mass-aware seed descent into heavy cuts.</b> These are the
 * cross-cutting correctness/placement guards for the feature defaults. Companion to {@code
 * ShapeRegressionCorpusTest.deepMassBelowSeedDepth_*} (which pins the shallow-descent baseline;
 * these pin the mass-aware descent's target behavior).
 *
 * <p><b>Toggle binding.</b> The {@link EngineToggles} flag defaults ON; the default-state guard pins
 * that choice, while the explicit-OFF rails compare equivalent constructions. These guards bind the toggle by
 * its {@code --engine-toggle} STRING name {@value #TOGGLE_NAME} through {@link
 * EngineToggles#parse}, so there is ZERO compile dependency on any {@code EngineToggles} field —
 * see {@link #massAware}'s javadoc for the {@link EngineToggles#DEFAULT} registration fallback. If
 * the registered name changes, update {@link #TOGGLE_NAME} alone.
 *
 * <p>New {@code SEED.*} engagement counters mass-aware seed descent emits via {@code
 * recordStealReason("SEED", …)}: {@value #HEAVY_CUT_DESCENDED}, {@value #EXPLOSION_CONFIRMED},
 * {@value #HEAVY_CUT_BANDED}, {@value #MASS_WEIGHTED_SUBSAMPLE}, {@value #TOP_PROBE_PAGINATED}. The
 * counter guards are string-bound (assert {@code SEED.<reason> > 0}), with no symbol dependency.
 */
final class SeedMassAwareDescentTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /** The {@code --engine-toggle} name for this behavior. See class javadoc. */
    private static final String TOGGLE_NAME = "mass_aware_seed";

    // The new SEED.* engagement counters — bound as strings.
    private static final String HEAVY_CUT_DESCENDED = "heavy_cut_descended";
    private static final String EXPLOSION_CONFIRMED = "explosion_confirmed";
    private static final String HEAVY_CUT_BANDED = "heavy_cut_banded";
    private static final String MASS_WEIGHTED_SUBSAMPLE = "mass_weighted_subsample";
    private static final String TOP_PROBE_PAGINATED = "top_probe_paginated";
    // The pre-existing engagement counter (SeedStep.recordSeed("fanout_tiled")) — needed as a
    // Negative control: a plain-hex explosion must NOT route through fanout_tiled.
    private static final String FANOUT_TILED = "fanout_tiled";
    // Pre-existing counters reused by the frontier-continuation guard below.
    private static final String TINY_LEAF_EXPLOSION = "tiny_leaf_explosion";
    // The frontier-continuation counter (per-cut disposal, not global abandon) added alongside it.
    private static final String FRONTIER_CONTINUED_PAST_EXPLOSION = "frontier_continued_past_explosion";

    /**
     * The {@link EngineToggles} with mass-aware seed descent explicitly {@code on} or {@code off}.
     * Bound by string through {@link EngineToggles#parse}; the fallback below (used only if the
     * name is ever unregistered) is dead but harmless. The DEFAULT is ON, so
     * {@code massAware("off")} is the explicit opt-out, not equivalent to DEFAULT.
     */
    private static EngineToggles massAware(String value) {
        try {
            return EngineToggles.parse(List.of(TOGGLE_NAME + "=" + value), false);
        } catch (InvalidArgsException notYetRegistered) {
            return EngineToggles.DEFAULT;
        }
    }

    // ================================================================================================
    // Heavy-tail fixture — narrow top, mass concentrated in ONE deep, PLAIN-named, truncated subtree.
    // With mass-aware seed descent OFF, the heavy cut is a truncated-with-CommonPrefixes level with
    // plain (non key=value) child dirs, so SeedStep classifies it tiny_leaf_explosion and LEAVES IT
    // WHOLE — the entire heavy mass becomes a SINGLE seed range. With it ON, the second-level
    // sampling must prove the cut heavy (each child holds many objects, not ~1) and BAND it.
    // Contrast the INT-8 control below, whose children hold ~1 object each (confirmed explosion →
    // correctly left whole).
    // ================================================================================================

    private static final byte[] HEAVY_LO = "heavy/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEAVY_HI = prefixCeil(HEAVY_LO);   // "heavy0" — the heavy region ceiling

    /**
     * Narrow top ({@code lite000/}..{@code liteNNN/} single-object siblings + one {@code heavy/}
     * prefix), with the heavy prefix fanning out into {@code partitions} plain {@code heavy/pNNNN/}
     * directories (truncated when {@code partitions > 1000}), each holding {@code perPartition}
     * objects — the bulk of the mass, several levels below the seeded depth.
     */
    private static List<byte[]> narrowTopHeavyDeepSubtree(int liteSiblings, int partitions, int perPartition) {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < liteSiblings; i++) {
            keys.add(("lite%03d/o".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < perPartition; o++) {
                keys.add(("heavy/p%04d/obj%03d".formatted(p, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    /**
     * The INT-8 control (essential-web shape): a narrow {@code crawl=2024/} top over a 1:1
     * {@code pid=<hex>/} explosion (one object per leaf). The sampling confirms the explosion
     * (each child ≈ 1 object) and leaves it whole — no wasted descent — firing {@code explosion_confirmed}.
     */
    private static List<byte[]> explodedOneObjectPerLeaf(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("crawl=2024/pid=%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * The PLAIN-hex control: identical shape to {@link
     * #explodedOneObjectPerLeaf} — a narrow {@code data/} top over a 1:1 {@code <hex>/} explosion (one
     * object per leaf) — but WITHOUT the {@code key=value} naming convention. {@link
     * #explodedOneObjectPerLeaf}'s {@code pid=<hex>/} leaf names contain {@code '='}, so once the
     * mass-aware sampling proves the cut not-heavy it still falls into the {@code fanout_tiled} branch
     * ({@link SeedStep#isPartitionFanout} sees a majority of {@code '='}-bearing final segments) —
     * the {@code tiny_leaf_explosion} "left whole" branch (SeedStep.java:454) is never
     * exercised by any ON-path fixture in this class without this one. Plain {@code data/<hex>/} names
     * have no {@code '='} in their final path segment (exactly the {@link SeedStep#isPartitionFanout}
     * javadoc's own {@code data/5bd1e995/} counter-example), so this fixture dodges that detector
     * and must actually reach the tiny-leaf "left whole" branch.
     */
    private static List<byte[]> explodedPlainHexOneObjectPerLeaf(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("data/%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * A NON-truncated wide top whose cut-point count exceeds {@code targetSeeds}, with the mass
     * concentrated in the last {@code heavyCount} regions (each a deep sub-tree). This exposes a
     * positional, rather than mass-weighted, subsampling defect. The weight-proportional allocation must
     * fire {@code mass_weighted_subsample}.
     */
    private static List<byte[]> wideNonTruncatedTopHeavyTail(int totalRegions, int heavyCount,
            int heavyBranches, int heavyPerBranch) {
        List<byte[]> keys = new ArrayList<>();
        int thin = totalRegions - heavyCount;
        for (int r = 0; r < thin; r++) {
            keys.add(("region%04d/o".formatted(r)).getBytes(StandardCharsets.UTF_8));
        }
        for (int r = thin; r < totalRegions; r++) {
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
     * A TRUNCATED wide top (&gt; 1000 top prefixes) whose heavy regions sit past the first page's
     * 1000-prefix cut, so only a +1-page top probe can even see them. The top pagination must fire
     * {@code top_probe_paginated}.
     */
    private static List<byte[]> truncatedTopHeavyBeyondFirstPage(int totalRegions, int heavyStart,
            int heavyCount, int heavyPerRegion) {
        List<byte[]> keys = new ArrayList<>();
        for (int r = 0; r < totalRegions; r++) {
            if (r >= heavyStart && r < heavyStart + heavyCount) {
                for (int o = 0; o < heavyPerRegion; o++) {
                    keys.add(("top%05d/obj%04d".formatted(r, o)).getBytes(StandardCharsets.UTF_8));
                }
            } else {
                keys.add(("top%05d/o".formatted(r)).getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    // ================================================================================================
    // Placement guard: with the toggle ON the seed places a MATERIAL fraction of its ranges INSIDE
    // the heavy subtree (OFF/breadth-first leaves the whole heavy mass as ~1 range).
    // ================================================================================================

    @Test
    void massAwareOn_placesManyRangesInsideHeavySubtree() throws Exception {
        int workers = 64;
        List<byte[]> keyspace = narrowTopHeavyDeepSubtree(10, 1200, 50);   // 1200 heavy partitions x 50 objs
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, null, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);

        // Still an exact I2/I3 tiling regardless of how the heavy region is sub-tiled.
        assertTiles(specs);

        long inHeavy = specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(lo -> lo != null
                        && Arrays.compareUnsigned(lo, HEAVY_LO) >= 0
                        && Arrays.compareUnsigned(lo, HEAVY_HI) < 0)
                .count();
        assertThat(inHeavy)
                .as("mass-aware ON sub-tiles the heavy cut into MANY ranges (today: ~1, breadth-first)")
                .isGreaterThanOrEqualTo(8L);
    }

    // ================================================================================================
    // Parallelization guard: mass-aware ON bands the heavy cut into MANY ranges at the
    // FULL SCAN scale used below (the same defect the placement guard catches via SeedStep alone, but reconfirmed
    // at this larger, engine-driving fixture), plus a byte-exact full-scan run.
    //
    // Do not assert `avg_in_flight >= 4.0` here: it is an achieved-concurrency, wall-clock-scheduling-
    // fraction reading taken from a full WorkStealingScan run, time-weighted over the slow-start
    // T-ramp (ConcurrencyGauge's AIMD-style target-concurrency ramp), so it is sensitive to how many
    // real wall-clock "clean windows" elapse before the (short, in-memory) run finishes -- a
    // CI-load/scheduling-sensitive quantity, not a placement/structure fact.
    //
    // The underlying defect this guard exists to catch is NOT a runtime-scheduling problem: it is the
    // SAME seed-time "one un-tiled heavy range" collapse the placement guard already measures directly and
    // deterministically (byte-exact seed-range placement, unaffected by core count or CI load). So this
    // guard is anchored on that structural signal, computed at the SAME (workers=64, perPartition=80)
    // scale the full scan below runs at, instead of the indirect, timing-noisy avg_in_flight proxy for
    // it. avg_in_flight/peak_in_flight/serial_frac are still measured and printed for observability.
    //
    // RED-check (the guard still catches what it is written to catch): ablating to massAware("off")
    // (same construction as this guard, otherwise unchanged) drops ranges_inside_heavy to 1L, failing
    // the `>= 8L` floor -- a clean, deterministic fail with no runtime-timing variance.
    // ================================================================================================

    @Test
    @Tag("deep")
    @Timeout(120)
    void massAwareOn_heavyTailParallelizes(@TempDir Path dir) throws Exception {
        int workers = 64;
        List<byte[]> keyspace = narrowTopHeavyDeepSubtree(10, 1200, 80);   // ~96k objs in the heavy tail

        // Structural (seed-time) signal, mirroring the placement guard at this test's larger scale: mass-
        // aware ON must sub-tile the heavy cut into MANY ranges.
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> seedSpecs = SeedSteps.of(seedFetcher, NO_PREFIX, workers, null, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);
        long inHeavy = seedSpecs.stream()
                .map(NodeSpec::rangeStart)
                .filter(lo -> lo != null
                        && Arrays.compareUnsigned(lo, HEAVY_LO) >= 0
                        && Arrays.compareUnsigned(lo, HEAVY_HI) < 0)
                .count();

        Run run = scan(dir, "heavy-tail-avg", keyspace, workers, 1000, Duration.ofMillis(2), massAware("on"));

        // Use the byte-content helper (EngineHarness.assertExactlyOnce, I10), not a local helper that
        // only compares duplicate-count + distinct-set SIZE — a wrong set of N unique keys would
        // still pass that.
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        System.out.printf("[parallelization] seed=%d ranges_inside_heavy=%d avg_in_flight=%.2f serial_frac=%.3f "
                + "peak_in_flight=%d (harness ceiling ~6-8; target 0.5*W=%.0f)%n",
                run.seedCount, inHeavy, run.avgInFlight, run.serialFrac, run.peakInFlight, 0.5 * workers);
        assertThat(inHeavy)
                .as("mass-aware ON bands the heavy cut into MANY ranges at full-scan scale (today: ~1, "
                        + "un-tiled) -- the direct structural signature the old avg_in_flight uplift assert "
                        + "was only an indirect (CI-flaky) proxy for")
                .isGreaterThanOrEqualTo(8L);
    }

    // ================================================================================================
    // WIRING/SANITY (not the serial-collapse guard) — the new SEED_SCHED.distinct_seed_worker
    // engagement counter is actually emitted by WorkStealingScan when seed ranges are consumed. This
    // only asserts the counter FIRES (>0) on a concurrent run, proving it is wired end-to-end
    // (recorded at the claim site -> readable via diagnostics().stealReasons()). It deliberately does
    // NOT assert the multi-worker (>= N) property nor force a serial-collapse RED — that adversarial
    // guard is maintained separately, below. The precise serial-collapse contract is in the
    // SEED_SCHED.distinct_seed_worker row of docs/internals/metrics-internals.md §5a.
    // ================================================================================================

    @Test
    void seedSchedDistinctWorkerCounter_isWiredOnHeavyTailRun(@TempDir Path dir) throws Exception {
        int workers = 8;
        List<byte[]> keyspace = narrowTopHeavyDeepSubtree(10, 1200, 50);
        Run run = scan(dir, "heavy-tail-seedsched-wiring", keyspace, workers, 1000, Duration.ofMillis(2),
                massAware("on"));
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        assertThat(run.distinctSeedWorkers)
                .as("SEED_SCHED.distinct_seed_worker must fire (>0) when seed ranges are consumed — "
                        + "the counter is wired at the claim site and readable via diagnostics()")
                .isPositive();
    }

    // ================================================================================================
    // SERIAL-COLLAPSE GUARD —
    // restores runtime-scheduling coverage with a structural, deterministic, ZERO-timing-dependence
    // signal, replacing the CI-flaky `avg_in_flight >= 4.0` fraction this guard used to rely on.
    //
    // Pairs with the placement guard (the heavy band becomes >= 8 seed RANGES): together
    // they read "heavy band placed into >= 8 seed ranges AND consumed by >= 8 DISTINCT workers at runtime".
    // The signal is SEED_SCHED.distinct_seed_worker (see the wiring test above and
    // docs/internals/metrics-internals.md §5a) — the number of distinct workers that each claimed an INITIAL
    // seed range at runtime (thief/owner-split children excluded), so it reflects the banded heavy tail
    // actually spreading across the pool.
    //
    // GREEN arm (concurrent, workers=64): the >= 8 heavy seeds are consumed by >= 8 distinct workers.
    // RED/ablation arm (workers=1, forced serial collapse): distinct_seed_worker == 1 by construction —
    // which is BELOW the concurrent floor, i.e. the guard's `>= 8` assertion would FAIL under a
    // serialization regression. Kept as a permanent companion so a future regression that serializes
    // heavy-band consumption is caught. Do not reintroduce in-flight/achieved-concurrency fractions as
    // the discriminator here — that is exactly the flake class this guard replaces.
    // ================================================================================================

    private static final long DISTINCT_SEED_WORKER_FLOOR = 8L;

    @Test
    @Tag("deep")
    @Timeout(120)
    void distinctSeedWorkers_concurrentHeavyTailSpreadsAcrossManyWorkers(@TempDir Path dir)
            throws Exception {
        int workers = 64;
        List<byte[]> keyspace = narrowTopHeavyDeepSubtree(10, 1200, 50);
        Run run = scan(dir, "heavy-tail-seedsched-concurrent", keyspace, workers, 1000, Duration.ofMillis(2),
                massAware("on"));
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        assertThat(run.distinctSeedWorkers)
                .as("mass-aware ON bands the heavy cut into >= 8 seed ranges which are then "
                        + "CONSUMED by >= 8 DISTINCT workers at runtime — the deterministic structural "
                        + "restoration of the runtime-scheduling coverage the removed avg_in_flight fraction "
                        + "used to provide (NO timing/in-flight proxy)")
                .isGreaterThanOrEqualTo(DISTINCT_SEED_WORKER_FLOOR);
    }

    @Test
    @Timeout(120)
    void distinctSeedWorkers_serialCollapseAblationDropsToOneAndFailsFloor(@TempDir Path dir)
            throws Exception {
        // ABLATION (permanent RED discriminator): the SAME fixture/toggle, forced to a single worker.
        // Seed ranges can then only ever be claimed by that one worker, so distinct_seed_worker collapses
        // to 1 — proving the signal genuinely discriminates a serialized run, and that the `>= 8`
        // floor would FAIL under a heavy-band-serialization regression.
        int workers = 1;
        List<byte[]> keyspace = narrowTopHeavyDeepSubtree(10, 1200, 50);
        Run run = scan(dir, "heavy-tail-seedsched-serial", keyspace, workers, 1000, Duration.ofMillis(2),
                massAware("on"));
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        assertThat(run.distinctSeedWorkers)
                .as("workers=1 serial collapse: exactly ONE distinct worker claims every seed range")
                .isEqualTo(1L);
        assertThat(run.distinctSeedWorkers)
                .as("the collapsed serial run sits BELOW the concurrent floor — i.e. a serialization "
                        + "regression would fail the >= 8 guard (this companion pins that discrimination)")
                .isLessThan(DISTINCT_SEED_WORKER_FLOOR);
    }

    // ================================================================================================
    // Budget guard: the sampling sub-budget is carved OUT of maxProbes
    // (<= 256), and the control stays inside its INT-8 corridor. No-regression rails.
    // ================================================================================================

    @Test
    void seedProbeBudgetStaysWithinMaxProbes() throws Exception {
        int workers = 64;   // maxProbes = min(256, 4*W) = 256

        // Heavy-tail fixture: total seed LIST probes must never exceed the 256 wallet, ON or OFF.
        for (String v : List.of("on", "off")) {
            MockPageFetcher heavy = MockPageFetcher.builder()
                    .keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build();
            SeedSteps.of(heavy, NO_PREFIX, workers, null, massAware(v)).seedSpecs(1L, SeedMode.SHALLOW);
            assertThat(heavy.apiCalls())
                    .as("heavy-tail seed probes stay within the 256 wallet (sampling carved OUT, not added) [%s]", v)
                    .isLessThanOrEqualTo(256L);
        }

        // Control (INT-8): the ON path samples a FIXED small number of children for the decision, so
        // its LIST count stays within a bounded corridor of the OFF path — never O(directories).
        MockPageFetcher ctlOff = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        SeedSteps.of(ctlOff, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW);
        MockPageFetcher ctlOn = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        SeedSteps.of(ctlOn, NO_PREFIX, workers, null, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(ctlOn.apiCalls())
                .as("INT-8 control: ON samples a fixed few children, stays inside the OFF corridor")
                .isLessThanOrEqualTo(ctlOff.apiCalls() + 64L)
                .isLessThanOrEqualTo(256L);
    }

    // ================================================================================================
    // Control guard: the INT-8 1:1 explosion seeds the SAME with the toggle ON (no
    // wasted descent), and the explosion_confirmed counter fires when the sample mechanism is actually
    // exercised.
    // ================================================================================================

    @Test
    void int8ControlSeedsIdenticallyWithToggleOn() throws Exception {
        int workers = 64;
        MockPageFetcher off = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        List<NodeSpec> offSpecs = SeedSteps.of(off, NO_PREFIX, workers, null, massAware("off"))
                .seedSpecs(1L, SeedMode.SHALLOW);
        MockPageFetcher on = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        List<NodeSpec> onSpecs = SeedSteps.of(on, NO_PREFIX, workers, null, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(boundaries(onSpecs))
                .as("INT-8 explosion left whole: ON seeds byte-identically to OFF (no wasted descent)")
                .isEqualTo(boundaries(offSpecs));
    }

    @Test
    void int8ControlFiresExplosionConfirmedCounter() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Explicit fanout_tiling=off alongside mass_aware_seed=on. explodedOneObjectPerLeaf's
        // `pid=<hex>/` names are ALSO a key=value partition shape (the identity test's own "left whole"
        // framing is about boundaries only — the underlying route is actually fanout_tiled,
        // see that fixture's javadoc), so with fanout_tiling at its parse default (on) the
        // fanout-tiling-precedence hoist (SeedStep ~L439) short-circuits the mass-aware sample
        // entirely on this cut before it ever runs (the sharp partition signal wins and fires
        // `banding_deferred_to_fanout` instead — see SeedStepFanoutTilingContractTest for that
        // interaction). This guard's actual claim is about the SAMPLE mechanism proving a 1:1
        // explosion, independent of the tiling decision, so pin fanout_tiling off to isolate it: the
        // truncated pid=<hex>/ cut then falls through to the mass-aware sample regardless of its
        // key=value-shaped names.
        EngineToggles toggles = EngineToggles.parse(List.of(TOGGLE_NAME + "=on", "fanout_tiling=off"), false);
        SeedSteps.of(MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build(),
                NO_PREFIX, workers, metrics, toggles).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, EXPLOSION_CONFIRMED, "INT-8 control: second-level sample proves 1:1 explosion");
    }

    // ================================================================================================
    // Engagement-counter guard: new SEED.* counters fire on the shapes where they apply.
    // ================================================================================================

    @Test
    void heavyTailFiresHeavyCutDescendedAndBanded() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build(),
                NO_PREFIX, workers, metrics, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, HEAVY_CUT_DESCENDED, "descended a heavy truncated cut");
        assertReasonFired(metrics, HEAVY_CUT_BANDED, "banded the confirmed-heavy cut");
    }

    @Test
    void wideNonTruncatedTopFiresMassWeightedSubsample() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(wideNonTruncatedTopHeavyTail(600, 20, 6, 500)).build(),
                NO_PREFIX, workers, metrics, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, MASS_WEIGHTED_SUBSAMPLE,
                "wide non-truncated top: weight-proportional subsample of the over-cap cut set");
    }

    @Test
    void truncatedTopFiresTopProbePaginated() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder()
                .keys(truncatedTopHeavyBeyondFirstPage(1400, 1100, 60, 40)).build(),
                NO_PREFIX, workers, metrics, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, TOP_PROBE_PAGINATED,
                "truncated top: +1-page pagination recovers heavy prefixes past the first page");
        // The pagination probe is a real extra LIST, so it must be its own
        // seed.decisions[] entry (index 1, right after the top-level entry at index 0) — otherwise
        // decisions[] disagrees with the probes/delimiter_fanout counters it is cross-checked against.
        RunSummary summary = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        List<RunSummary.SeedSummary.SeedDecision> decisions = summary.seed().decisions();
        assertThat(decisions).as("top-level entry + the pagination probe's own entry")
                .hasSizeGreaterThanOrEqualTo(2);
        assertThat(decisions.get(1).classification())
                .as("the +1-page pagination probe gets its own decisions[] entry, not folded into the top level")
                .isEqualTo("top_probe_paginated");
    }

    // ================================================================================================
    // Frontier-continuation guard: an exploding sibling probed FIRST in frontier order must not
    // strand an UNRELATED sibling with real, splittable mass at one un-split range. Regression
    // guard for the commoncrawl s3://commoncrawl shape (an 8M-object uniform directory sitting next
    // to a sibling that exploded first, never descended because the old descent loop broke out of
    // the WHOLE frontier the instant it hit the first truncated sub-level).
    // ================================================================================================

    /**
     * Narrow top ({@code explode/}, {@code narrow/}) — {@code explode/}'s own sub-level fans out
     * into {@code explodeLeaves} plain (non-{@code key=value}) 1:1 directories (truncated once past
     * {@code PROBE_PAGE}), while the UNRELATED {@code narrow/} sibling fans out into {@code
     * narrowChildren} children each holding real mass ({@code perChild} objects) — narrow enough to
     * never truncate. {@code "explode/"} sorts before {@code "narrow/"}, so plain FIFO frontier
     * order (mass-aware reorder off) probes the exploding sibling first, exactly the ordering that
     * used to make the old {@code break} abandon {@code narrow/} entirely.
     */
    private static List<byte[]> explodingSiblingBesideNarrowMassySibling(int explodeLeaves,
            int narrowChildren, int perChild) {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < explodeLeaves; i++) {
            keys.add(("explode/leaf%04d/o".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        for (int c = 0; c < narrowChildren; c++) {
            for (int o = 0; o < perChild; o++) {
                keys.add(("narrow/child%02d/obj%03d".formatted(c, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    @Test
    void explodingSiblingDoesNotStrandNarrowSiblingWithRealMass() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> keyspace = explodingSiblingBesideNarrowMassySibling(1500, 3, 30);

        List<NodeSpec> specs = SeedSteps.of(MockPageFetcher.builder().keys(keyspace).build(),
                NO_PREFIX, workers, metrics, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW);

        // Still an exact I2/I3 tiling regardless of how the frontier continuation sub-tiled things.
        assertTiles(specs);

        // The exploding sibling's own disposal still happened (left whole, INT-8 shape) — the fix
        // changes what happens to the REST of the frontier, not this cut's own classification.
        assertReasonFired(metrics, TINY_LEAF_EXPLOSION, "explode/ is still classified and left whole");

        // The narrow sibling was actually descended: its children became their OWN seed cut-points,
        // instead of the whole narrow/ subtree being swallowed into one un-split range because
        // explode/ exploded first.
        List<String> cutBoundaries = boundaries(specs);
        for (int c = 0; c < 3; c++) {
            byte[] child = ("narrow/child%02d/".formatted(c)).getBytes(StandardCharsets.UTF_8);
            assertThat(cutBoundaries)
                    .as("narrow/child%02d/ must be its own cut-point — narrow/ was descended, not "
                            + "abandoned when explode/ exploded first", c)
                    .contains(hex(child));
        }

        // Fires exactly where the old unconditional break would have abandoned the rest of the
        // frontier: explode/'s disposal completed while narrow/ was still queued.
        assertReasonFired(metrics, FRONTIER_CONTINUED_PAST_EXPLOSION,
                "the descent kept probing narrow/ after explode/ exploded, instead of abandoning "
                        + "the rest of the frontier");
    }

    // ================================================================================================
    // Explicit-OFF identity guard: with the flag explicitly off, seeding is
    // byte-identical no matter how many times/where it is constructed — the no-regression rail for
    // the fanout-tiling-precedence hoist (SeedStep ~L439-482 reordered the fanout_tiling check ABOVE
    // the mass-aware sample; the OFF path takes the exact same
    // branch order before and after the reorder, since `heavy` is always false when massAware is off
    // regardless of where `isPartitionFanout` is checked). DEFAULT is no longer OFF — mass_aware_seed
    // is default-ON; see the default-state guard below.
    // ================================================================================================

    @Test
    void explicitOffIsByteIdenticalAcrossConstructionsOnBothFixtures() throws Exception {
        int workers = 64;

        // Heavy-tail fixture
        MockPageFetcher heavyOff1 = MockPageFetcher.builder().keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build();
        List<String> heavyOff1Boundaries = boundaries(
                SeedSteps.of(heavyOff1, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        MockPageFetcher heavyOff2 = MockPageFetcher.builder().keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build();
        List<String> heavyOff2Boundaries = boundaries(
                SeedSteps.of(heavyOff2, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        assertThat(heavyOff2Boundaries)
                .as("heavy-tail explicit OFF is deterministic/unaffected by the hoist").isEqualTo(heavyOff1Boundaries);

        // Control (INT-8)
        MockPageFetcher ctlOff1 = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        List<String> ctlOff1Boundaries = boundaries(
                SeedSteps.of(ctlOff1, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        MockPageFetcher ctlOff2 = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();
        List<String> ctlOff2Boundaries = boundaries(
                SeedSteps.of(ctlOff2, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        assertThat(ctlOff2Boundaries)
                .as("control: explicit OFF is deterministic/unaffected by the hoist").isEqualTo(ctlOff1Boundaries);
    }

    // The preceding method covers the heavy-tail and INT-8 fixtures; these two shapes need the same
    // cheap explicit-OFF byte-identity rail.

    @Test
    void explicitOffIsByteIdenticalAcrossConstructionsOnWideNonTruncatedTop() throws Exception {
        int workers = 64;
        MockPageFetcher off1 = MockPageFetcher.builder()
                .keys(wideNonTruncatedTopHeavyTail(600, 20, 6, 500)).build();
        List<String> off1Boundaries = boundaries(
                SeedSteps.of(off1, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        MockPageFetcher off2 = MockPageFetcher.builder()
                .keys(wideNonTruncatedTopHeavyTail(600, 20, 6, 500)).build();
        List<String> off2Boundaries = boundaries(
                SeedSteps.of(off2, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        assertThat(off2Boundaries)
                .as("wide non-truncated top: explicit OFF is deterministic/unaffected by the hoist")
                .isEqualTo(off1Boundaries);
    }

    @Test
    void explicitOffIsByteIdenticalAcrossConstructionsOnTruncatedTop() throws Exception {
        int workers = 64;
        MockPageFetcher off1 = MockPageFetcher.builder()
                .keys(truncatedTopHeavyBeyondFirstPage(1400, 1100, 60, 40)).build();
        List<String> off1Boundaries = boundaries(
                SeedSteps.of(off1, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        MockPageFetcher off2 = MockPageFetcher.builder()
                .keys(truncatedTopHeavyBeyondFirstPage(1400, 1100, 60, 40)).build();
        List<String> off2Boundaries = boundaries(
                SeedSteps.of(off2, NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW));
        assertThat(off2Boundaries)
                .as("truncated top: explicit OFF is deterministic/unaffected by the hoist")
                .isEqualTo(off1Boundaries);
    }

    @Test
    void defaultIsMassAwareOnAndBandsHeavyCut() throws Exception {
        assertThat(EngineToggles.DEFAULT.massAwareSeed())
                .as("the default flip lands mass_aware_seed=ON in the supported EngineToggles.DEFAULT "
                        + "configuration")
                .isTrue();

        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<NodeSpec> defSpecs = SeedSteps.of(
                MockPageFetcher.builder().keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build(),
                NO_PREFIX, workers, metrics, EngineToggles.DEFAULT).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, HEAVY_CUT_BANDED,
                "a DEFAULT-constructed SeedStep bands the heavy non-partition cut "
                        + "(mass-aware ON by default) instead of leaving it whole");

        List<NodeSpec> offSpecs = SeedSteps.of(
                MockPageFetcher.builder().keys(narrowTopHeavyDeepSubtree(10, 1200, 50)).build(),
                NO_PREFIX, workers, null, massAware("off")).seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(boundaries(defSpecs))
                .as("DEFAULT (mass-aware ON) diverges from explicit OFF on the heavy-tail fixture — the flip "
                        + "changes seeding, not just the flag's stored value")
                .isNotEqualTo(boundaries(offSpecs));
    }

    // ================================================================================================
    // Plain-hex negative control: the 1:1 explosion (no key=value naming) must
    // actually reach the tiny-leaf "left whole" branch: byte-identical to OFF, explosion_confirmed
    // fires, heavy_cut_banded and fanout_tiled do NOT fire. See explodedPlainHexOneObjectPerLeaf.
    // ================================================================================================

    @Test
    void plainHexExplosionSeedsIdenticallyWithToggleOn() throws Exception {
        int workers = 64;
        MockPageFetcher off = MockPageFetcher.builder().keys(explodedPlainHexOneObjectPerLeaf(4000)).build();
        List<NodeSpec> offSpecs = SeedSteps.of(off, NO_PREFIX, workers, null, massAware("off"))
                .seedSpecs(1L, SeedMode.SHALLOW);
        MockPageFetcher on = MockPageFetcher.builder().keys(explodedPlainHexOneObjectPerLeaf(4000)).build();
        List<NodeSpec> onSpecs = SeedSteps.of(on, NO_PREFIX, workers, null, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(boundaries(onSpecs))
                .as("plain-hex explosion left whole: ON seeds byte-identically to OFF (no wasted descent, "
                        + "no fanout tiling — the shape dodges the key=value detector)")
                .isEqualTo(boundaries(offSpecs));
    }

    @Test
    void plainHexExplosionFiresExplosionConfirmedOnlyNotBandedOrFanoutTiled() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(explodedPlainHexOneObjectPerLeaf(4000)).build(),
                NO_PREFIX, workers, metrics, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);
        assertReasonFired(metrics, EXPLOSION_CONFIRMED,
                "plain-hex control: second-level sample proves 1:1 explosion");
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + HEAVY_CUT_BANDED, 0L))
                .as("plain-hex explosion must NOT be classified heavy/banded")
                .isZero();
        assertThat(reasons.getOrDefault("SEED." + FANOUT_TILED, 0L))
                .as("plain-hex explosion dodges the key=value detector — fanout_tiled must NOT fire")
                .isZero();
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private static void assertReasonFired(RunMetrics metrics, String reason, String why) {
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + reason, 0L))
                .as("SEED.%s must fire — %s", reason, why)
                .isPositive();
    }

    /** The sorted list of finite range-end cut boundaries (hex) — a byte-exact seed fingerprint. */
    private static List<String> boundaries(List<NodeSpec> specs) {
        List<String> out = new ArrayList<>();
        for (NodeSpec s : specs) {
            out.add(s.rangeStart() == null ? "_" : hex(s.rangeStart()));
        }
        for (NodeSpec s : specs) {
            out.add(s.rangeEnd() == null ? "_" : hex(s.rangeEnd()));
        }
        return out;
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }

    private static void assertTiles(List<NodeSpec> specs) {
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /** Increment the last byte of a prefix to form its exclusive ceiling (all prefix keys sort below it). */
    private static byte[] prefixCeil(byte[] prefix) {
        byte[] c = Arrays.copyOf(prefix, prefix.length);
        c[c.length - 1]++;
        return c;
    }

    // ---- engine harness (avg-in-flight), modeled on ShapeRegressionCorpusTest.scan -----------------

    private record Run(List<byte[]> emitted, int seedCount, long apiCalls, long peakInFlight,
            double avgInFlight, double serialFrac, long distinctSeedWorkers) {
    }

    private static Run scan(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys,
            Duration pageLatency, EngineToggles toggles) throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Latency only on worker listing pages — seed/thief probes stay instant, so this exposes
                // concurrency without confounding the seed (the ShapeRegressionCorpusTest idiom).
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

            metrics.markRunStarted();
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        RunSummary.TrajectorySummary traj =
                metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).trajectory();
        double serialFrac = traj != null ? traj.serialFrac() : -1.0;
        // The deterministic multi-worker-consumption signal — how many DISTINCT workers claimed
        // a seed range at runtime (serial run => 1, concurrent spread of the banded heavy tail => many).
        long distinctSeedWorkers = metrics.diagnostics(Duration.ZERO).stealReasons()
                .getOrDefault("SEED_SCHED.distinct_seed_worker", 0L);
        return new Run(emitted, seedCount, mock.apiCalls(), metrics.peakInFlight(),
                metrics.avgInFlight(), serialFrac, distinctSeedWorkers);
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

}
