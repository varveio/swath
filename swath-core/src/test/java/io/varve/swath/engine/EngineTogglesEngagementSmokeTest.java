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
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * One engagement smoke per {@code --engine-toggle} name (§5 discipline), proving the
 * OFF state is observable from the engine's own metrics alone (never inferred from silence). {@code
 * owner_split} is covered by {@link OwnerSplitKillSwitchTest#toggleOffMarkFiresOnlyWhenOwnerSplitDisabled};
 * this file covers the other five. These are ordinary unit guards of the toggle wiring — the
 * cross-cutting PROP-1/RES-3/CONC interleavings are covered separately.
 */
final class EngineTogglesEngagementSmokeTest {

    private static RunKey key(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private record ScanResult(List<byte[]> emitted, Map<String, Long> stealReasons,
                              SimpleMeterRegistry registry, int seedCount) {
        double meterCount(String name) {
            var meter = registry.find(name).counter();
            return meter == null ? 0.0 : meter.count();
        }
    }

    /** Seed via {@code SeedStep} (SHALLOW) then drive the full engine — for scenarios needing structure. */
    private static ScanResult runShallowSeeded(Path dir, String label, List<byte[]> keyspace, int workers,
                                               int maxKeys, EngineToggles toggles) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            List<NodeSpec> specs = SeedSteps.of(fetcher, new byte[0], workers, metrics, toggles)
                    .seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            drain(engine, emitted);
        }
        return new ScanResult(emitted, metrics.diagnostics(Duration.ZERO).stealReasons(), registry, seedCount);
    }

    /** Seed via a single root range {@code (⊥, ⊤]} — for scenarios needing one wide bounded range. */
    private static ScanResult runFromRoot(Path dir, String label, List<byte[]> keyspace, int workers,
                                          int maxKeys, EngineToggles toggles) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            drain(engine, emitted);
        }
        return new ScanResult(emitted, metrics.diagnostics(Duration.ZERO).stealReasons(), registry, 1);
    }

    private static void drain(WorkStealingScan engine, List<byte[]> emitted) throws Exception {
        PipelineDrain.collectKeys(5000, engine, emitted);
    }

    private static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(distinctEmitted).as("full byte-exact coverage, no duplicates").isEqualTo(distinctKeyspace);
    }

    // =========================================================================
    // structure_probes
    // =========================================================================

    /** A deep funnel: single-child chain {@code f/g/h/} then a broad fan-out the shallow seed can't reach. */
    private static List<byte[]> deepFunnel(int dirs, int keysPerDir) {
        List<byte[]> keys = new ArrayList<>(dirs * keysPerDir + 1);
        for (int d = 0; d < dirs; d++) {
            for (int k = 0; k < keysPerDir; k++) {
                keys.add(("f/g/h/%04d/obj-%03d".formatted(d, k)).getBytes(StandardCharsets.UTF_8));
            }
        }
        keys.add("zzz/end".getBytes(StandardCharsets.UTF_8));   // far bound so byte-midpoint funnels
        return keys;
    }

    @Test
    @Timeout(120)
    void structureProbesOffSilencesDelimiterDiscoveryOnAShapeThatOtherwiseProbes(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = deepFunnel(1024, 4);
        int workers = 16;
        int maxKeys = 16;

        ScanResult on = runShallowSeeded(dir, "structure-on", keyspace, workers, maxKeys, EngineToggles.DEFAULT);
        assertExactlyOnce(on.emitted(), keyspace);
        assertThat(on.meterCount("swath.probe.structure_fetches"))
                .as("default toggles: this shape DOES fire structure probes")
                .isGreaterThan(0.0);

        EngineToggles structureProbesOff = EngineToggles.DEFAULT.withStructureProbes(false);
        ScanResult off = runShallowSeeded(dir, "structure-off", keyspace, workers, maxKeys, structureProbesOff);
        assertExactlyOnce(off.emitted(), keyspace);
        assertThat(off.meterCount("swath.probe.structure_fetches"))
                .as("structure_probes=off: zero probe.structure_fetches on the same shape")
                .isZero();
        assertThat(off.stealReasons().getOrDefault("TOGGLE.structure_probes_off", 0L))
                .as("the once-per-scan TOGGLE mark fired")
                .isEqualTo(1L);
    }

    // =========================================================================
    // alphabet_pivots
    // =========================================================================

    // {@code alphabet_pivots}'s engagement smoke lives in
    // {@link ThiefTest#alphabetPivotsOffFallsBackToThePlainCodePointPivot} — a deterministic
    // single-attempt setup (the same controlled digest as {@code farAheadPivotWinRecordsPivotAndAlphabetCounters})
    // rather than hoping a randomized full-engine run happens to deflect a pivot.

    // =========================================================================
    // radix_bands
    // =========================================================================

    private static List<byte[]> flatDenseHexRoot(int n, long seed) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("%016x", r.nextLong()).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    @Timeout(60)
    void radixBandsOffLeavesADenseFlatRootAsOneRange(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = flatDenseHexRoot(6000, 909L);
        int workers = 16;

        SimpleMeterRegistry onRegistry = new SimpleMeterRegistry();
        RunMetrics onMetrics = new RunMetrics(onRegistry);
        MockPageFetcher onFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> onSpecs = SeedSteps.of(onFetcher, new byte[0], workers, onMetrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(onSpecs.size())
                .as("default toggles: the dense flat root IS radix-banded into many seed ranges")
                .isGreaterThan(1);
        assertThat(onMetrics.diagnostics(Duration.ZERO).stealReasons()
                        .getOrDefault("SEED.dense_root_radix_banded", 0L))
                .isEqualTo(1L);

        SimpleMeterRegistry offRegistry = new SimpleMeterRegistry();
        RunMetrics offMetrics = new RunMetrics(offRegistry);
        MockPageFetcher offFetcher = MockPageFetcher.builder().keys(keyspace).build();
        EngineToggles radixBandsOff = EngineToggles.DEFAULT.withRadixBands(false);
        List<NodeSpec> offSpecs = SeedSteps.of(offFetcher, new byte[0], workers, offMetrics, radixBandsOff)
                .seedSpecs(2L, SeedMode.SHALLOW);
        assertThat(offSpecs)
                .as("radix_bands=off: the dense flat root is left as ONE un-subdivided range")
                .hasSize(1);
        Map<String, Long> offReasons = offMetrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(offReasons.getOrDefault("SEED.dense_root_radix_banded", 0L))
                .as("radix_bands=off: the banding engagement counter never fires")
                .isZero();
        assertThat(offReasons.getOrDefault("TOGGLE.radix_bands_off", 0L)).isEqualTo(1L);
    }

    // =========================================================================
    // density_ewma / far_ahead — the composed TOGGLE-mark-presence smoke
    // =========================================================================

    /**
     * {@code density_ewma}/{@code far_ahead} both act on the far-ahead pivot-fraction arithmetic
     * (never their own engagement counter category — they modify {@code PIVOT.far_ahead}'s
     * inputs, not a distinct code path), so their OFF state is proven the way §5 prescribes for
     * exactly that shape of mechanism: an explicit {@code TOGGLE.<name>_off} mark, independent of
     * whether this particular run's shape happens to steal from a bounded range at all.
     */
    @Test
    @Timeout(30)
    void densityEwmaAndFarAheadOffRecordToggleMarks(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = List.of("a".getBytes(StandardCharsets.UTF_8));
        EngineToggles bothOff = EngineToggles.DEFAULT.withDensityEwma(false).withFarAhead(false);

        ScanResult result = runFromRoot(dir, "density-far-off", keyspace, 2, 10, bothOff);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.density_ewma_off", 0L)).isEqualTo(1L);
        assertThat(result.stealReasons().getOrDefault("TOGGLE.far_ahead_off", 0L)).isEqualTo(1L);
        // the toggles that stayed on must NOT record a mark
        assertThat(result.stealReasons().getOrDefault("TOGGLE.owner_split_off", 0L)).isZero();
        assertThat(result.stealReasons().getOrDefault("TOGGLE.structure_probes_off", 0L)).isZero();
        assertThat(result.stealReasons().getOrDefault("TOGGLE.alphabet_pivots_off", 0L)).isZero();
    }

    /**
     * {@code density_ewma=off} disables the far-ahead pivot-fraction arithmetic
     * ({@link WorkerState#densityFraction()}, {@link EngineToggles#farAheadFraction}) AND the
     * OWNER_SPLIT observed-mass child-tail floor ({@code StealMath#childTailBelowObservedMassFloor}),
     * which otherwise reads {@link WorkerState#observedDensityRatio()} straight off the victim
     * unconditionally — both must go dark together or a {@code density_ewma=off} run stays
     * density-EWMA-controlled for {@code OWNER_SPLIT.floor_reflected_blocked}, confounding A/B
     * interpretation. Drives a real EWMA thinning-tail transition (the same pattern as {@code
     * ObservedMassFloorContractTest}) through {@link EngineToggles#observedDensityRatio(WorkerState)}
     * rather than a full noisy engine run: a full-keyspace drive can't isolate the EWMA-driven floor
     * from the ordinary tiny-remainder span floor (which fires under EITHER toggle state near the
     * true end of any range), so this pins the exact toggle-routed value/decision the mechanism
     * depends on instead.
     */
    @Test
    void densityEwmaOffAlsoDisablesTheOwnerSplitObservedMassFloor() {
        double f = WorkerState.MAX_FAR_FRACTION;   // a dense drainer's far-ahead fraction
        double bigEst = 100_000.0;                  // large enough that only the density signal floors it

        WorkerState w = WorkerStates.of(1, "a".getBytes(StandardCharsets.UTF_8),
                "y".getBytes(StandardCharsets.UTF_8), "z".getBytes(StandardCharsets.UTF_8));
        w.addKeysEmitted(200);
        for (int i = 0; i < 60; i++) {
            w.recordPage("m".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8), 3);
        }
        double thinRatio = w.observedDensityRatio();
        assertThat(thinRatio).as("victim's own EWMA has actually thinned below f").isLessThan(f);

        double onRatio = EngineToggles.DEFAULT.observedDensityRatio(w);
        assertThat(onRatio).as("density_ewma=on: the toggle passes the real EWMA ratio through").isEqualTo(thinRatio);
        assertThat(StealMath.childTailBelowObservedMassFloor(bigEst, f, onRatio, 100))
                .as("density_ewma=on: the thinning tail floors (blocks) the confetti split")
                .isTrue();

        EngineToggles densityEwmaOff = EngineToggles.DEFAULT.withDensityEwma(false);
        double offRatio = densityEwmaOff.observedDensityRatio(w);
        assertThat(offRatio)
                .as("density_ewma=off: the floor's own no-signal fallback (+INF), NOT the real EWMA ratio")
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(StealMath.childTailBelowObservedMassFloor(bigEst, f, offRatio, 100))
                .as("density_ewma=off: collapses to the plain (1-f)*est span floor -- floor_reflected_blocked "
                        + "never fires on this same thinning-tail victim/est")
                .isFalse();
    }

    @Test
    @Timeout(30)
    void defaultTogglesRecordNoToggleMarks(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = List.of("a".getBytes(StandardCharsets.UTF_8));
        ScanResult result = runFromRoot(dir, "all-default", keyspace, 2, 10, EngineToggles.DEFAULT);
        assertExactlyOnce(result.emitted(), keyspace);
        for (String name : EngineToggles.NAMES) {
            assertThat(result.stealReasons().getOrDefault("TOGGLE." + name + "_off", 0L))
                    .as("no TOGGLE mark for %s at the supported default", name)
                    .isZero();
        }
    }
}
