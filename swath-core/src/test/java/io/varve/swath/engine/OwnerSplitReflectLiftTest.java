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
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The reflect-lift decision {@link StealMath#shouldLiftToReflected}: the zero-page-per-carve
 * fix. Do not replace it with an owner-kept mass floor (suppresses carves in the wrong reference
 * frame) or a structural relay-pivot guard (suppresses carves outright and collapses the
 * owner-split mechanism's own healthy split count) — both were tried and rejected. Exercised
 * directly (pure arithmetic + byte compares, package-private) for the five contract cases — trigger
 * boundary, lift-only-up, no-room-to-lift, and the lifted child-tail floor — plus a full-engine
 * toggle-off bypass smoke. Ordinary unit guards of the lift mechanics, mirroring {@link
 * OwnerSplitReflectClampTest}'s style for the clamp (the same reflected-pivot machinery pulling in
 * the opposite direction) — not the PROP-1/RES-3/CONC cross-cutting interleavings.
 */
final class OwnerSplitReflectLiftTest {

    private static final int MAX_KEYS = 100;

    // ---- pure decision: shouldLiftToReflected ---------------------------------------------------
    //
    // Shared byte geometry for every pure-decision case below (single-byte keys, no common prefix,
    // so spanIn's base-256 fraction is exact, unrounded arithmetic):
    //   lo=0, cursorTo=40, m=50, H=100 (mReflect varies per case).
    //   remSpan  = spanIn(cursorTo, H, lo, H)      = (100-40)/256 = 60/256
    //   fKeptLo  = spanIn(cursorTo, m, lo, H) / remSpan = (10/256) / (60/256) = 1/6
    //   est=600 -> est*fKeptLo == 100 == maxKeys exactly (boundary, "<=" triggers); est=606 -> 101 (no trigger).

    private static byte[] b(int v) {
        return new byte[] {(byte) v};
    }

    private static final byte[] LO = b(0);
    private static final byte[] CURSOR_TO = b(40);
    private static final byte[] M = b(50);
    private static final byte[] H = b(100);

    @Test
    void triggerBoundaryExactlyAtOnePageLifts() {
        // est * fKeptLo == 100 == maxKeys exactly: "<=" is inclusive, so the trigger fires; mReflect=70
        // moves m strictly up (50 -> 70) with room before H (100), and the lifted child tail clears
        // the observed-mass floor (fReflectLifted=0.5 -> realized mass 600*(1-0.5)=300 > 2*maxKeys=200).
        byte[] mReflect = b(70);
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflect, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("trigger boundary inclusive (est*fKeptLo == maxKeys) + valid lift ⇒ lifts")
                .isTrue();
    }

    @Test
    void justAboveTheTriggerBoundaryNeverLifts() {
        // est=606 -> est*fKeptLo = 101 > 100 = maxKeys: the owner already keeps a shade over one page
        // in est's own frame, so there is nothing to fix -- no lift, regardless of a valid mReflect.
        byte[] mReflect = b(70);
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflect, LO, H, 606.0, 1.0, MAX_KEYS))
                .as("owner already keeps > one page ⇒ never lifts")
                .isFalse();
    }

    @Test
    void liftOnlyUpNeverLiftsWhenMReflectDoesNotExceedM() {
        // Trigger fires (est=600, boundary), but mReflect=45 <= m=50: the reflect clamp already owns the
        // DOWN direction (mReflect < m would be a clamp candidate there, not a lift candidate here).
        byte[] mReflectBelow = b(45);
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflectBelow, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("mReflect <= m ⇒ not a lift candidate (down direction belongs to the reflect clamp)")
                .isFalse();

        byte[] mReflectEqual = M;
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflectEqual, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("mReflect == m ⇒ no actual move, never lifts")
                .isFalse();
    }

    @Test
    void noRoomToLiftWhenMReflectAtOrPastH() {
        // Trigger fires, mReflect strictly exceeds m, but mReflect >= H leaves no room to lift into.
        byte[] mReflectAtH = H;
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflectAtH, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("mReflect == H ⇒ no room to lift into, never lifts")
                .isFalse();

        byte[] mReflectPastH = b(120);
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflectPastH, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("mReflect > H ⇒ no room to lift into, never lifts")
                .isFalse();
    }

    @Test
    void nullMReflectNeverLifts() {
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, null, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("no reflected pivot available (e.g. unstarted frontier) ⇒ never lifts")
                .isFalse();
    }

    @Test
    void liftedChildTailFailingThe1cFloorNeverLifts() {
        // mReflect=90 moves m up validly (50 -> 90, still < H=100), but fReflectLifted = (90-40)/(100-40)
        // = 50/60 = 0.8333 -> the LIFTED child tail (mReflect, H] realizes only 600*(1-0.8333) = 100
        // <= 2*maxKeys=200 -- the lift itself would fission the child into confetti, so it is rejected.
        byte[] mReflect = b(90);
        assertThat(StealMath.shouldLiftToReflected(CURSOR_TO, M, mReflect, LO, H, 600.0, 1.0, MAX_KEYS))
                .as("a lift whose own child tail would be confetti-sized is rejected")
                .isFalse();
    }

    // ---- full-engine toggle-off bypass ------------------------------------------------------------

    private static final int ENGINE_MAX_KEYS = 100;
    private static final byte[] ENGINE_LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ENGINE_HI = "d/05".getBytes(StandardCharsets.UTF_8);

    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static NodeSpec seed(long runId, byte[] lo, byte[] hi) {
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, null, null);
    }

    private Map<String, Long> runScan(Path dir, String label, List<byte[]> keyspace, EngineToggles toggles)
            throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(seed(run.id(), ENGINE_LO, ENGINE_HI));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, 8, ENGINE_MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        assertThat(emitted).hasSize(keyspace.size());
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    @Test
    @Timeout(60)
    void reflectLiftOffNeverLiftsButOwnerSplitStillFires(@TempDir Path dir) throws Exception {
        EngineToggles reflectLiftOff = EngineToggles.DEFAULT.withReflectLift(false);
        Map<String, Long> reasons = runScan(dir, "lift-off", denseFlat(40_000), reflectLiftOff);

        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_lifted", 0L))
                .as("reflect_lift=off must never lift a pivot")
                .isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.self_published", 0L))
                .as("reflect_lift=off must not disable owner-split itself")
                .isGreaterThanOrEqualTo(1L);
        assertThat(reasons.getOrDefault("TOGGLE.reflect_lift_off", 0L))
                .as("the once-per-scan TOGGLE mark fired")
                .isEqualTo(1L);
    }

    @Test
    @Timeout(60)
    void reflectLiftOnLiftsOnThisSameShape(@TempDir Path dir) throws Exception {
        Map<String, Long> reasons = runScan(dir, "lift-on", denseFlat(40_000), EngineToggles.DEFAULT);

        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_lifted", 0L))
                .as("default toggles (reflect_lift=on) must lift on this relay-prone shape")
                .isGreaterThan(0L);
    }

    // ---- toggle hierarchy: reflect=off must kill the lift too, even when
    // reflect_lift is separately on — the lift is itself a reflection application (it calls the SAME
    // StealMath.extrapolate the clamp uses), so it is gated on reflect() && reflectLift(), never
    // reflectLift() alone. reflect_lift=off in isolation must leave the clamp active. ------------

    @Test
    @Timeout(60)
    void reflectOffKillsTheLiftEvenWithReflectLiftSeparatelyOn(@TempDir Path dir) throws Exception {
        // reflect=off, reflect_lift=on: the lift must never fire (reflect=off wins), matching
        // docs/configuration.md's "reflect=off restores exact pre-reflection placement" claim -- behavior
        // identical to a full reflect=off ablation (clamp and lift both silent), not just the lift toggle.
        EngineToggles reflectOffLiftOn = EngineToggles.DEFAULT.withReflect(false);
        Map<String, Long> reasons = runScan(dir, "reflect-off-lift-on", denseFlat(40_000), reflectOffLiftOn);

        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_lifted", 0L))
                .as("reflect=off must kill the lift regardless of reflect_lift's own value")
                .isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_clamped", 0L))
                .as("reflect=off also silences the reflect clamp (full reflection ablation)")
                .isZero();
        assertThat(reasons.getOrDefault("TOGGLE.reflect_off", 0L))
                .as("the once-per-scan reflect ablation mark fired")
                .isEqualTo(1L);
        assertThat(reasons.getOrDefault("OWNER_SPLIT.self_published", 0L))
                .as("reflect=off must not disable owner-split itself")
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Timeout(60)
    void reflectOnReflectLiftOffLeaves1bClampActive(@TempDir Path dir) throws Exception {
        // reflect=on, reflect_lift=off: the lift must never fire, but the clamp (the OTHER
        // reflection consumer) must still be able to engage -- reflect_lift=off ablates only the
        // lift, in isolation, per the EngineToggles javadoc.
        EngineToggles reflectOnLiftOff = EngineToggles.DEFAULT.withReflectLift(false);
        Map<String, Long> reasons = runScan(dir, "reflect-on-lift-off", denseFlat(40_000), reflectOnLiftOff);

        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_lifted", 0L))
                .as("reflect_lift=off must never lift")
                .isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.pivot_reflect_clamped", 0L))
                .as("the reflect clamp stays active when only reflect_lift is off")
                .isGreaterThan(0L);
        assertThat(reasons.getOrDefault("TOGGLE.reflect_lift_off", 0L))
                .as("the once-per-scan reflect_lift ablation mark fired")
                .isEqualTo(1L);
        assertThat(reasons.getOrDefault("TOGGLE.reflect_off", 0L))
                .as("reflect itself is on -- its own ablation mark must not fire")
                .isZero();
    }
}
