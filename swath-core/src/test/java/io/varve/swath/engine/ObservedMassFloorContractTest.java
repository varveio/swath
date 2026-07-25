/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Adversarial guard for the owner-split floor/clamp redesign (observed-mass
 * child-tail floor + reflection clamp). The focused unit tests pass the
 * density ratio in as a literal; this guard instead drives {@link WorkerState#observedDensityRatio()}
 * through a real EWMA transition (dense head → thinning tail, and the inverse recovery) and proves the
 * floor decision FLIPS with it — blocking confetti only once the tail actually thins, and NOT starving
 * owner-splits once the tail re-densifies. It also pins the floor/clamp interaction those unit tests
 * miss (a clamp that clears the OLD span floor but must be refused by the observed-mass floor) and the
 * in-bounds invariant of the clamped pivot.
 *
 * <p>Disjoint from {@code OwnerSplitChildMassFloorTest} / {@code OwnerSplitReflectClampTest}. Fully
 * deterministic — pure arithmetic plus the zero-I/O EWMA digest, no engine, no clock, no threads.
 */
final class ObservedMassFloorContractTest {

    private static final int MAX_KEYS = 100;
    /** A dense drainer's far-ahead fraction (WorkerState MAX_FAR_FRACTION): the child owns the top quarter. */
    private static final double F = 0.75;
    /** A large remaining mass — a genuinely honest split on a UNIFORM tail, never floored. */
    private static final double BIG_EST = 100_000.0;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ============================================================================================
    // The floor tracks a real EWMA transition — dense head allows, thinning tail blocks.
    // ============================================================================================

    @Test
    void noDensitySignalNeverBlocksAnHonestSplit() {
        // A fresh victim (no page recorded) has no density basis: observedDensityRatio == +infinity, so
        // reach clamps to 1 and the floor reduces to the old span floor — a large remaining mass is
        // ALWAYS allowed (the "never block on an absent signal" fallback).
        WorkerState fresh = WorkerStates.of(1, b("a"), b("a"), b("z"));
        assertThat(fresh.observedDensityRatio())
                .as("no page recorded ⇒ +infinity fallback").isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(StealMath.childTailBelowObservedMassFloor(BIG_EST, F, fresh.observedDensityRatio(), MAX_KEYS))
                .as("no signal ⇒ honest split not floored").isFalse();
    }

    @Test
    void denseHeadAllowsThenTheThinningTailBlocksTheSameSplit() {
        // Overall (average) density is FIXED for this victim: cursor near the top, a modest emitted count.
        // Only the TRAILING EWMA moves, via recorded pages — so this isolates the EWMA transition driving
        // the floor. A dense recent page ⇒ ratio > 1 ⇒ the honest split is ALLOWED; then a long run of
        // sparse pages decays the EWMA below the average ⇒ ratio < f ⇒ the SAME split is now BLOCKED
        // (the confetti the old span floor would have passed on this skewed tail).
        WorkerState w = WorkerStates.of(2, b("a"), b("y"), b("z"));
        w.addKeysEmitted(200);   // fixes overall = emitted / consumed-span

        // Dense head: one dense recent page seeds the EWMA high (region ahead denser than average).
        w.recordPage(b("m"), b("n"), 500);
        double denseRatio = w.observedDensityRatio();
        assertThat(denseRatio).as("dense recent page ⇒ ratio > 1").isGreaterThan(1.0);
        assertThat(StealMath.childTailBelowObservedMassFloor(BIG_EST, F, denseRatio, MAX_KEYS))
                .as("dense head: an honest owner-split is NOT floored").isFalse();

        // The tail thins: a long run of sparse pages decays the trailing EWMA far below the average.
        for (int i = 0; i < 60; i++) {
            w.recordPage(b("m"), b("x"), 3);
        }
        double thinRatio = w.observedDensityRatio();
        assertThat(thinRatio).as("the tail thinned ⇒ ratio decayed below f").isLessThan(F);
        assertThat(StealMath.childTailBelowObservedMassFloor(BIG_EST, F, thinRatio, MAX_KEYS))
                .as("thinning tail: the same split is now BLOCKED (confetti guard)").isTrue();
    }

    @Test
    void thinTailThenReDensifiesDoesNotStarveOwnerSplits() {
        // The inverse: a tail that thins first (floor blocks) but then re-densifies must NOT stay blocked
        // — the ratio recovers as the EWMA climbs back, so owner-splits resume (no permanent starvation
        // after a transient sparse stretch, the failure mode a one-way latch would cause).
        WorkerState w = WorkerStates.of(3, b("a"), b("y"), b("z"));
        w.addKeysEmitted(200);

        // Thin first: sparse recent pages ⇒ ratio < f ⇒ blocked.
        for (int i = 0; i < 20; i++) {
            w.recordPage(b("m"), b("x"), 3);
        }
        double thinRatio = w.observedDensityRatio();
        assertThat(thinRatio).as("sparse stretch ⇒ ratio < f").isLessThan(F);
        assertThat(StealMath.childTailBelowObservedMassFloor(BIG_EST, F, thinRatio, MAX_KEYS))
                .as("thin stretch: blocked").isTrue();

        // Re-densify: dense recent pages climb the EWMA back above the average.
        for (int i = 0; i < 20; i++) {
            w.recordPage(b("m"), b("n"), 500);
        }
        double recoveredRatio = w.observedDensityRatio();
        assertThat(recoveredRatio).as("densification recovers the ratio above 1").isGreaterThan(1.0);
        assertThat(StealMath.childTailBelowObservedMassFloor(BIG_EST, F, recoveredRatio, MAX_KEYS))
                .as("re-densified tail: owner-splits are NOT starved (unblocked)").isFalse();
    }

    // ============================================================================================
    // The floor/clamp interaction — a clamp that clears the OLD span floor must be refused by the
    // observed-mass floor.
    // ============================================================================================

    @Test
    void clampThatClearsTheOldSpanFloorIsRefusedByTheObservedMassFloor() {
        // A real overshoot geometry (cursor < m_r < m, using the proven unit-test byte layout). est is huge
        // so the OLD span floor (== the ratio>=1 branch, byte-for-byte) PASSES and the clamp would fire.
        // But with a thinning tail (ratio 0.1 < f_reflect) the observed-mass floor collapses the clamped
        // child tail to ~0 and REFUSES the clamp — so the clamp defers to the observed-mass floor, never
        // carving reflected vacuum.
        byte[] lo = b("d/00");
        byte[] cursor = b("d/02");
        byte[] m = b("d/08");          // f-interpolated pivot (overshoots)
        byte[] mReflect = b("d/04");   // reflected pivot, strictly in (cursor, m)
        byte[] H = b("d/09");

        assertThat(KeyBytes.compareUnsigned(cursor, mReflect)).as("cursor < m_r").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(mReflect, m)).as("m_r < m (a real overshoot)").isLessThan(0);

        // ratio == 1.0 is byte-for-byte the OLD span floor: with a huge est the clamp is allowed.
        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, H, BIG_EST, 1.0, MAX_KEYS))
                .as("clears the OLD span floor (ratio>=1) ⇒ clamp would fire").isTrue();
        // Same geometry + est, but a thinning tail: the observed-mass floor REFUSES the clamp.
        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, H, BIG_EST, 0.1, MAX_KEYS))
                .as("thinning tail (ratio < f_reflect) ⇒ observed-mass floor refuses the clamp").isFalse();
    }

    @Test
    void clampedPivotNeverLandsOutsideTheOwnerRange() {
        // Whenever the clamp fires, the committed pivot becomes m_r — which must stay strictly inside the
        // owner's live (cursor, H]. Sweep several overshoot geometries (uniform ratio so the floor never
        // refuses on mass grounds) and assert every clamp-true case keeps cursor <_u m_r <_u H.
        byte[] lo = b("d/00");
        byte[] cursor = b("d/02");
        byte[] H = b("d/09");
        String[][] geometries = {
                {"d/04", "d/08"}, {"d/03", "d/07"}, {"d/05", "d/08"}, {"d/04", "d/06"},
        };
        int clamps = 0;
        for (String[] g : geometries) {
            byte[] mReflect = b(g[0]);
            byte[] m = b(g[1]);
            boolean clamp = StealMath.shouldClampToReflected(cursor, m, mReflect, lo, H, BIG_EST, 1.0, MAX_KEYS);
            if (clamp) {
                clamps++;
                assertThat(KeyBytes.compareUnsigned(cursor, mReflect))
                        .as("clamp fired ⇒ cursor <_u m_r for %s", g[0]).isLessThan(0);
                assertThat(KeyBytes.compareUnsigned(mReflect, H))
                        .as("clamp fired ⇒ m_r <_u H for %s (never outside the owner range)", g[0]).isLessThan(0);
            }
        }
        assertThat(clamps).as("at least one geometry actually clamped (the invariant is not vacuous)")
                .isGreaterThanOrEqualTo(1);
    }
}
