/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Boundary + skew coverage for {@code WorkStealingScan}'s owner-split OBSERVED-MASS child-tail
 * floor — the redesign of the 2-page span floor. The span-based estimate {@code (1-f)*est}
 * over-passes on skewed keyspaces whose tail thins out (a large span estimate over a thinning tail
 * lets tiny confetti children slip through). The redesign corrects the tail with the worker's
 * observed local-vs-average density ratio: {@code >= 1} (uniform / still-dense, incl. the
 * no-signal fallback) is byte-for-byte the old floor so honest splits are unaffected; {@code < f}
 * (a thinning tail) blocks the confetti.
 *
 * <p>{@link StealMath#childTailBelowObservedMassFloor} is exercised directly (pure arithmetic,
 * package-private) so the exact math is pinned without driving the whole engine to a precise input
 * combination. This is an ordinary unit guard of the arithmetic; the PROP-1/RES-3/CONC
 * cross-cutting interleavings are covered separately.
 */
final class OwnerSplitChildMassFloorTest {

    private static final int MAX_KEYS = 100;
    private static final double FLOOR = 2.0 * MAX_KEYS;   // 200
    /** A uniform / still-dense region: trailing EWMA >= average ⇒ the floor is exactly (1-f)*est. */
    private static final double UNIFORM = 1.0;
    /** The no-density-signal fallback the engine passes (also clamps to reach=1, i.e. the old floor). */
    private static final double NO_SIGNAL = Double.POSITIVE_INFINITY;

    // -------------------------------------------------------------------------------------------
    // Uniform / still-dense (ratio >= 1): reduces byte-for-byte to the old (1-f)*est span floor.
    // -------------------------------------------------------------------------------------------

    @Test
    void uniformReducesToTheSpanFloorAtTheBoundary() {
        // f=0.5, est=400 -> (1-f)*est = 200 == 2*maxKeys exactly; "<=" boundary is floored (unchanged).
        assertThat(StealMath.childTailBelowObservedMassFloor(400.0, 0.5, UNIFORM, MAX_KEYS))
                .as("uniform == 2*maxKeys is floored (boundary inclusive, old behavior)")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(402.0, 0.5, UNIFORM, MAX_KEYS))
                .as("uniform just above the boundary is allowed")
                .isFalse();
    }

    @Test
    void noSignalFallbackIsAlsoTheOldSpanFloor() {
        // +infinity density ratio (no EWMA signal yet) clamps reach to 1 -> identical to (1-f)*est.
        assertThat(StealMath.childTailBelowObservedMassFloor(400.0, 0.5, NO_SIGNAL, MAX_KEYS)).isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(1_000_000.0, 0.5, NO_SIGNAL, MAX_KEYS))
                .as("a large est with no signal is never floored (fallback)")
                .isFalse();
    }

    @Test
    void uniformLargeRemainderIsNeverFloored() {
        assertThat(StealMath.childTailBelowObservedMassFloor(1_000_000.0, 0.5, UNIFORM, MAX_KEYS))
                .as("a large uniform remaining mass is never floored (pmc / dense-flat honest split)")
                .isFalse();
    }

    @Test
    void uniformFarAheadFractionShrinksTheChildShareTowardTheFloor() {
        // f=0.75 (a uniformly-dense drainer): the child gets only the top quarter. est=800 -> 0.25*800 =
        // 200 == floor -> floored, even though est is double the f=0.5 boundary case. Unchanged from old.
        assertThat(StealMath.childTailBelowObservedMassFloor(800.0, 0.75, UNIFORM, MAX_KEYS))
                .as("far-ahead 0.75: est*0.25 == floor is floored")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(804.0, 0.75, UNIFORM, MAX_KEYS))
                .as("just above the far-ahead-adjusted boundary is allowed")
                .isFalse();
    }

    @Test
    void aboveUniformRatioStillClampsToTheOldFloor() {
        // ratio > 1 (region ahead DENSER than average) clamps to reach=1 — never inflates the child share
        // above the honest span floor, so it can neither over- nor under-carve relative to (1-f)*est.
        assertThat(StealMath.childTailBelowObservedMassFloor(400.0, 0.5, 3.0, MAX_KEYS))
                .as("ratio > 1 clamps to reach=1 (== the old floor)")
                .isTrue();
    }

    // -------------------------------------------------------------------------------------------
    // Thinning tail (ratio < 1): blocks confetti the span floor would pass.
    // -------------------------------------------------------------------------------------------

    @Test
    void thinningTailIsBlockedEvenThoughTheSpanFloorPassesIt() {
        // A huge span estimate (dense drained head over a wide code-point span) but the tail is
        // thinning hard — trailing density only 0.30 of the drained average.
        double est = 100_000.0;
        double f = 0.75;
        double densityRatio = 0.30;

        // The OLD span floor PASSES this (tiny confetti slips through):
        assertThat((1.0 - f) * est)
                .as("span floor (1-f)*est is far above the floor — the old floor over-passes")
                .isGreaterThan(FLOOR);
        // The observed-mass floor BLOCKS it: reach=min(1,0.30)=0.30 < f=0.75 -> realized child mass 0.
        assertThat(StealMath.childTailBelowObservedMassFloor(est, f, densityRatio, MAX_KEYS))
                .as("thinning tail (ratio < f) blocks the confetti child the span floor would pass")
                .isTrue();
    }

    @Test
    void mildlyThinningTailThatStillClearsTwoPagesIsAllowed() {
        // ratio=0.7, f=0.5: reach-f = 0.2. est=2000 -> realized = 400 > 200 -> allowed. A tail that
        // thins only mildly and still carries two pages of observed mass is NOT blocked.
        assertThat(StealMath.childTailBelowObservedMassFloor(2000.0, 0.5, 0.7, MAX_KEYS))
                .as("a mildly-thinning tail that still clears two pages of observed mass is allowed")
                .isFalse();
        // ...but the same ratio with a small est is blocked: est=800 -> 0.2*800 = 160 < 200.
        assertThat(StealMath.childTailBelowObservedMassFloor(800.0, 0.5, 0.7, MAX_KEYS))
                .as("the same thinning tail with too little mass is blocked")
                .isTrue();
    }

    @Test
    void zeroOrNegativeEstIsFloored() {
        assertThat(StealMath.childTailBelowObservedMassFloor(0.0, 0.5, UNIFORM, MAX_KEYS))
                .as("zero remaining mass is floored")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(-50.0, 0.5, UNIFORM, MAX_KEYS))
                .as("negative remaining mass is floored")
                .isTrue();
    }
}
