/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * The {@code --engine-toggle tail_floor} arms of the owner-split observed-mass child-tail floor
 * ({@link StealMath#childTailBelowObservedMassFloor}), pinned against the disease they were cut for
 * and against the shapes the shipped window term is right about.
 *
 * <p><b>The measured profile.</b> On the `nara` tail (one range, node 256, the wide-flat
 * {@code tiff/1950census/} subtree) every one of 5,326 owner-split attempts ended
 * {@code floor_reflected_blocked} with: {@code est} 322,500 → 1,653,750 keys (the promoted
 * rate-anchored sensor's honest reading), {@code densityRatio} 0.0002–0.0008 (median 0.0003),
 * {@code f} pinned at ~0.5, page size 1,000. {@code min(1, densityRatio) − f} is then structurally
 * negative, so the shipped floor's realized-child-mass term is {@code est × 0} — the estimate never
 * reaches the comparison. {@link #naraTailProfileIsRefusedByCurrentAndAdmittedByBothArms} is that
 * profile as a regression test: it fails against the pre-cure behavior on both arms.
 *
 * <p>Pure arithmetic, exercised directly like {@code OwnerSplitChildMassFloorTest} (which pins the
 * shipped {@code current} mode's own boundaries and stays the guard for it). Ordinary unit guards
 * of the arms' arithmetic; the cross-cutting tiling properties are unaffected by any mode — the
 * modes decide only WHETHER an owner carves, never where.
 */
final class OwnerSplitTailFloorModeTest {

    /** The live run's page size on the measured tail (`--max-keys` default). */
    private static final int NARA_MAX_KEYS = 1_000;
    /** The lowest honest estimate seen across the 5,326 refused attempts. */
    private static final double NARA_EST_LOW = 322_500.0;
    /** The highest. */
    private static final double NARA_EST_HIGH = 1_653_750.0;
    /** The median trailing-density ratio over the same attempts. */
    private static final double NARA_DENSITY_RATIO = 0.0003;
    /** The far-ahead fraction, pinned by the EWMA at ~0.5 throughout the tail. */
    private static final double NARA_F = 0.5;

    private static final int MAX_KEYS = 100;

    // -------------------------------------------------------------------------------------------
    // The measured disease.
    // -------------------------------------------------------------------------------------------

    @Test
    void naraTailProfileIsRefusedByCurrentAndAdmittedByBothArms() {
        for (double est : new double[] {NARA_EST_LOW, NARA_EST_HIGH}) {
            assertThat(StealMath.childTailBelowObservedMassFloor(est, NARA_F, NARA_DENSITY_RATIO, NARA_MAX_KEYS,
                    TailFloorMode.CURRENT))
                    .as("current: the window term zeroes est=%s and refuses the carve (5,326/5,326)", est)
                    .isTrue();
            assertThat(StealMath.childTailBelowObservedMassFloor(est, NARA_F, NARA_DENSITY_RATIO, NARA_MAX_KEYS,
                    TailFloorMode.EST_DIRECT))
                    .as("est_direct: est=%s is 322x the two-page floor, so the tail is carved", est)
                    .isFalse();
            assertThat(StealMath.childTailBelowObservedMassFloor(est, NARA_F, NARA_DENSITY_RATIO, NARA_MAX_KEYS,
                    TailFloorMode.REACH_FLOORED))
                    .as("reach_floored: est=%s x 1/16 still clears two pages, so the tail is carved", est)
                    .isFalse();
        }
    }

    @Test
    void theShippedFloorsRealizedMassOnTheMeasuredProfileIsExactlyZero() {
        // The structural zero itself, stated as arithmetic: the estimate is irrelevant under current.
        assertThat(Math.max(0.0, Math.min(1.0, NARA_DENSITY_RATIO) - NARA_F))
                .as("min(1, densityRatio) - f is negative on a wide-flat tail, so the reach term is 0")
                .isZero();
    }

    // -------------------------------------------------------------------------------------------
    // Shapes the window term is RIGHT about: an honestly thin tail stays blocked under every mode.
    // -------------------------------------------------------------------------------------------

    @Test
    void aGenuinelyTinyTailIsBlockedByEveryMode() {
        // Healthy density (uniform, ratio=1.0 >= f=0.5) and est=150: the range holds less than two
        // pages in total, so every mode must refuse -- current 75 <= 200, est_direct 150 <= 200,
        // reach_floored 75 <= 200 (its max() picks the real reach 0.5, not the 1/16 floor).
        for (TailFloorMode mode : TailFloorMode.values()) {
            assertThat(StealMath.childTailBelowObservedMassFloor(150.0, 0.5, 1.0, MAX_KEYS, mode))
                    .as("%s blocks a range holding under two pages in total", mode)
                    .isTrue();
        }
    }

    @Test
    void estDirectDeliberatelyAdmitsAThinFarShareWhoseTotalMassClearsTwoPages() {
        // The arm's disclosed cost, pinned rather than left implicit: est=300, ratio=1.0, f=0.5,
        // maxKeys=100. The child's honest share is 150 (under two pages) so current and
        // reach_floored refuse; est_direct reads only the sensor's 300 and carves.
        assertThat(StealMath.childTailBelowObservedMassFloor(300.0, 0.5, 1.0, MAX_KEYS, TailFloorMode.CURRENT))
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(300.0, 0.5, 1.0, MAX_KEYS,
                TailFloorMode.REACH_FLOORED))
                .as("reach_floored keeps the window product here (reach-f = 0.5 > 1/16): 150 <= 200")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(300.0, 0.5, 1.0, MAX_KEYS, TailFloorMode.EST_DIRECT))
                .as("est_direct drops the geometry entirely, so a two-to-four-page range is carved")
                .isFalse();
    }

    // -------------------------------------------------------------------------------------------
    // Where the two arms disagree -- the band the race is actually about.
    // -------------------------------------------------------------------------------------------

    @Test
    void theTwoArmsSplitInTheMidBandBetweenTwoAndThirtyTwoPages() {
        // est = 10,000 = 10 pages of proven mass, thinning tail (ratio 0.3 < f 0.75), maxKeys 1000.
        double est = 10_000.0;
        double f = 0.75;
        double ratio = 0.30;
        int maxKeys = 1_000;

        assertThat(StealMath.childTailBelowObservedMassFloor(est, f, ratio, maxKeys, TailFloorMode.CURRENT))
                .as("current: reach term 0 -> blocked")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(est, f, ratio, maxKeys, TailFloorMode.EST_DIRECT))
                .as("est_direct: 10,000 > 2,000 -> carved (it reads only the sensor)")
                .isFalse();
        assertThat(StealMath.childTailBelowObservedMassFloor(est, f, ratio, maxKeys, TailFloorMode.REACH_FLOORED))
                .as("reach_floored: 10,000/16 = 625 <= 2,000 -> still blocked (it keeps a geometry haircut)")
                .isTrue();
    }

    // -------------------------------------------------------------------------------------------
    // Boundaries.
    // -------------------------------------------------------------------------------------------

    @Test
    void estDirectBoundaryIsExactlyTwoPagesInclusive() {
        double twoPages = 2.0 * MAX_KEYS;
        assertThat(StealMath.childTailBelowObservedMassFloor(twoPages, NARA_F, NARA_DENSITY_RATIO, MAX_KEYS,
                TailFloorMode.EST_DIRECT))
                .as("est == 2*maxKeys is floored (inclusive, matching the shipped comparison)")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(Math.nextUp(twoPages), NARA_F, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.EST_DIRECT))
                .as("the very next double above 2*maxKeys is admitted")
                .isFalse();
    }

    @Test
    void reachFlooredBoundaryIsThirtyTwoPagesInclusive() {
        // est/16 <= 2*maxKeys  <=>  est <= 32*maxKeys. maxKeys=100 -> 3200.
        assertThat(StealMath.childTailBelowObservedMassFloor(3_200.0, NARA_F, NARA_DENSITY_RATIO, MAX_KEYS,
                TailFloorMode.REACH_FLOORED))
                .as("est == 32*maxKeys is floored (3200/16 == 200 == the two-page floor)")
                .isTrue();
        assertThat(StealMath.childTailBelowObservedMassFloor(3_216.0, NARA_F, NARA_DENSITY_RATIO, MAX_KEYS,
                TailFloorMode.REACH_FLOORED))
                .as("one page of estimate above it (3216/16 == 201) is admitted")
                .isFalse();
    }

    @Test
    void reachFlooredIsTheShippedFloorWheneverTheReachTermClearsTheFloor() {
        // densityRatio > f by more than TAIL_REACH_MIN: the max() picks the real reach, so the arm is
        // byte-for-byte `current` -- including at current's own boundary (est=400, f=0.5, maxKeys=100).
        for (double densityRatio : new double[] {1.0, 3.0, Double.POSITIVE_INFINITY, 0.7}) {
            for (double est : new double[] {400.0, 402.0, 100_000.0}) {
                assertThat(StealMath.childTailBelowObservedMassFloor(est, 0.5, densityRatio, MAX_KEYS,
                        TailFloorMode.REACH_FLOORED))
                        .as("reach_floored == current at ratio=%s, est=%s (reach-f >= 1/16)", densityRatio, est)
                        .isEqualTo(StealMath.childTailBelowObservedMassFloor(est, 0.5, densityRatio, MAX_KEYS,
                                TailFloorMode.CURRENT));
            }
        }
    }

    @Test
    void zeroOrNegativeEstIsFlooredUnderEveryMode() {
        for (TailFloorMode mode : TailFloorMode.values()) {
            assertThat(StealMath.childTailBelowObservedMassFloor(0.0, 0.5, 1.0, MAX_KEYS, mode))
                    .as("%s floors a zero estimate", mode).isTrue();
            assertThat(StealMath.childTailBelowObservedMassFloor(-50.0, 0.5, 0.0003, MAX_KEYS, mode))
                    .as("%s floors a negative estimate (no arm may turn it into a carve)", mode).isTrue();
        }
    }

    @Test
    void theDefaultArityIsTheCurrentMode() {
        // The plain 4-arg overload every shipped-behavior guard calls must stay `current`, so the
        // toggle's default-off promise is one delegation, not a second copy of the formula.
        for (double ratio : new double[] {0.0003, 0.7, 1.0, Double.POSITIVE_INFINITY}) {
            for (double est : new double[] {0.0, 400.0, 3_200.0, 100_000.0}) {
                assertThat(StealMath.childTailBelowObservedMassFloor(est, 0.5, ratio, MAX_KEYS))
                        .as("4-arg == current at ratio=%s est=%s", ratio, est)
                        .isEqualTo(StealMath.childTailBelowObservedMassFloor(est, 0.5, ratio, MAX_KEYS,
                                TailFloorMode.CURRENT));
            }
        }
    }

    /**
     * The monotonicity the governor's {@code TAIL_FLOOR.*_would_block_current_admits} counters exist
     * to falsify on a live run: neither arm may refuse a carve the shipped floor admits. Swept over
     * the realistic input box (f is a density fraction in [0,1]; densityRatio spans the wide-flat
     * regime through the no-signal fallback).
     */
    @Test
    void neitherArmEverBlocksACarveTheShippedFloorAdmits() {
        double[] ests = {-1.0, 0.0, 1.0, 199.0, 200.0, 201.0, 3_200.0, 100_000.0, 1_653_750.0};
        double[] fractions = {0.0, 0.3, 0.5, 0.75, 1.0};
        double[] ratios = {0.0, 0.0003, 0.3, 0.5, 0.7, 1.0, 3.0, Double.POSITIVE_INFINITY};
        for (double est : ests) {
            for (double f : fractions) {
                for (double ratio : ratios) {
                    if (StealMath.childTailBelowObservedMassFloor(est, f, ratio, MAX_KEYS,
                            TailFloorMode.CURRENT)) {
                        continue;   // current blocks: the arms are free to block or admit
                    }
                    for (TailFloorMode arm : new TailFloorMode[] {
                            TailFloorMode.EST_DIRECT, TailFloorMode.REACH_FLOORED}) {
                        assertThat(StealMath.childTailBelowObservedMassFloor(est, f, ratio, MAX_KEYS, arm))
                                .as("%s must not block what current admits (est=%s f=%s ratio=%s)",
                                        arm, est, f, ratio)
                                .isFalse();
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // The mode reaches the two reflected-pivot consults, not just the gate.
    // -------------------------------------------------------------------------------------------

    @Test
    void theReflectionClampReadsTheFloorAtTheSelectedMode() {
        byte[] lo = "d/00".getBytes(StandardCharsets.UTF_8);
        byte[] cursor = "d/02".getBytes(StandardCharsets.UTF_8);
        byte[] m = "d/08".getBytes(StandardCharsets.UTF_8);
        byte[] mReflect = "d/04".getBytes(StandardCharsets.UTF_8);
        byte[] hi = "d/09".getBytes(StandardCharsets.UTF_8);
        double est = 100_000.0;

        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.CURRENT))
                .as("current: the clamped tail's window term is zero on a wide-flat ratio -> no clamp")
                .isFalse();
        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.EST_DIRECT))
                .as("est_direct: the clamped tail clears the floor -> clamp into the mass")
                .isTrue();
        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.REACH_FLOORED))
                .as("reach_floored: 100,000/16 clears two pages -> clamp into the mass")
                .isTrue();
        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS))
                .as("the mode-less arity stays the shipped reading")
                .isFalse();
    }

    @Test
    void theReflectLiftReadsTheFloorAtTheSelectedMode() {
        byte[] lo = "d/00".getBytes(StandardCharsets.UTF_8);
        byte[] cursorTo = "d/02".getBytes(StandardCharsets.UTF_8);
        byte[] m = "d/03".getBytes(StandardCharsets.UTF_8);
        byte[] mReflect = "d/06".getBytes(StandardCharsets.UTF_8);
        byte[] hi = "d/09".getBytes(StandardCharsets.UTF_8);
        // Kept share (d/02, d/03] is 1/7 of the remaining span; est*1/7 <= maxKeys keeps condition 1
        // open, so only the floor (condition 4) can differ between modes.
        double est = 700.0;

        assertThat(StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.CURRENT))
                .as("current: the lifted tail's window term is zero -> no lift")
                .isFalse();
        assertThat(StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS, TailFloorMode.EST_DIRECT))
                .as("est_direct: est=700 > 2*maxKeys=200 -> the lifted carve is allowed")
                .isTrue();
        assertThat(StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, hi, est, NARA_DENSITY_RATIO,
                MAX_KEYS))
                .as("the mode-less arity stays the shipped reading")
                .isFalse();
    }

    /**
     * {@link StealMath#TAIL_REACH_MIN} is a tuned, load-bearing constant: pinned by LITERAL so an
     * accidental edit fails a build instead of silently retuning the arm (the same discipline
     * {@code OwnerSplitGovernorTest} applies to the gate-chain thresholds).
     */
    @Test
    void theReachFloorConstantIsPinnedToItsLiteralValue() {
        assertThat(StealMath.TAIL_REACH_MIN).isEqualTo(0.0625);
    }
}
