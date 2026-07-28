/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.RateAnchoredEstimator;

/**
 * <b>E1 and E2 together: a rate estimate that geometry may adjust but not overrule</b> — and, since
 * the corpus race promoted its quarter-floor rung, <b>the engine's own object</b>. Every reading here
 * delegates to {@link RateAnchoredEstimator} exactly as {@link WindowEstimator} delegates to the
 * engine's {@code StealMath}, so the arm a race installs is the shipped implementation rather than a
 * copy of it that could drift. What stays here is what belongs to the simulator: which rung of the
 * ladder an arm installs, and what a degenerate reading means for an estimator whose position is the
 * emitted count.
 *
 * <p>The composition itself, its band and the derivation of both are documented on
 * {@link RateAnchoredEstimator}. In short: the magnitude is the range's proven mass, the anchored
 * geometry adjusts it, and {@link RateAnchoredEstimator#GEOMETRY_BAND} says that adjustment is at most
 * a factor of sixteen.
 *
 * <h2>The band's lower half is a separate decision, and it has a name</h2>
 * {@code minGeometry} is where the band stops <em>cutting</em> the rate estimate, and it is a
 * constructor argument rather than a constant because the settings are candidates raced against each
 * other, not a default and a tweak:
 * <ul>
 *   <li>{@link #SYMMETRIC_MIN_GEOMETRY} — geometry may move the estimate by sixteen either way. A
 *       range's proven mass can therefore read as a sixteenth of itself, which is what an estimate
 *       compared against multiples of a page has to be read carefully as: a range that has emitted
 *       sixty-four pages then scores four.</li>
 *   <li>{@link #LIFT_ONLY_MIN_GEOMETRY} — geometry may lift the estimate and not cut it. The rate
 *       half's own thesis is that emitted mass is a <b>lower</b> bound on remaining mass under a
 *       heavy-tailed size law, so a geometric factor below one asserts the opposite of the evidence
 *       the estimator is built on. The exact bound test still scores a finished range zero, so what
 *       this drops is the <em>inferred</em> shortfall, not the measured one.</li>
 *   <li>{@link #EIGHTH_MIN_GEOMETRY}, {@link #QUARTER_MIN_GEOMETRY}, {@link #HALF_MIN_GEOMETRY} —
 *       the ladder between those two ends. Removing the whole lower half discards the inferred
 *       shortfall <em>and</em> the measured one, and only the first of those is the estimator's own
 *       under-statement; a floor part-way down keeps a cut the measured shortfall can still express
 *       while refusing the deep cuts the inferred one needs. <b>The quarter is the rung the corpus
 *       race promoted</b>, and this class reads its value off the engine so the arm and the shipped
 *       sensor cannot be set to different floors; the others are kept so the races that rejected them
 *       stay reproducible.</li>
 * </ul>
 *
 * <p>What a floor decides, in the units the gate consuming it uses: a range clears the owner's
 * admission floor once its emitted mass reaches {@code SELF_SPLIT_MIN_REMAINING_PAGES / minGeometry}
 * pages, so the ladder moves that boundary as the floor's reciprocal — sixty-four pages under the
 * symmetric band, four under the lift-only one, sixteen under the promoted quarter.
 */
final class RateAnchoredArm implements RemainingWorkEstimator {

    /** How far the anchored geometry may lift the rate estimate — the engine's own band. */
    static final double GEOMETRY_BAND = RateAnchoredEstimator.GEOMETRY_BAND;

    /** The band read symmetrically: geometry may cut the estimate as far as it may lift it. */
    static final double SYMMETRIC_MIN_GEOMETRY = 1.0 / GEOMETRY_BAND;

    /** The band read upwards only: geometry may lift a range's proven mass but never cut it. */
    static final double LIFT_ONLY_MIN_GEOMETRY = 1.0;

    /** Interior floor: geometry may cut a range's proven mass by eight, and no further. */
    static final double EIGHTH_MIN_GEOMETRY = 1.0 / 8.0;

    /** The promoted floor, read off the engine: geometry may cut proven mass by four, and no further. */
    static final double QUARTER_MIN_GEOMETRY = RateAnchoredEstimator.QUARTER_MIN_GEOMETRY;

    /** Interior floor: geometry may cut a range's proven mass in half, and no further. */
    static final double HALF_MIN_GEOMETRY = 1.0 / 2.0;

    private final RateAnchoredEstimator reading;
    private final double minGeometry;

    /**
     * @param pageSize    the no-evidence floor, exactly as {@link RateEstimator} uses it
     * @param minGeometry how far geometry may cut the rate estimate — the promoted
     *                    {@link #QUARTER_MIN_GEOMETRY}, one of the two settled ends, or an interior
     *                    rung of the ladder between them
     */
    RateAnchoredArm(int pageSize, double minGeometry) {
        this.reading = new RateAnchoredEstimator(pageSize, minGeometry);
        this.minGeometry = minGeometry;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        // Infinite remaining mass clears every floor built on this estimate without exception, so an
        // open-frontier range never gets an owner-side carve on any minGeometry setting, and a guard
        // fixture whose run ends on one is reading its tail fraction against a drain nothing here can
        // divide (see SensingRaceTest.theHashFannedGuardsFinalRangeAtSeed987654321IsAnUnsplitOpenFrontier).
        return reading.estRemaining(cursor, lo, hi, keysEmitted);
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        return false;   // the emitted count is the magnitude; geometry only adjusts it
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        return true;    // as in RateEstimator: the emitted count moves on every counted commit
    }

    @Override
    public String toString() {
        return minGeometry >= LIFT_ONLY_MIN_GEOMETRY
                ? "rate+anchored lift-only"
                : "rate+anchored floor 1/" + Math.round(1.0 / minGeometry);
    }
}
