/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.model.KeyBytes;

/**
 * <b>E1 and E2 together: a rate estimate that geometry may adjust but not overrule.</b> The two cures
 * differ by exactly one factor — the incumbent and {@link CursorAnchoredEstimator} both read
 * {@code keysEmitted × remaining / consumed}, and {@link RateEstimator} is that with the geometric
 * factor forced to 1 — so combining them means keeping the rate estimate as the magnitude and letting
 * the anchored geometry modulate it within a stated band.
 *
 * <p>{@link #GEOMETRY_BAND} is a chosen constant, not a derived one: it says the measured position may
 * move the estimate by at most a factor of sixteen. It is stated here so it can be read as the tunable
 * it is — the natural thing to sweep if this variant is worth keeping.
 *
 * <h2>The band's lower half is a separate decision, and it has a name</h2>
 * {@code minGeometry} is where the band stops <em>cutting</em> the rate estimate, and it is a
 * constructor argument rather than a constant because the two settings are two candidates raced
 * against each other, not a default and a tweak:
 * <ul>
 *   <li>{@link #SYMMETRIC_MIN_GEOMETRY} — geometry may move the estimate by sixteen either way. A
 *       range's proven mass can therefore read as a sixteenth of itself, which is what an estimate
 *       compared against multiples of a page has to be read carefully as: a range that has emitted
 *       sixty-four pages then scores four.</li>
 *   <li>{@link #LIFT_ONLY_MIN_GEOMETRY} — geometry may lift the estimate and not cut it. The rate
 *       half's own thesis is that emitted mass is a <b>lower</b> bound on remaining mass under a
 *       heavy-tailed size law, so a geometric factor below one asserts the opposite of the evidence
 *       the estimator is built on. The exact bound test below still scores a finished range zero, so
 *       what this drops is the <em>inferred</em> shortfall, not the measured one.</li>
 *   <li>{@link #EIGHTH_MIN_GEOMETRY}, {@link #QUARTER_MIN_GEOMETRY}, {@link #HALF_MIN_GEOMETRY} —
 *       the ladder between those two ends. Removing the whole lower half discards the inferred
 *       shortfall <em>and</em> the measured one, and only the first of those is the estimator's own
 *       under-statement; a floor part-way down keeps a cut the measured shortfall can still express
 *       while refusing the deep cuts the inferred one needs. Which of them, if any, separates the two
 *       is a fact about real keyspaces, so they are arms of a sweep rather than a default.</li>
 * </ul>
 *
 * <p>What a floor decides, in the units the gate consuming it uses: a range clears the owner's
 * admission floor once its emitted mass reaches {@code SELF_SPLIT_MIN_REMAINING_PAGES / minGeometry}
 * pages, so the ladder moves that boundary as the floor's reciprocal — sixty-four pages under the
 * symmetric band, four under the lift-only one.
 */
final class RateAnchoredEstimator implements RemainingWorkEstimator {

    /** How far the anchored geometry may lift the rate estimate. */
    static final double GEOMETRY_BAND = 16.0;

    /** The band read symmetrically: geometry may cut the estimate as far as it may lift it. */
    static final double SYMMETRIC_MIN_GEOMETRY = 1.0 / GEOMETRY_BAND;

    /** The band read upwards only: geometry may lift a range's proven mass but never cut it. */
    static final double LIFT_ONLY_MIN_GEOMETRY = 1.0;

    /** Interior floor: geometry may cut a range's proven mass by eight, and no further. */
    static final double EIGHTH_MIN_GEOMETRY = 1.0 / 8.0;

    /** Interior floor: geometry may cut a range's proven mass by four, and no further. */
    static final double QUARTER_MIN_GEOMETRY = 1.0 / 4.0;

    /** Interior floor: geometry may cut a range's proven mass in half, and no further. */
    static final double HALF_MIN_GEOMETRY = 1.0 / 2.0;

    private final int pageSize;
    private final double minGeometry;

    /**
     * @param pageSize    the no-evidence floor, exactly as {@link RateEstimator} uses it
     * @param minGeometry how far geometry may cut the rate estimate — one of the two settled ends,
     *                    {@link #SYMMETRIC_MIN_GEOMETRY} or {@link #LIFT_ONLY_MIN_GEOMETRY}, or one of
     *                    the interior floors between them
     */
    RateAnchoredEstimator(int pageSize, double minGeometry) {
        this.pageSize = pageSize;
        this.minGeometry = minGeometry;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;
        }
        if (KeyBytes.compareUnsigned(RemainingWorkEstimator.orBottom(cursor), hi) >= 0) {
            return 0.0;
        }
        double geometry = CursorAnchoredEstimator.geometricFactor(cursor, lo, hi);
        double banded = Math.min(GEOMETRY_BAND, Math.max(minGeometry, geometry));
        return Math.max(keysEmitted, pageSize) * banded;
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
