/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.RateAnchoredEstimator;
import io.varve.swath.engine.policy.Engagement;
import java.util.List;

/**
 * Simulator arm backed by core {@link RateAnchoredEstimator}. Core supplies both the estimate and its
 * classification; this wrapper names the geometry-floor rungs used by simulator experiments. The
 * promoted quarter rung is production's default sensor since 0.2.0.
 *
 * <p>The estimate is emitted mass, with a page-size no-evidence floor, multiplied by cursor-anchored
 * geometry clamped between {@code minGeometry} and {@link #GEOMETRY_BAND}. Exact open-frontier
 * infinity and finished-range zero are delegated unchanged.
 */
final class RateAnchoredArm implements RemainingWorkEstimator {

    /** Maximum geometry lift. */
    static final double GEOMETRY_BAND = RateAnchoredEstimator.GEOMETRY_BAND;

    /** Symmetric rung: geometry may cut by the same factor that it may lift. */
    static final double SYMMETRIC_MIN_GEOMETRY = 1.0 / GEOMETRY_BAND;

    /** Lift-only rung: geometry cannot cut emitted mass. */
    static final double LIFT_ONLY_MIN_GEOMETRY = 1.0;

    /** One-eighth floor rung. */
    static final double EIGHTH_MIN_GEOMETRY = 1.0 / 8.0;

    /** Promoted production floor: geometry may cut emitted mass by at most four. */
    static final double QUARTER_MIN_GEOMETRY = RateAnchoredEstimator.QUARTER_MIN_GEOMETRY;

    /** One-half floor rung. */
    static final double HALF_MIN_GEOMETRY = 1.0 / 2.0;

    private final RateAnchoredEstimator reading;
    private final double minGeometry;

    /**
     * @param pageSize no-evidence mass floor
     * @param minGeometry minimum geometry multiplier
     */
    RateAnchoredArm(int pageSize, double minGeometry) {
        this.reading = new RateAnchoredEstimator(pageSize, minGeometry);
        this.minGeometry = minGeometry;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        // Core preserves the exact open-frontier and finished-range readings.
        return reading.estRemaining(cursor, lo, hi, keysEmitted);
    }

    @Override
    public void classify(String category, byte[] cursor, byte[] lo, byte[] hi, long keysEmitted,
                         List<Engagement> collector) {
        // Use core's classification for the same reading.
        reading.classify(category, cursor, lo, hi, keysEmitted, collector);
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        return false;   // Emitted mass is the magnitude; geometry only adjusts it.
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        return true;    // Every emitting commit changes the magnitude.
    }

    @Override
    public String toString() {
        return minGeometry >= LIFT_ONLY_MIN_GEOMETRY
                ? "rate+anchored lift-only"
                : "rate+anchored floor 1/" + Math.round(1.0 / minGeometry);
    }
}
