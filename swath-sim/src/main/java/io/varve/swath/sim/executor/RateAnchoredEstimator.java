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
 * move the estimate by at most a factor of sixteen either way. It is stated here so it can be read as
 * the tunable it is — the natural thing to sweep if this variant is worth keeping.
 */
final class RateAnchoredEstimator implements RemainingWorkEstimator {

    /** How far the anchored geometry may move the rate estimate, either way. */
    static final double GEOMETRY_BAND = 16.0;

    private final int pageSize;

    /** @param pageSize the no-evidence floor, exactly as {@link RateEstimator} uses it */
    RateAnchoredEstimator(int pageSize) {
        this.pageSize = pageSize;
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
        double banded = Math.min(GEOMETRY_BAND, Math.max(1.0 / GEOMETRY_BAND, geometry));
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
        return "rate+anchored";
    }
}
