/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.model.KeyBytes;

/**
 * Estimates a bounded range as {@code max(keysEmitted, pageSize)} without inferring position from key
 * shape. An open frontier is infinite and an exact {@code cursor >= hi} is zero. Emitted mass is the
 * position signal, so it is never ignored and every emitting commit reports a visible advance.
 */
final class RateEstimator implements RemainingWorkEstimator {

    private final int pageSize;

    /** @param pageSize no-evidence floor for a range with no emitted keys */
    RateEstimator(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;
        }
        if (KeyBytes.compareUnsigned(RemainingWorkEstimator.orBottom(cursor), hi) >= 0) {
            return 0.0;   // Exact bound, not a key-shape inference.
        }
        return Math.max(keysEmitted, pageSize);
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        return false;   // Emitted mass is the estimate.
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        // Called only for an emitting commit, which changes the measured mass.
        return true;
    }

    @Override
    public String toString() {
        return "rate";
    }
}
