/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.StealMath;

/**
 * Legacy WINDOW control, delegated to core {@link StealMath}: local density times remaining span in
 * the range-bound window. A zero estimate removes a bounded candidate from selection; a zero consumed
 * span falls back to raw width, discarding emitted mass. Cursor advances within one fraction reading
 * are invisible.
 */
final class WindowEstimator implements RemainingWorkEstimator {

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        return StealMath.estRemaining(cursor, lo, hi, keysEmitted);
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        return StealMath.spanIn(lo, cursor, lo, hi) <= 0.0;
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        return StealMath.fracIn(cursorTo, lo, hi) - StealMath.fracIn(cursorFrom, lo, hi) > 0.0;
    }

    @Override
    public String toString() {
        return "current";
    }
}
