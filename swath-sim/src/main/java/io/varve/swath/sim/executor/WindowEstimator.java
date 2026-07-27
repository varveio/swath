/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.StealMath;

/**
 * The incumbent: local density times remaining span, both measured over the byte window anchored at
 * the divergence of a range's own bounds. Every method delegates to the engine's public arithmetic,
 * so a control leg is the shipped algorithm and not a re-implementation of it that could drift.
 *
 * <p>Its two degenerate readings are the ones the position-sensor work named: a zero estimate, which
 * takes a candidate out of selection; and a zero consumed span, which leaves the estimate a raw width
 * with the range's emitted keys discarded.
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
