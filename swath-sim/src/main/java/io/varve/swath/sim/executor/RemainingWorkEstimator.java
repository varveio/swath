/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

/**
 * Extends the core remaining-work seam with the simulator's two degeneracy classifications.
 * Implementations are pure. Keys use raw unsigned order; {@code null} is ⊥ for a lower bound or
 * cursor and an open frontier for an upper bound.
 */
interface RemainingWorkEstimator extends io.varve.swath.engine.RemainingWorkEstimator {

    /** Whether a bounded-range reading discards the emitted-key count. */
    boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi);

    /** Whether an emitting page commit moved this estimator's sense of position. */
    boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi);

    // Byte-window arithmetic shared by the window-based readings.

    /** Precision of the engine's window-relative fraction, in bytes. */
    int WINDOW_BYTES = 12;

    /** The ⊥ sentinel as bytes. */
    byte[] BOTTOM = new byte[0];

    /** Maps {@code null} (⊥) to the empty byte array. */
    static byte[] orBottom(byte[] key) {
        return key == null ? BOTTOM : key;
    }

    /** Longest common-prefix length; {@code null} is empty. */
    static int commonPrefixLen(byte[] a, byte[] b) {
        if (a == null || b == null) {
            return 0;
        }
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * Base-256 fraction from {@code offset}, over at most {@link #WINDOW_BYTES} bytes. This deliberate
     * local copy is needed because core exposes the operation only at {@code fracIn}'s fixed anchor.
     */
    static double fracFromOffset(byte[] key, int offset) {
        if (key == null || key.length <= offset) {
            return 0.0;
        }
        double f = 0.0;
        double scale = 1.0 / 256.0;
        int limit = Math.min(key.length, offset + WINDOW_BYTES);
        for (int i = offset; i < limit; i++) {
            f += (key[i] & 0xFF) * scale;
            scale /= 256.0;
        }
        return f;
    }
}
