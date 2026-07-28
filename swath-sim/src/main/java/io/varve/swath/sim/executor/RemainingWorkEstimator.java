/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

/**
 * <b>The engine's position sensor, plus what only a simulated run asks of it.</b> The engine steers
 * victim choice, pivot placement, the owner-side self-split and the density feedback on one quantity —
 * estimated remaining work on a range — and {@link io.varve.swath.engine.RemainingWorkEstimator} is
 * that quantity behind the seam the engine already has. This interface <b>is</b> that one, widened by
 * the two degeneracy readings a race needs; a variant implemented here is therefore installed into the
 * engine's own thief and owner-split policies directly, with nothing in this module reproducing what
 * they decide.
 *
 * <p><b>What the engine owns, this interface delegates to rather than reimplements.</b> The incumbent
 * reading is {@link WindowEstimator}, which calls the engine's public {@code StealMath}; the promoted
 * rate+anchored composition is {@link RateAnchoredArm}, which calls the engine's own
 * {@code RateAnchoredEstimator} (the arm this race promoted, selectable there under
 * {@code --engine-toggle rate_anchored_sensing=on}). So a race's control leg AND its promoted arm are
 * both the shipped algorithm rather than a re-implementation of it that could drift; what is
 * implemented here is what the race rejected.
 *
 * <p><b>The two degeneracy readings are part of the interface, not of the executor.</b> A run's
 * sensor counters answer "could the sensor this run steers on see this?", so they have to be asked of
 * the estimator in use — reading the incumbent's arithmetic while a variant drives the decisions
 * would report the disease under the cure. Each implementation therefore says what a degenerate
 * reading means for <em>it</em>.
 *
 * <p>Implementations are pure functions of their arguments: no clock, no randomness, no state carried
 * between calls. Keys are raw bytes in S3-lexicographic order, {@code null} meaning ⊥ for a lower
 * bound or a cursor and the open frontier for an upper one — the same conventions the policy views
 * use.
 */
interface RemainingWorkEstimator extends io.varve.swath.engine.RemainingWorkEstimator {

    /**
     * Whether this estimator's reading of a bounded range <b>discards the keys it has emitted</b> —
     * the incumbent's zero-consumed-span branch, where a worker that emitted a million keys scores
     * identically to one that emitted none. An estimator that never consults key bytes answers
     * {@code false} unconditionally, and says so.
     */
    boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi);

    /**
     * Whether a page commit that moved the cursor from {@code cursorFrom} to {@code cursorTo} moved
     * this estimator's own sense of position at all. Called only for bounded ranges that emitted keys,
     * so an estimator whose position <em>is</em> the emitted count answers {@code true}.
     */
    boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi);

    // ---- byte-window arithmetic shared by the window-based readings ----------------------

    /** Precision in bytes for a window-relative fraction — the engine's own {@code StealMath.K}. */
    int WINDOW_BYTES = 12;

    /** The ⊥ sentinel as bytes. */
    byte[] BOTTOM = new byte[0];

    /** {@code null}-tolerant view of a key as bytes, ⊥ becoming the empty array. */
    static byte[] orBottom(byte[] key) {
        return key == null ? BOTTOM : key;
    }

    /** Length in bytes of the longest common prefix of {@code a} and {@code b}; {@code null} is empty. */
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
     * Window-relative base-256 fraction: up to {@link #WINDOW_BYTES} bytes of {@code key} read from
     * byte {@code offset}. A key shorter than {@code offset} reads 0.0. Byte-for-byte the engine's own
     * {@code StealMath} private helper, reproduced here because a variant that anchors the window
     * somewhere else needs the same digit arithmetic at a different offset, and the engine exports the
     * arithmetic only through {@code fracIn}'s fixed anchor.
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
