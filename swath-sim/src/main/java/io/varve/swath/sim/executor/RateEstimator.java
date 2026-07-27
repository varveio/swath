/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.model.KeyBytes;

/**
 * <b>E1 — stop inferring mass from key bytes.</b> A range's remaining work is estimated from what it
 * has already produced, which the engine tracks exactly, instead of from a byte-window fraction that
 * a deep-nested keyspace can make degenerate. Nothing here reads a key's <em>shape</em>: the only
 * byte comparison is the exact "has the cursor reached the bound" test, which is a fact rather than
 * an inference.
 *
 * <h2>Why {@code keysEmitted} is an estimate of what is left</h2>
 * Under a heavy-tailed size law — which is what a real archive's directories are, and what the bench
 * is built to be — the expected remaining size of an object already known to exceed {@code t} grows
 * with {@code t}: a range that has emitted a million keys and has not finished is evidence of a big
 * range, not of a nearly-finished one. Taking the estimate to be exactly {@code keysEmitted} is the
 * mean residual life of a Pareto law with exponent 2, and it is the simplest statement of "rank by
 * realized mass" that stays in key units, which is what the owner-side floors need: they compare the
 * estimate against multiples of a page.
 *
 * <h2>What it gives up, stated rather than hidden</h2>
 * It carries no notion of position. A range whose cursor is one key from its bound scores exactly as
 * high as one that has barely started, provided both have emitted the same number of keys — the
 * incumbent would score the first near zero. The exact bound test below is the only protection
 * against that, and it only fires once the cursor has actually reached {@code hi}. This is the honest
 * cost of an estimator immune to key shape, and the race is what says whether it is worth paying.
 *
 * <h2>Page count adds nothing here</h2>
 * The candidate was written as "{@code keysEmitted} / page rate". On a range drained by full pages,
 * committed pages are {@code keysEmitted} divided by the page size, so the page count carries no
 * information the emitted count does not already carry, and asking the engine to widen a view to
 * expose it would buy nothing. The page size does appear, as the floor below.
 */
final class RateEstimator implements RemainingWorkEstimator {

    private final int pageSize;

    /**
     * @param pageSize the run's page size, used as the no-evidence floor: a range that has emitted
     *                 nothing yet is presumed to hold at least the page currently in flight, so a
     *                 freshly claimed range is not scored zero and dropped from selection
     */
    RateEstimator(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;
        }
        if (KeyBytes.compareUnsigned(RemainingWorkEstimator.orBottom(cursor), hi) >= 0) {
            return 0.0;   // the cursor has reached the bound: exact, not inferred
        }
        return Math.max(keysEmitted, pageSize);
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        return false;   // the emitted count IS the estimate
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        // Called only for a commit that emitted keys, and the emitted count is what this estimator
        // measures position by, so every such commit moves it.
        return true;
    }

    @Override
    public String toString() {
        return "rate";
    }
}
