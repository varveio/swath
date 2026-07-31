/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.StealMath;
import io.varve.swath.model.KeyBytes;

/**
 * Density-times-span estimator re-anchored at the cursor's divergence from {@code lo}. For a bounded
 * cursor strictly inside {@code (lo, hi)}, let {@code d0 = cpl(lo, hi)} and
 * {@code d = cpl(lo, cursor)}. When {@code d == d0}, its consumed and remaining terms are exactly
 * WINDOW's. When {@code d > d0}, it reads from {@code d} to the top of that shared prefix
 * ({@code 1.0} in the local frame):
 * {@code estimate = keysEmitted / consumed × remaining}.
 *
 * <p>The deeper reading is local to the cursor's subtree and can understate later subtrees before
 * {@code hi}. Its {@link RemainingWorkEstimator#WINDOW_BYTES}-byte fraction also has bounded
 * resolution, so some page advances remain invisible. It retains emitted mass only when the
 * cursor-local consumed fraction resolves positive; open-frontier, unstarted width-only, and
 * zero-consumed readings report that mass ignored. An advance is visible when it changes the anchor,
 * or when it increases the fraction in that anchor.
 */
final class CursorAnchoredEstimator implements RemainingWorkEstimator {

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;
        }
        byte[] cur = RemainingWorkEstimator.orBottom(cursor);
        if (KeyBytes.compareUnsigned(cur, RemainingWorkEstimator.orBottom(lo)) <= 0) {
            // No consumed evidence: use WINDOW's width-only reading.
            return StealMath.spanIn(cursor, hi, lo, hi);
        }
        if (KeyBytes.compareUnsigned(cur, hi) >= 0) {
            return 0.0;
        }
        double consumed = consumedIn(cur, lo, hi);
        double remaining = remainingIn(cur, lo, hi);
        if (consumed <= 0.0) {
            return remaining;
        }
        return (keysEmitted / consumed) * remaining;
    }

    @Override
    public boolean ignoresEmittedKeys(byte[] cursor, byte[] lo, byte[] hi) {
        byte[] cur = RemainingWorkEstimator.orBottom(cursor);
        if (hi == null || KeyBytes.compareUnsigned(cur, RemainingWorkEstimator.orBottom(lo)) <= 0) {
            return true;   // The width-only branch discards emitted mass.
        }
        return consumedIn(cur, lo, hi) <= 0.0;
    }

    @Override
    public boolean advanceVisible(byte[] lo, byte[] cursorFrom, byte[] cursorTo, byte[] hi) {
        int before = RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo),
                RemainingWorkEstimator.orBottom(cursorFrom));
        int after = RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo),
                RemainingWorkEstimator.orBottom(cursorTo));
        if (before != after) {
            return true;   // Changing frames is itself visible.
        }
        return RemainingWorkEstimator.fracFromOffset(cursorTo, after)
                - RemainingWorkEstimator.fracFromOffset(cursorFrom, after) > 0.0;
    }

    @Override
    public String toString() {
        return "anchored";
    }

    /** Cursor-local window offset. */
    private static int anchor(byte[] cursor, byte[] lo) {
        return RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo), cursor);
    }

    /** Consumed span {@code (lo, cursor]} in the anchored frame. */
    private static double consumedIn(byte[] cursor, byte[] lo, byte[] hi) {
        int d = anchor(cursor, lo);
        return RemainingWorkEstimator.fracFromOffset(cursor, d)
                - RemainingWorkEstimator.fracFromOffset(lo, d);
    }

    /** Remaining span to {@code hi}, or to local-prefix top ({@code 1.0}) after re-anchoring. */
    private static double remainingIn(byte[] cursor, byte[] lo, byte[] hi) {
        int d = anchor(cursor, lo);
        int d0 = RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo), hi);
        double top = (d <= d0) ? RemainingWorkEstimator.fracFromOffset(hi, d) : 1.0;
        return Math.max(0.0, top - RemainingWorkEstimator.fracFromOffset(cursor, d));
    }
}
