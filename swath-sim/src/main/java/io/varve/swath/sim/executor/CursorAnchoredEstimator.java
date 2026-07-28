/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.StealMath;
import io.varve.swath.model.KeyBytes;

/**
 * <b>E2 — anchor the measurement window where the cursor actually moves.</b> The incumbent reads a
 * range's position over the bytes that follow the divergence of {@code lo} and {@code hi}. On a
 * deep-nested keyspace the cursor travels many bytes below that divergence, inside a subtree both
 * bounds share, so the window it is read in does not change and the consumed span reads zero. This
 * variant re-anchors the window at the cursor's <em>own</em> divergence from {@code lo}.
 *
 * <h2>The frame, and why it is closed</h2>
 * Let {@code d0 = cpl(lo, hi)} (the incumbent's anchor) and {@code d = cpl(lo, cursor)}. For a cursor
 * inside {@code (lo, hi]} we always have {@code d >= d0}: a cursor that left {@code lo}'s prefix
 * before {@code hi} does would already be past {@code hi}. Two cases follow, and they are the whole
 * of this class:
 *
 * <ul>
 *   <li><b>{@code d == d0} — the cursor diverges from {@code lo} exactly where {@code hi} does.</b>
 *       The re-anchoring is a no-op and every term below is the incumbent's, digit for digit. <b>That
 *       is the whole of the identity: this variant is byte-identical to the shipped estimator exactly
 *       on {@code cpl(lo, cursor) == cpl(lo, hi)}, and not one byte wider.</b> The shipped window can
 *       still <em>see</em> a cursor for any {@code d} below {@code d0 + }{@link
 *       RemainingWorkEstimator#WINDOW_BYTES} — its consumed span is positive there, so those ranges are
 *       not the degenerate case — and across that strictly wider set the two readings diverge, by more
 *       than an order of magnitude at a single byte past {@code d0} (4.1e7 against 3.1e6 on the worked
 *       example), because each divides by a span measured at a different depth. Whether a
 *       healthy keyspace is out of this variant's blast radius is therefore a measurement and not a
 *       construction: both regression-guard fixtures hold it at 4 of 4 seeds, which is what the race
 *       reports.</li>
 *   <li><b>{@code d > d0} — the cursor has descended into a subtree shared with {@code lo}.</b> The
 *       window is read from byte {@code d} instead, and its ceiling is the top of the {@code d}-byte
 *       prefix that {@code lo} and the cursor share. That ceiling is <em>inside</em> {@code [lo, hi]}
 *       — every key under a prefix that agrees with {@code lo} at byte {@code d0} is below {@code hi},
 *       which diverges upward there — so the frame is closed and both terms are ordinary fractions in
 *       {@code [0, 1]}. In this frame the estimate reads "at the rate this range has been consuming
 *       the subtree its cursor is in, how much of that subtree is left". <b>Because that ceiling is
 *       the constant 1.0, this frame's geometric factor drops below one only for a cursor whose own
 *       divergence from {@code lo} is on a byte at or above {@code 0x80}</b> — on the printable bytes
 *       object keys diverge on it lifts and never cuts, which is what {@code SensingEstimatorTest}
 *       pins and what leaves a floor conditioned on this branch with nothing to act on.</li>
 * </ul>
 *
 * <h2>What it gives up, stated rather than hidden</h2>
 * <b>The estimate is local in the deep case.</b> It measures the subtree the cursor is in and not the
 * rest of {@code (cursor, hi]} above it, so it under-states the work on a range whose cursor is
 * finishing one heavy subtree with more subtrees to come. The alternative — carrying the two
 * divergences in one frame with their scales — makes the estimate a density extrapolation across
 * dozens of byte positions, which is arithmetically valid and produces numbers like 10^60 keys, and
 * the owner-side floors compare the estimate against multiples of a page. A local reading that stays
 * calibrated was preferred to a global one that cannot be compared to anything.
 *
 * <p><b>Re-anchoring buys a non-zero consumed span, not unlimited resolution.</b> The window is still
 * {@link RemainingWorkEstimator#WINDOW_BYTES} bytes wide, so once the cursor is travelling <em>more</em>
 * than that far below its own divergence from {@code lo} — inside a directory several path segments
 * deeper — one page's advance stops moving the position digits again. What re-anchoring fixes
 * unconditionally is the reading that discards a range's emitted keys: the consumed span is positive
 * whenever the cursor has left {@code lo} at all, so the density term engages and the estimate
 * responds to what the range has produced even where the position digits have stalled. Recovering
 * per-page resolution at arbitrary depth needs a second window at the cursor's own depth, which is a
 * different candidate.
 */
final class CursorAnchoredEstimator implements RemainingWorkEstimator {

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;
        }
        byte[] cur = RemainingWorkEstimator.orBottom(cursor);
        if (KeyBytes.compareUnsigned(cur, RemainingWorkEstimator.orBottom(lo)) <= 0) {
            // No consumed evidence yet — rank by width alone, exactly as the incumbent does, and in
            // the incumbent's own window: there is no cursor divergence to anchor anything at.
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
            return true;   // the width-only branch above, which is where the emitted keys are dropped
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
            return true;   // the cursor changed which window it is read in, which is a move by itself
        }
        return RemainingWorkEstimator.fracFromOffset(cursorTo, after)
                - RemainingWorkEstimator.fracFromOffset(cursorFrom, after) > 0.0;
    }

    @Override
    public String toString() {
        return "anchored";
    }

    /** The window offset this range's cursor is read at: its own divergence from {@code lo}. */
    private static int anchor(byte[] cursor, byte[] lo) {
        return RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo), cursor);
    }

    /** The consumed span {@code (lo, cursor]} in the anchored frame. */
    private static double consumedIn(byte[] cursor, byte[] lo, byte[] hi) {
        int d = anchor(cursor, lo);
        return RemainingWorkEstimator.fracFromOffset(cursor, d)
                - RemainingWorkEstimator.fracFromOffset(lo, d);
    }

    /**
     * The remaining span {@code (cursor, top]} in the anchored frame, where {@code top} is {@code hi}
     * itself when the anchor did not move, and the top of the shared {@code d}-byte prefix — value 1.0
     * in a frame read from byte {@code d} — when it did.
     */
    private static double remainingIn(byte[] cursor, byte[] lo, byte[] hi) {
        int d = anchor(cursor, lo);
        int d0 = RemainingWorkEstimator.commonPrefixLen(RemainingWorkEstimator.orBottom(lo), hi);
        double top = (d <= d0) ? RemainingWorkEstimator.fracFromOffset(hi, d) : 1.0;
        return Math.max(0.0, top - RemainingWorkEstimator.fracFromOffset(cursor, d));
    }
}
