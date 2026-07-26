/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Per-victim futility-pacing cooldown arithmetic (algorithms.md §3.2 / {@code WorkerState}'s
 * javadoc), extracted from it: consecutive futile steal outcomes against ONE victim trip a
 * bounded-exponential cooldown of steal-selection skips, reset only by that victim's own productive
 * progress (a carve). Every method here is a pure function of a single {@code int} — never a
 * combined snapshot of {@code WorkerState}'s three counters as one value.
 *
 * <p><b>Why per-field, not a combined view.</b> {@code WorkerState} holds {@code
 * consecutiveFutileSteals}/{@code futilityTrips}/{@code stealPacingSkips} as three independent
 * {@code AtomicInteger}s, deliberately unlocked (two different thieves can race the SAME victim
 * outside any lock — see {@code Thief#commit} and the per-attempt cascade). No call site ever needs
 * a coherent snapshot of all three at once: each transition below consumes exactly one field's
 * fresh atomic {@code incrementAndGet}/{@code updateAndGet} result. Collapsing the three into one
 * {@code record} view and writing it back as a single non-atomic transition would trade that
 * per-field atomicity for a compound one the pre-extraction code never had — a real concurrency
 * behavior change, not a preserving extraction — so this stays scoped to the arithmetic each
 * {@code AtomicInteger} call already isolates.
 */
public final class FutilityPacingPolicy {

    /** Consecutive futile outcomes against one victim that trip a cooldown. */
    public static final int FUTILITY_PACE_THRESHOLD = 4;
    /** The cap on the bounded-exponential per-victim cooldown length, in steal-selection skips. */
    public static final int FUTILITY_PACE_MAX_COOLDOWN = 64;
    /** The trip/cooldown counters' reset-on-carve value: a victim that just yielded a child is not a phantom drainer. */
    public static final int RESET = 0;

    private FutilityPacingPolicy() {
    }

    /**
     * Whether {@code consecutiveFutileSteals} (already incremented by the caller's atomic
     * {@code incrementAndGet}) has reached the threshold that trips this victim's cooldown.
     */
    public static boolean trips(int consecutiveFutileSteals) {
        return consecutiveFutileSteals >= FUTILITY_PACE_THRESHOLD;
    }

    /**
     * The bounded-exponential cooldown length, in steal-selection skips, for a victim on its
     * {@code trips}-th trip (already incremented by the caller's atomic {@code incrementAndGet}).
     */
    public static int cooldownForTrips(int trips) {
        return (int) Math.min((long) FUTILITY_PACE_MAX_COOLDOWN, 1L << Math.min(trips, 20));
    }

    /** Whether {@code stealPacingSkips} still holds at least one unconsumed cooldown skip. */
    public static boolean paced(int stealPacingSkips) {
        return stealPacingSkips > 0;
    }

    /**
     * The decay step: one steal-selection pass's worth of cooldown consumed. Unconditional — a
     * negative result (consuming past zero) is treated identically to zero by {@link #paced}, so
     * this never needs to floor (see {@code WorkerState#consumePacingSkip()}'s javadoc for the
     * bounded consequence of a stale consume racing a fresh trip).
     */
    public static int decay(int stealPacingSkips) {
        return stealPacingSkips - 1;
    }
}
