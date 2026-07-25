/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.observability.RunMetrics;

/**
 * Shared pacing for the idle thief fleet. <b>At most one</b> speculative steal attempt is in
 * flight fleet-wide, and that bound is strict: the slot is owned by the acquiring worker and
 * released only by it, in a {@code finally} covering the whole acquired region
 * ({@code WorkStealingScan#nextClaim}). Concurrent steals would stay safe via
 * {@code victim.lock()} + the CAS split guard, but they are not efficient — thieves race for the
 * same argmax victim and lose the CAS — so the bound is a throughput property, not a safety one.
 *
 * <p><b>Pacing state and slot ownership are separate.</b> {@link #reset()} clears the *pacing*
 * state (backoff level, next-attempt instant) and is called by unrelated workers on every ordinary
 * claim and every non-empty page commit; it must never clear {@code attemptInFlight}, or those
 * high-frequency callers would hand the slot away under a running attempt and the fleet-wide bound
 * would degrade to {@code 1 + (reset rate x probe duration)} — tens in flight under load.
 *
 * <p>Consecutive non-productive outcomes exponentially space the next attempt for the whole fleet,
 * while a created child, claimed work, or a non-empty page commit resets that spacing immediately.
 * A worker denied the slot parks on the seconds-scale {@code attemptParkNanos} backstop rather than
 * the ~5 ms base: the release broadcasts on the ledger, so the backstop is what bounds the wait for
 * an attempt that outlives it (or, rarely, for a signal that found no one parked yet) — not the
 * mechanism that ends an ordinary wait. Polling at the base interval instead is pure denial churn.
 * Enqueue/decrement signals still wake parked workers, so this never delays quiescence detection.
 */
final class IdleStealBackoff {
    /** {@code recordStealReason} category for the two attempt-slot denial regimes (§5). */
    private static final String DENIAL_CATEGORY = "IDLE_SLOT";

    private final long baseNanos;
    private final long capNanos;
    private final long attemptParkNanos;
    private final RunMetrics metrics;
    private int consecutiveNonProductive;
    private long nextAttemptNanos;
    private boolean attemptInFlight;

    IdleStealBackoff(long baseNanos, long capNanos, long attemptParkNanos, RunMetrics metrics) {
        this.baseNanos = baseNanos;
        this.capNanos = capNanos;
        this.attemptParkNanos = attemptParkNanos;
        this.metrics = metrics;
    }

    synchronized boolean tryAcquireAttemptSlot() {
        if (attemptInFlight) {
            deny("in_flight");
            return false;
        }
        if (System.nanoTime() < nextAttemptNanos) {
            deny("paced");
            return false;
        }
        attemptInFlight = true;
        return true;
    }

    /**
     * A denial and <b>which of the two regimes</b> caused it (§5 engagement idiom): {@code
     * in_flight} — another worker owns the slot, so this one waits for the release signal on the
     * seconds-scale backstop; {@code paced} — the slot is free but a non-productive streak has the
     * fleet in exponential backoff. The aggregate {@code slot_denied} counter cannot separate them,
     * and they call for opposite responses (wait vs. nothing to wait for), so post-hoc analysis
     * needs the split to tell "the bound is holding" from "the fleet is backed off".
     */
    private void deny(String reason) {
        metrics.recordIdleBackoffSlotDenied();
        metrics.recordStealReason(DENIAL_CATEGORY, reason);
    }

    /** Whether the sole attempt slot is currently owned — the CONC guards' observation point. */
    synchronized boolean attemptInFlight() {
        return attemptInFlight;
    }

    /**
     * Release the in-flight slot. <b>Only the worker that acquired it may call this</b>, and it must
     * do so exactly once, from a {@code finally} covering everything it did while holding the slot —
     * any escape (including an {@code Error} out of metrics, logging or child enqueue) would
     * otherwise leave {@code attemptInFlight} set forever and disable stealing for the whole run.
     *
     * <p>The caller signals the ledger <b>after</b> this returns and <b>outside</b> any monitor:
     * {@code Worklist.park} holds its gate while evaluating {@link #parkNanos()} (which takes this
     * monitor), so signalling from under this monitor would invert gate&harr;backoff. That same gate
     * hold is why the release cannot be lost: a parked worker either reads the cleared flag in
     * {@code parkNanos} and parks briefly, or is already awaiting when the signal lands.
     */
    synchronized void releaseSlot() {
        attemptInFlight = false;
    }

    synchronized void recordNonProductive() {
        int shift = Math.min(consecutiveNonProductive, 16);
        consecutiveNonProductive++;
        long delay = Math.min(capNanos, baseNanos << shift);
        nextAttemptNanos = System.nanoTime() + delay;
        metrics.setIdleBackoffLevel(consecutiveNonProductive);
    }

    /**
     * Clear the pacing state so the next eligible worker may attempt immediately. Called by
     * <b>unrelated</b> workers on every ordinary claim and every non-empty page commit, so it
     * deliberately does <b>not</b> touch {@code attemptInFlight} — that is the attempt owner's,
     * released via {@link #releaseSlot()}.
     */
    synchronized void reset() {
        // Only a genuine recovery-from-backoff (level was already >0) counts as a "reset" —
        // this is also called on every ordinary claim (§ nextClaim), which would otherwise
        // dominate the counter with routine churn and drown the signal.
        if (consecutiveNonProductive > 0) {
            metrics.recordIdleBackoffReset();
        }
        consecutiveNonProductive = 0;
        nextAttemptNanos = 0L;
        metrics.setIdleBackoffLevel(0);
    }

    synchronized long parkNanos() {
        if (attemptInFlight) {
            // Nothing this worker can do until the owner releases, and the release signals the
            // ledger — so wait on that signal, not on a ~5 ms poll that can only re-deny.
            return attemptParkNanos;
        }
        long remaining = nextAttemptNanos - System.nanoTime();
        if (remaining <= 0L) {
            return baseNanos;
        }
        return remaining;
    }
}
