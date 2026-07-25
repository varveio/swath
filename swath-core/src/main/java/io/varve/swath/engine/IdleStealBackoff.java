/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.observability.RunMetrics;

/**
 * Shared pacing for the idle thief fleet. <b>At most one</b> speculative steal attempt per
 * backoff window (best-effort — a progress reset can briefly let a second through; concurrent
 * steals stay safe via {@code victim.lock()} + the CAS split guard). Consecutive non-productive
 * outcomes exponentially space the next probe for the whole fleet, while a created child,
 * claimed work, or a non-empty page commit resets immediately. Enqueue/decrement signals still
 * wake parked workers, so this never delays quiescence detection.
 */
final class IdleStealBackoff {
    private final long baseNanos;
    private final long capNanos;
    private final RunMetrics metrics;
    private int consecutiveNonProductive;
    private long nextAttemptNanos;
    private boolean attemptInFlight;

    IdleStealBackoff(long baseNanos, long capNanos, RunMetrics metrics) {
        this.baseNanos = baseNanos;
        this.capNanos = capNanos;
        this.metrics = metrics;
    }

    synchronized boolean tryAcquireAttemptSlot() {
        long now = System.nanoTime();
        if (attemptInFlight || now < nextAttemptNanos) {
            return false;
        }
        attemptInFlight = true;
        return true;
    }

    synchronized void recordNonProductive() {
        int shift = Math.min(consecutiveNonProductive, 16);
        consecutiveNonProductive++;
        long delay = Math.min(capNanos, baseNanos << shift);
        nextAttemptNanos = System.nanoTime() + delay;
        attemptInFlight = false;
        metrics.setIdleBackoffLevel(consecutiveNonProductive);
    }

    synchronized void reset() {
        // Only a genuine recovery-from-backoff (level was already >0) counts as a "reset" —
        // this is also called on every ordinary claim (§ nextClaim), which would otherwise
        // dominate the counter with routine churn and drown the signal.
        if (consecutiveNonProductive > 0) {
            metrics.recordIdleBackoffReset();
        }
        consecutiveNonProductive = 0;
        nextAttemptNanos = 0L;
        attemptInFlight = false;
        metrics.setIdleBackoffLevel(0);
    }

    synchronized long parkNanos() {
        if (attemptInFlight) {
            return baseNanos;
        }
        long remaining = nextAttemptNanos - System.nanoTime();
        if (remaining <= 0L) {
            return baseNanos;
        }
        return remaining;
    }
}
