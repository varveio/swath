/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The fleet-wide idle-steal backoff's PACING arithmetic (algorithms.md §3 / {@code
 * IdleStealBackoff}'s javadoc), extracted from it: consecutive non-productive steal attempts
 * exponentially space the next attempt for the whole fleet, reset immediately by a created child,
 * claimed work, or a non-empty page commit. Pure function of an {@link IdleStealPacingState} and an
 * injected {@link DecisionClock} read — no lock, no {@code RunMetrics}, no field of its own.
 *
 * <p><b>The one-attempt SLOT does not move.</b> {@code IdleStealBackoff} still owns {@code
 * attemptInFlight}'s ownership/release/ledger-broadcast mechanics entirely; this class only ever
 * decides the PACING half of {@code tryAcquireAttemptSlot()}/{@code parkNanos()} — the exponential
 * window a free slot may still be paced behind.
 */
public final class IdleStealPacingPolicy {

    private final long baseNanos;
    private final long capNanos;

    /**
     * @param baseNanos the un-paced park interval, and the floor of the exponential ladder
     * @param capNanos  the ladder's cap — the longest the fleet is ever paced between attempts
     */
    public IdleStealPacingPolicy(long baseNanos, long capNanos) {
        this.baseNanos = baseNanos;
        this.capNanos = capNanos;
    }

    /** Whether {@code state}'s pacing window has elapsed as of {@code nowNanos}. */
    public IdleStealPacingDecision decide(IdleStealPacingState state, long nowNanos) {
        return nowNanos < state.nextAttemptNanos() ? IdleStealPacingDecision.PACED : IdleStealPacingDecision.ELIGIBLE;
    }

    /**
     * One more non-productive steal attempt fleet-wide: grow the exponential window (capped) and arm
     * it from {@code nowNanos}.
     */
    public IdleStealPacingState onNonProductive(IdleStealPacingState state, long nowNanos) {
        int shift = Math.min(state.consecutiveNonProductive(), 16);
        // Saturate BEFORE capping. `baseNanos << shift` wraps to a NEGATIVE delay once
        // `baseNanos` exceeds 2^(63-shift), and `Math.min(capNanos, negative)` then returns the
        // negative — arming `nextAttemptNanos` in the PAST, so an attempt the ladder meant to pace
        // becomes immediately eligible and the backoff silently stops backing off. The engine's own
        // constants (5ms base, 50ms cap) are ~25 bits clear of that, so this is unreachable in
        // production today; it is guarded because this type is public and a simulator supplies its
        // own. Behaviour is bit-identical for every input that does not overflow.
        long grown = shift < Long.numberOfLeadingZeros(baseNanos) ? baseNanos << shift : Long.MAX_VALUE;
        long delay = Math.min(capNanos, grown);
        return new IdleStealPacingState(state.consecutiveNonProductive() + 1, nowNanos + delay);
    }

    /** A created child, claimed work, or a non-empty page commit: clear the pacing window entirely. */
    public IdleStealPacingState onReset() {
        return IdleStealPacingState.INITIAL;
    }

    /**
     * How long an idle worker should park before re-evaluating, given {@code state} and {@code
     * nowNanos} — the un-paced base once the window has elapsed, the remaining window otherwise.
     * Does not account for the in-flight slot backstop; {@code IdleStealBackoff#parkNanos()} applies
     * that executor-only branch on top of this.
     */
    public long parkNanos(IdleStealPacingState state, long nowNanos) {
        long remaining = state.nextAttemptNanos() - nowNanos;
        return remaining <= 0L ? baseNanos : remaining;
    }
}
