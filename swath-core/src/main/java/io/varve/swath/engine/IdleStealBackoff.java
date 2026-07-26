/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.DecisionClock;
import io.varve.swath.engine.policy.IdleStealPacingDecision;
import io.varve.swath.engine.policy.IdleStealPacingPolicy;
import io.varve.swath.engine.policy.IdleStealPacingState;
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
 * an attempt that outlives it — not the mechanism that ends an ordinary wait, and not merely a
 * lost-signal fallback. Polling at the base interval instead is pure denial churn.
 * Enqueue/decrement signals still wake parked workers, so this never delays quiescence detection.
 *
 * <p><b>The pacing arithmetic (growth/reset/park-remaining) is {@code
 * io.varve.swath.engine.policy}'s {@link IdleStealPacingPolicy}</b>, a pure function of an {@link
 * IdleStealPacingState} and an injected {@link DecisionClock} read — the SAME {@code
 * System::nanoTime} the engine always read, just no longer read from inside the policy itself
 * (mirrors {@code DecisionRng}'s treatment of randomness). The fleet-wide one-attempt SLOT
 * ({@code attemptInFlight}, its ownership/release, and this class's own {@link RunMetrics}
 * reference) stays right here — executor infrastructure, never a policy concern.
 *
 * <p><b>Why the pacing state is one combined record here, unlike {@code WorkerState}'s per-victim
 * futility counters.</b> {@code consecutiveNonProductive}/{@code nextAttemptNanos} used to be plain
 * (non-atomic) fields, and every method that touches them is already {@code synchronized} on this
 * instance — the monitor, not field-level atomicity, is what makes their combined read-then-write
 * safe. Collapsing them into one immutable {@link IdleStealPacingState}, read and replaced as a
 * whole inside the SAME synchronized method that used to touch the two fields directly, changes
 * nothing about that guarantee. Contrast {@code WorkerState}'s futility-pacing counters, which are
 * lock-free {@code AtomicInteger}s two racing thieves can touch with no shared monitor at all — a
 * combined snapshot there would trade away the per-field atomicity those fields currently rely on
 * for a compound one they never had, so that extraction stays per-field (see {@code
 * FutilityPacingPolicy}'s javadoc). The rule: <b>monitor-protected state may be combined into one
 * record; lock-free per-field atomics may not.</b> Every read and write of {@link #pacingState}
 * below stays inside this class's existing {@code synchronized} methods — it never escapes the
 * monitor, so it is never a torn-read hazard the plain fields it replaced didn't already avoid by
 * the same lock.
 */
final class IdleStealBackoff {
    private final long attemptParkNanos;
    private final RunMetrics metrics;
    private final IdleStealPacingPolicy pacingPolicy;
    private final DecisionClock clock;
    private IdleStealPacingState pacingState = IdleStealPacingState.INITIAL;
    private boolean attemptInFlight;

    IdleStealBackoff(long baseNanos, long capNanos, long attemptParkNanos, RunMetrics metrics) {
        this(baseNanos, capNanos, attemptParkNanos, metrics, System::nanoTime);
    }

    /**
     * As the 4-arg constructor, but with a caller-supplied {@link DecisionClock} instead of the
     * engine's live ambient {@code System::nanoTime} default — for tests (and a future simulator)
     * that need a controllable clock. The 4-arg constructor delegates here with {@code
     * System::nanoTime}, the SAME ambient source the engine read before this fix — deliberately zero
     * behavior delta for every live run; only where the ambient-ness lives (an executor concern now,
     * not inside the policy package) changed.
     */
    IdleStealBackoff(long baseNanos, long capNanos, long attemptParkNanos, RunMetrics metrics, DecisionClock clock) {
        this.attemptParkNanos = attemptParkNanos;
        this.metrics = metrics;
        this.pacingPolicy = new IdleStealPacingPolicy(baseNanos, capNanos);
        this.clock = clock;
    }

    synchronized boolean tryAcquireAttemptSlot() {
        if (attemptInFlight) {
            deny("in_flight");
            return false;
        }
        if (pacingPolicy.decide(pacingState, clock.nanoTime()) == IdleStealPacingDecision.PACED) {
            deny("paced");
            return false;
        }
        attemptInFlight = true;
        return true;
    }

    /**
     * A denial and <b>which of the two regimes</b> caused it (§5 engagement idiom): {@code
     * in_flight} — another worker owns the slot, and a release is coming; {@code paced} — the slot
     * is free but a non-productive streak has the fleet in exponential backoff, so there is no
     * release to wait for, only a window that elapses or is {@link #reset()} early. The aggregate
     * {@code slot_denied} counter cannot separate them, so post-hoc analysis needs the split to
     * tell "the bound is holding" from "the fleet is backed off".
     */
    private void deny(String reason) {
        metrics.recordIdleBackoffSlotDenied();
        // Category inlined, not hoisted to a constant: check-instrumentation-drift.py resolves only
        // literal arguments, and a name it cannot resolve degrades the §5a registry check to a
        // human-review warning for this pair.
        metrics.recordStealReason("IDLE_SLOT", reason);
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
        pacingState = pacingPolicy.onNonProductive(pacingState, clock.nanoTime());
        metrics.setIdleBackoffLevel(pacingState.consecutiveNonProductive());
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
        if (pacingState.consecutiveNonProductive() > 0) {
            metrics.recordIdleBackoffReset();
        }
        pacingState = pacingPolicy.onReset();
        metrics.setIdleBackoffLevel(0);
    }

    synchronized long parkNanos() {
        if (attemptInFlight) {
            // Nothing this worker can do until the owner releases, and the release signals the
            // ledger — so wait on that signal, not on a ~5 ms poll that can only re-deny.
            return attemptParkNanos;
        }
        return pacingPolicy.parkNanos(pacingState, clock.nanoTime());
    }
}
