/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Fleet-wide idle-steal pacing state (one instance per run, not per victim) — {@code
 * IdleStealBackoff}'s {@code consecutiveNonProductive}/{@code nextAttemptNanos} pair, snapshotted
 * so {@link IdleStealPacingPolicy}'s exponential-backoff arithmetic is a pure function of it. Does
 * NOT carry the fleet-wide one-attempt SLOT ({@code attemptInFlight}) — that ownership/release
 * mechanism is executor infrastructure and stays in {@code IdleStealBackoff} (seam-notes.md; the
 * pacing decision moves, the slot mechanics do not).
 *
 * @param consecutiveNonProductive the run of non-productive steal attempts so far, unbroken by a
 *                                 productive split or an explicit reset
 * @param nextAttemptNanos         the {@link DecisionClock} instant before which a new attempt is
 *                                 paced
 */
public record IdleStealPacingState(int consecutiveNonProductive, long nextAttemptNanos) {

    /** The state a fresh run — or an explicit {@code reset()} — starts from. */
    public static final IdleStealPacingState INITIAL = new IdleStealPacingState(0, 0L);
}
