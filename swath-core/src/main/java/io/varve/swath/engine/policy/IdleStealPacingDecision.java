/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Whether {@link IdleStealPacingPolicy#decide} finds the fleet-wide pacing window open right now.
 * Deliberately narrower than the executor's own {@code tryAcquireAttemptSlot()} refusal: the
 * fleet-wide one-attempt SLOT ({@code attemptInFlight}) is a separate, executor-only reason to
 * refuse (the {@code in_flight} denial) that never reaches this decision at all.
 */
public enum IdleStealPacingDecision {
    /** The pacing window has elapsed (or never armed): a new attempt may proceed. */
    ELIGIBLE,
    /** A prior non-productive streak still has the fleet in exponential backoff. */
    PACED
}
