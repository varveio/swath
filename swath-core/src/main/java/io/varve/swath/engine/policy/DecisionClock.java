/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The one ambient read the fleet-wide idle-steal pacing decision needs ({@link
 * IdleStealPacingPolicy}) — a policy-owned seam, not {@code System.nanoTime()} called directly
 * from inside a policy. Reaching for the wall clock directly from inside a policy would make that
 * decision unreproducible from its view alone, the same problem {@link DecisionRng} solves for the
 * structure-probe suppression recovery's random draw.
 *
 * <p>{@code IdleStealBackoff} supplies the engine's live default ({@code System::nanoTime}) —
 * deliberately the SAME ambient source the engine read before this fix, so live-run behavior is
 * unchanged. Ambient-ness becomes an executor concern outside the policy package instead of living
 * inside it. Tests and a future simulator inject a fake clock instead.
 */
@FunctionalInterface
public interface DecisionClock {

    /** The current instant, in nanoseconds, from whatever clock the executor supplies. */
    long nanoTime();
}
