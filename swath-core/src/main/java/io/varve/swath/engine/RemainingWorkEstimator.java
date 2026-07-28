/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.Engagement;
import java.util.List;

/**
 * <b>The engine's position sensor, behind a seam.</b> Victim choice, the owner-side self-split and
 * its pivot mass floors all steer on one quantity — estimated remaining work on a range — and this
 * interface is that quantity, so which arithmetic a run steers on is a run-time choice
 * ({@link EngineToggles#remainingWorkEstimator}) rather than a hard-wired call.
 *
 * <p>{@link #WINDOW} is the shipped reading and the default: {@link StealMath#estRemaining} itself,
 * expressed through this seam rather than re-implemented behind it, so selecting the default costs
 * nothing and cannot drift from the shipped arithmetic by construction.
 *
 * <p>Implementations are <b>pure functions of their arguments</b>, exactly as {@link StealMath} is:
 * no clock, no randomness, no state carried between calls, and no allocation on the estimate call
 * itself — this is scan-hot (every scored victim, every qualifying page commit). Keys are raw bytes
 * in S3-lexicographic order, {@code null} meaning ⊥ for a lower bound or a cursor and the open
 * frontier for an upper one, which are the policy views' own conventions.
 */
public interface RemainingWorkEstimator {

    /**
     * Estimated remaining work in {@code (cursor, hi]}, in keys, for a range {@code [lo, hi]} that
     * has emitted {@code keysEmitted} keys. Contract shared with {@link StealMath#estRemaining}: an
     * open frontier ({@code hi == null}) scores {@link Double#POSITIVE_INFINITY}, and a non-positive
     * score takes a candidate out of victim selection entirely.
     */
    double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted);

    /**
     * Report what this estimator's reading of {@code [lo, hi]} <em>did</em> — the cheap
     * keyspace-classification signal AGENTS.md requires of every algo path — into {@code collector},
     * the caller's own pending-{@link Engagement} list (never {@code RunMetrics} directly,
     * contracts.md §2.1). Called once per decision rather than once per scored candidate, and only
     * where the caller already holds such a list.
     *
     * <p>{@code category} is the CALLER's, because the two decision sites count different things: an
     * owner-gate consult happens once per qualifying page commit and a steal classification once per
     * attempt that found a winner, so pooling them into one namespace would leave every ratio drawn
     * from these counters without an interpretable denominator. Pass {@code "SENSING_OWNER"} from the
     * owner-split gate chain and {@code "SENSING_STEAL"} from victim selection (metrics-internals.md
     * §5a carries a row per reason under each).
     *
     * <p>The default reports nothing, which is what {@link #WINDOW} wants: the shipped reading is the
     * baseline every existing counter is already measured against, and adding a counter to it would
     * change every run that has ever existed.
     */
    default void classify(String category, byte[] cursor, byte[] lo, byte[] hi, long keysEmitted,
                          List<Engagement> collector) {
    }

    /**
     * The shipped reading: local density times remaining span, both measured over the byte window
     * anchored at the divergence of the range's own bounds ({@link StealMath#estRemaining},
     * algorithms.md §3.2). The default of every construction path, and the only supported one.
     */
    RemainingWorkEstimator WINDOW = StealMath::estRemaining;
}
