/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/**
 * The purposes an actor draws for, each with its own independent tape (see {@link SimRng}). The
 * split is what keeps one kind of randomness from perturbing another: a latency model that starts
 * drawing twice per call must not shift the decision draws a policy makes.
 *
 * <p>Ordinals participate in the seed derivation, so <b>appending</b> a constant is safe while
 * reordering or removing one silently re-tapes every run recorded before the change.
 *
 * <p>The isolation is between constants, not within one: a single stream is consumed in event order,
 * so moving when an actor reaches a draw changes the value it gets (see {@link SimRng}).
 *
 * <h2>Per-mechanism, not just per-purpose</h2>
 * The decision streams are split <b>per policy mechanism</b>, not pooled into one decision tape,
 * because ranking two variants of one mechanism against each other is what a policy sweep is for: if
 * every mechanism drew from one tape, a variant that merely changes how often the thief consults its
 * escape hatch would move the values every other mechanism sees, and the difference between the two
 * runs would no longer be attributable to the change. One tape per (actor, mechanism) makes a
 * mechanism's draw sequence independent of every other mechanism's draw count.
 *
 * <p>A mechanism whose policy draws nothing today gets <b>no</b> constant here: appending one when it
 * starts drawing is safe (ordinals only grow), whereas a constant that names a stream nothing
 * consumes is a claim about the model that no code backs. The owner-split governor and the seed
 * planner are both in that position — they are deterministic functions of their views, with no random
 * draw anywhere in either.
 */
public enum SimRngStream {

    /**
     * The generic decision tape, for a decision with no finer-grained stream of its own. Retained
     * ahead of the per-mechanism constants below because its ordinal is part of every trace recorded
     * before they existed — removing or reordering it would silently re-tape all of them.
     */
    DECISION,
    /** Per-call service-time draws from the {@link io.varve.swath.sim.model.LatencyModel}. */
    LATENCY,
    /** Per-page draws from the client-cost model. */
    CLIENT_COST,
    /**
     * The thief's own decision tape: the pivot cascade's structure-probe suppression-recovery escape
     * hatch, drawn through the engine's {@code DecisionRng} seam. One per worker actor, so a variant
     * that changes how often <em>one</em> actor consults the hatch cannot re-tape another actor's.
     */
    STEAL_DECISION,
    /**
     * The AIMD controller's shed-window jitter. Drawn on the reserved fleet actor
     * ({@code SimKernel#FLEET_ACTOR}), never on the worker that happened to trigger the window roll —
     * the gauge is one instrument for the whole fleet, and attributing its draws to a worker would
     * make them a function of the interleaving.
     */
    AIMD_JITTER
}
