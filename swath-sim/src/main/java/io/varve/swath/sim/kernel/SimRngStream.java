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
 */
public enum SimRngStream {

    /** Draws a policy makes — the simulator's counterpart to the engine's {@code DecisionRng}. */
    DECISION,
    /** Per-call service-time draws from the {@link io.varve.swath.sim.model.LatencyModel}. */
    LATENCY,
    /** Per-page draws from the client-cost model. */
    CLIENT_COST
}
