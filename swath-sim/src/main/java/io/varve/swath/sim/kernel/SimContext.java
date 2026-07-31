/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import io.varve.swath.sim.model.EngineTimeBudgets;

/**
 * The sole scenario- and seed-derived ambient surface available to a {@link SimAction}. The kernel
 * reuses it and changes {@link #actorId()} on dispatch; actions must not retain the context.
 */
public interface SimContext {

    /** The current virtual instant, in nanoseconds since the run started. */
    long nowNanos();

    /** The actor this action is running on behalf of. */
    int actorId();

    /** This actor's draw tape for {@code stream} — stable across the whole run. */
    SimRng rng(SimRngStream stream);

    /** Schedules {@code action} for THIS actor, {@code delayNanos} from now. */
    void schedule(long delayNanos, String kind, SimAction action);

    /**
     * Schedules {@code action} for {@code actorId}, {@code delayNanos} from now — how one actor
     * wakes another (a released resource, a completed handoff).
     */
    void scheduleFor(int actorId, long delayNanos, String kind, SimAction action);

    /** Appends a trace entry at the current instant, attributed to the current actor. */
    void record(String kind, String detail);

    /** Adds {@code delta} to the named run counter, creating it at zero on first use. */
    void count(String counter, long delta);

    /** The engine time budgets this run declared (never inherited silently from the engine). */
    EngineTimeBudgets budgets();
}
