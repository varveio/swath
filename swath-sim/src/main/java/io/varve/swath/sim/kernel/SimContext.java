/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import io.varve.swath.sim.model.EngineTimeBudgets;

/**
 * Everything a running {@link SimAction} may reach: the clock, its own draw streams, the schedule,
 * the trace, the counters, and the run's declared time budgets. It is the <b>only</b> ambient
 * surface inside the simulator, and every member of it is a function of the scenario and the seed —
 * which is what makes a run reproducible.
 *
 * <p>The instance an action receives is valid only for the duration of that call: the kernel reuses
 * one context and re-points {@link #actorId()} before each dispatch, so an action must not retain it
 * past its own body (retain the values, not the context).
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
