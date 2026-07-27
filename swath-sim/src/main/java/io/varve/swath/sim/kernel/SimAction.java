/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/**
 * What an actor does when one of its scheduled events fires — the kernel's only unit of execution.
 *
 * <p><b>An action body is atomic in virtual time.</b> The kernel is single-threaded and never
 * interleaves two actions: the clock does not move, and no other actor runs, between the first and
 * last statement of a {@code run} call. Everything an action reads and writes it therefore reads and
 * writes at one indivisible instant. That is the whole mechanism by which a simulated executor
 * expresses a lock hold — a region other actors cannot observe half-completed is one action body —
 * and, symmetrically, by which it expresses a <em>widened</em> read window: state read in one action
 * and used in a later one is open to every event another actor manages to run in between.
 */
@FunctionalInterface
public interface SimAction {

    /** Runs this action at {@code ctx.nowNanos()}, on behalf of {@code ctx.actorId()}. */
    void run(SimContext ctx);
}
