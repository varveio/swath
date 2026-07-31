/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

/**
 * An atomic unit of virtual-time execution. The clock cannot advance and no other actor can run
 * during {@link #run}; separating a read from its use across actions widens that window.
 */
@FunctionalInterface
public interface SimAction {

    /** Runs this action at {@code ctx.nowNanos()}, on behalf of {@code ctx.actorId()}. */
    void run(SimContext ctx);
}
