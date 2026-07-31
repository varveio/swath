/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.FifoServer;
import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;

/**
 * Charges pages through a finite-capacity FIFO shared by the run's actors.
 *
 * <p>When no request waits, it agrees with {@link IidClientCost}; queueing models bursty arrivals.
 * Run entry points call {@link #requireReadyForNewRun()} to reject FIFO residue from an earlier run.
 */
public final class ContendedClientCost implements ClientCostModel {

    /** Prefix for this resource's counters and events. */
    public static final String RESOURCE_NAME = "client_cost.server";

    private final ClientCostTerm term;
    private final FifoServer server;

    /**
     * @param term per-page and per-key service time
     * @param capacity concurrent pages; {@code 1} is serial
     */
    public ContendedClientCost(ClientCostTerm term, int capacity) {
        if (term == null) {
            throw new MissingSimDependencyException("client cost term (per-page client service cost)");
        }
        this.term = term;
        this.server = new FifoServer(RESOURCE_NAME, capacity);
    }

    @Override
    public ClientCostTerm term() {
        return term;
    }

    @Override
    public void chargePage(SimContext ctx, int keys, SimAction onComplete) {
        long cost = term.costNanos(keys);
        ctx.count(CHARGES_COUNTER, 1);
        ctx.count(NANOS_COUNTER, cost);
        server.submit(ctx, cost, onComplete);
    }

    @Override
    public void requireReadyForNewRun() {
        if (!server.isIdle()) {
            throw new IllegalStateException("this " + RESOURCE_NAME + " still holds work from an earlier "
                    + "run (" + server.queueDepth() + " queued); a contended resource belongs to one run");
        }
    }

    /** Current queued-page count. */
    public int queueDepth() {
        return server.queueDepth();
    }
}
