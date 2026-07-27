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
 * The contended form: every page queues for a shared, finite-capacity resource, so a page's
 * client-side cost is its own service time plus however long it waited behind everyone else's. Two
 * pages arriving at once at a single-capacity resource cost two pages' worth of elapsed time.
 *
 * <p>See {@link ClientCostModel} for when this form is the right one. Note that it is <em>not</em>
 * simply "the pessimistic version" of the independent form: at low occupancy the queue is empty and
 * the two agree exactly, so choosing this form does not uniformly slow a run down — it makes the
 * cost depend on arrival burstiness, which is the property being modelled.
 *
 * <p>The resource is stateful and is shared by every actor in a run, so one instance belongs to one
 * run. Constructing a scenario with a model instance already used by another run would carry that
 * run's queue over, which the driver prevents by taking the model per run.
 */
public final class ContendedClientCost implements ClientCostModel {

    /** The resource name its counters and completion events are recorded under. */
    public static final String RESOURCE_NAME = "client_cost.server";

    private final ClientCostTerm term;
    private final FifoServer server;

    /**
     * @param term     the per-page/per-key service time of the shared resource
     * @param capacity how many pages it processes at once — {@code 1} for a single serial stage
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
        ctx.count("client_cost.charges", 1);
        ctx.count("client_cost.nanos", cost);
        server.submit(ctx, cost, onComplete);
    }

    @Override
    public void requireReadyForNewRun() {
        if (!server.isIdle()) {
            throw new IllegalStateException("this " + RESOURCE_NAME + " still holds work from an earlier "
                    + "run (" + server.queueDepth() + " queued); a contended resource belongs to one run");
        }
    }

    /** Pages waiting for the resource right now — the depth a saturation assertion reads. */
    public int queueDepth() {
        return server.queueDepth();
    }
}
