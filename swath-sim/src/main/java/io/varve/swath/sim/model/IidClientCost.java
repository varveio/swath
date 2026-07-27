/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;

/**
 * The independent form: a page's client-side cost is paid on the timeline of whichever actor
 * received it, in parallel with every other actor's. Two pages arriving at once cost one page's
 * worth of elapsed time.
 *
 * <p>See {@link ClientCostModel} for when this form is the right one and what it gets wrong when it
 * is not.
 */
public record IidClientCost(ClientCostTerm term) implements ClientCostModel {

    /** The event kind a charge is traced under. */
    public static final String CHARGE_EVENT = "client_cost.iid";

    public IidClientCost {
        if (term == null) {
            throw new MissingSimDependencyException("client cost term (per-page client service cost)");
        }
    }

    @Override
    public void chargePage(SimContext ctx, int keys, SimAction onComplete) {
        long cost = term.costNanos(keys);
        ctx.count(CHARGES_COUNTER, 1);
        ctx.count(NANOS_COUNTER, cost);
        ctx.schedule(cost, CHARGE_EVENT, onComplete);
    }
}
