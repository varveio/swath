/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;

/**
 * Charges each actor independently, so client costs can overlap.
 *
 * <p>See {@link ClientCostModel} for when this model is appropriate.
 */
public record IidClientCost(ClientCostTerm term) implements ClientCostModel {

    /** Trace event for a charge. */
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
