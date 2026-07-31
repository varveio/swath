/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;

/**
 * How a {@link ClientCostTerm} is paid: independently, through a shared resource, or as measured
 * named stages. This is separate from the term's magnitude.
 */
public sealed interface ClientCostModel permits IidClientCost, ContendedClientCost, CompositeClientCost {

    /** Cross-form counter for charged pages. */
    String CHARGES_COUNTER = "client_cost.charges";

    /** Cross-form counter for charged client nanoseconds. */
    String NANOS_COUNTER = "client_cost.nanos";

    /** The charged term, including its provenance. */
    ClientCostTerm term();

    /**
     * Schedules payment for a page and then its continuation for the same actor.
     *
     * <p>Zero-cost charges also go through the schedule so they preserve normal interleavings.
     */
    void chargePage(SimContext ctx, int keys, SimAction onComplete);

    /** Stateful forms override to reject residue from an earlier run; stateless forms do nothing. */
    default void requireReadyForNewRun() {
    }
}
