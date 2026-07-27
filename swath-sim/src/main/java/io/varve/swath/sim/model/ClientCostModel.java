/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;

/**
 * How a {@link ClientCostTerm} is actually paid: the model FORM, as distinct from the term's
 * magnitude.
 *
 * <p>The first two forms are the two answers to "is this cost private to a page, or shared between all
 * pages in flight?"; the third is the answer measurement actually gave, which is "both, in named
 * stages".
 *
 * <ul>
 *   <li>{@link IidClientCost} — each page pays its own cost on its own timeline. Right when the work
 *       is genuinely per-page and parallel: N workers each doing their own parsing on their own
 *       core.</li>
 *   <li>{@link ContendedClientCost} — every page queues for a shared resource. Right when the work
 *       funnels through something serial: a single consumer stage, one writer thread, one database
 *       connection.</li>
 *   <li>{@link CompositeClientCost} — a parallel per-page term in series with a serial writer and a
 *       serial consumer stage, plus an optional parallel offload pool. Right because that is what the
 *       spans measure: one form for the whole client would have to be wrong about one of the stages,
 *       and which stage it was wrong about would decide which policies the simulator prefers.</li>
 * </ul>
 *
 * <p>They are not interchangeable, and the difference is not a second-order correction. Under the
 * first form a burst of pages costs one page's time; under the second it costs the sum. A policy
 * that produces bursty arrivals is therefore rewarded by one and penalised by the other, and no
 * amount of retuning the term's magnitude converts one into the other — a form fitted to a single
 * run's aggregate can reproduce that run exactly and still rank two policies backwards. Choosing
 * between them is a measurement question, which is why both are expressible here rather than one
 * being assumed.
 */
public sealed interface ClientCostModel permits IidClientCost, ContendedClientCost, CompositeClientCost {

    /**
     * Counter naming the number of pages charged. Declared once here, and not per form, because a
     * run's counters are compared <em>across</em> the two forms — which is the whole point of having
     * both — and a name that drifted in one of them would silently compare nothing.
     */
    String CHARGES_COUNTER = "client_cost.charges";

    /** Counter naming the total client-side nanoseconds charged. See {@link #CHARGES_COUNTER}. */
    String NANOS_COUNTER = "client_cost.nanos";

    /** The term this form is paying out, including its provenance. */
    ClientCostTerm term();

    /**
     * Charges the client-side cost of one arrived page of {@code keys} keys, then runs
     * {@code onComplete} for the same actor at whatever instant the charge finishes.
     *
     * <p>The charge is always paid through the schedule, even when it is zero: an implementation that
     * short-circuited a zero cost by running the continuation inline would make it atomic with the
     * caller's current event, quietly changing which interleavings are possible. The cost of a
     * zero-cost charge is one event, and that is a price worth paying for the semantics not moving
     * when a term goes to zero.
     */
    void chargePage(SimContext ctx, int keys, SimAction onComplete);

    /**
     * Throws if this model still holds state from an earlier run. A stateless form has nothing to
     * check; a form backed by a shared resource does, because a run stopped early by a duration or
     * event ceiling leaves that resource mid-service, and silently carrying it into the next run
     * would make the next run's result depend on the previous one's — the one thing a reproducible
     * simulator may not do.
     */
    default void requireReadyForNewRun() {
    }
}
