/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * How long a store call of a given class takes. The one place service time enters the simulation, so
 * that a scenario can swap the whole latency story without touching an actor.
 *
 * <p>Three implementation shapes are anticipated, in increasing order of fidelity and of what they
 * demand from measurement:
 *
 * <ol>
 *   <li>{@link ConstantLatencyModel} — one number per class. Not a fidelity claim: it is the mode in
 *       which the kernel's analytic invariants are <em>exact</em>, which is the only way to test a
 *       simulator's arithmetic separately from its physics.</li>
 *   <li>{@link FittedLatencyModel} — a parametric distribution per class, fitted to measurements.
 *       Reproduces a distribution's location and spread from parameters that fit in a config file.</li>
 *   <li>An empirical bootstrap — resampling observed call durations directly, which needs no
 *       distributional assumption at all but does need the samples shipped alongside the scenario.
 *       Not implemented here; the interface is shaped so that it is an addition rather than a
 *       redesign.</li>
 * </ol>
 *
 * <p>The draw takes the calling actor's {@link SimRng} explicitly rather than holding one, so a model
 * instance can be shared by every actor in a run while each actor keeps its own reproducible tape.
 */
@FunctionalInterface
public interface LatencyModel {

    /**
     * The service time of one call, in nanoseconds — never negative.
     *
     * @param callClass what kind of call this is
     * @param rng       the calling actor's latency tape
     */
    long drawNanos(CallClass callClass, SimRng rng);
}
