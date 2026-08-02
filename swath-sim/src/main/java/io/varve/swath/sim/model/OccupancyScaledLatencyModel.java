/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * Synthetic capped-linear latency inflation: the base draw plus a rate per call already in flight,
 * capped at a validated ceiling. This is a control for the adaptive-concurrency latency-freeze path,
 * not a measured queueing law.
 */
public final class OccupancyScaledLatencyModel implements LatencyModel {

    private final LatencyModel base;
    private final long perInFlightNanos;
    private final long ceilingNanos;

    /**
     * @param base             the uncontended service time this model inflates
     * @param perInFlightNanos added per call already outstanding at issue
     * @param ceilingNanos     the most any one call may take, however deep the fleet goes
     */
    public OccupancyScaledLatencyModel(LatencyModel base, long perInFlightNanos, long ceilingNanos) {
        if (base == null) {
            throw new MissingSimDependencyException("base latency model (the uncontended service time "
                    + "an occupancy-scaled store inflates)");
        }
        if (perInFlightNanos < 0) {
            throw new IllegalArgumentException("perInFlightNanos must be >= 0, got " + perInFlightNanos);
        }
        if (ceilingNanos <= 0) {
            throw new IllegalArgumentException("ceilingNanos must be positive, got " + ceilingNanos);
        }
        this.base = base;
        this.perInFlightNanos = perInFlightNanos;
        this.ceilingNanos = ceilingNanos;
    }

    /** Draws with zero already-outstanding calls. */
    @Override
    public long drawNanos(CallClass callClass, SimRng rng) {
        return drawNanos(callClass, rng, 0);
    }

    @Override
    public long drawNanos(CallClass callClass, SimRng rng, int inFlight) {
        long drawn = base.drawNanos(callClass, rng);
        long inflated = drawn + perInFlightNanos * Math.max(0, inFlight);
        return Math.min(ceilingNanos, inflated);
    }
}
