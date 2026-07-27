/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * A store whose latency <b>rises with the number of calls in flight</b>: every call pays the
 * underlying model's draw plus {@code perInFlightNanos} for each other call outstanding when it was
 * issued, up to a ceiling.
 *
 * <p>This is the deliberate exception to modelling the store generously. Measurement of the live
 * system found it client-bound, with no store ceiling reached at the rates a real run drives — so the
 * regimes worth simulating are shaped by the client and its policies, and a store model that fights
 * back would be modelling a system nobody has. The exception exists because one control rung reacts to
 * exactly this shape and nothing else can exercise it: the adaptive-concurrency controller's
 * latency-freeze is armed by an inflating successful-attempt latency, so a store that answers a wider
 * fleet more slowly is the only way to reach it. Staging that against a real store is not something
 * anyone can do on purpose; here it costs a constructor argument.
 *
 * <p>The rise is linear and capped rather than, say, exponential, because the point is to cross the
 * freeze rung's threshold in a legible way, not to claim a particular queueing law — a claim no
 * measurement here supports. A scenario that wants the queueing law instead states it structurally,
 * with a shared server in front of the store, and gets the waiting time out of the queue rather than
 * out of a coefficient.
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

    /**
     * The uncontended draw. Reached only by a caller that has no occupancy to report; the executor
     * always calls the three-argument overload.
     */
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
