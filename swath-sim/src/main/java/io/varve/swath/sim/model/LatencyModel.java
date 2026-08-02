/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * The simulator's service-time seam. Implementations return nonnegative nanoseconds and use the
 * calling actor's RNG, so one model can be shared without sharing a random stream. Constant and
 * fitted implementations exist; empirical shapes can use the same seam.
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

    /**
     * Returns the service time with calls already outstanding at issue, excluding this call. The
     * default ignores occupancy.
     *
     * @param inFlight calls already outstanding when this call is issued
     */
    default long drawNanos(CallClass callClass, SimRng rng, int inFlight) {
        return drawNanos(callClass, rng);
    }
}
