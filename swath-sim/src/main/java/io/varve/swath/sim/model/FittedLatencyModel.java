/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;
import java.util.EnumMap;
import java.util.Map;

/**
 * Shifted-exponential service time by call class: a floor plus an exponential tail. It is a compact
 * fitted model, not a claim that observed latency is exponential. Each non-constant call consumes one
 * actor-RNG draw. Every call class needs parameters; equal floor and mean is constant.
 */
public final class FittedLatencyModel implements LatencyModel {

    /**
     * One class's fitted parameters.
     *
     * @param minNanos  the floor: no call of this class completes faster
     * @param meanNanos the mean; must be at least the floor. Equal to the floor gives a constant.
     */
    public record Params(long minNanos, long meanNanos) {
        public Params {
            if (minNanos < 0) {
                throw new IllegalArgumentException("minNanos must be >= 0, got " + minNanos);
            }
            if (meanNanos < minNanos) {
                throw new IllegalArgumentException("meanNanos (" + meanNanos + ") must be >= minNanos ("
                        + minNanos + ")");
            }
        }

        /** The mean above the fixed floor. */
        long tailMeanNanos() {
            return meanNanos - minNanos;
        }
    }

    private final Map<CallClass, Params> perClass;

    private FittedLatencyModel(Map<CallClass, Params> perClass) {
        this.perClass = perClass;
    }

    /** Parameters per class; all call classes must be present. */
    public static FittedLatencyModel of(Map<CallClass, Params> paramsByClass) {
        EnumMap<CallClass, Params> copy = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            Params params = paramsByClass.get(callClass);
            if (params == null) {
                throw new IllegalArgumentException("no fitted parameters given for call class " + callClass
                        + "; every class must be specified");
            }
            copy.put(callClass, params);
        }
        return new FittedLatencyModel(copy);
    }

    @Override
    public long drawNanos(CallClass callClass, SimRng rng) {
        Params params = perClass.get(callClass);
        long tailMean = params.tailMeanNanos();
        if (tailMean == 0) {
            return params.minNanos();
        }
        // nextDouble() is in [0, 1), so this inverse transform is finite. StrictMath.log makes the
        // transform bit-reproducible across platforms.
        double excess = -StrictMath.log(1.0 - rng.nextDouble()) * tailMean;
        // Saturate extreme tails: overflow must not wrap the largest draw into a fast call.
        if (excess >= (double) (Long.MAX_VALUE - params.minNanos())) {
            return Long.MAX_VALUE;
        }
        return params.minNanos() + (long) excess;
    }
}
