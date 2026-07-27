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
 * A shifted-exponential service time per call class: a floor no call can beat, plus an exponential
 * tail above it.
 *
 * <p><b>Why this form.</b> It is the simplest distribution that says the two things a request
 * latency measurement reliably shows — there is a hard minimum (the round trip and the fixed
 * server-side work) and the excess over it is long-tailed — and it is fully determined by two
 * numbers per class, a floor and a mean. That matters, because a calibration set that reports means
 * rather than full distributions can parameterise this and cannot parameterise anything richer. It
 * is a deliberate floor on fidelity, not a claim that request latencies are exponential: where the
 * tail's shape is load-bearing for a result, that result wants the empirical bootstrap shape
 * {@link LatencyModel} anticipates instead.
 *
 * <p>The draw is inverse-transform sampled, so it consumes exactly one draw from the actor's tape per
 * call — a property worth keeping, since a rejection-sampled model would make the number of random
 * values consumed depend on the values themselves and so couple every later draw to this one.
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

        /** The exponential tail's mean — the mean above the floor. */
        long tailMeanNanos() {
            return meanNanos - minNanos;
        }
    }

    private final Map<CallClass, Params> perClass;

    private FittedLatencyModel(Map<CallClass, Params> perClass) {
        this.perClass = perClass;
    }

    /** Parameters per class; every class must be present, for {@link ConstantLatencyModel}'s reason. */
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
        // Inverse transform of Exp(1/tailMean). nextDouble() is in [0, 1), so 1 - u is in (0, 1] and
        // the logarithm is always finite -- the draw can be zero but never infinite.
        double excess = -Math.log(1.0 - rng.nextDouble()) * tailMean;
        return params.minNanos() + (long) excess;
    }
}
