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
 *
 * <p><b>{@code StrictMath}, not {@code Math}, and this is not a style preference.</b> The logarithm
 * below is the only floating-point operation anywhere in a run's timeline: its result becomes a
 * nanosecond count, which becomes an event instant, which orders the whole trace. {@code Math.log} is
 * permitted to differ from the exact result by one unit in the last place and is free to be replaced
 * by a platform intrinsic, so two JVMs — or two CPU architectures — may legitimately return values
 * that differ in that last bit. One bit is enough: it rounds to a different nanosecond, which
 * reorders two events, which produces a different trace. {@code StrictMath} is specified to be
 * bit-for-bit identical everywhere, which is what makes the byte-identical-trace claim portable
 * rather than true only on the machine that first recorded it. The module's source guard rejects the
 * {@code Math} transcendental family outright so this cannot quietly come back.
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
        // the logarithm is always finite -- the draw can be zero but never infinite. StrictMath, not
        // Math: see the class javadoc, this is the run's only floating-point timeline input.
        double excess = -StrictMath.log(1.0 - rng.nextDouble()) * tailMean;
        // The interface promises a non-negative service time, and a tail mean large enough to overflow
        // the long cast would break that promise -- so it is enforced here rather than assumed to
        // follow from the arithmetic. It saturates rather than clamping to the floor: adding an
        // already-saturated excess to minNanos wraps negative, and taking max(minNanos, negative)
        // would turn the most extreme draw in the tail into the fastest call the class can make --
        // exactly inverting the sample, which is worse than a value that is merely too large.
        if (excess >= (double) (Long.MAX_VALUE - params.minNanos())) {
            return Long.MAX_VALUE;
        }
        return params.minNanos() + (long) excess;
    }
}
