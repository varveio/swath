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
 * One fixed service time per call class, drawing nothing.
 *
 * <p>This is the model the kernel's closed-form invariants are stated in. With constant latency and
 * client costs explicitly zeroed, a run's wall time is an arithmetic consequence of the fixture and
 * the driver — so a disagreement is a defect in the kernel, not a plausible difference of opinion
 * about physics. Every other model form is validated against a real system; this one is validated
 * against algebra.
 */
public final class ConstantLatencyModel implements LatencyModel {

    private final Map<CallClass, Long> perClass;

    private ConstantLatencyModel(Map<CallClass, Long> perClass) {
        this.perClass = perClass;
    }

    /** The same service time for every class. */
    public static ConstantLatencyModel uniform(long nanos) {
        requireNonNegative(nanos);
        EnumMap<CallClass, Long> all = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            all.put(callClass, nanos);
        }
        return new ConstantLatencyModel(all);
    }

    /**
     * A service time per class. Every class must be present — a partially-specified latency model
     * would silently answer zero for the class a scenario forgot, which is the exact shape of a
     * result that looks like a policy win and is a missing input.
     */
    public static ConstantLatencyModel perClass(Map<CallClass, Long> nanosByClass) {
        EnumMap<CallClass, Long> copy = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            Long nanos = nanosByClass.get(callClass);
            if (nanos == null) {
                throw new IllegalArgumentException("no latency given for call class " + callClass
                        + "; every class must be specified");
            }
            requireNonNegative(nanos);
            copy.put(callClass, nanos);
        }
        return new ConstantLatencyModel(copy);
    }

    @Override
    public long drawNanos(CallClass callClass, SimRng rng) {
        return perClass.get(callClass);
    }

    private static void requireNonNegative(long nanos) {
        if (nanos < 0) {
            throw new IllegalArgumentException("latency must be >= 0, got " + nanos);
        }
    }
}
