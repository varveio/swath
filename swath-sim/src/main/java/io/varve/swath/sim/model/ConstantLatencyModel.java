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
 * <p>This algebraic model anchors the kernel's closed-form checks when client costs are zero.
 */
public final class ConstantLatencyModel implements LatencyModel {

    private final Map<CallClass, Long> perClass;

    private ConstantLatencyModel(Map<CallClass, Long> perClass) {
        this.perClass = perClass;
    }

    /** Creates a model with the same service time for every call class. */
    public static ConstantLatencyModel uniform(long nanos) {
        requireNonNegative(nanos);
        EnumMap<CallClass, Long> all = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            all.put(callClass, nanos);
        }
        return new ConstantLatencyModel(all);
    }

    /** Creates a per-class model; every {@link CallClass} must be present. */
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
