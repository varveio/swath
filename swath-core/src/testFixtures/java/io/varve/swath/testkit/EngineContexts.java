/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.engine.EngineContext;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;

/**
 * Terse {@link EngineContext} factories for engine tests: the run identity plus the same effective
 * defaults the (now removed) short {@code WorkStealingScan} constructor overloads supplied — a fresh
 * throwaway metrics sink and the canonical toggle/trace/retry policy seams. A test that needs a
 * non-default seam layers it on with the context's own {@code withX} methods
 * (e.g. {@code EngineContexts.of(runId, prefix, mode, metrics).withToggles(toggles)}).
 */
public final class EngineContexts {

    private EngineContexts() {
    }

    /**
     * The run identity plus a fresh throwaway metrics sink and the canonical policy defaults — the
     * effective context the metrics-less short overload built.
     */
    public static EngineContext of(long runId, byte[] prefix, ListingMode mode) {
        return of(runId, prefix, mode, freshMetrics());
    }

    /** As {@link #of(long, byte[], ListingMode)}, but with a caller-supplied metrics sink. */
    public static EngineContext of(long runId, byte[] prefix, ListingMode mode, RunMetrics metrics) {
        return new EngineContext(runId, prefix, mode, metrics, null, null, null);
    }

    /** A fresh throwaway metrics sink over a {@link SimpleMeterRegistry} — the short overload's default. */
    public static RunMetrics freshMetrics() {
        return new RunMetrics(new SimpleMeterRegistry());
    }
}
