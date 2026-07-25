/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;

/**
 * The run-scoped clump threaded into {@link WorkStealingScan}: the identity fixed for the whole run
 * ({@code runId}, {@code prefix}, listing {@code mode}), the {@link RunMetrics} sink, and the three
 * opt-in policy seams — the {@link EngineToggles} ablation namespace, the {@link TraceSink} flight
 * recorder, and the {@link RetryConfig} transient-retry policy. Bundling them into one immutable value
 * is what lets the engine expose a <b>single</b> public construction path: the caller assembles this
 * context (filling any policy seam it does not care about with its canonical default) and passes it
 * alongside the run's unit-specific collaborators — the fetcher, store, worker count, key budget, seeds,
 * and filters.
 *
 * <p><b>Canonical defaults.</b> The three policy seams null-default to their canonical values —
 * {@link EngineToggles#DEFAULT}, the no-op {@link TraceSink#NONE}, and {@link RetryConfig#DEFAULT}
 * (a {@link RetryPolicy#BOUNDED} policy, never an owner-less unbounded ride-out) — so a caller that
 * passes {@code null} for one lands on the documented baseline. Layer a non-default seam onto a base
 * context with the {@code withX} methods (e.g. {@code context.withToggles(t)}); each returns a new
 * context, leaving the original untouched.
 *
 * <p><b>The cancellation token is deliberately not carried here.</b> It is bound late — once per
 * {@code produce} call, via a volatile read — so the throttle-retry loop can abort a persistently
 * throttled fetch promptly; a token captured at construction could not observe that late binding.
 * The test-only {@link GaugeClock} window-compression seam is likewise kept out: it is consumed into
 * the {@link ConcurrencyGauge} at construction rather than being run-scoped state.
 */
public record EngineContext(long runId, byte[] prefix, ListingMode mode, RunMetrics metrics,
                            EngineToggles toggles, TraceSink trace, RetryConfig retryConfig) {

    public EngineContext {
        toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        trace = trace == null ? TraceSink.NONE : trace;
        retryConfig = retryConfig == null ? RetryConfig.DEFAULT : retryConfig;
    }

    /** A copy of this context with the {@link EngineToggles} ablation namespace replaced. */
    public EngineContext withToggles(EngineToggles toggles) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig);
    }

    /** A copy of this context with the {@link TraceSink} flight recorder replaced. */
    public EngineContext withTrace(TraceSink trace) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig);
    }

    /** A copy of this context with the {@link RetryConfig} transient-retry policy replaced. */
    public EngineContext withRetryConfig(RetryConfig retryConfig) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig);
    }
}
