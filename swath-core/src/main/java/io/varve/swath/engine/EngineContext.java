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
 * ({@code runId}, {@code prefix}, listing {@code mode}), the {@link RunMetrics} sink, and the four
 * opt-in policy seams — the {@link EngineToggles} ablation namespace, the {@link TraceSink} flight
 * recorder, the {@link RetryConfig} transient-retry policy, and {@code decisionRngSeed}. Bundling them
 * into one immutable value is what lets the engine expose a <b>single</b> public construction path: the
 * caller assembles this context (filling any policy seam it does not care about with its canonical
 * default) and passes it alongside the run's unit-specific collaborators — the fetcher, store, worker
 * count, key budget, seeds, and filters.
 *
 * <p><b>Canonical defaults.</b> The three record-typed policy seams null-default to their canonical
 * values — {@link EngineToggles#DEFAULT}, the no-op {@link TraceSink#NONE}, and {@link
 * RetryConfig#DEFAULT} (a {@link RetryPolicy#BOUNDED} policy, never an owner-less unbounded ride-out) —
 * so a caller that passes {@code null} for one lands on the documented baseline. {@code
 * decisionRngSeed} is {@code null}-by-default too, but {@code null} <b>is</b> its documented baseline
 * (not a substitution to some other canonical value): unset, {@link WorkStealingScan} threads {@link
 * Thief}'s live ambient {@link io.varve.swath.engine.policy.DecisionRng} default, byte-identical to
 * every run before this seam existed; set, every worker instead draws from a {@link
 * SeededDecisionRng} stream deterministically derived from this seed and that worker's own stable
 * identity (see {@link SeededDecisionRng}). Layer a non-default seam onto a base context with the
 * {@code withX} methods (e.g. {@code context.withToggles(t)}); each returns a new context, leaving the
 * original untouched.
 *
 * <p><b>The cancellation token is deliberately not carried here.</b> It is bound late — once per
 * {@code produce} call, via a volatile read — so the throttle-retry loop can abort a persistently
 * throttled fetch promptly; a token captured at construction could not observe that late binding.
 * The test-only {@link GaugeClock} window-compression seam is likewise kept out: it is consumed into
 * the {@link ConcurrencyGauge} at construction rather than being run-scoped state.
 */
public record EngineContext(long runId, byte[] prefix, ListingMode mode, RunMetrics metrics,
                            EngineToggles toggles, TraceSink trace, RetryConfig retryConfig,
                            Long decisionRngSeed) {

    public EngineContext {
        toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        trace = trace == null ? TraceSink.NONE : trace;
        retryConfig = retryConfig == null ? RetryConfig.DEFAULT : retryConfig;
        // decisionRngSeed stays null when unset: null IS its documented default (ambient DecisionRng),
        // not a placeholder substituted for some other canonical value.
    }

    /**
     * Additive compatibility constructor (issue: opt-in seeded {@code DecisionRng}, 2026-07-26):
     * every pre-existing 7-arg call site keeps compiling unchanged, landing on {@code
     * decisionRngSeed == null} (ambient, byte-identical to before this field existed).
     */
    public EngineContext(long runId, byte[] prefix, ListingMode mode, RunMetrics metrics,
                         EngineToggles toggles, TraceSink trace, RetryConfig retryConfig) {
        this(runId, prefix, mode, metrics, toggles, trace, retryConfig, null);
    }

    /** A copy of this context with the {@link EngineToggles} ablation namespace replaced. */
    public EngineContext withToggles(EngineToggles toggles) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig, decisionRngSeed);
    }

    /** A copy of this context with the {@link TraceSink} flight recorder replaced. */
    public EngineContext withTrace(TraceSink trace) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig, decisionRngSeed);
    }

    /** A copy of this context with the {@link RetryConfig} transient-retry policy replaced. */
    public EngineContext withRetryConfig(RetryConfig retryConfig) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig, decisionRngSeed);
    }

    /**
     * A copy of this context with the opt-in seeded-{@code DecisionRng} base seed replaced ({@code
     * null} restores the ambient live default). See this record's class javadoc.
     */
    public EngineContext withDecisionRngSeed(Long decisionRngSeed) {
        return new EngineContext(runId, prefix, mode, metrics, toggles, trace, retryConfig, decisionRngSeed);
    }
}
