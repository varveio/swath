/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * The transient-retry configuration the CLI resolves once and threads into both retry
 * loops — the seed's {@link TransientRetryFetcher} and the engine's {@code GaugedFetcher}
 * — bundling the {@link RetryPolicy} (RIDE_OUT vs BOUNDED, chosen from whether a real {@code
 * LivenessWatchdog} is armed) with the injectable backoff {@link TransientRetryFetcher.Sleeper} (null in
 * production; a test override mirroring {@code fetcherOverride}). One value carried through
 * {@code ListRunner} instead of two loose parameters.
 */
public final class RetryConfig {

    /**
     * Safe implicit default: {@link RetryPolicy#BOUNDED} with real {@link Thread#sleep} backoffs. Used
     * only when a caller does NOT thread an explicit config — the public/direct {@code ListRunner}/{@code
     * WorkStealingScan} constructors and embedded callers with no {@code LivenessWatchdog} armed. This
     * is deliberately NOT {@code RIDE_OUT}: an unbounded ride-out with no watchdog to end it would
     * retry a permanent storm forever, violating the "RIDE_OUT only when a real watchdog is armed" rule
     * and the no-infinite-loop/resumability contract. The CLI resolves {@code RIDE_OUT} explicitly (in
     * {@code ListCommand.retryConfig()}) iff {@code watchdog.isArmed()}, so production ride-out is
     * unchanged; only implicit/embedded defaults fall back to the bounded (resumable-STUCK) semantics.
     */
    public static final RetryConfig DEFAULT =
            new RetryConfig(RetryPolicy.BOUNDED, TransientRetryFetcher.DEFAULT_SLEEPER);

    private final RetryPolicy policy;
    private final TransientRetryFetcher.Sleeper sleeper;

    public RetryConfig(RetryPolicy policy, TransientRetryFetcher.Sleeper sleeper) {
        this.policy = policy == null ? RetryPolicy.BOUNDED : policy;   // safe fallback (never an owner-less ride-out)
        this.sleeper = sleeper == null ? TransientRetryFetcher.DEFAULT_SLEEPER : sleeper;
    }

    public RetryPolicy policy() {
        return policy;
    }

    public TransientRetryFetcher.Sleeper sleeper() {
        return sleeper;
    }
}
