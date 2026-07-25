/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.error.ListingException;
import io.varve.swath.observability.RunMetrics;

/**
 * The {@code --rate-limit-api} wiring: wraps a {@link PageFetcher} so every
 * {@link #fetchPage} call — one real S3 API request — first waits on {@code limiter}, capping
 * the aggregate request rate across every caller that shares this instance.
 *
 * <p><b>Global across worker threads.</b> {@code WorkStealingScan} threads the SAME
 * {@link PageFetcher} field into both its worker path and its {@code Thief} probe path (see its
 * constructor); {@code ListCommand} wraps the fetcher exactly once, before either path is built,
 * so one {@link ApiRateLimiter} instance gates the run's aggregate rate — never per-thread.
 *
 * <p><b>Proactive vs. reactive.</b> This is a client-side cap the caller opts into ahead of
 * time; it is distinct from the AIMD 503/SlowDown backoff ({@code ConcurrencyGauge}), which is a
 * reactive response to a real server throttle signal. The two accrue to SEPARATE
 * meters: this cap's blocked time goes to {@code swath.rate_limit.api_wait}, while
 * {@code swath.rate_limit.wait} stays the AIMD concurrency-slot wait's historical meaning. See
 * {@code docs/metrics-and-observability.md} §1.1 for the full throttle-coverage note.
 *
 * <p><b>No-op when unset.</b> Only ever constructed when {@code --rate-limit-api} is configured
 * (see {@code ListCommand#maybeRateLimited}) — when unset the plain, unwrapped fetcher is used
 * instead, so the default hot path pays zero added overhead (no extra indirection, no timer,
 * {@code swath.rate_limit.api_wait} stays genuinely zero).
 */
public final class RateLimitedPageFetcher implements PageFetcher {

    private final PageFetcher delegate;
    private final ApiRateLimiter limiter;
    private final RunMetrics metrics;

    public RateLimitedPageFetcher(PageFetcher delegate, ApiRateLimiter limiter, RunMetrics metrics) {
        this.delegate = delegate;
        this.limiter = limiter;
        this.metrics = metrics;
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        Timer.Sample sample = metrics.startApiRateLimitWaitTimer();
        limiter.acquire();
        metrics.recordApiRateLimitWait(sample);
        return delegate.fetchPage(req);
    }

    @Override
    public StoreCapabilities capabilities() {
        return delegate.capabilities();
    }
}
