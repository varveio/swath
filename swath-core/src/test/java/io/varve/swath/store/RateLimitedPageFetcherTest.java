/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import static io.varve.swath.store.PageFetcherTestSupport.REQ;
import static io.varve.swath.store.PageFetcherTestSupport.stubFetcher;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The {@code --rate-limit-api} wiring at the {@link RateLimitedPageFetcher} layer:
 * pass-through behavior, and that a blocked {@link ApiRateLimiter#acquire()} accrues into its OWN
 * dedicated {@code swath.rate_limit.api_wait} meter, never the AIMD concurrency-slot wait's
 * {@code swath.rate_limit.wait} — see {@code RunMetrics}.
 */
class RateLimitedPageFetcherTest {

    @Test
    void delegatesFetchPageAndCapabilitiesToTheWrappedFetcher() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PageFetcher delegate = stubFetcher(calls);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RateLimitedPageFetcher fetcher = new RateLimitedPageFetcher(
                delegate, ApiRateLimiter.perSecond(1_000_000.0), metrics);

        ListPage page = fetcher.fetchPage(REQ);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(page.httpStatus()).isEqualTo(200);
        assertThat(fetcher.capabilities()).isEqualTo(StoreCapabilities.s3());
    }

    @Test
    @Timeout(10)
    void accruesApiWaitTimerWhenTheLimiterBlocks() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PageFetcher delegate = stubFetcher(calls);
        MeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        // A slow enough cap (20 req/s) that back-to-back calls are guaranteed to block, but a
        // small enough request count (3) that the whole test stays well under a second even on a
        // loaded CI box — generous tolerance, no upper bound asserted (guardrail: avoid a flaky
        // wall-clock test).
        RateLimitedPageFetcher fetcher = new RateLimitedPageFetcher(
                delegate, ApiRateLimiter.perSecond(20.0), metrics);

        for (int i = 0; i < 3; i++) {
            fetcher.fetchPage(REQ);
        }

        assertThat(calls.get()).isEqualTo(3);
        assertThat(registry.get("swath.rate_limit.api_wait").timer().count()).isEqualTo(3L);
        assertThat(registry.get("swath.rate_limit.api_wait").timer().totalTime(TimeUnit.NANOSECONDS))
                .isGreaterThan(0.0);
        // This fetcher's proactive cap must never touch the AIMD concurrency-slot wait's
        // meter — the two are cleanly separate.
        assertThat(registry.get("swath.rate_limit.wait").timer().count()).isZero();
    }

    @Test
    void apiWaitMeterIsZeroWhenTheFetcherIsNeverWrapped() {
        // The no-op path — `--rate-limit-api` unset means `RateLimitedPageFetcher` is never
        // constructed at all (see ListCommand#maybeRateLimited), so the api_wait meter registered
        // on a plain RunMetrics instance stays at zero. This test guards that the meter is
        // GENUINELY zero (not merely "no additional accrual") when unwrapped.
        MeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        assertThat(registry.get("swath.rate_limit.api_wait").timer().count()).isZero();
        assertThat(registry.get("swath.rate_limit.api_wait").timer().totalTime(TimeUnit.NANOSECONDS))
                .isZero();
    }
}
