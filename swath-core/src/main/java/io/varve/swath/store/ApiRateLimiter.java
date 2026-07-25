/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BlockingBucket;
import io.github.bucket4j.BlockingStrategy;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.TimeMeter;
import java.time.Duration;

/**
 * The {@code --rate-limit-api} client-side cap: a one-token Bucket4j local bucket, refilled at
 * exactly {@code requestsPerSecond} tokens/sec, gating whatever calls {@link #acquire()}.
 * Capacity {@code 1} with a greedy refill means no burst beyond "catch up instantly after an
 * idle gap" — a deliberate zero-burst, fixed-interval spacing shape. That pacing behavior is part
 * of the public contract; swapping libraries must preserve it.
 *
 * <p><b>Cancellation-safe.</b> {@link #acquire()} calls {@link BlockingBucket#consume(long,
 * BlockingStrategy)} — the INTERRUPTIBLE blocking path (throws {@link InterruptedException}),
 * never {@code consumeUninterruptibly}. In production the strategy is
 * {@link BlockingStrategy#PARKING}, which parks via {@code LockSupport.parkNanos} and re-checks
 * {@code Thread.interrupted()} after every park, so a worker parked here is woken the same way a
 * worker parked in the AIMD throttle backoff is: the engine's {@code executor.shutdownNow()} on
 * cancellation ({@code CancellationToken}'s javadoc) interrupts every blocked task, including
 * this one.
 *
 * <p><b>Global, not per-thread.</b> One instance is meant to be shared by every caller in a run
 * (see {@link RateLimitedPageFetcher}) — Bucket4j's local bucket is internally thread-safe, so
 * many threads can call {@link #acquire()} concurrently and are served in arrival order.
 */
public final class ApiRateLimiter {

    private final BlockingBucket bucket;
    private final BlockingStrategy blockingStrategy;

    /**
     * @param requestsPerSecond the cap; must be {@code > 0} (callers should simply not construct
     *                          this class at all for the unset/no-cap case — see
     *                          {@code ListCommand#maybeRateLimited}).
     */
    public static ApiRateLimiter perSecond(double requestsPerSecond) {
        return new ApiRateLimiter(requestsPerSecond, TimeMeter.SYSTEM_NANOTIME, BlockingStrategy.PARKING);
    }

    /**
     * Test seam (package-private): an injected {@link TimeMeter} — Bucket4j's own custom-clock
     * hook — drives the bucket's refill accounting deterministically, and an injected
     * {@link BlockingStrategy} replaces the real {@code LockSupport.parkNanos} wait with a fake
     * that advances the same fake clock, so a spacing/rate assertion never depends on a real
     * wall-clock sleep.
     */
    static ApiRateLimiter forTest(double requestsPerSecond, TimeMeter clock, BlockingStrategy blockingStrategy) {
        return new ApiRateLimiter(requestsPerSecond, clock, blockingStrategy);
    }

    private ApiRateLimiter(double requestsPerSecond, TimeMeter clock, BlockingStrategy blockingStrategy) {
        if (!(requestsPerSecond > 0)) {
            throw new IllegalArgumentException(
                    "requestsPerSecond must be > 0 (got " + requestsPerSecond + ")");
        }
        long intervalNanos = Math.max(1L, Math.round(1_000_000_000.0 / requestsPerSecond));
        Bandwidth limit = Bandwidth.builder()
                .capacity(1)
                .refillGreedy(1, Duration.ofNanos(intervalNanos))
                .build();
        this.bucket = Bucket.builder()
                .addLimit(limit)
                .withCustomTimePrecision(clock)
                .build()
                .asBlocking();
        this.blockingStrategy = blockingStrategy;
    }

    /** Blocks (interruptibly) until this call is within the configured rate, then returns. */
    public void acquire() throws InterruptedException {
        bucket.consume(1, blockingStrategy);
    }
}
