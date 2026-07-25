/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bucket4j.BlockingStrategy;
import io.github.bucket4j.TimeMeter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

/**
 * {@link ApiRateLimiter}'s spacing accounting, driven by Bucket4j's own {@link TimeMeter}/
 * {@link BlockingStrategy} test seams (no real wall-clock sleeps) so the assertions are exact and
 * non-flaky.
 */
class ApiRateLimiterTest {

    /** A fake {@link TimeMeter} that only advances via the fake {@link BlockingStrategy} below
     *  (or an explicit test-driven "idle gap" advance). */
    private static final class FakeClock implements TimeMeter {
        final AtomicLong nowNanos = new AtomicLong();

        @Override
        public long currentTimeNanos() {
            return nowNanos.get();
        }

        @Override
        public boolean isWallClockBased() {
            return false;
        }
    }

    /** A fake {@link BlockingStrategy} that advances the fake clock by exactly the requested
     *  park duration instead of really parking — every spacing/rate assertion below is
     *  deterministic, with zero real wall-clock sleep. */
    private static final class FakeBlocking implements BlockingStrategy {
        final FakeClock clock;
        final List<Long> parks = new ArrayList<>();

        FakeBlocking(FakeClock clock) {
            this.clock = clock;
        }

        @Override
        public void park(long nanosToPark) {
            parks.add(nanosToPark);
            clock.nowNanos.addAndGet(nanosToPark);
        }
    }

    @Test
    void firstAcquireOnAFreshLimiterIsImmediate() throws InterruptedException {
        FakeClock clock = new FakeClock();
        FakeBlocking blocking = new FakeBlocking(clock);
        ApiRateLimiter limiter = ApiRateLimiter.forTest(10.0, clock, blocking);

        limiter.acquire();

        assertThat(blocking.parks).isEmpty();   // the bucket starts full (capacity 1)
    }

    @Test
    void backToBackAcquiresAreSpacedByExactlyOneOverRate() throws InterruptedException {
        FakeClock clock = new FakeClock();
        FakeBlocking blocking = new FakeBlocking(clock);
        double rate = 10.0;   // 1 request every 100ms == 100_000_000 ns
        ApiRateLimiter limiter = ApiRateLimiter.forTest(rate, clock, blocking);
        long expectedIntervalNanos = Math.round(1_000_000_000.0 / rate);

        int calls = 5;
        for (int i = 0; i < calls; i++) {
            limiter.acquire();
        }

        // First call is free (bucket starts full); the clock only ever advances by however much
        // the (calls - 1) blocked calls actually parked for — so the total elapsed "time" is
        // exactly (calls - 1) complete refill periods, and the realized rate over that window
        // sits at exactly the configured cap, regardless of how many individual park() calls
        // Bucket4j's own retry loop made to get there.
        assertThat(clock.nowNanos.get()).isEqualTo((calls - 1) * expectedIntervalNanos);
        double elapsedSeconds = clock.nowNanos.get() / 1_000_000_000.0;
        double realizedRate = (calls - 1) / elapsedSeconds;
        assertThat(realizedRate).isCloseTo(rate, Offset.offset(1e-6));
    }

    @Test
    void anIdleGapIsNotBankedAsBurstAllowance() throws InterruptedException {
        FakeClock clock = new FakeClock();
        FakeBlocking blocking = new FakeBlocking(clock);
        ApiRateLimiter limiter = ApiRateLimiter.forTest(10.0, clock, blocking);

        limiter.acquire();   // immediate (bucket starts full)
        // Simulate the caller going idle for a long time (e.g. no requests to make) — this must
        // not let a subsequent burst of calls all through instantly (capacity is 1, not unbounded).
        clock.nowNanos.addAndGet(10_000_000_000L);   // +10s idle

        limiter.acquire();   // caught up after the idle gap — no debt, no penalty, no extra park

        assertThat(blocking.parks).isEmpty();
    }

    @Test
    void rejectsNonPositiveRates() {
        assertThatThrownBy(() -> ApiRateLimiter.perSecond(0.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ApiRateLimiter.perSecond(-5.0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBlockedAcquirePropagatesInterruption() throws Exception {
        FakeClock clock = new FakeClock();
        BlockingStrategy throwing = nanosToPark -> {
            throw new InterruptedException("cancelled while parked on the rate limiter");
        };
        ApiRateLimiter limiter = ApiRateLimiter.forTest(1.0, clock, throwing);
        limiter.acquire();   // first call: immediate (bucket starts full), no park

        assertThatThrownBy(limiter::acquire).isInstanceOf(InterruptedException.class);
    }
}
