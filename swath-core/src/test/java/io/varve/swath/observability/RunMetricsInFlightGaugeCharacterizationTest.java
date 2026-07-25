/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Characterization: the in-flight gauge's OBSERVABLE behaviour, single-threaded and under
 * concurrent transitions.
 *
 * <p>The gauge is a CAS'd {@code (value, lastTransitionNanos, areaNanos)} triple that four separate
 * concerns read — {@code currentInFlight}, {@code peakInFlight}, the time-weighted
 * {@code swath.in_flight.avg} area integral, and the trajectory rollup, all folded on the SAME
 * transition seam. That coupling is exactly what an extraction is likely to split apart, so what is
 * pinned here is what must survive the split. Only INVARIANTS are asserted for the concurrent case
 * (the interleaving itself is genuinely racy — peak and the area integral depend on scheduling);
 * the deterministic single-threaded arithmetic is pinned exactly against a fake clock.
 */
final class RunMetricsInFlightGaugeCharacterizationTest {

    private static final int THREADS = 8;
    private static final int PAIRS_PER_THREAD = 250;
    private static final int REPETITIONS = 3;

    @Test
    void transitionsDriveCountPeakAndTheAreaIntegralOffOneFakeClock() {
        AtomicLong clock = new AtomicLong();
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry(), clock::get);
        clock.set(1_000L);
        m.markRunStarted();

        assertThat(m.currentInFlight()).isZero();
        assertThat(m.peakInFlight()).isZero();
        assertThat(m.avgInFlight()).isEqualTo(0.0);

        clock.set(2_000L);
        assertThat(m.incrementInFlight()).isEqualTo(1L);
        clock.set(4_000L);
        assertThat(m.incrementInFlight()).isEqualTo(2L);
        clock.set(5_000L);
        assertThat(m.decrementInFlight()).isEqualTo(1L);

        assertThat(m.currentInFlight()).isEqualTo(1L);
        assertThat(m.peakInFlight()).isEqualTo(2L);
        // area = 0*1000 + 1*2000 + 2*1000 = 4000 held-nanos; at now=5000 the tail is 0.
        // elapsed since markRunStarted = 5000 - 1000 = 4000 -> avg = 1.0.
        assertThat(m.avgInFlight()).isEqualTo(1.0);
        // The tail since the last transition is folded in on READ, with no further transition.
        clock.set(9_000L);
        assertThat(m.avgInFlight()).isEqualTo((4_000.0 + 1 * 4_000.0) / 8_000.0);
    }

    @Test
    void avgInFlightGaugeReadsTheSameValueAsTheAccessor() {
        AtomicLong clock = new AtomicLong(1_000L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics m = new RunMetrics(registry, clock::get);
        m.markRunStarted();
        clock.set(2_000L);
        m.incrementInFlight();
        clock.set(6_000L);

        Gauge gauge = registry.find("swath.in_flight.avg").gauge();

        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(m.avgInFlight());
    }

    @Test
    void markRunStartedCarriesTheLiveCountForwardAndResetsTheIntegrationWindow() {
        AtomicLong clock = new AtomicLong(1_000L);
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry(), clock::get);
        m.markRunStarted();
        clock.set(2_000L);
        m.incrementInFlight();
        m.incrementInFlight();

        clock.set(10_000L);
        m.markRunStarted();

        // Whatever was in flight carries forward (a resumed run's warm-start work is real).
        assertThat(m.currentInFlight()).isEqualTo(2L);
        // The area integral restarts from here: at now == run start, elapsed is 0 -> 0.0.
        assertThat(m.avgInFlight()).isEqualTo(0.0);
        clock.set(12_000L);
        assertThat(m.avgInFlight()).isEqualTo(2.0);
    }

    @Test
    void concurrentTransitionsNeverLoseOrDuplicateACount() throws Exception {
        for (int repetition = 0; repetition < REPETITIONS; repetition++) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            RunMetrics m = new RunMetrics(registry);
            m.markRunStarted();

            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Long>> futures = new ArrayList<>();
                for (int t = 0; t < THREADS; t++) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        long maxObserved = 0L;
                        for (int i = 0; i < PAIRS_PER_THREAD; i++) {
                            maxObserved = Math.max(maxObserved, m.incrementInFlight());
                            m.decrementInFlight();
                        }
                        return maxObserved;
                    }));
                }
                start.countDown();
                long widestObserved = 0L;
                for (Future<Long> future : futures) {
                    widestObserved = Math.max(widestObserved, future.get(60, TimeUnit.SECONDS));
                }

                // Balanced increments/decrements settle back to exactly zero: no lost or
                // double-applied transition, which is the whole point of the CAS'd triple.
                assertThat(m.currentInFlight()).isZero();
                // Every value an increment returned is a legal concurrency level, and the run's
                // peak is exactly the widest level any thread actually observed.
                assertThat(widestObserved).isBetween(1L, (long) THREADS);
                assertThat(m.peakInFlight()).isEqualTo(widestObserved);
                // The area integral stays non-negative and finite — the clamped clock and the
                // per-attempt re-read exist to stop a CAS loser committing a backwards window.
                assertThat(m.avgInFlight()).isNotNaN().isGreaterThanOrEqualTo(0.0);
                // The trajectory rollup folded on the same seam stays bounded and self-consistent.
                m.recordListingPageShape(1L, false, 1_000);
                RunSummary.TrajectorySummary trajectory =
                        m.summary(Duration.ofSeconds(1), "s", 0L, 0L).trajectory();
                assertThat(trajectory).isNotNull();
                assertThat(trajectory.inFlight()).hasSizeLessThanOrEqualTo(RunMetrics.TRAJECTORY_BINS);
                assertThat(trajectory.inFlight()).hasSameSizeAs(trajectory.progressRate());
                assertThat(trajectory.serialFrac()).isBetween(0.0, 1.0);
                assertThat(trajectory.peakWorkers()).isEqualTo((int) m.peakInFlight());
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
