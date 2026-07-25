/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.registry.otlp.AggregationTemporality;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.varve.swath.error.ThrottleType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The liveness watchdog's {@link RunMetrics#progressSignal()} MUST be monotonic non-decreasing
 * regardless of the backing registry's export temporality. Do not derive it from summed
 * Micrometer {@code Counter.count()} values: under a DELTA/step OTLP registry ({@link
 * OtlpStepCounter}) {@code count()} reflects only the current step and resets on each step
 * rollover, so a progressSignal built on it DROPS across a rollover (non-monotonic) — which the
 * watchdog would misread as "progress" and re-arm forever, a fault every {@code
 * SimpleMeterRegistry} test hides.
 *
 * <p>This drives a real {@link OtlpMeterRegistry} pinned to {@link AggregationTemporality#DELTA} on a
 * {@link MockClock}, forcing deterministic step rollovers, and asserts {@code progressSignal()} never
 * decreases. A {@code count()}-based implementation would fall back toward 0 after a rollover and
 * fail this test; the AtomicLong-backed implementation holds.
 */
final class ProgressSignalMonotonicTest {

    private static final Duration STEP = Duration.ofMinutes(10);

    private static OtlpConfig deltaConfig() {
        return new OtlpConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public String url() {
                // Never actually pushed within the test window (STEP is 10m; the test rolls the MockClock,
                // not real time), but must be non-null for the registry to construct.
                return "http://localhost:1";
            }

            @Override
            public Duration step() {
                return STEP;
            }

            @Override
            public AggregationTemporality aggregationTemporality() {
                return AggregationTemporality.DELTA;
            }
        };
    }

    @Test
    void progressSignalIsMonotonicAcrossDeltaStepRollovers() {
        MockClock clock = new MockClock();
        OtlpMeterRegistry registry = new OtlpMeterRegistry(deltaConfig(), clock);
        try {
            RunMetrics metrics = new RunMetrics(registry);

            long s0 = metrics.progressSignal();

            metrics.recordEntriesEmitted(100);   // listing/writing progress (a step-counter under DELTA)
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
            metrics.recordProgress(40);           // cascade-merge rows (progressUnits only)
            long s1 = metrics.progressSignal();
            assertThat(s1).as("progress advances the signal").isGreaterThan(s0);

            // Roll the DELTA step: a count()-backed signal would now expose the just-completed step's
            // value, then DROP it to 0 on the next roll — the AtomicLong-backed signal must not.
            clock.add(STEP);
            long s2 = metrics.progressSignal();
            assertThat(s2).as("must not drop when the step rolls").isGreaterThanOrEqualTo(s1);

            // Roll again with NO new activity: a step counter's count() falls back toward 0 here —
            // the exact non-monotonicity the AtomicLong-backed signal avoids. The signal must stay put.
            clock.add(STEP);
            long s3 = metrics.progressSignal();
            assertThat(s3).as("must not fall back toward 0 across an idle step rollover")
                    .isGreaterThanOrEqualTo(s2);

            // Fresh progress after several rollovers still advances it.
            metrics.recordProgress(25);
            clock.add(STEP);
            long s4 = metrics.progressSignal();
            assertThat(s4).as("fresh progress after rollovers still advances the signal").isGreaterThan(s3);
        } finally {
            registry.close();
        }
    }
}
