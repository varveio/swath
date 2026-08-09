/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * {@code swath.aimd.freeze_gate_checks} counts the successes that actually REACHED the growth-freeze
 * gates — the denominator that makes {@code latency_freeze}/{@code growth_freeze} comparable across
 * runs. A success that returns earlier (at {@code Tmax}, or inside a throttle cool-down) could never
 * have frozen, so without this counter a healthy saturated run reads zero freezes by construction
 * and looks identical to a run whose rungs genuinely never engaged.
 */
final class ConcurrencyGaugeFreezeGateDenominatorTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    private static final long MS_100 = 100_000_000L;
    private static final long MS_300 = 300_000_000L;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, ConcurrencyGauge.defaultInitialT(tMax));
    }

    private static double freezeGateChecks(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.freeze_gate_checks").counter().count();
    }

    private static double latencyFreezes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.latency_freeze").counter().count();
    }

    @Test
    void successesAtTmaxAreNotTrials() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // tMax == the slow-start initial T, so the gauge starts pinned at its ceiling.
        ConcurrencyGauge gauge = newGauge(ConcurrencyGauge.SLOW_START_INITIAL_T, metrics);
        assertThat(gauge.effectiveT()).isEqualTo(ConcurrencyGauge.SLOW_START_INITIAL_T);

        for (int i = 0; i < 5; i++) {
            gauge.reportStatus(200);
        }

        assertThat(freezeGateChecks(metrics))
                .as("a success at Tmax returns before the freeze gates — it can never freeze, so it "
                        + "is not a trial the freeze counters can be read against")
                .isZero();
    }

    @Test
    void successesInsideAThrottleCooldownAreNotTrials() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T below Tmax, cool-down armed

        gauge.reportStatus(200);

        assertThat(freezeGateChecks(metrics))
                .as("the cool-down eats this success's growth opportunity before the gates")
                .isZero();
    }

    @Test
    void everySuccessThatReachesTheGatesCounts() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // below Tmax
        gauge.forceCleanWindow();                               // past the cool-down

        gauge.reportStatus(200);
        gauge.reportStatus(200);
        gauge.reportStatus(200);

        assertThat(freezeGateChecks(metrics))
                .as("below Tmax and past the cool-down, every success is a genuine freeze trial")
                .isEqualTo(3.0);
        assertThat(latencyFreezes(metrics))
                .as("healthy latency freezes nothing, but the trials still counted — that gap is "
                        + "exactly what the denominator makes visible")
                .isZero();
    }

    @Test
    void freezesNeverOutnumberTheTrialsTheyHappenedIn() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
        gauge.forceCleanWindow();
        gauge.onAttemptLatency(MS_100);                         // baseline := 100ms
        for (int i = 0; i < 5; i++) {
            gauge.onAttemptLatency(MS_300);                     // EWMA climbs past 2x the baseline
        }

        for (int i = 0; i < 4; i++) {
            gauge.reportStatus(200);
        }

        assertThat(latencyFreezes(metrics)).isPositive();
        assertThat(latencyFreezes(metrics))
                .as("a freeze is only recordable from inside a trial, so the ratio is a real rate "
                        + "in [0,1] — never above 1")
                .isLessThanOrEqualTo(freezeGateChecks(metrics));
    }
}
