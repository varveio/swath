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
 * FAST/deterministic unit tests for the progress-gated sustained-timeout SHED on
 * {@link ConcurrencyGauge}. These use the injected clock + fixed shed-window seam (no real sleep), so
 * they stay in the per-commit tier. They cover the ORDINARY shed behavior; the adversarial
 * cross-cutting adversarial suite (tail-vs-storm, storm-then-recover) is owned separately.
 */
final class ConcurrencyGaugeShedTest {

    /** A fixed shed-window length so window rolls are driven purely by the injected clock. */
    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 is the "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, tMax);   // Start AT Tmax: shed/AIMD tests isolate from the slow-start ramp.
    }

    private static double timeoutSheds(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.timeout_shed").counter().count();
    }

    private static double aimdVotes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.votes").counter().count();
    }

    /** Feed a starved storm (many timeouts, zero successes) then advance the clock past the window. */
    private void stormWindow(ConcurrencyGauge gauge, int timeouts) {
        for (int i = 0; i < timeouts; i++) {
            gauge.onTransientTimeout();
        }
        now.addAndGet(WINDOW + 1);
    }

    // ---- storm + starved sheds ~0.5x per window down to the floor ---------------

    @Test
    void starvedStorm_shedsHalfPerWindow_downToFloor_atHighT() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, metrics);

        // One shed per window (the latch caps it), factor 0.5: 256→128→64→32→16→8→4→2→1 = 8 sheds.
        int[] expected = {128, 64, 32, 16, 8, 4, 2, 1};
        for (int i = 0; i < expected.length; i++) {
            stormWindow(gauge, 100);
            assertThat(gauge.effectiveT())
                    .as("shed #%d halves T", i + 1)
                    .isEqualTo(expected[i]);
        }
        assertThat(timeoutSheds(metrics)).as("one timeout_shed per window").isEqualTo(8.0);
        assertThat(aimdVotes(metrics)).as("a shed is NEVER a 503 down-vote").isEqualTo(0.0);

        // A further starved window at the floor keeps T at 1 (floor(0.5*1)=1, no reduction).
        stormWindow(gauge, 100);
        assertThat(gauge.effectiveT()).isEqualTo(1);
    }

    @Test
    void starvedStorm_sheds_atSmallT_scaleInvariant() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(2, metrics);

        // At T=2 the gate is max(K=3, ceil(0.3*2)=1)=3 timeouts; the 30s window makes K=3 reachable.
        stormWindow(gauge, 100);
        assertThat(gauge.effectiveT()).as("T=2 sheds to floor 1 under a starved storm").isEqualTo(1);
        assertThat(timeoutSheds(metrics)).isEqualTo(1.0);
        assertThat(aimdVotes(metrics)).isEqualTo(0.0);
    }

    // ---- tail + progress does NOT shed -------------------------------------------

    @Test
    void tailWithProgress_doesNotShed() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);

        // Interleave timeouts with real completions: successes stay well above the starvation gate
        // (successGate = max(1, 64/32) = 2), so the storm condition never triggers a shed.
        for (int i = 0; i < 200; i++) {
            gauge.reportStatus(200);          // a real completed page (onSuccess)
            gauge.onTransientTimeout();
        }
        assertThat(gauge.effectiveT()).as("a timeout tail on a progressing run must NOT shed").isEqualTo(64);
        assertThat(timeoutSheds(metrics)).isEqualTo(0.0);
        assertThat(aimdVotes(metrics)).isEqualTo(0.0);
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    // ---- the shed reuses the decrease+reduce machinery: permits track effectiveT ----

    @Test
    void shed_reducesPermitsToMatchEffectiveT() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(8, metrics);
        assertThat(gauge.availablePermits()).isEqualTo(8);

        stormWindow(gauge, 100);
        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(gauge.availablePermits()).as("permit pool shrinks with T").isEqualTo(4);

        stormWindow(gauge, 100);
        assertThat(gauge.effectiveT()).isEqualTo(2);
        assertThat(gauge.availablePermits()).isEqualTo(2);
    }

    // ---- a shed records timeout_shed only, never an aimd vote -------------------

    @Test
    void shed_recordsTimeoutShed_notAimdVote() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        ConcurrencyGauge gauge = newGauge(8, metrics);

        stormWindow(gauge, 100);

        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(timeoutSheds(metrics)).as("exactly one shed fired").isEqualTo(1.0);
        assertThat(aimdVotes(metrics)).as("the shed casts ZERO aimd votes (guard)").isEqualTo(0.0);
        // A shed pauses stealing (like onThrottle) and records the target reduction + a distinct steal_reason.
        assertThat(gauge.isStealingAllowed()).isFalse();
        assertThat(registry.get("swath.aimd.target_reductions").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("swath.steal_reason").tag("outcome", "SHED").tag("reason", "timeout_storm")
                .counter().count()).isEqualTo(1.0);
    }
}
