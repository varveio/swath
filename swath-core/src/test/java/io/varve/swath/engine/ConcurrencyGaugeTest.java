/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.ConcurrencyGauges;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConcurrencyGauge} (algorithms.md §5).
 *
 * <p>These tests cover the gauge's own policy logic — 503 decrease, floor clamping,
 * clean-window +1 recovery, and the stealing-allowed toggle. They do NOT cover
 * THR-1 (sustained 503 injection end-to-end with {@code MockPageFetcher}), which
 * is covered separately.
 */
final class ConcurrencyGaugeTest {

    // ---- AIMD decrease ----------------------------------------------------------

    @Test
    void throttle_decreases_T_by_factor_and_floors_at_1() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(4);
        assertThat(gauge.effectiveT()).isEqualTo(4);

        // floor(0.7 * 4) = floor(2.8) = 2
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(2);

        // floor(0.7 * 2) = floor(1.4) = 1
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(1);

        // floor(0.7 * 1) = 0 → clamped to floor 1
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(1);
    }

    @Test
    void throttle_floors_at_1_for_T_1() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(1);
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(1);
    }

    // ---- stealing-allowed toggle ------------------------------------------------

    @Test
    void stealing_allowed_is_true_initially() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(4);
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    @Test
    void throttle_disables_stealing() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(4);
        gauge.reportStatus(503);
        assertThat(gauge.isStealingAllowed()).isFalse();
    }

    // ---- clean-window recovery --------------------------------------------------

    @Test
    void clean_window_restores_T_by_one_and_re_enables_stealing() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(4);

        // Reduce T to 2
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(2);
        assertThat(gauge.isStealingAllowed()).isFalse();

        // Simulate a clean window (no 503 for the required interval)
        gauge.forceCleanWindow();

        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).isEqualTo(3);   // +1 recovery step
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    @Test
    void clean_window_recovers_T_to_tmax_over_multiple_steps() {
        // The +1 is paced to at most one step per GROWTH_PACE_NANOS (see the regrowth-pacing notes
        // below), so a multi-step recovery needs the injected clock advanced a full pace between steps.
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, new RunMetrics(new SimpleMeterRegistry()),
                now::get, () -> ConcurrencyGauge.SHED_WINDOW_BASE_NANOS, ConcurrencyGauge.defaultInitialT(4));

        // Drive to floor
        gauge.reportStatus(503);
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT()).isEqualTo(1);

        // Three recovery steps: 1 → 2 → 3 → 4, one +1 per pace interval.
        for (int i = 0; i < 3; i++) {
            gauge.forceCleanWindow();
            gauge.reportStatus(200);
            now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS);
        }
        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    @Test
    void clean_window_does_not_exceed_tmax() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(2);

        // T starts at 2 (= Tmax); success calls should not increase beyond Tmax
        gauge.forceCleanWindow();
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).isEqualTo(2);   // already at ceiling
    }

    @Test
    void success_during_cool_down_does_not_recover_T() {
        ConcurrencyGauge gauge = ConcurrencyGauges.of(4);
        gauge.reportStatus(503);
        int tAfterThrottle = gauge.effectiveT();

        // Success called WITHOUT forceCleanWindow — the throttle timestamp is recent,
        // so the cool-down window has not elapsed and T must not increase.
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).isEqualTo(tAfterThrottle);
        assertThat(gauge.isStealingAllowed()).isFalse();
    }

    // ---- Regrowth pacing: +1 is uniform +1/s, not per-success -------------------
    // The +1 additive increase is throttled to at most one step per GROWTH_PACE_NANOS on the injected
    // clock. These use the injected-clock seam so the pacing is exercised deterministically with no real
    // sleep. Firing the +1 on every success once the cool-down clears would rocket T back to Tmax in
    // seconds — combined with the window-latched timeout shed, the pure-timeout regime would
    // relaxation-oscillate instead of holding a sustainable width.

    /** Seam gauge on an injected clock + fixed shed window; the shed window never rolls in these tests. */
    private static ConcurrencyGauge pacedGauge(int tMax, AtomicLong now) {
        return new ConcurrencyGauge(tMax, new RunMetrics(new SimpleMeterRegistry()),
                now::get, () -> ConcurrencyGauge.SHED_WINDOW_BASE_NANOS, ConcurrencyGauge.defaultInitialT(tMax));
    }

    /**
     * Ramps T up from the slow-start floor to {@code tMax} via clean doublings (advancing the clock
     * one pace per step so the paced growth proceeds), giving a test that exercises POST-congestion
     * additive pacing a Tmax starting state undisturbed by the slow-start ramp.
     */
    private static void rampToTmax(ConcurrencyGauge gauge, AtomicLong now, int tMax) {
        while (gauge.effectiveT() < tMax) {
            gauge.reportStatus(200);
            now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS);
        }
    }

    /** Drive T to the floor of 1 with repeated real-503 decreases, then clear the cool-down. */
    private static void toFloorThenClean(ConcurrencyGauge gauge) {
        while (gauge.effectiveT() > 1) {
            gauge.reportStatus(503);
        }
        gauge.forceCleanWindow();
    }

    @Test
    void growth_isPaced_manySuccessesAtOneInstant_grantAtMostOnePlusOne() {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        ConcurrencyGauge gauge = pacedGauge(8, now);
        rampToTmax(gauge, now, 8);               // Slow-start begins at 4; ramp to Tmax=8 first.
        gauge.reportStatus(503);                 // T := floor(0.7*8) = 5 (the 503 also ends slow-start)
        gauge.forceCleanWindow();
        int reduced = gauge.effectiveT();
        assertThat(reduced).isEqualTo(5);

        // 50 successes at ONE fixed clock instant: the first +1 fires (stale lastGrowthNs), the rest are
        // paced out — T rises by exactly 1, never by N.
        for (int i = 0; i < 50; i++) {
            gauge.reportStatus(200);
        }
        assertThat(gauge.effectiveT())
                .as("a burst of successes at one instant grants AT MOST one +1")
                .isEqualTo(reduced + 1);
    }

    @Test
    void growth_climbsLinearly_whenClockAdvancesByOnePacePerSuccess() {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        ConcurrencyGauge gauge = pacedGauge(8, now);
        rampToTmax(gauge, now, 8);               // Ramp out of slow-start to Tmax=8 first.
        gauge.reportStatus(503);                 // T := 5 (also ends slow-start → additive recovery below)
        gauge.forceCleanWindow();

        // Advancing the clock a full pace between successes grants +1 each: 5 → 6 → 7 → 8, then capped.
        int[] expected = {6, 7, 8, 8};
        for (int i = 0; i < expected.length; i++) {
            gauge.reportStatus(200);
            assertThat(gauge.effectiveT()).as("paced step #%d", i + 1).isEqualTo(expected[i]);
            now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS);
        }
    }

    @Test
    void growth_neverRocketsToTmax_inASameInstantBurst() {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        ConcurrencyGauge gauge = pacedGauge(256, now);
        toFloorThenClean(gauge);
        assertThat(gauge.effectiveT()).isEqualTo(1);

        // 1000 successes at ONE instant must NOT rocket T anywhere near Tmax — exactly one +1 lands.
        for (int i = 0; i < 1000; i++) {
            gauge.reportStatus(200);
        }
        assertThat(gauge.effectiveT())
                .as("a same-instant burst never rockets T to Tmax — the pace holds it to one +1")
                .isEqualTo(2);
    }

    @Test
    void recovery_fromFloorToTmax_takesAboutTmaxPaceSteps() {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        int tMax = 16;
        ConcurrencyGauge gauge = pacedGauge(tMax, now);
        toFloorThenClean(gauge);
        assertThat(gauge.effectiveT()).isEqualTo(1);

        // One +1 per pace step: T == 1 + steps. Spot-check a few points along the linear climb, and that
        // reaching Tmax from the floor takes ~Tmax-1 paced steps (15 for 1 → 16).
        for (int step = 1; step <= 15; step++) {
            gauge.reportStatus(200);
            assertThat(gauge.effectiveT()).as("after %d paced steps", step).isEqualTo(Math.min(tMax, 1 + step));
            now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS);
        }
        assertThat(gauge.effectiveT()).as("recovery reaches Tmax after ~Tmax pace-steps").isEqualTo(tMax);
    }

    // ---- constants accessible for THR-1 cross-check ----------------------------

    @Test
    void constants_match_spec() {
        assertThat(ConcurrencyGauge.DECREASE_FACTOR).isEqualTo(0.7);
        assertThat(ConcurrencyGauge.INCREASE_STEP).isEqualTo(1);
        assertThat(ConcurrencyGauge.CLEAN_WINDOW_NANOS).isEqualTo(10_000_000_000L);
        assertThat(ConcurrencyGauge.GROWTH_PACE_NANOS).isEqualTo(1_000_000_000L);
        assertThat(ConcurrencyGauge.SLOWDOWN_STATUS).isEqualTo(503);
    }

    // ---- unified throttle metric: single recording site + honest AIMD vote ------
    // The gauge is PURELY the AIMD controller: the swath.throttle.events{type} EVENT is recorded
    // exactly once at its S3PageFetcher classification site, never at the gauge. The gauge only
    // casts the honest AIMD down-vote (swath.aimd.votes) for genuine backpressure.

    @Test
    void reportStatus_503_drivesAimdVote_butDoesNotRecordTheThrottleEvent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);

        gauge.reportStatus(503);

        // The gauge reacts to the 503 (0.7-factor decrease + one honest AIMD vote)...
        assertThat(gauge.effectiveT()).isEqualTo(2);
        assertThat(gauge.isStealingAllowed()).isFalse();
        assertThat(registry.find("swath.aimd.votes").counter().count()).isEqualTo(1.0);
        // ...but it must NOT record the throttle EVENT itself — that is the fetcher's single site,
        // so no double-count on swath.throttle.events{type}.
        assertThat(metrics.throttleEventsTotal()).isEqualTo(0.0);
    }

    @Test
    void onTransientTimeout_doesNotVoteAimdDown_norRecordAnEvent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);

        gauge.onTransientTimeout();

        // A client attempt-timeout is NOT store backpressure: it must not multiply T down, must
        // not cast an AIMD vote, and records no throttle event at the gauge. The typed
        // observability lives on the fetcher's swath.throttle.events{type=attempt_timeout} series;
        // the gauge only tracks the growth-freeze window internally.
        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(gauge.isStealingAllowed()).isTrue();
        assertThat(registry.find("swath.aimd.votes").counter().count()).isEqualTo(0.0);
        assertThat(metrics.throttleEventsTotal()).isEqualTo(0.0);
    }
}
