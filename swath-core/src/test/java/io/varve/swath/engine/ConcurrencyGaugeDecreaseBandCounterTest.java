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
 * FAST/deterministic gauge-level guards for the T-band engagement counters, the
 * {@code floor_noop_rearm} numeric-no-op observable, and the {@code growth_blocked_cooldown}
 * suppressed-growth counter added to {@link ConcurrencyGauge#multiplicativeDecrease} /
 * {@link ConcurrencyGauge#onSuccess}. Pure counters-first instrumentation (repo §5 rule): ZERO
 * behavior change, so every assertion on {@code effectiveT()} here pins the PRE-EXISTING AIMD
 * arithmetic, unchanged by this unit — only the new {@code swath.steal_reason{outcome="AIMD",...}}
 * emissions are new. Same injected-clock / fixed shed-window seam as {@link ConcurrencyGaugeShedTest}
 * / {@link ConcurrencyGaugeFrozenGrowthValveTest} (no real sleep, per-commit tier).
 */
final class ConcurrencyGaugeDecreaseBandCounterTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, int initialT, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, initialT);
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double atFloor(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "decrease_at_floor");
    }

    private static double lowT(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "decrease_low_t");
    }

    private static double midT(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "decrease_mid_t");
    }

    private static double highT(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "decrease_high_t");
    }

    private static double floorNoopRearm(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "floor_noop_rearm");
    }

    private static double growthBlockedCooldown(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "growth_blocked_cooldown");
    }

    // ---- (a) T-band engagement, tagged by the T value read at entry -------------------------------

    @Test
    void decreaseHighT_firesAt33_andOnlyThatBand() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 33, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T=33 -> floor(0.7*33)=23

        assertThat(gauge.effectiveT()).as("AIMD arithmetic is unchanged by this instrumentation unit").isEqualTo(23);
        assertThat(highT(metrics)).as("33 > 32 -> high_t band").isEqualTo(1.0);
        assertThat(atFloor(metrics)).isZero();
        assertThat(lowT(metrics)).isZero();
        assertThat(midT(metrics)).isZero();
        assertThat(floorNoopRearm(metrics)).as("T actually moved (33->23): no floor no-op").isZero();
    }

    @Test
    void decreaseMidT_firesAt10() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 10, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T=10 -> floor(0.7*10)=7

        assertThat(gauge.effectiveT()).isEqualTo(7);
        assertThat(midT(metrics)).as("9 <= 10 <= 32 -> mid_t band").isEqualTo(1.0);
        assertThat(atFloor(metrics)).isZero();
        assertThat(lowT(metrics)).isZero();
        assertThat(highT(metrics)).isZero();
        assertThat(floorNoopRearm(metrics)).as("T actually moved (10->7): no floor no-op (case (b))").isZero();
    }

    @Test
    void decreaseLowT_firesAt3() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 3, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T=3 -> floor(0.7*3)=2

        assertThat(gauge.effectiveT()).isEqualTo(2);
        assertThat(lowT(metrics)).as("3 <= 3 <= 8 -> low_t band").isEqualTo(1.0);
        assertThat(atFloor(metrics)).isZero();
        assertThat(midT(metrics)).isZero();
        assertThat(highT(metrics)).isZero();
        assertThat(floorNoopRearm(metrics)).as("T actually moved (3->2): no floor no-op").isZero();
    }

    /**
     * At {@code T=1} the CAS loop's own arithmetic ({@code max(1, floor(0.7*1))==1 >= 1}) breaks WITHOUT
     * any CAS or {@code target_reductions} emission — but {@code markCongestion}/the vote/{@code
     * stealingAllowed=false} all still fire unconditionally upstream. The cool-down re-arm is NOT among
     * those side effects: a floor no-op removes zero concurrency and does NOT re-arm
     * {@code lastThrottleNs} (it records {@code AIMD/floor_noop_rearm} as the no-op-EVENT counter PLUS the
     * {@code AIMD/floor_rearm_suppressed} suppression counter) — a floor-pinned 503 buys NO fresh
     * cool-down.
     */
    @Test
    void decreaseAtFloor_firesAt1_andFloorNoopRearmFires() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 1, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T=1 -> next=max(1,0)=1 >= cur -> numeric no-op

        assertThat(gauge.effectiveT()).as("T pinned at the floor; unchanged by this instrumentation unit").isEqualTo(1);
        assertThat(atFloor(metrics)).as("1 <= 2 -> at_floor band").isEqualTo(1.0);
        assertThat(lowT(metrics)).isZero();
        assertThat(midT(metrics)).isZero();
        assertThat(highT(metrics)).isZero();
        assertThat(floorNoopRearm(metrics))
                .as("the decrease produced NO numeric change; floor_noop_rearm counts the no-op EVENT while "
                        + "the re-arm itself is SUPPRESSED (the vote/stealing side effects still fire, but no "
                        + "fresh cool-down is armed)")
                .isEqualTo(1.0);
        assertThat(metrics.registry().get("swath.aimd.votes").counter().count())
                .as("the down-vote itself is unaffected by this unit -- still cast even at the numeric floor")
                .isEqualTo(1.0);
    }

    // ---- (c) growth_blocked_cooldown: fires per success suppressed by the cool-down ----------------

    @Test
    void successInsideCooldown_firesGrowthBlockedCooldown_andTUnchanged() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 32, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // 503: T := floor(0.7*32) = 22; cool-down starts
        int afterThrottle = gauge.effectiveT();
        assertThat(afterThrottle).isEqualTo(22);
        assertThat(growthBlockedCooldown(metrics)).as("no success yet: not fired").isZero();

        // Still WITHIN the 10 s cool-down (no clock advance).
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT())
                .as("a success inside the cool-down must not grow T -- unchanged from the AIMD arithmetic")
                .isEqualTo(afterThrottle);
        assertThat(growthBlockedCooldown(metrics))
                .as("the suppressed growth opportunity is counted")
                .isEqualTo(1.0);

        // A second success, still inside the cool-down, increments again.
        gauge.reportStatus(200);
        assertThat(growthBlockedCooldown(metrics)).as("fires once per suppressed success").isEqualTo(2.0);

        // Advance PAST the 10 s cool-down: a success now recovers (unchanged pre-existing behavior) and
        // must NOT fire the counter again.
        now.addAndGet(ConcurrencyGauge.CLEAN_WINDOW_NANOS + 1L);
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT())
                .as("once the cool-down clears, the pre-existing +1 recovery fires -- unchanged by this unit")
                .isEqualTo(afterThrottle + 1);
        assertThat(growthBlockedCooldown(metrics))
                .as("a success after the cool-down window elapsed does not fire the suppressed-growth counter")
                .isEqualTo(2.0);
    }

    // ---- (d) shed-kind decreases are kind-agnostic: same band counters as THROTTLE ------------------

    /**
     * Drives a genuine {@link ConcurrencyGauge.DecreaseKind#TIMEOUT_SHED} decrease via the
     * starved-timeout-storm pattern ({@link ConcurrencyGaugeShedTest}'s {@code stormWindow}) and pins that
     * the SAME {@code AIMD.decrease_*} band counters fire for it, exactly as they do for a THROTTLE (503)
     * decrease -- the band/floor-noop instrumentation sits in the shared {@code multiplicativeDecrease}
     * and does not distinguish kind (kind attribution already exists separately via
     * {@code swath.aimd.votes} vs. {@code swath.aimd.timeout_shed}/{@code SHED.timeout_storm}, which this
     * unit does not duplicate).
     */
    @Test
    void shedKindDecrease_firesSameBandCounters_kindAgnostic() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 256, metrics);   // start AT Tmax, isolates from slow-start

        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout();   // starved storm: zero successes fed
        }
        now.addAndGet(WINDOW + 1);        // roll the shed window so maybeShed() evaluates the storm

        // One more timeout to trip maybeShed() on the rolled window (mirrors ConcurrencyGaugeShedTest).
        gauge.onTransientTimeout();

        assertThat(gauge.effectiveT()).as("shed fired: T := floor(0.5*256) = 128").isEqualTo(128);
        assertThat(metrics.registry().get("swath.aimd.timeout_shed").counter().count())
                .as("a genuine TIMEOUT_SHED decrease occurred").isEqualTo(1.0);
        assertThat(metrics.registry().get("swath.aimd.votes").counter().count())
                .as("a shed is never an AIMD vote (unchanged pre-existing behavior)").isZero();
        assertThat(highT(metrics))
                .as("the T-band counter fires identically for a SHED-kind decrease as it would for a "
                        + "THROTTLE-kind one at the same entry T (256 > 32 -> high_t) -- kind-agnostic by design")
                .isEqualTo(1.0);
        assertThat(floorNoopRearm(metrics)).as("T actually moved (256->128): no floor no-op").isZero();
    }
}
