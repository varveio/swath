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
 * FAST/deterministic unit tests for the Vegas-style latency-freeze rung on
 * {@link ConcurrencyGauge}. These use the injected clock + direct latency feeds (no real sleep, no
 * real fetch), so they stay in the per-commit tier. They cover the ORDINARY rung behavior:
 * baseline establishment, a 2× inflation freezing a would-be +1 (T held — NEVER multiplied down —
 * until the progress-gated valve admits a paced additive +1; see {@link
 * #latencyInflation_freezesGrowth_tHeldNotLowered}), stable latency NOT freezing, a censored timed-out
 * attempt never polluting the baseline, and the rolling-minimum baseline decaying upward when the true
 * floor genuinely rises. {@link ConcurrencyGaugeFrozenGrowthValveTest} covers the adversarial
 * cross-cutting cases and the valve's dedicated behavior.
 */
final class ConcurrencyGaugeLatencyFreezeTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    private static final long DECAY = ConcurrencyGauge.LATENCY_BASELINE_DECAY_NANOS;

    private static final long MS_50 = 50_000_000L;
    private static final long MS_100 = 100_000_000L;
    private static final long MS_200 = 200_000_000L;
    private static final long MS_300 = 300_000_000L;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, ConcurrencyGauge.defaultInitialT(tMax));
    }

    private static double latencyFreezes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.latency_freeze").counter().count();
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double baselineMs(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.latency_baseline_ms").gauge().value();
    }

    private static void feed(ConcurrencyGauge gauge, long nanos, int times) {
        for (int i = 0; i < times; i++) {
            gauge.onAttemptLatency(nanos);
        }
    }

    /** Reduce T below Tmax (via a real 503) and clear the cool-down so onSuccess is free to grow again. */
    private ConcurrencyGauge belowCeilingGauge(int tMax, RunMetrics metrics) {
        ConcurrencyGauge gauge = newGauge(tMax, metrics);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // T := floor(0.7*T)
        gauge.forceCleanWindow();                               // clear the 503 cool-down
        return gauge;
    }

    // ---- baseline establishes from successful samples ---------------------------

    @Test
    void baselineEstablishesFromFirstSuccessfulSample() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);

        gauge.onAttemptLatency(MS_100);
        assertThat(baselineMs(metrics)).as("first success seeds the baseline").isEqualTo(100.0);

        // A slower sample does NOT raise the rolling minimum.
        gauge.onAttemptLatency(MS_300);
        assertThat(baselineMs(metrics)).as("a slower sample never raises the rolling min").isEqualTo(100.0);

        // A faster sample floors it immediately (fast-down).
        gauge.onAttemptLatency(MS_50);
        assertThat(baselineMs(metrics)).as("a genuine new minimum floors the baseline").isEqualTo(50.0);
    }

    // ---- a 2x inflation freezes the +1 -- a DAMPER, not a latch ------------------
    // The freeze itself NEVER multiplies T down (the 503 reduction above is the only decrease in this
    // test, and it sticks). The latency-inflation rung is a progress-gated relaxation valve
    // (ConcurrencyGauge#VALVE_PACE_NANOS / #lastValveGrowthNs) — while latency-frozen (and not also
    // growth-frozen) it holds the +1 ONLY until the run has both made progress (shedWindowSuccesses
    // above the starvation gate) and the ~30s valve cool-down has elapsed, then admits ONE paced
    // additive +1 (never a double).

    @Test
    void latencyInflation_freezesGrowth_tHeldNotLowered() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = belowCeilingGauge(64, metrics);
        int reduced = gauge.effectiveT();               // 2 = floor(0.7*4) (slow-start initial T=4)
        assertThat(reduced).isLessThan(64);

        feed(gauge, MS_100, 1);                          // baseline := 100ms
        feed(gauge, MS_300, 5);                          // EWMA climbs > 2*100ms = 200ms

        gauge.reportStatus(200);                         // onSuccess: would +1, but latency-frozen and starved
        assertThat(gauge.effectiveT()).as("+1 growth is frozen — T held (starved: first success in the "
                + "window, valve's progress gate not yet cleared)").isEqualTo(reduced);
        assertThat(gauge.availablePermits()).as("freeze NEVER decreases T/permits").isEqualTo(reduced);
        assertThat(latencyFreezes(metrics)).as("the rung engaged once per suppressed +1").isEqualTo(1.0);

        // A second suppressed success clears the valve's progress (starvation) gate; with the pace
        // auto-passing on this gauge's first valve step (lastValveGrowthNs starts at 0), the valve now
        // legitimately admits its one paced additive +1 — a damper relaxing, not the old forever-latch.
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("progress clears the valve's starvation gate -- the "
                + "damper admits one paced additive +1 (2->3), NOT a permanent latch").isEqualTo(reduced + 1);
        assertThat(latencyFreezes(metrics)).as("latency_inflation still fires on every frozen opportunity, "
                + "valve-admitted or held (consistent before and after the change)").isEqualTo(2.0);
        assertThat(stealReason(metrics, "GROWTH", "frozen_growth_valve"))
                .as("the valve's own engagement counter fired exactly once, for the admitted step")
                .isEqualTo(1.0);
    }

    // ---- stable latency does NOT freeze (growth resumes) ------------------------

    @Test
    void stableLatency_doesNotFreeze_growthResumes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = belowCeilingGauge(64, metrics);
        int reduced = gauge.effectiveT();

        feed(gauge, MS_100, 5);                          // baseline 100ms, EWMA ~100ms (< 200ms)

        gauge.reportStatus(200);                         // onSuccess: not frozen → +1
        assertThat(gauge.effectiveT()).as("stable latency lets growth resume").isEqualTo(reduced + 1);
        assertThat(latencyFreezes(metrics)).as("the rung did not engage").isEqualTo(0.0);
    }

    // ---- a timed-out (censored ≥10s) attempt does NOT pollute the baseline -------

    @Test
    void timedOutAttempt_doesNotPolluteBaseline() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = belowCeilingGauge(64, metrics);
        int reduced = gauge.effectiveT();

        feed(gauge, MS_100, 3);                          // baseline 100ms from successful samples
        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout();                  // censored ≥10s — must NEVER enter the baseline/EWMA
        }
        assertThat(baselineMs(metrics)).as("timeouts never raise the healthy baseline").isEqualTo(100.0);

        // The EWMA is likewise untouched: a success now records NO latency freeze. (Had the 10s
        // censored timeouts poisoned the EWMA, it would sit >> 2*100ms and the rung WOULD engage.)
        // T itself is held here by the independent transient-count growth-freeze — expected, and
        // orthogonal to the latency claim under test.
        gauge.reportStatus(200);
        assertThat(latencyFreezes(metrics)).as("the latency rung did not engage — EWMA not polluted")
                .isEqualTo(0.0);
        assertThat(reduced).isLessThan(64);              // sanity: we were genuinely below Tmax
    }

    // ---- baseline decays upward when the true floor genuinely rises -------------

    @Test
    void baselineDecaysUpward_whenTrueFloorRises() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);

        gauge.onAttemptLatency(MS_50);                   // anomalously-fast seed: baseline 50ms
        assertThat(baselineMs(metrics)).isEqualTo(50.0);

        // The environment's true floor rises to 200ms. Within window 1 the baseline is pinned at the
        // seed (rolling min), then re-floors to the window's min (still 50ms — it contained the seed).
        feed(gauge, MS_200, 3);
        now.addAndGet(DECAY + 1);
        gauge.onAttemptLatency(MS_200);                  // rolls window 1 → baseline still 50ms
        assertThat(baselineMs(metrics)).as("first roll still remembers the anomalous seed").isEqualTo(50.0);

        // Window 2 sees only the raised floor; the next roll re-floors to 200ms (seed forgotten).
        feed(gauge, MS_200, 3);
        now.addAndGet(DECAY + 1);
        gauge.onAttemptLatency(MS_200);                  // rolls window 2 → baseline 200ms
        assertThat(baselineMs(metrics)).as("baseline decays upward to the genuine new floor").isEqualTo(200.0);
    }

    // ---- the freeze THAWS: it is a GROWTH GATE, not a forever-latch --------------------------------
    // The latency-freeze rung suppresses +1 growth while success-latency is inflated; it must never
    // latch permanently (that would pin T below Tmax forever after one transient blip). Two independent
    // thaw paths, each disproving a forever-latch:

    /**
     * THAW path 1 — inflate → normal → +1 resumes. A latency inflation freezes the +1 (T held); then a
     * run of NORMAL (baseline-or-below) successful latencies pulls the EWMA back under {@code
     * 2×baseline}, so the freeze clears and the paced +1 growth resumes. A forever-latch mutant (once
     * frozen, stay frozen) keeps T pinned and FAILS the {@code reduced+1} assertion. The +1 pace is
     * exercised too: a second immediate success is paced out, and only a success one {@code
     * GROWTH_PACE_NANOS} later grows again.
     */
    @Test
    void latencyFreeze_thaws_whenLatencyReturnsToNormal_growthResumes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = belowCeilingGauge(64, metrics);
        int reduced = gauge.effectiveT();
        assertThat(reduced).isLessThan(64);

        feed(gauge, MS_100, 1);                          // baseline := 100ms
        feed(gauge, MS_300, 5);                          // EWMA climbs > 2*100ms = 200ms → frozen
        gauge.reportStatus(200);                         // onSuccess: +1 suppressed
        assertThat(gauge.effectiveT()).as("latency inflation freezes the +1").isEqualTo(reduced);
        assertThat(latencyFreezes(metrics)).as("the rung engaged once").isEqualTo(1.0);

        // Latency returns to normal: the EWMA relaxes back below 2*baseline (baseline still 100ms).
        feed(gauge, MS_100, 6);
        gauge.reportStatus(200);                         // onSuccess: no longer frozen → +1 resumes
        assertThat(gauge.effectiveT()).as("freeze thaws when latency normalizes — +1 resumes")
                .isEqualTo(reduced + 1);
        assertThat(latencyFreezes(metrics)).as("no further freeze once thawed").isEqualTo(1.0);

        // +1/s pacing holds after the thaw: a second immediate success does NOT grow again...
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("the +1 is paced — a second immediate success is held")
                .isEqualTo(reduced + 1);
        // ...but one GROWTH_PACE_NANOS later it does.
        now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS + 1);
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("after the pace interval, growth continues").isEqualTo(reduced + 2);
    }

    /**
     * THAW path 2 — sustained-high re-floors the baseline → thaws. When latency inflates and then STAYS
     * high (a genuinely-raised environmental floor, not a transient blip), the rolling-minimum baseline
     * decays UPWARD across successive {@link ConcurrencyGauge#LATENCY_BASELINE_DECAY_NANOS} windows to
     * the new sustained level; once the baseline re-floors to ~the sustained latency, {@code 2×baseline}
     * exceeds the EWMA and the freeze thaws — growth resumes at the new floor. A never-thaw mutant that
     * skips the baseline re-floor keeps the baseline pinned low, so the freeze holds forever and the
     * {@code reduced+1} assertion FAILS.
     */
    @Test
    void latencyFreeze_thaws_whenSustainedHighReFloorsTheBaseline_growthResumes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = belowCeilingGauge(64, metrics);
        int reduced = gauge.effectiveT();
        assertThat(reduced).isLessThan(64);

        gauge.onAttemptLatency(MS_100);                  // baseline := 100ms (the OLD, low floor)
        feed(gauge, MS_300, 5);                          // sustained-high: EWMA > 2*100ms → frozen
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("the raised latency freezes the +1").isEqualTo(reduced);
        assertThat(latencyFreezes(metrics)).isEqualTo(1.0);

        // Hold HIGH across a full decay window: window 1's min still remembers the 100ms seed, so the
        // first roll re-floors to 100ms (still frozen).
        now.addAndGet(DECAY + 1);
        gauge.onAttemptLatency(MS_300);
        assertThat(baselineMs(metrics)).as("first roll still remembers the low seed").isEqualTo(100.0);
        // Window 2 sees only the sustained 300ms floor; the next roll re-floors the baseline to 300ms.
        feed(gauge, MS_300, 3);
        now.addAndGet(DECAY + 1);
        gauge.onAttemptLatency(MS_300);
        assertThat(baselineMs(metrics)).as("sustained-high re-floors the baseline to the new floor")
                .isEqualTo(300.0);

        // Now 2*baseline (600ms) exceeds the ~300ms EWMA → the freeze thaws → +1 resumes at the new floor.
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("re-floored baseline thaws the freeze — +1 resumes")
                .isEqualTo(reduced + 1);
    }
}
