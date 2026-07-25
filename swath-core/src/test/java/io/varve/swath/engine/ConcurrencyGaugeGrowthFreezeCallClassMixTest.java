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
 * FAST/deterministic gauge-level guards for the GROWTH-FREEZE call-class split on {@link
 * ConcurrencyGauge}'s transient-timeout growth-freeze. Same injected-clock/fixed-window
 * seam as {@link ConcurrencyGaugeShedCallClassMixTest} (no real sleep, per-commit tier).
 *
 * <p><b>Probe timeouts LEAVE the growth-freeze decision.</b>
 * {@code onTransientTimeout(boolean)} calls {@code markCongestion()} (ends slow-start: growth
 * reverts doubling→additive +1) and feeds the 10 s transient growth-freeze window ONLY for a WORKER
 * timeout — the {@code if (!worker)} early return excludes probe timeouts from both. A storm of PROBE
 * timeouts (a thief's slot-free structure/pivot probes, which carry no S3-backpressure signal) neither
 * SHEDs workers nor ends slow-start nor freezes the +1/doubling growth step — growth keeps climbing
 * through a probe storm. Probe timeouts still count {@code shedWindowProbeTimeouts} for diagnostics;
 * only WORKER timeouts end slow-start and feed the freeze.
 *
 * <p>Instrumentation pinned by these guards:
 * <ul>
 *   <li>{@code recordStealReason("FREEZE", "transient_timeouts")} — the growth-freeze rung's OWN
 *       attribution counter, distinct from the latency rung's existing {@code FREEZE/latency_inflation},
 *       so post-hoc can tell WHICH freeze rung suppressed a growth step.</li>
 *   <li>{@code recordStealReason("GROWTH", "probe_timeout_excluded")} — the engagement counter,
 *       fired when a probe timeout is excluded from congestion/freeze.</li>
 * </ul>
 */
final class ConcurrencyGaugeGrowthFreezeCallClassMixTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);

    private ConcurrencyGauge newGauge(int tMax, int initialT, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, initialT);
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    /** One PACED successful page: advance the injected clock past the +1 pace interval, then report a 200. */
    private void pacedSuccess(ConcurrencyGauge gauge) {
        now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS + 1L);
        gauge.reportStatus(200);   // non-503 -> onSuccess (a real completed worker page)
    }

    /**
     * A pure PROBE-timeout storm must NOT freeze growth: after ≥3 probe timeouts within the window,
     * paced worker successes must keep growing {@code effectiveT}. <b>Mutant caught:</b> an impl that
     * feeds probe timeouts into the transient growth-freeze window ({@code transientWindowCount >= 3})
     * would have every subsequent success return at the {@code growthFrozen()} gate, pinning {@code T}
     * at its initial 4.
     */
    @Test
    void pureProbeStorm_doesNotFreezeGrowth() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 4, metrics);

        // 3 probe timeouts at one instant (all inside the 10 s transient window). They are
        // fully excluded from the freeze feed.
        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(false);
        }
        // 3 paced worker successes across ~3 s (well within the 10 s window). Growth is unfrozen (and
        // slow-start intact) so T climbs 4 -> 8 -> 16 -> 32.
        for (int i = 0; i < 3; i++) {
            pacedSuccess(gauge);
        }

        assertThat(gauge.effectiveT())
                .as("a pure probe-timeout storm does NOT freeze growth -- worker successes still "
                        + "climb T above the initial 4 (before probe timeouts were excluded, the 3 "
                        + "probes froze it at 4)")
                .isGreaterThan(4);
    }

    /**
     * A pure PROBE-timeout storm must NOT end slow-start. Observed two ways: the
     * {@code AIMD/slow_start_exit_congestion} latch counter must stay 0 (probes never call
     * {@code markCongestion()}), and the growth that follows must be MULTIPLICATIVE
     * ({@code AIMD/slow_start_double} fires once per paced success), i.e. 4→8→16→32 not 4→5→6→7.
     * This also catches a PARTIAL fix that only drops the freeze feed but forgets the
     * {@code markCongestion} exclusion: that would show {@code exit_congestion=1} and additive growth.
     * <b>Mutant caught:</b> an impl that lets the first probe latch congestion (exit_congestion=1) and
     * freezes the window (slow_start_double=0, T pinned at 4).
     */
    @Test
    void pureProbeStorm_doesNotEndSlowStart() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 4, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(false);
        }
        for (int i = 0; i < 3; i++) {
            pacedSuccess(gauge);
        }

        assertThat(stealReason(metrics, "AIMD", "slow_start_exit_congestion"))
                .as("probe timeouts do NOT end slow-start -- congestion latch never flips "
                        + "(before probe timeouts were excluded, the first probe flipped it to 1)")
                .isZero();
        assertThat(stealReason(metrics, "AIMD", "slow_start_double"))
                .as("slow-start intact -> the 3 paced successes DOUBLE (before probe timeouts were "
                        + "excluded: 0, frozen)")
                .isEqualTo(3.0);
        assertThat(gauge.effectiveT())
                .as("doubling out of the initial 4 reaches 32, not additive 7 (partial-fix guard)")
                .isEqualTo(32);
    }

    /**
     * A WORKER-timeout storm freezes growth. At initial {@code T=200} the
     * sustained-timeout shed gate is {@code ceil(0.3*200)=60}, so 3 worker timeouts (the freeze
     * threshold) are far below it and no SHED fires — this isolates the FREEZE from the shed. The 3
     * worker timeouts push {@code transientWindowCount} to 3, so every subsequent success returns at the
     * {@code growthFrozen()} gate and {@code T} holds at 200 (never climbs toward Tmax=256).
     */
    @Test
    void workerStorm_stillFreezesGrowth() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);   // worker leg -- freezes growth (and, at T=200, sheds nothing)
        }
        // Successes at the same instant (inside the window): each returns at the freeze gate, no growth.
        for (int i = 0; i < 3; i++) {
            gauge.reportStatus(200);
        }

        assertThat(gauge.effectiveT())
                .as("worker storm freezes growth -- T holds at 200 (unaffected by excluding probe-class "
                        + "timeouts from the freeze)")
                .isEqualTo(200);
        assertThat(stealReason(metrics, "SHED", "timeout_storm"))
                .as("3 worker timeouts are far below the T=200 shed gate (60): this is a FREEZE, not a shed")
                .isZero();
    }

    /**
     * The growth-freeze attribution counter {@code FREEZE/transient_timeouts} fires when a
     * WORKER-storm freeze suppresses a genuine growth step. Setup mirrors {@link
     * #workerStorm_stillFreezesGrowth}: 3 worker timeouts at {@code T=200} (below the shed gate 60) freeze
     * growth; one success below Tmax and past the throttle cool-down is a genuine +1 opportunity held by
     * the freeze. The latency rung's counter must NOT be confused (no latency samples were fed, so
     * {@code FREEZE/latency_inflation} stays 0).
     */
    @Test
    void growthFreezeCounter_firesOnWorkerFreeze() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);
        }
        gauge.reportStatus(200);   // one genuine growth step (T=200<256, cool-down clear) held by the freeze

        assertThat(stealReason(metrics, "FREEZE", "transient_timeouts"))
                .as("the growth-freeze rung records its OWN attribution counter, once per suppressed "
                        + "growth step (RED today -- the counter does not exist yet)")
                .isEqualTo(1.0);
        assertThat(stealReason(metrics, "FREEZE", "latency_inflation"))
                .as("the transient-timeout freeze is DISTINCT from the latency-inflation rung (no latency "
                        + "samples were fed here)")
                .isZero();
        assertThat(metrics.registry().find("swath.aimd.growth_freeze").counter().count())
                .as("the scalar swath.aimd.growth_freeze counter (JsonRunSummaryWriter's growth_freezes "
                        + "rollup source) fires alongside the FREEZE/transient_timeouts steal-reason above -- "
                        + "a mutation deleting recordGrowthFreeze() must fail this assertion even if the "
                        + "steal-reason tag survives")
                .isEqualTo(1.0);
    }

    /**
     * Both freeze rungs answer DIFFERENT questions (latency-inflation vs the transient-timeout count)
     * and are evaluated independently in {@code onSuccess()} — by design, BOTH may fire on the very
     * same suppressed growth step. Drive the latency rung frozen (baseline established, then samples
     * > 2x baseline) AND the transient rung frozen (3 WORKER timeouts within the window) before a
     * single suppressed success, and assert all four counters (both steal-reason tags and both scalar
     * counters) land on that one step. <b>Mutant caught:</b> an accidental {@code else}/exclusivity
     * between the two rungs would zero out one side.
     */
    @Test
    void bothRungsFrozen_bothFreezeCountersFireOnSameStep() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);   // below Tmax=256, no throttle cool-down active

        // Transient rung: 3 WORKER timeouts within the 10s window (>= TRANSIENT_FREEZE_THRESHOLD).
        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);
        }
        // Latency rung: establish a 100ms baseline, then inflate the EWMA past 2x (200ms).
        gauge.onAttemptLatency(100_000_000L);
        for (int i = 0; i < 5; i++) {
            gauge.onAttemptLatency(300_000_000L);
        }

        gauge.reportStatus(200);   // one success: below Tmax, no cool-down -> onSuccess evaluates BOTH rungs

        assertThat(stealReason(metrics, "FREEZE", "latency_inflation"))
                .as("the latency rung fired on the suppressed step").isEqualTo(1.0);
        assertThat(stealReason(metrics, "FREEZE", "transient_timeouts"))
                .as("the transient-timeout rung ALSO fired on the SAME suppressed step (non-exclusive)")
                .isEqualTo(1.0);
        assertThat(metrics.registry().find("swath.aimd.growth_freeze").counter().count())
                .as("the scalar growth_freeze counter fired alongside").isEqualTo(1.0);
        assertThat(metrics.registry().find("swath.aimd.latency_freeze").counter().count())
                .as("the scalar latency_freeze counter fired alongside").isEqualTo(1.0);
    }

    /**
     * The engagement counter {@code GROWTH/probe_timeout_excluded} fires once per probe timeout
     * that is excluded from congestion/freeze. As corroboration that the exclusion actually took
     * effect, the congestion latch must not have flipped ({@code AIMD/slow_start_exit_congestion == 0}).
     */
    @Test
    void probeExclusion_counterFires() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 4, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(false);
        }

        assertThat(stealReason(metrics, "GROWTH", "probe_timeout_excluded"))
                .as("each excluded probe timeout records the engagement counter (RED today -- the "
                        + "counter does not exist yet)")
                .isEqualTo(3.0);
        assertThat(stealReason(metrics, "AIMD", "slow_start_exit_congestion"))
                .as("corroboration: the excluded probes never latched congestion")
                .isZero();
    }

    /**
     * Mixed storm: the WORKER leg ALONE dictates the freeze. 2 worker + 5 probe timeouts arrive
     * interleaved at {@code T=200}. Only the 2 worker timeouts feed the freeze window
     * ({@code count=2}, below the threshold of 3), so growth is NOT frozen and the next success takes
     * an additive +1 (congestion latched by the 2 workers) → {@code T=201}. <b>Mutant caught:</b> an
     * impl that lets probe timeouts feed the window too would count all 7 ({@code count=7 >= 3}),
     * freeze growth, and hold {@code T} at 200.
     */
    @Test
    void mixedStorm_workerLegAloneDrivesFreeze() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);

        // Interleave 2 worker timeouts among 5 probe timeouts (all at one instant, inside the window).
        gauge.onTransientTimeout(false);
        gauge.onTransientTimeout(true);
        gauge.onTransientTimeout(false);
        gauge.onTransientTimeout(false);
        gauge.onTransientTimeout(true);
        gauge.onTransientTimeout(false);
        gauge.onTransientTimeout(false);

        gauge.reportStatus(200);   // one growth step: not frozen (worker count 2 < 3)

        assertThat(gauge.effectiveT())
                .as("only the 2 WORKER timeouts feed the freeze (2 < 3 threshold), so growth resumes "
                        + "with an additive +1 -> 201; the 5 probes do not count (before the exclusion: "
                        + "frozen at 200)")
                .isEqualTo(201);
    }
}
