/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * FAST/deterministic gauge-level guards for the PROGRESS-GATED RECOVERY VALVE that demotes the
 * latency-inflation FREEZE rung from a hard latch to a slow additive damper. Same injected-clock /
 * fixed-window seam as {@link ConcurrencyGaugeGrowthFreezeCallClassMixTest} and {@link
 * ConcurrencyGaugeLatencyFreezeTest} (no real sleep, per-commit tier).
 *
 * <p>The frozen-growth valve mechanism ships in {@code ConcurrencyGauge}.
 *
 * <p>The semantics-PRESERVATION guards ({@link #valveStaysShut_underWorkerTimeoutStorm}, {@link
 * #bothRungsFrozen_valveStaysShut_gateOrderGrowthFirst}, {@link
 * #latencyInflationCounting_unchangedByValve}) pin what must NOT change: a worker-timeout storm still
 * freezes hard, and {@code FREEZE/latency_inflation} still counts every frozen opportunity (a
 * pre/post-comparability hard constraint). Each method's javadoc states the mutant it
 * catches.
 *
 * <p><b>What the valve does.</b> While {@code latencyFrozen()} is true AND {@code
 * growthFrozen()} is false AND the run is making progress (NOT starved — the exact complement of the
 * shed starvation gate {@code shedWindowSuccesses > max(1, T/SHED_SUCCESS_DIVISOR)}), it admits ONE
 * paced additive {@code +1}, paced at {@code VALVE_PACE_NANOS == SHED_WINDOW_BASE_NANOS} (30 s). {@code
 * FREEZE/latency_inflation} still fires on every frozen opportunity (unchanged); the valve additionally
 * fires {@code GROWTH/frozen_growth_valve} on each admitted step.
 *
 * <p><b>Deterministic driving note.</b>
 * The valve's progress numerator {@code shedWindowSuccesses} RESETS on each shed-window roll ({@link
 * ConcurrencyGauge#SHED_WINDOW_BASE_NANOS} = 30 s), and the valve pace is the SAME 30 s. So advancing the
 * clock past the pace ALSO rolls the shed window and zeroes the progress counter. Feeding two
 * successes before that advance would therefore leave the run starved and never admit. The correct
 * deterministic technique, verified against the gauge source, is: (1)
 * the FIRST valve step needs no clock advance at all, because {@code lastValveGrowthNs} starts at 0 so
 * the pace auto-passes; (2) rebuild in-window progress by feeding successes AT ONE INSTANT (no clock
 * advance between them) so they accumulate rather than roll — the first same-instant success after a
 * roll is starved (count 1) and the second clears the gate (count 2) and admits. This preserves the
 * intended behavior (valve admits under latFrozen + progress + no storm). See
 * {@link #oneValveStepAtFloor}.
 *
 * <p>Three additional CONC/boundary guards close races and boundary cases: {@link
 * #concurrentSuccessBurst_singleWinner_permitsConserved} (the {@code lastValveGrowthNs} CAS elects
 * exactly one winner per pace slot under a real concurrent burst, and permits stay conserved), {@link
 * #atTMax_noValveGrowth_permitsAtCeiling} (the {@code cur >= tMax} early return upstream of the valve
 * holds even with every valve precondition otherwise met), and {@link
 * #cooldownSuppressesValve_thenAdmitsOnceCleared} (the valve sits DOWNSTREAM of the 503 clean-window
 * cool-down gate). Each javadoc states the specific mutant/regression it would catch.
 *
 * <p>One further CONC guard, {@link #mixedThrottleAndValveBurst_decreaseNeverLost_valveAtMostOneStep},
 * covers the accepted-by-design race between a valve success and a CONCURRENT {@code
 * reportStatus(503)}: it pins the BOUNDED property (the multiplicative decrease is never lost; the
 * valve adds at most one step; permits stay conserved) rather than forbidding the race, and documents
 * the one-step-inside-cooldown outcome as the accepted case.
 */
final class ConcurrencyGaugeFrozenGrowthValveTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    /**
     * The valve pace equals {@code SHED_WINDOW_BASE_NANOS}; this class references the gauge's existing
     * constant so the relationship remains explicit.
     */
    private static final long VALVE_PACE = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    private static final long GROWTH_PACE = ConcurrencyGauge.GROWTH_PACE_NANOS;
    /** Thread count for the round-2 CONC single-winner burst guard. */
    private static final int CONC_THREADS = 8;
    /** Success-thread count for the round-3 mixed 503-vs-valve race guard (plus one 503 thread). */
    private static final int MIXED_SUCCESS_THREADS = 6;
    /** Fresh-gauge iterations for the round-3 mixed race, so the interleaving is actually explored. */
    private static final int MIXED_ITERATIONS = 40;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, int initialT, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, initialT);
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double valveCount(RunMetrics metrics) {
        return stealReason(metrics, "GROWTH", "frozen_growth_valve");
    }

    private static double latencyInflationCount(RunMetrics metrics) {
        return stealReason(metrics, "FREEZE", "latency_inflation");
    }

    private static double latencyFreezeScalar(RunMetrics metrics) {
        var c = metrics.registry().find("swath.aimd.latency_freeze").counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * Drive the latency-inflation rung FROZEN: seed a low (100 ms) rolling-minimum baseline, then
     * feed successful-attempt latencies 3x higher (300 ms) so the short EWMA climbs past {@code 2x
     * baseline}. Exactly the technique the sibling {@code bothRungsFrozen…} guard uses. Uses
     * {@code onAttemptLatency} ONLY, which does NOT touch {@code shedWindowSuccesses}, so the freeze is
     * established WITHOUT granting progress — letting a test isolate the starvation gate.
     */
    private void driveLatencyFrozen(ConcurrencyGauge gauge) {
        gauge.onAttemptLatency(100_000_000L);            // baseline := 100 ms
        for (int i = 0; i < 5; i++) {
            gauge.onAttemptLatency(300_000_000L);        // EWMA climbs > 2*100 ms = 200 ms
        }
    }

    /**
     * Admit EXACTLY ONE valve {@code +1} at a target where the starvation gate is 1 (i.e. {@code T <= 63},
     * so {@code max(1, T/SHED_SUCCESS_DIVISOR) == 1}). Advances the clock past {@link #VALVE_PACE} (which
     * rolls the shed window, zeroing {@code shedWindowSuccesses}) then feeds TWO same-instant successes:
     * the first rebuilds the window count to 1 and is STARVED-held (records {@code latency_inflation}, no
     * valve step); the second reaches count 2, clears the gate, and — the pace having elapsed — admits the
     * single {@code +1}. On the FIRST call {@code lastValveGrowthNs == 0} makes the pace auto-pass; on
     * subsequent calls the {@code +VALVE_PACE+1} advance clears it. Net per call: {@code T += 1}, {@code
     * frozen_growth_valve += 1}, {@code latency_inflation += 2}.
     */
    private void oneValveStepAtFloor(ConcurrencyGauge gauge) {
        now.addAndGet(VALVE_PACE + 1L);
        gauge.reportStatus(200);   // rolls the shed window -> successes=1 -> STARVED hold (records latency_inflation)
        gauge.reportStatus(200);   // successes=2 -> progress clears -> valve admits the paced +1
    }

    // ---- 1. valve admits a paced +1 under latFrozen + progress (no worker storm) ------------------

    /**
     * The headline: under a latency-inflation-ONLY freeze with progress and no
     * worker-timeout storm, the valve demotes the latch to a damper and admits one paced additive {@code
     * +1}. {@code initialT=4} (starvation gate {@code max(1,4/32)=1}); drive {@code latFrozen}; two
     * same-instant successes build progress to 2 (> 1) and admit. <b>Mutant caught:</b> an impl that
     * returns unconditionally at the {@code latFrozen} gate would hold {@code T} at 4 and never emit
     * {@code GROWTH/frozen_growth_valve}.
     */
    @Test
    void valveAdmitsPacedPlusOne_underLatencyFreezeWithProgress() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);
        oneValveStepAtFloor(gauge);

        assertThat(gauge.effectiveT())
                .as("the latency-freeze valve: a latency-ONLY freeze with progress admits one additive "
                        + "+1 (4->5); RED today (current code returns at the latFrozen gate, T pinned at 4)")
                .isEqualTo(5);
        assertThat(valveCount(metrics))
                .as("the engagement counter fires once per admitted valve step; RED today (counter absent)")
                .isEqualTo(1.0);
    }

    // ---- 2. valve does NOT fire under a worker-timeout storm (growthFrozen) ------------------------

    /**
     * A semantics-PRESERVATION guard: pins what must NOT change. A worker-timeout
     * storm keeps the FULL hard latch: {@code growthFrozen()} is checked FIRST, before the valve, so the
     * valve NEVER relaxes a worker-storm freeze. {@code initialT=200} (3 worker timeouts are far below the
     * shed gate {@code ceil(0.3*200)=60}, so no SHED — this isolates the FREEZE); also drive {@code
     * latFrozen} and feed genuine progress (8 successes > gate {@code max(1,200/32)=6}). The discriminating
     * assertion is {@code frozen_growth_valve == 0} WITH progress present — it pins that the valve is gated
     * off by {@code growthFroz}, not merely by starvation (the {@code T}-unchanged assertion alone would
     * pass even if the valve fired). The {@code growthFroz}-first gate keeps it 0 regardless of progress.
     *
     * <p>No clock advance: advancing past the 10 s {@code TRANSIENT_WINDOW} would THAW {@code
     * growthFrozen()} and defeat the premise; and since {@code growthFroz} is checked before the valve pace,
     * no advance is needed to prove the valve stays shut.
     */
    @Test
    void valveStaysShut_underWorkerTimeoutStorm() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);              // worker leg -> growthFrozen (and, at T=200, sheds nothing)
        }
        driveLatencyFrozen(gauge);
        for (int i = 0; i < 8; i++) {
            gauge.reportStatus(200);                     // genuine progress (8 > gate 6), same instant (no roll)
        }

        assertThat(gauge.effectiveT())
                .as("worker storm hard-freezes growth -- T holds at 200 (both before and after the valve)")
                .isEqualTo(200);
        assertThat(valveCount(metrics))
                .as("DISCRIMINATING: with progress present the valve is still 0 -- it is gated OFF by "
                        + "growthFroz (checked first), not by starvation")
                .isZero();
        assertThat(stealReason(metrics, "FREEZE", "transient_timeouts"))
                .as("the worker-timeout freeze rung is intact (hard latch preserved)")
                .isGreaterThan(0.0);
        assertThat(stealReason(metrics, "SHED", "timeout_storm"))
                .as("3 worker timeouts are far below the T=200 shed gate (60): a FREEZE, not a shed")
                .isZero();
    }

    // ---- 3. valve does NOT fire without progress (starved), and DOES once progress clears ----------

    /**
     * The strongest single form of the progress gate: it pins BOTH the starvation hold
     * AND that clearing the gate admits. {@code initialT=4} (gate {@code max(1,4/32)=1}); drive {@code
     * latFrozen} via {@code onAttemptLatency} only (no progress yet). The FIRST success rolls the shed
     * window to {@code successes=1 <= 1} → STARVED → held. A SECOND same-instant success reaches {@code
     * successes=2 > 1} → progress → the valve admits (pace auto-passes, {@code lastValveGrowthNs==0}).
     */
    @Test
    void valveStarved_thenAdmitsWhenProgressClears() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);

        gauge.reportStatus(200);   // rolls window -> successes=1 <= gate 1 -> starved
        assertThat(gauge.effectiveT())
                .as("starved (1 success <= gate 1): the valve holds the freeze -- T stays 4").isEqualTo(4);
        assertThat(valveCount(metrics))
                .as("no valve step while starved").isZero();

        gauge.reportStatus(200);   // successes=2 > gate 1 -> progress clears -> valve admits +1
        assertThat(gauge.effectiveT())
                .as("progress clears the starvation gate -> valve admits +1 (4->5); RED today (stays 4)")
                .isEqualTo(5);
        assertThat(valveCount(metrics))
                .as("exactly one valve step once progress clears; RED today (counter absent)")
                .isEqualTo(1.0);
    }

    // ---- 4. pacing bound: <=1 valve +1 per VALVE_PACE, much slower than normal +1 growth -----------

    /**
     * Pins the ~30 s cadence the convergence asymmetry depends on: the valve admits at
     * most one {@code +1} per {@link #VALVE_PACE}, ~30x slower than a normal {@code +1} (which paces at
     * {@link #GROWTH_PACE} = 1 s). After one admitted step, feed 3 more successes 1 s apart (all inside the
     * 30 s window): each is progress but the valve is PACE-held, so none grows — a normal {@code +1} would
     * have fired on every 1 s step. Only after advancing a full {@code VALVE_PACE} does the next step admit
     * (to {@code +2}).
     */
    @Test
    void valvePacedToOnePerWindow_muchSlowerThanNormalGrowth() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);

        // First valve step (no advance needed; lastValveGrowthNs==0 auto-passes the pace).
        gauge.reportStatus(200);   // successes=1 -> starved hold
        gauge.reportStatus(200);   // successes=2 -> admit +1 (4->5)
        assertThat(gauge.effectiveT()).as("first valve step admits 4->5").isEqualTo(5);
        assertThat(valveCount(metrics)).isEqualTo(1.0);

        // 3 more successes 1 s apart, all within the SAME 30 s window: progress present, but PACE-held.
        for (int i = 0; i < 3; i++) {
            now.addAndGet(GROWTH_PACE + 1L);
            gauge.reportStatus(200);
        }
        assertThat(gauge.effectiveT())
                .as("valve is paced to one step per 30 s -- 3 successes 1 s apart do NOT grow T (a normal "
                        + "+1 would fire on each); RED today (never grows under latFrozen)")
                .isEqualTo(5);
        assertThat(valveCount(metrics)).as("still only one admitted step in this window").isEqualTo(1.0);

        // Advance a full VALVE_PACE past the last valve step -> the next step admits (rolls the window, so
        // rebuild progress with two same-instant successes).
        now.addAndGet(VALVE_PACE);
        gauge.reportStatus(200);   // roll -> successes=1 -> starved
        gauge.reportStatus(200);   // successes=2 -> admit the second step (5->6)
        assertThat(gauge.effectiveT())
                .as("after a full VALVE_PACE elapses, the valve admits its next +1 (5->6)").isEqualTo(6);
        assertThat(valveCount(metrics)).as("exactly two admitted steps across two pace windows").isEqualTo(2.0);
    }

    // ---- 5. counter fires per ENGAGEMENT, not per frozen opportunity -------------------------------

    /**
     * {@code GROWTH/frozen_growth_valve} counts ADMITTED steps only, distinguishing
     * it from the opportunity-counter {@code FREEZE/latency_inflation} (guarded in {@link
     * #latencyInflationCounting_unchangedByValve}). Over 3 admitted valve steps plus 2 pace-held
     * frozen successes (all {@code latFrozen}), assert {@code frozen_growth_valve == 3} — the admitted
     * count, not the 8 total frozen opportunities.
     */
    @Test
    void valveCounterFiresPerEngagement_notPerFrozenOpportunity() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities, 1 admitted (4->5)
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities, 1 admitted (5->6)
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities, 1 admitted (6->7)
        // 2 extra pace-held successes at the same instant: frozen opportunities, but NOT admitted.
        gauge.reportStatus(200);
        gauge.reportStatus(200);

        assertThat(valveCount(metrics))
                .as("the engagement counter counts the 3 ADMITTED steps, not the 8 frozen opportunities; "
                        + "RED today (counter absent)")
                .isEqualTo(3.0);
        assertThat(gauge.effectiveT()).as("three admitted +1 steps: 4->5->6->7").isEqualTo(7);
    }

    // ---- 6. FREEZE/latency_inflation semantics UNCHANGED (pre/post-comparability hard constraint) --

    /**
     * The pre/post-comparability guard. The
     * SAME success sequence as {@link #valveCounterFiresPerEngagement_notPerFrozenOpportunity} (8 frozen
     * opportunities: 3 admitted + 5 held) records {@code FREEZE/latency_inflation} on EVERY frozen
     * opportunity, valve-admitted or held, because the valve fires
     * {@code frozen_growth_valve} IN ADDITION to, never INSTEAD of, the existing {@code
     * recordLatencyFreeze()} + {@code recordStealReason("FREEZE","latency_inflation")} (which run BEFORE
     * the valve gate). <b>Mutant caught:</b> an impl that skipped {@code recordLatencyFreeze()} on
     * valve-admitted steps would report 5, not 8, and fail here.
     */
    @Test
    void latencyInflationCounting_unchangedByValve() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities
        oneValveStepAtFloor(gauge);   // 2 frozen opportunities
        gauge.reportStatus(200);      // held frozen opportunity
        gauge.reportStatus(200);      // held frozen opportunity

        assertThat(latencyInflationCount(metrics))
                .as("every frozen opportunity is counted (8), whether or not the valve then admitted -- "
                        + "the valve fires IN ADDITION to, never INSTEAD of, latency_inflation (GREEN both "
                        + "sides -- the pre/post-comparability hard constraint)")
                .isEqualTo(8.0);
        assertThat(latencyFreezeScalar(metrics))
                .as("the scalar swath.aimd.latency_freeze matches, counting every frozen opportunity")
                .isEqualTo(8.0);
    }

    // ---- 7. both rungs frozen -> valve stays shut (gate order: growthFroz before the valve) --------

    /**
     * A gate-order / semantics-PRESERVATION guard. When BOTH rungs are frozen the
     * worker-storm freeze dominates: {@code growthFroz} is checked before the valve, so the valve stays
     * shut even though a latency freeze co-fires. Mirrors the sibling {@code bothRungsFrozen…} setup (3
     * worker timeouts at {@code T=200} + latency inflated), then feeds progress and asserts BOTH {@code
     * FREEZE/latency_inflation} and {@code FREEZE/transient_timeouts} fire while {@code
     * frozen_growth_valve == 0} and {@code T} is unchanged. The {@code growthFroz}-first gate keeps the
     * valve at 0 regardless of the co-firing latency freeze. No clock advance (would
     * thaw the 10 s transient window).
     */
    @Test
    void bothRungsFrozen_valveStaysShut_gateOrderGrowthFirst() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 200, metrics);

        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);              // transient rung -> growthFrozen
        }
        driveLatencyFrozen(gauge);                       // latency rung -> latencyFrozen
        for (int i = 0; i < 8; i++) {
            gauge.reportStatus(200);                     // progress present (8 > gate 6)
        }

        assertThat(gauge.effectiveT())
                .as("both rungs frozen: the worker-storm freeze dominates -- T holds at 200").isEqualTo(200);
        assertThat(valveCount(metrics))
                .as("gate order: growthFroz is checked BEFORE the valve, so a co-firing latency freeze "
                        + "never admits under a worker storm")
                .isZero();
        assertThat(latencyInflationCount(metrics))
                .as("the latency rung still records every frozen opportunity").isGreaterThan(0.0);
        assertThat(stealReason(metrics, "FREEZE", "transient_timeouts"))
                .as("the transient-timeout rung also fires").isGreaterThan(0.0);
    }

    // ---- 8. additive-only: the valve never doubles, even when congestionSeen is false -------------

    /**
     * The valve does a DEDICATED additive {@code +1}, never routing through the
     * slow-start doubling block — a frozen-latency relaxation must be gentle (doubling out of a freeze
     * would be the rebound-storm the damper exists to prevent). {@code initialT=4}; drive {@code latFrozen}
     * via {@code onAttemptLatency} ONLY (never a worker timeout, never a 503 → the {@code congestionSeen}
     * slow-start latch stays FALSE). One admitted step must be {@code +1} (4->5), NOT a slow-start double
     * (4->8), and {@code AIMD/slow_start_double} must stay 0. <b>Mutant caught:</b> an impl that routed
     * the valve through the normal grow-block would double here (congestionSeen is false) to 8 and
     * record a slow_start_double.
     */
    @Test
    void valveIsAdditiveOnly_neverDoubles() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 4, metrics);

        driveLatencyFrozen(gauge);   // latFrozen via latency samples ONLY -> congestionSeen stays false
        oneValveStepAtFloor(gauge);

        assertThat(gauge.effectiveT())
                .as("the valve step is a dedicated additive +1 (4->5), NOT a slow-start double (4->8); RED "
                        + "today (frozen, stays 4)")
                .isEqualTo(5);
        assertThat(stealReason(metrics, "AIMD", "slow_start_double"))
                .as("the valve never routes through the doubling block, even with congestionSeen==false")
                .isZero();
    }

    // ---- 9. floor lift-off ratchet (dynamic behavior of the asymmetry) ---------------------------

    /**
     * From the collapsed floor {@code initialT=1}, sustained {@code latFrozen} + progress
     * + no timeouts ratchets {@code T} up exactly {@code +1} per {@link #VALVE_PACE} window, instead of
     * holding flat at 1 forever. Advance the clock a full pace per window (one admitted step
     * each) across 4 windows and assert {@code T} climbs {@code 1 -> 2 -> 3 -> 4 -> 5} monotonically, no
     * overshoot.
     */
    @Test
    void floorLiftOff_ratchetsOnePerWindow() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        driveLatencyFrozen(gauge);

        int expected = 1;
        for (int window = 0; window < 4; window++) {
            oneValveStepAtFloor(gauge);
            expected++;
            assertThat(gauge.effectiveT())
                    .as("window %d: the valve ratchets the floor up by exactly +1 (RED today: flat at 1)", window)
                    .isEqualTo(expected);
        }
        assertThat(gauge.effectiveT()).as("four paced windows lift 1 -> 5").isEqualTo(5);
        assertThat(valveCount(metrics)).as("one admitted step per window").isEqualTo(4.0);
    }

    // ---- 10. multiplicative decrease outruns the valve (asymmetry safety) ------------------------

    /**
     * The asymmetry the convergence rests on: additive-up (valve {@code +1}) is
     * dominated by multiplicative-down (503 {@code x0.7}) whenever a real congestion signal fires. Two
     * valve steps lift {@code T} {@code 32 -> 33 -> 34}; then a single real 503 knocks it to {@code
     * floor(0.7*34)=23} — net DOWN from the starting 32 despite the valve having engaged twice. The
     * post-503 value pins that the valve does not defeat AIMD under a real 503.
     */
    @Test
    void multiplicativeDecreaseOutrunsValve() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 32, metrics);

        driveLatencyFrozen(gauge);
        oneValveStepAtFloor(gauge);   // 32 -> 33
        oneValveStepAtFloor(gauge);   // 33 -> 34
        assertThat(gauge.effectiveT())
                .as("the valve lifted T to a peak of 34 (RED today: frozen, stays 32)").isEqualTo(34);
        assertThat(valveCount(metrics)).as("two admitted valve steps").isEqualTo(2.0);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // one real 503: T := floor(0.7*34) = 23

        assertThat(gauge.effectiveT())
                .as("a single 503 x0.7 (34->23) more than undoes the two valve +1s -- net DOWN from the "
                        + "starting 32: multiplicative decrease dominates the additive valve")
                .isEqualTo(23);
    }

    // ---- 11. CONC: single-winner election + permit conservation under a real concurrent burst -----

    /**
     * Adversarial CONC guard on the valve's {@code
     * lastValveGrowthNs} CAS: a burst of {@link #CONC_THREADS} real threads calls {@code
     * reportStatus(200)} SIMULTANEOUSLY (released off one {@link CountDownLatch}) at a single fixed
     * {@code now}, all latency-frozen, all past the pace boundary, all past the starvation gate — so
     * every thread reaches the CAS and races for the SAME pace slot. Because {@code now} does not
     * advance during the burst, the outcome is fully deterministic: exactly ONE thread's {@code
     * compareAndSet(lastValve, now)} can win, so exactly one {@code +1} is admitted.
     *
     * <p><b>What mutant this catches.</b> A mutant that drops the CAS (or races the {@code effectiveT}
     * CAS + {@code semaphore.release} OUTSIDE {@code growthLock}, or checks-then-acts on {@code
     * lastValveGrowthNs} non-atomically) would let MULTIPLE threads win the same pace slot under real
     * concurrency: {@code frozen_growth_valve} would read {@code > 1} and/or {@code effectiveT} would
     * jump by more than 1 in one pace window — both asserted exactly here. A mutant that drops the
     * {@code delta > 0} guard around {@code semaphore.release(delta)} (or otherwise double-releases)
     * would desync {@code availablePermits()} from {@code effectiveT()} — caught by the final permit-
     * conservation assertion, which a correct single-winner admission satisfies trivially (no
     * {@code acquireSlot()} calls are made in this test, so permits should track {@code effectiveT()}
     * exactly).
     */
    @Test
    @Timeout(10)
    void concurrentSuccessBurst_singleWinner_permitsConserved() throws InterruptedException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 32, metrics);

        driveLatencyFrozen(gauge);
        // Deterministic (single-threaded) FIRST valve step: builds progress and arms lastValveGrowthNs.
        gauge.reportStatus(200);   // successes=1 -> starved hold
        gauge.reportStatus(200);   // successes=2 -> admits the first valve step (32->33)
        assertThat(gauge.effectiveT()).as("setup: first valve step admits").isEqualTo(33);
        assertThat(valveCount(metrics)).isEqualTo(1.0);

        // Open the NEXT pace slot, then freeze `now` for the whole burst: every racing thread observes
        // the identical clock value, so the pace check passes for all of them and the CAS alone decides
        // the winner -- isolating the concurrency invariant from any clock-driven determinism.
        now.addAndGet(VALVE_PACE + 1L);
        int before = gauge.effectiveT();

        CountDownLatch ready = new CountDownLatch(CONC_THREADS);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < CONC_THREADS; i++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                gauge.reportStatus(200);   // races: rolls no window (same `now`), all see the same lastValve
            }, "valve-burst-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }
        ready.await();
        go.countDown();
        for (Thread t : workers) {
            t.join(4_000);
        }

        assertThat(gauge.effectiveT())
                .as("exactly ONE of %d racing threads wins the pace-slot CAS: T grows by exactly +1 "
                        + "(not 0, not > 1)", CONC_THREADS)
                .isEqualTo(before + 1);
        assertThat(valveCount(metrics))
                .as("total admitted steps across the run: the deterministic setup step + exactly one "
                        + "winner from the burst")
                .isEqualTo(2.0);
        assertThat(gauge.availablePermits())
                .as("permit conservation: no acquireSlot() calls were made, so availablePermits() must "
                        + "track effectiveT() exactly -- a lost or over-released permit fails this")
                .isEqualTo(gauge.effectiveT());
    }

    // ---- 12. tMax boundary: no valve growth once effectiveT is already at the ceiling -------------

    /**
     * Drives {@code latFrozen}, progress, AND an elapsed
     * valve pace -- every valve precondition a mutant could otherwise satisfy -- while {@code
     * effectiveT() == tMax} already. Because {@code onSuccess()}'s {@code if (cur >= tMax) return;}
     * (upstream of both freeze rungs and the valve) fires first, no growth, no {@code
     * frozen_growth_valve} emission, and no {@code FREEZE/latency_inflation} emission occur even though
     * the valve's own preconditions would all pass.
     *
     * <p><b>What mutant this catches.</b> Any mutant that reorders/removes the {@code cur >= tMax} early
     * return relative to the valve block, OR that drops the {@code Math.min(tMax, curNow +
     * INCREASE_STEP)} clamp inside the valve's own grow-step — either defect would let {@code effectiveT}
     * exceed the configured ceiling. This test pins {@code effectiveT() == tMax} AND {@code
     * availablePermits() == tMax} after repeated latency-frozen successes at the ceiling, so a
     * clamp-dropping mutant (which would grow T to {@code tMax + 1} and over-release a permit) fails
     * here.
     */
    @Test
    void atTMax_noValveGrowth_permitsAtCeiling() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        int tMax = 16;
        ConcurrencyGauge gauge = newGauge(tMax, tMax, metrics);   // already at the ceiling

        driveLatencyFrozen(gauge);
        gauge.reportStatus(200);                  // successes=1 (starved, but irrelevant: cur>=tMax bails first)
        gauge.reportStatus(200);                  // successes=2 -> progress WOULD clear the starvation gate
        now.addAndGet(VALVE_PACE + 1L);            // the valve pace WOULD also be elapsed
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT())
                .as("at the ceiling, no growth path applies -- T stays at tMax even though latFrozen, "
                        + "progress, and the valve pace all independently hold")
                .isEqualTo(tMax);
        assertThat(valveCount(metrics))
                .as("the valve never emits once effectiveT() == tMax")
                .isZero();
        assertThat(latencyInflationCount(metrics))
                .as("the cur>=tMax early return precedes even the latFrozen evaluation, so the freeze "
                        + "opportunity counter is not recorded either at the ceiling")
                .isZero();
        assertThat(gauge.availablePermits())
                .as("permits never exceed the configured ceiling -- a clamp-dropping mutant fails here")
                .isEqualTo(tMax);
    }

    // ---- 13. 503 cool-down suppresses the valve; it sits DOWNSTREAM of the clean-window gate -------

    /**
     * A fresh 503 sets {@code lastThrottleNs} and starts
     * the {@code CLEAN_WINDOW_NANOS} (10 s) cool-down. WHILE still inside that cool-down, latency-frozen
     * successes with progress built up do NOT valve-grow -- the {@code last != 0 && (now - last) <
     * CLEAN_WINDOW_NANOS} return sits upstream of both freeze
     * rungs and the valve. Once the cool-down elapses (still well short of the 30 s valve pace, since
     * {@code lastValveGrowthNs} is still unset/0 for this gauge), the SAME accumulated progress and the
     * still-frozen latency immediately admit -- proving the earlier zero-growth window was the cool-down
     * gate specifically, not some other accidental blocker.
     *
     * <p><b>What mutant this catches.</b> A mutant that hoists the valve check (or the {@code latFrozen}
     * evaluation feeding it) ABOVE the cool-down early return would let a success land a valve {@code +1}
     * WHILE still inside the 503 cool-down -- exactly the relaxation-oscillation the pacing
     * exists to prevent (a cleared-but-still-cooling-down 503 rebounding immediately). The first block of
     * assertions (unchanged {@code T}, zero valve count, mid-cool-down) fails under such a mutant because
     * it would already show growth/emission before the cool-down elapses.
     */
    @Test
    void cooldownSuppressesValve_thenAdmitsOnceCleared() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(256, 32, metrics);

        driveLatencyFrozen(gauge);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // 503: T := floor(0.7*32) = 22; cool-down starts
        int afterThrottle = gauge.effectiveT();
        assertThat(afterThrottle).isEqualTo(22);

        // Still WITHIN the 10 s cool-down (no clock advance): build progress past the starvation gate
        // (max(1, 22/32) = 1) with two same-instant successes.
        gauge.reportStatus(200);   // successes=1 -> would be starved anyway; also blocked by the cool-down
        gauge.reportStatus(200);   // successes=2 -> progress WOULD clear the starvation gate

        assertThat(gauge.effectiveT())
                .as("still inside the 503 cool-down: the valve must NOT grow even with latFrozen + "
                        + "progress present")
                .isEqualTo(afterThrottle);
        assertThat(valveCount(metrics))
                .as("no valve emission while the cool-down is active")
                .isZero();

        // Advance PAST the 10 s cool-down (but nowhere near the 30 s valve pace -- lastValveGrowthNs is
        // still 0/unset for this gauge, so the pace auto-passes once reached). Same shed window (10s+1 <
        // 30s), so the progress built above survives the advance.
        now.addAndGet(ConcurrencyGauge.CLEAN_WINDOW_NANOS + 1L);
        gauge.reportStatus(200);   // cool-down cleared; latFrozen still true; progress still present -> admits

        assertThat(gauge.effectiveT())
                .as("once the cool-down clears, the SAME latency-frozen + progress state immediately "
                        + "admits -- proving the earlier hold was specifically the cool-down gate")
                .isEqualTo(afterThrottle + 1);
        assertThat(valveCount(metrics)).isEqualTo(1.0);
        assertThat(gauge.availablePermits())
                .as("permit conservation across the 503 decrease + valve increase")
                .isEqualTo(gauge.effectiveT());
    }

    // ---- 14. CONC (mixed race): a concurrent 503 vs a valve success burst stays BOUNDED ------------

    /**
     * The accepted-by-design race: in {@code onSuccess()} a success can pass the 503 cool-down gate and the
     * {@code latFrozen}/starvation/pace gates and win the valve pace-slot CAS on {@code
     * lastValveGrowthNs} while a CONCURRENT {@code reportStatus(503)} sets {@code lastThrottleNs}
     * and multiplicatively reduces {@code T}; the winning success then takes {@code growthLock} and
     * admits {@code +1} on the ALREADY-REDUCED {@code T} (it re-reads {@code curNow} inside the lock).
     * This guard does NOT forbid the race — it pins the BOUNDED property the design accepts: the
     * multiplicative DECREASE is never lost, and the valve contributes AT MOST one additive step. The
     * worst case — a single {@code +1} landing on the freshly-reduced {@code T} (a valve step inside a
     * fresh cool-down) — is the DOCUMENTED accepted race (see the valve block's accepted-race javadoc
     * note in {@code ConcurrencyGauge.onSuccess}); it is the SAME race shape as the pre-existing
     * normal-growth path.
     *
     * <p>A latency-frozen, progress-cleared gauge is armed at {@code before=33} (deterministic setup:
     * one valve step {@code 32->33} builds progress and arms {@code lastValveGrowthNs}); the next pace
     * slot is opened and {@code now} frozen; then {@link #MIXED_SUCCESS_THREADS} threads call {@code
     * reportStatus(200)} and ONE thread calls {@code reportStatus(503)}, all released off a single
     * latch. After join, whichever ordering occurred, {@code effectiveT()} settles at either {@code
     * floor(0.7*before)} (503 last, or valve-then-503) or {@code floor(0.7*before)+1} (503-then-valve,
     * the accepted one-step-inside-cooldown outcome) — never higher, and never above the pre-burst
     * {@code before}. The whole thing runs {@link #MIXED_ITERATIONS} times with fresh gauges so the
     * interleaving is actually explored (both "no valve step" and "one valve step" outcomes occur).
     *
     * <p><b>What mutant this catches.</b> (a) A valve that grew from a STALE pre-decrease {@code T}
     * read (e.g. reusing the pre-lock {@code cur} instead of re-reading {@code curNow} inside {@code
     * growthLock}) would admit {@code before+1 == 34}, ABOVE the {@code floor(0.7*before)+1 == 24}
     * bound — caught by (a). (b) A valve that double-admits under the race (a dropped {@code
     * lastValveGrowthNs} CAS letting >1 success win the slot) would push {@code T} past the bound and
     * emit {@code frozen_growth_valve > 1} for the burst — caught by (a) and (c). (c) A valve or 503
     * path that leaks or over-releases permits (a lost {@code reduceAvailable} or a double {@code
     * release}) would desync {@code availablePermits()} from {@code effectiveT()} under the mixed race
     * — caught by (b). No {@code acquireSlot()} calls are made, so permits track {@code effectiveT()}
     * exactly.
     */
    @Test
    @Timeout(10)
    void mixedThrottleAndValveBurst_decreaseNeverLost_valveAtMostOneStep() throws InterruptedException {
        for (int iter = 0; iter < MIXED_ITERATIONS; iter++) {
            now.set(1_000_000_000_000L);   // fresh clock base per iteration (gauge is fresh -> 0 sentinels reset)
            RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
            ConcurrencyGauge gauge = newGauge(256, 32, metrics);

            driveLatencyFrozen(gauge);
            // Deterministic (single-threaded) setup: build progress and arm lastValveGrowthNs with the
            // first valve step, exactly as the sibling single-winner burst does.
            gauge.reportStatus(200);   // successes=1 -> starved hold
            gauge.reportStatus(200);   // successes=2 -> admits the first valve step (32->33)
            int before = gauge.effectiveT();
            assertThat(before).as("setup: first valve step armed the gauge at 33").isEqualTo(33);
            double valveBefore = valveCount(metrics);

            // Open the NEXT valve pace slot, then freeze `now` for the whole mixed burst so the pace
            // check passes for every success thread and the CAS + the concurrent 503 alone decide the
            // interleaving.
            now.addAndGet(VALVE_PACE + 1L);

            CountDownLatch ready = new CountDownLatch(MIXED_SUCCESS_THREADS + 1);
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < MIXED_SUCCESS_THREADS; i++) {
                Thread t = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    gauge.reportStatus(200);   // may pass or bail at the cool-down gate depending on the 503
                }, "mixed-success-" + iter + "-" + i);
                t.setDaemon(true);
                workers.add(t);
            }
            // The single CONCURRENT 503: sets lastThrottleNs (opening a fresh cool-down) and reduces T.
            Thread throttle = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
            }, "mixed-throttle-" + iter);
            throttle.setDaemon(true);
            workers.add(throttle);

            for (Thread t : workers) {
                t.start();
            }
            ready.await();
            go.countDown();
            for (Thread t : workers) {
                t.join(4_000);
            }

            int reducedByThrottle = Math.max(1, (int) (before * ConcurrencyGauge.DECREASE_FACTOR));
            int bound = reducedByThrottle + 1;   // 503 dominates; the valve contributes at most one +1

            assertThat(gauge.effectiveT())
                    .as("iter %d: the 503 multiplicative decrease is never lost and the valve adds at "
                            + "most one step -- T settles at <= floor(0.7*%d)+1 == %d, never above the "
                            + "pre-burst %d", iter, before, bound, before)
                    .isLessThanOrEqualTo(bound)
                    .isLessThan(before);
            assertThat(valveCount(metrics) - valveBefore)
                    .as("iter %d: the single-winner CAS admits at most ONE frozen_growth_valve step for "
                            + "the burst, even racing a concurrent 503", iter)
                    .isLessThanOrEqualTo(1.0);
            assertThat(gauge.availablePermits())
                    .as("iter %d: permit conservation under the mixed race -- no acquireSlot() calls, so "
                            + "availablePermits() tracks effectiveT() exactly (a leaked/over-released "
                            + "permit from the 503 reduce or the valve release fails here)", iter)
                    .isEqualTo(gauge.effectiveT());
        }
    }
}
