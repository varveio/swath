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
 * The progress-gated sustained-timeout SHED on {@link ConcurrencyGauge}.
 *
 * <p>These are the ADVERSARIAL, cross-cutting cases that complement the ordinary shed unit tests in
 * {@code ConcurrencyGaugeShedTest}: the full storm-down-then-RECOVER arc, the boundary-precise
 * scale-invariant gates, one-shed-per-window, held-permit conservation, and distinct vote accounting.
 * Each method's javadoc names the mutant(s) it kills.
 *
 * <p>All FAST/per-commit: they drive the {@linkplain ConcurrencyGauge#ConcurrencyGauge(int,
 * RunMetrics, java.util.function.LongSupplier, java.util.function.LongSupplier) injected-clock seam}
 * — a fake nano clock plus a fixed shed-window length — so windows roll deterministically with NO
 * real sleep.
 *
 * <p><b>The two-dimensional contract.</b> A shed fires only when a rolling shed window shows BOTH a
 * timeout storm AND starved progress:
 * {@code timeouts >= max(3, ceil(0.3*T))  AND  successes <= max(1, floor(T/32))}
 * → {@code T := max(1, floor(0.5*T))}, at most once per window. The progress gate ensures a timeout
 * tail on a run that keeps committing pages clears the starvation gate and never sheds. A shed
 * records {@code swath.aimd.timeout_shed} + a {@code SHED/timeout_storm} steal_reason but NEVER
 * {@code swath.aimd.votes} (that stays real-503-only).
 */
final class AimdTimeoutShedContractTest {

    /** A fixed shed-window length so window rolls are driven purely by the injected clock. */
    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;   // 30 s

    /** Injected fake nano clock — non-zero base (0 is the gauge's "unset" sentinel). */
    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, tMax);   // start AT Tmax (shed/AIMD tests isolate from the slow-start ramp)
    }

    private static double timeoutSheds(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.timeout_shed").counter().count();
    }

    private static double aimdVotes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.votes").counter().count();
    }

    /** Feed a starved storm (many timeouts, zero successes) at the current clock, then roll the window. */
    private void stormWindow(ConcurrencyGauge gauge, int timeouts) {
        for (int i = 0; i < timeouts; i++) {
            gauge.onTransientTimeout();
        }
        now.addAndGet(WINDOW + 1);   // elapse the current shed window
    }

    // ---- 1: storm sheds ~0.5x per window to the floor, THEN recovers +1 once the storm lifts -----

    /**
     * The full arc. A sustained starved storm sheds T by exactly 0.5x each successive window down to
     * the floor of 1 (16→8→4→2→1); then, once the storm lifts and real completions resume, the +1
     * clean-window recovery climbs T back to Tmax. No aimd vote is ever cast.
     *
     * <p>Kills: <b>remove-shed</b> (T never leaves 16 → the shed-sequence assertions FAIL);
     * <b>wrong-factor</b> (any factor ≠ 0.5 breaks the exact 16→8→4→2→1 chain);
     * <b>no-recovery</b> (a shed that permanently pins T low would never climb back to 16).
     */
    @Test
    void starvedStorm_shedsHalfPerWindow_toFloor_thenRecoversToTmax() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(16, metrics);

        int[] expected = {8, 4, 2, 1};   // exactly 0.5x per shed, one shed per window
        for (int i = 0; i < expected.length; i++) {
            stormWindow(gauge, 100);
            assertThat(gauge.effectiveT()).as("shed #%d halves T (factor 0.5)", i + 1).isEqualTo(expected[i]);
        }
        assertThat(timeoutSheds(metrics)).as("exactly one shed per window").isEqualTo(4.0);
        assertThat(aimdVotes(metrics)).as("a shed is NEVER a 503 down-vote").isEqualTo(0.0);

        // Storm lifts: real completions resume. The clock is already >1 clean-window past the last
        // shed (stormWindow advanced a full 30s window), so the +1 recovery is free to climb — but the
        // pace grants at most one +1 per GROWTH_PACE_NANOS, so advance the clock a full pace between
        // successes for the recovery to climb one step each.
        for (int i = 0; i < 20; i++) {
            gauge.reportStatus(200);
            now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS);
        }
        assertThat(gauge.effectiveT())
                .as("once the storm lifts and successes resume, +1 recovery climbs T back to Tmax")
                .isEqualTo(16);
        assertThat(timeoutSheds(metrics)).as("recovery casts no further sheds").isEqualTo(4.0);
        assertThat(aimdVotes(metrics)).as("recovery casts no aimd vote").isEqualTo(0.0);
    }

    // ---- 2: a timeout TAIL with progress never sheds (the progress gate) --------------------------

    /**
     * A long timeout tail INTERLEAVED with real completions keeps successes above the starvation gate
     * ({@code successGate = max(1, floor(64/32)) = 2}), so the storm condition never triggers — even
     * though the raw timeout count vastly exceeds the timeout gate.
     *
     * <p>Kills: <b>shed-ignores-progress</b> — a shed that looked only at the timeout count and not the
     * success gate would punish a progressing run and FAIL.
     */
    @Test
    void timeoutTailWithProgress_neverSheds() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);

        for (int i = 0; i < 200; i++) {
            gauge.reportStatus(200);      // a real completed page (forward progress)
            gauge.onTransientTimeout();   // ... interleaved with an attempt-timeout
        }
        assertThat(gauge.effectiveT()).as("a timeout tail on a progressing run must NOT shed").isEqualTo(64);
        assertThat(timeoutSheds(metrics)).isEqualTo(0.0);
        assertThat(aimdVotes(metrics)).isEqualTo(0.0);
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    // ---- 3: the timeout gate is boundary-precise and scale-invariant (T=2 and T=256) --------------

    /**
     * The timeout gate is exactly {@code max(3, ceil(0.3*T))} at BOTH ends of the scale: at T=2 it is
     * 3 (K dominates), at T=256 it is 77 (the fraction dominates). One timeout below the gate must NOT
     * shed; the gate-crossing timeout MUST shed.
     *
     * <p>Kills: <b>wrong-timeout-gate</b> — a hardcoded threshold, or {@code floor} instead of
     * {@code ceil}, or dropping the {@code K=3} floor, would shed one timeout early or late at one of
     * the two scales.
     */
    @Test
    void timeoutGate_isBoundaryPrecise_scaleInvariant() {
        // T=2: gate = max(3, ceil(0.6)) = 3.
        RunMetrics small = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge tiny = newGauge(2, small);
        tiny.onTransientTimeout();
        tiny.onTransientTimeout();
        assertThat(tiny.effectiveT()).as("T=2: 2 timeouts is below the gate (3) — no shed").isEqualTo(2);
        assertThat(timeoutSheds(small)).isEqualTo(0.0);
        tiny.onTransientTimeout();     // the 3rd crosses the gate
        assertThat(tiny.effectiveT()).as("T=2: the 3rd timeout crosses the gate → shed to floor 1").isEqualTo(1);
        assertThat(timeoutSheds(small)).isEqualTo(1.0);

        // T=256: gate = max(3, ceil(76.8)) = 77.
        RunMetrics big = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge huge = newGauge(256, big);
        for (int i = 0; i < 76; i++) {
            huge.onTransientTimeout();
        }
        assertThat(huge.effectiveT()).as("T=256: 76 timeouts is below the gate (77) — no shed").isEqualTo(256);
        assertThat(timeoutSheds(big)).isEqualTo(0.0);
        huge.onTransientTimeout();     // the 77th crosses the gate
        assertThat(huge.effectiveT()).as("T=256: the 77th timeout crosses the gate → shed (floor(0.5*256)=128)").isEqualTo(128);
        assertThat(timeoutSheds(big)).isEqualTo(1.0);
    }

    /**
     * The starvation (success) gate is exactly {@code max(1, floor(T/32))} and scales with T: at T=256
     * it is 8, at T=2 it is 1. Successes at or below the gate still shed under a storm; one success
     * above it blocks the shed.
     *
     * <p>Kills: <b>wrong-success-gate</b> — a hardcoded success threshold would misfire at one of the
     * two scales (e.g. a fixed gate of 1 would wrongly shed at T=256 with 8 successes, or wrongly
     * block at T=2 with 0 successes handled differently).
     */
    @Test
    void successGate_isBoundaryPrecise_scaleInvariant() {
        // T=256: gate = max(1, floor(256/32)) = 8.
        assertThat(shedFires(256, 8, 77)).as("T=256: 8 successes (== gate) still sheds under a storm").isTrue();
        assertThat(shedFires(256, 9, 77)).as("T=256: 9 successes (> gate 8) blocks the shed").isFalse();
        // T=2: gate = max(1, floor(2/32)) = 1.
        assertThat(shedFires(2, 1, 3)).as("T=2: 1 success (== gate) still sheds under a storm").isTrue();
        assertThat(shedFires(2, 2, 3)).as("T=2: 2 successes (> gate 1) blocks the shed").isFalse();
    }

    /** Fresh gauge: feed {@code successes} completions then {@code timeouts} timeouts in one window; did it shed? */
    private boolean shedFires(int tMax, int successes, int timeouts) {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(tMax, metrics);
        for (int i = 0; i < successes; i++) {
            gauge.reportStatus(200);
        }
        for (int i = 0; i < timeouts; i++) {
            gauge.onTransientTimeout();
        }
        return timeoutSheds(metrics) > 0.0;
    }

    // ---- 4: after a shed the permit pool is consistent with effectiveT (permit conservation) ------

    /**
     * The shed reuses the audited {@code reduceAvailable} machinery, so the invariant
     * {@code availablePermits == effectiveT - inFlightHolders} holds ACROSS a shed — even with a permit
     * held in-flight. Hold one slot, shed 8→4, and the pool must be exactly {@code 4 - 1 = 3}; release
     * it and the pool returns to exactly {@code effectiveT}.
     *
     * <p>Kills: <b>leaky-shed</b> — a shed that decremented effectiveT but forgot to reduce the
     * semaphore (or over-reduced it) would leave availablePermits inconsistent with effectiveT.
     */
    @Test
    void shed_conservesPermits_withInFlightHolder() throws InterruptedException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(8, metrics);
        assertThat(gauge.availablePermits()).isEqualTo(8);

        gauge.acquireSlot();                       // one worker in-flight: 8 -> 7 available
        assertThat(gauge.availablePermits()).isEqualTo(7);

        stormWindow(gauge, 100);                   // shed 8 -> 4 (reduce the pool by 4)
        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(gauge.availablePermits())
                .as("permits == effectiveT - inFlight (4 - 1)")
                .isEqualTo(3);

        gauge.releaseSlot();                        // worker returns its permit
        assertThat(gauge.availablePermits())
                .as("with no holder, the pool equals effectiveT")
                .isEqualTo(gauge.effectiveT());
    }

    // ---- 5: at most one shed per window; the next window sheds again ------------------------------

    /**
     * The one-shed-per-window latch caps a single window to ONE shed no matter how many timeouts pour
     * in; only after the window rolls can the next shed fire.
     *
     * <p>Kills: <b>shed-per-timeout</b> — a shed evaluated without the once-per-window latch would fire
     * on every gate-crossing timeout and collapse T to 1 in a single window (timeout_shed would balloon
     * far past 1 per window).
     */
    @Test
    void oneShedPerWindow_thenTheNextWindowShedsAgain() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(8, metrics);

        // 100 timeouts in ONE window (no clock advance): the latch caps it at a single shed.
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout();
        }
        assertThat(gauge.effectiveT()).as("one window sheds at most once (8 -> 4)").isEqualTo(4);
        assertThat(timeoutSheds(metrics)).as("exactly one shed in the first window").isEqualTo(1.0);

        // Roll the window, then storm again: the latch cleared, so a second shed fires.
        now.addAndGet(WINDOW + 1);
        for (int i = 0; i < 100; i++) {
            gauge.onTransientTimeout();
        }
        assertThat(gauge.effectiveT()).as("the next window sheds again (4 -> 2)").isEqualTo(2);
        assertThat(timeoutSheds(metrics)).as("exactly two sheds across two windows").isEqualTo(2.0);
    }

    // ---- 6: a shed accounts distinctly — timeout_shed + SHED steal_reason, NEVER an aimd vote -----

    /**
     * A shed increments {@code swath.aimd.timeout_shed}, records the target reduction, and emits a
     * {@code SHED/timeout_storm} steal_reason — but casts ZERO {@code swath.aimd.votes}. The 503 vote
     * denominator must stay clean of attempt-timeout noise.
     *
     * <p>Kills: <b>shed-votes</b> — a shed that reused {@code recordAimdVote()} (folding attempt-timeout
     * pressure into the 503 denominator) would push {@code aimd.votes} above 0.
     */
    @Test
    void shed_recordsTimeoutShedAndStealReason_neverAnAimdVote() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        ConcurrencyGauge gauge = newGauge(8, metrics);

        stormWindow(gauge, 100);

        assertThat(gauge.effectiveT()).isEqualTo(4);
        assertThat(timeoutSheds(metrics)).as("the shed's own distinct counter fired once").isEqualTo(1.0);
        assertThat(aimdVotes(metrics)).as("a shed casts ZERO aimd votes (guard)").isEqualTo(0.0);
        assertThat(registry.get("swath.aimd.target_reductions").counter().count())
                .as("the shed recorded a target reduction").isEqualTo(1.0);
        assertThat(registry.get("swath.steal_reason").tag("outcome", "SHED").tag("reason", "timeout_storm")
                .counter().count())
                .as("the shed emitted a distinct SHED/timeout_storm steal_reason").isEqualTo(1.0);
        assertThat(gauge.isStealingAllowed()).as("a shed pauses stealing, like onThrottle").isFalse();
    }
}
