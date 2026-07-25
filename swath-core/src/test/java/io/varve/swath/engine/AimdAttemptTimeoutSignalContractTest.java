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
import org.junit.jupiter.api.Timeout;

/**
 * The AIMD-signal guard for {@link ConcurrencyGauge}: client-side attempt-timeouts must never vote
 * the AIMD target down, yet a starved, sustained storm of them must still shed the target through a
 * distinct, non-voting path.
 *
 * <p><b>The invariant.</b> A tail of client-side attempt-timeouts (S3 sending no 503) feeds
 * {@link ConcurrencyGauge#onTransientTimeout()} ONLY (a growth-freeze hedge), never
 * {@code reportStatus(503)}; only a real 503 votes {@code T} down. The contract is two-dimensional:
 * <ul>
 *   <li><b>TAIL</b> — attempt-timeouts that coexist with real page commits (successes above the
 *       starvation gate): T unchanged, ZERO aimd votes, ZERO timeout sheds. The shed must never
 *       punish a progressing run.</li>
 *   <li><b>STORM</b> — attempt-timeouts with STARVED progress: a shed fires
 *       ({@code swath.aimd.timeout_shed} increments), but STILL casts ZERO aimd votes.</li>
 * </ul>
 *
 * <p><b>Why these are not tautologies.</b> Each asserts a concrete behavioral split. A gauge that let
 * an attempt-timeout re-drive the AIMD decrease (every timeout casts an aimd vote and multiplies T
 * down) fails the TAIL-half {@code effectiveT}-stays-at-Tmax / zero-votes assertions. A gauge with no
 * sustained-timeout shed fails the STORM-half {@code timeout_shed > 0} / T-drops assertions. And the
 * 503 contrast proves the guard is not merely "nothing ever lowers T" (a gauge that ignored all
 * backpressure would fail the 503 half).
 *
 * <p>All four methods drive the {@linkplain ConcurrencyGauge#ConcurrencyGauge(int,
 * RunMetrics, java.util.function.LongSupplier, java.util.function.LongSupplier) injected-clock seam}
 * — a fake nano clock plus a fixed shed-window length — so the freeze/thaw/shed windows roll
 * deterministically with NO real sleep. The no-vote-on-timeout policy is additionally pinned
 * per-commit by ConcurrencyGaugeTest, and the ordinary-shed cases by ConcurrencyGaugeShedTest.
 */
final class AimdAttemptTimeoutSignalContractTest {

    /** A fixed shed-window length so window rolls are driven purely by the injected clock. */
    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;   // 30 s

    /** Injected fake nano clock — non-zero base (0 is the gauge's "unset" sentinel). */
    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, tMax);   // start AT Tmax (shed/AIMD tests isolate from the slow-start ramp)
    }

    private static double aimdVotes(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.votes").counter().count();
    }

    private static double timeoutSheds(RunMetrics metrics) {
        return metrics.registry().get("swath.aimd.timeout_shed").counter().count();
    }

    // ---- A: TAIL casts no aimd vote and never lowers T; a real 503 votes; a STORM sheds (no vote) ----

    /**
     * Three phases exercise the two-dimensional invariant:
     * <ol>
     *   <li><b>TAIL</b> — attempt-timeouts interleaved 1:1 with real completions, so successes stay
     *       above the starvation gate ({@code successGate = max(1, floor(16/32)) = 1}): T must NOT move
     *       off Tmax, ZERO aimd votes, ZERO timeout sheds. A gauge that let attempt-timeouts re-drive
     *       the AIMD vote collapses this to 1 with ~50 votes → FAIL.</li>
     *   <li><b>503 contrast</b> — a single real 503 MUST both cast a vote and multiplicatively cut T
     *       ({@code floor(0.7*16)=11}), proving the guard is not "nothing ever lowers T".</li>
     *   <li><b>STORM</b> — after rolling to a fresh shed window, a STARVED burst of attempt-timeouts
     *       (zero successes) sheds T ({@code floor(0.5*11)=5}) and increments {@code timeout_shed}, yet
     *       casts NO further aimd vote. Delete the shed and this half → FAIL.</li>
     * </ol>
     */
    @Test
    void attemptTimeoutTailCastsNoAimdVoteAndNeverLowersT_realSlowdownDoesBoth() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(16, metrics);
        assertThat(gauge.effectiveT()).isEqualTo(16);

        // Phase 1 — TAIL: a long stream of attempt-timeouts on a run that keeps committing pages.
        // Each timeout is paired with a real completion, so shed successes stay above the starvation
        // gate and the storm condition never triggers — this is a genuine tail, not a starved storm.
        for (int i = 0; i < 50; i++) {
            gauge.reportStatus(200);          // a real completed page (forward progress)
            gauge.onTransientTimeout();       // ... interleaved with an attempt-timeout
        }
        assertThat(gauge.effectiveT())
                .as("attempt-timeout TAIL on a progressing run must NOT move T (not S3 backpressure)")
                .isEqualTo(16);
        assertThat(aimdVotes(metrics))
                .as("attempt-timeouts cast ZERO aimd votes")
                .isEqualTo(0.0);
        assertThat(timeoutSheds(metrics))
                .as("a TAIL (progress above the starvation gate) casts ZERO sheds — the progress gate")
                .isEqualTo(0.0);
        assertThat(gauge.isStealingAllowed())
                .as("attempt-timeouts do not pause stealing (only a real 503 does)")
                .isTrue();

        // Phase 2 — 503 contrast: a genuine 503 SlowDown DOES vote and DOES cut the target.
        gauge.reportStatus(503);
        assertThat(gauge.effectiveT())
                .as("a real 503 multiplicatively decreases T (floor(0.7*16)=11)")
                .isEqualTo(11);
        assertThat(aimdVotes(metrics))
                .as("a real 503 casts exactly one aimd vote")
                .isEqualTo(1.0);
        assertThat(timeoutSheds(metrics))
                .as("a 503 is a vote, not a timeout shed")
                .isEqualTo(0.0);

        // Phase 3 — STORM: roll to a fresh shed window, then a STARVED attempt-timeout burst (zero
        // successes). The sustained-timeout shed fires — a distinct, NON-voting multiplicative cut.
        now.addAndGet(WINDOW + 1);            // elapse the current shed window
        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout();
        }
        assertThat(gauge.effectiveT())
                .as("a STARVED attempt-timeout storm sheds T (floor(0.5*11)=5) — the adaptive path down")
                .isEqualTo(5);
        assertThat(timeoutSheds(metrics))
                .as("the storm recorded exactly one timeout shed")
                .isEqualTo(1.0);
        assertThat(aimdVotes(metrics))
                .as("the shed casts NO further aimd vote — still just the one real-503 vote")
                .isEqualTo(1.0);
    }

    // ---- B(b): a transient tail never multiplies T down and never raises the floor above 1 -------

    /**
     * {@code onTransientTimeout} never multiplies down and never raises the floor above 1. Drive the
     * gauge to the floor with real 503s, then bury it in attempt-timeouts: the floor must stay exactly
     * 1 and no aimd vote may be cast (a mutant that gave the growth-freeze a floor {@code >1}, or that
     * let a transient nudge the target, would drift off 1 → FAIL). The sustained-timeout shed is a
     * no-op here: at T=1, {@code floor(0.5*1)=1}, so a starved storm records its shed counter but
     * cannot move T below the floor — the deadlock-safety floor holds for BOTH decrease paths.
     */
    @Test
    void transientTimeoutsNeverMultiplyDown_andNeverRaiseFloorAboveOne() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = new ConcurrencyGauge(8, metrics);

        // Real 503s collapse T to the floor of 1.
        for (int i = 0; i < 6; i++) {
            gauge.reportStatus(503);
        }
        assertThat(gauge.effectiveT()).isEqualTo(1);
        double votesAtFloor = aimdVotes(metrics);

        // A storm of attempt-timeouts must neither lower T below 1 nor cast any further vote.
        for (int i = 0; i < 40; i++) {
            gauge.onTransientTimeout();
        }
        assertThat(gauge.effectiveT()).as("floor stays exactly 1 under a transient storm").isEqualTo(1);
        assertThat(aimdVotes(metrics))
                .as("no additional aimd vote from attempt-timeouts")
                .isEqualTo(votesAtFloor);
    }

    // ---- B(a): a sustained transient rate FREEZES the +1 recovery (growth-freeze) ---------------

    /**
     * While the recent-window attempt-timeout rate is high, the +1 clean-window recovery is
     * SUPPRESSED — concurrency must not expand into a sick network. Observed IN ISOLATION from the
     * sustained-timeout shed: at the reduced target ({@code 16 -> 11}) three timeouts clear the
     * freeze threshold (3) but stay BELOW the shed timeout gate ({@code max(3, ceil(0.3*11)) = 4}), so
     * the freeze fires with NO shed. A mutant with no growth-freeze (+1 always fires) would climb →
     * FAIL; freeze means "hold", not "cut", so T must not drop either. A separate STORM coda then
     * proves the shed still exists (delete-shed → FAIL).
     */
    @Test
    void sustainedTransientRateFreezesTheRecoveryStep() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(16, metrics);

        gauge.reportStatus(503);                 // 16 -> 11
        int reduced = gauge.effectiveT();
        assertThat(reduced).isEqualTo(11);
        gauge.forceCleanWindow();                // clear the 503 cool-down so ONLY the freeze can block +1

        // Fresh transient window above the freeze threshold (3) but below the shed gate (4 at T=11):
        // growth must be frozen WITHOUT tripping a shed.
        gauge.onTransientTimeout();
        gauge.onTransientTimeout();
        gauge.onTransientTimeout();

        gauge.reportStatus(200);                 // healthy page: would be a +1 recovery, but frozen
        assertThat(gauge.effectiveT())
                .as("high transient rate freezes the +1 recovery (T held, not climbing)")
                .isEqualTo(reduced);
        assertThat(timeoutSheds(metrics))
                .as("3 timeouts stay below the shed gate (4 at T=11): the freeze fires in isolation, no shed")
                .isEqualTo(0.0);
        assertThat(gauge.isStealingAllowed())
                .as("N1: the 503 cool-down elapsed, so stealing is re-enabled independently of the freeze")
                .isTrue();

        // STORM coda: roll to a fresh window and cross the shed gate → the shed still exists.
        now.addAndGet(WINDOW + 1);
        for (int i = 0; i < 4; i++) {
            gauge.onTransientTimeout();          // 4 >= gate(4) at T=11, zero successes → shed
        }
        assertThat(gauge.effectiveT())
                .as("a starved storm past the shed gate sheds T (floor(0.5*11)=5)")
                .isEqualTo(5);
        assertThat(timeoutSheds(metrics)).as("the storm coda fired exactly one shed").isEqualTo(1.0);
        assertThat(aimdVotes(metrics)).as("the shed casts no aimd vote — only the one 503 voted").isEqualTo(1.0);
    }

    // ---- B(c): the freeze THAWS after the transient timeouts stop -------------------------------

    /**
     * Once attempt-timeouts stop for the trailing window (~10s), the freeze lifts and the +1 recovery
     * resumes. Driven by the injected clock seam — no real sleep — so it runs FAST/per-commit. A
     * mutant that latched the freeze permanently (never thawed) would keep T pinned and FAIL the
     * post-advance recovery assertion. A STORM coda then proves the shed still exists.
     */
    @Test
    @Timeout(30)
    void freezeThawsAfterTransientTimeoutsStop() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(16, metrics);

        gauge.reportStatus(503);                 // 16 -> 11
        int reduced = gauge.effectiveT();
        assertThat(reduced).isEqualTo(11);
        gauge.forceCleanWindow();

        gauge.onTransientTimeout();              // 3 timeouts: freeze hot, below the shed gate (4 at T=11)
        gauge.onTransientTimeout();
        gauge.onTransientTimeout();
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("frozen while transient window is hot").isEqualTo(reduced);
        assertThat(timeoutSheds(metrics)).as("frozen, not shed").isEqualTo(0.0);

        // Advance the fake clock just past the trailing transient window (10s) with no new timeout → thaw.
        now.addAndGet(ConcurrencyGauge.TRANSIENT_WINDOW_NANOS + 1);

        gauge.forceCleanWindow();                // re-open the 503 cool-down window
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT())
                .as("after the transient window elapses the freeze thaws and +1 recovery resumes")
                .isEqualTo(reduced + 1);         // 11 -> 12

        // STORM coda: roll to a fresh shed window and cross the shed gate → the shed still exists.
        now.addAndGet(WINDOW + 1);
        for (int i = 0; i < 4; i++) {
            gauge.onTransientTimeout();          // 4 >= gate(4) at T=12, zero successes → shed
        }
        assertThat(gauge.effectiveT())
                .as("a starved storm past the shed gate sheds T (floor(0.5*12)=6)")
                .isEqualTo(6);
        assertThat(timeoutSheds(metrics)).as("the storm coda fired exactly one shed").isEqualTo(1.0);
        assertThat(aimdVotes(metrics)).as("the shed casts no aimd vote — only the one 503 voted").isEqualTo(1.0);
    }
}
