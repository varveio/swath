/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.ThrottleType;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Regression guard: the watchdog must not FALSE-trip during a phase that legitimately makes no
 * page-completions (seed probing, the merge/finalize tail, a sustained-throttle retry storm). It is
 * driven here through the pure {@link LivenessWatchdog#tick()} state machine over an injected fake
 * clock and a REAL {@link RunMetrics#progressSignal()} supplier — so each phase's actual progress
 * mechanism is exercised — with observable halt/interrupt/dump actions (the JVM is never killed).
 *
 * <p><b>Tautology avoidance.</b> The three NO-trip cases advance {@code progressSignal()} through a
 * NON-page phase channel (markProgress / entries / classified throttle events) while page completions
 * stay frozen. A watchdog keyed on page-completion ALONE would see a frozen page count and false-trip,
 * so these cases FAIL against it. The TRIP case (genuinely frozen signal) FAILS against a watchdog
 * that never escalates. Together they pin the signal to "phase-aware forward progress", not "pages
 * completed".
 */
final class LivenessWatchdogPhaseAwareContractTest {

    private static final int STUCK_CODE = 75;

    private static long secs(long s) {
        return Duration.ofSeconds(s).toNanos();
    }

    /**
     * Drive {@code polls} ticks where, before each tick, {@code makeProgress} advances a phase signal
     * and the clock jumps forward by {@code stepNanos}. With {@code stepNanos} just under the stall
     * window, this is the adversarial "progress arrives slower than pages, but never zero for a whole
     * window" shape — the ladder must stay HEALTHY throughout.
     */
    private static void assertNoTripWhilePhaseAdvances(LivenessWatchdogHarness h, int polls, long stepNanos,
                                                       Consumer<RunMetrics> makeProgress) {
        for (int i = 1; i <= polls; i++) {
            makeProgress.accept(h.metrics);
            h.now.addAndGet(stepNanos);
            h.watchdog.tick();
            assertThat(h.watchdog.state())
                    .as("phase progress within the window keeps the ladder HEALTHY (poll %d)", i)
                    .isEqualTo(LivenessWatchdog.State.HEALTHY);
        }
        assertThat(h.token.isCancelled()).isFalse();
        assertThat(h.interrupted).isFalse();
        assertThat(h.dumped).isFalse();
        assertThat(h.haltCode).hasValue(-1);
    }

    // ---- NO-trip: seed phase (per-probe markProgress) -------------------------------------------

    /**
     * A healthy seed phase makes NO page completions (probes are 1-key calls); it advances the
     * phase-appropriate signal via {@code markProgress}. Nearly a full stall window elapses between
     * each probe, yet the ladder must never trip.
     */
    @Test
    void seedPhaseAdvancingViaMarkProgressDoesNotTrip() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        assertNoTripWhilePhaseAdvances(h, 25, secs(119), RunMetrics::markProgress);
    }

    // ---- NO-trip: non-sort finalize tail (part-commit markProgress) -----------------------------

    /**
     * The merge/finalize tail is active (CPU/IO busy) but makes no page completions; it advances via
     * part-commit {@code markProgress} ticks. Keying on page-completion alone would false-trip this
     * exact phase; it must stay HEALTHY.
     */
    @Test
    void finalizeTailAdvancingViaPartCommitMarkProgressDoesNotTrip() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        // Interleave slow markProgress ticks; each arrives just before the window closes.
        assertNoTripWhilePhaseAdvances(h, 25, secs(115), RunMetrics::markProgress);
    }

    // ---- NO-trip: sustained-throttle retry storm (classified throttle/transient events) ---------

    /**
     * The "attempts finishing but failing" case: under a sustained throttle/attempt-timeout
     * storm NO page ever COMPLETES, but each classified transient event (recorded only when an attempt
     * actually returns) is forward progress. This is the sharpest phase-aware guard — a page-only
     * signal freezes here and false-trips a live-but-throttled run. It must stay HEALTHY.
     */
    @Test
    void sustainedThrottleRetryStormAdvancingViaClassifiedEventsDoesNotTrip() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        assertNoTripWhilePhaseAdvances(h, 25, secs(118), m -> {
            // No recordPage() at all — pages are frozen; only classified transient/throttle events fire.
            m.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        });
        // And a real-503 storm is likewise alive, not wedged.
        LivenessWatchdogHarness h2 = LivenessWatchdogHarness.fakeClockOverMetrics();
        assertNoTripWhilePhaseAdvances(h2, 25, secs(118), m -> m.recordThrottleEvent(ThrottleType.SLOWDOWN));
    }

    // ---- TRIP: a true wedge (progress signal frozen for the whole stall window) ------------------

    /**
     * A genuine wedge: NO forward progress of ANY kind (no page, no markProgress, no throttle event) —
     * {@code progressSignal()} is frozen. The ladder must escalate HEALTHY → CANCELLED(STUCK) →
     * INTERRUPTED (interrupt + forensic dump) → HALTED (injected halt with the stuck exit code). A
     * watchdog that never escalated a truly frozen signal would FAIL this.
     */
    @Test
    void trueWedgeWithFrozenSignalEscalatesAllTheWayToHalt() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();

        // A first poll with no elapsed stall stays HEALTHY.
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);

        // Stall window elapses with a frozen signal → cooperative cancel(STUCK).
        h.now.set(secs(120));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);
        assertThat(h.token.isCancelled()).isTrue();
        assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);
        assertThat(h.interrupted).isFalse();
        assertThat(h.haltCode).hasValue(-1);

        // Interrupt grace elapses → interrupt workers + forensic dump.
        h.now.set(secs(130));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.INTERRUPTED);
        assertThat(h.interrupted).isTrue();
        assertThat(h.dumped).isTrue();
        assertThat(h.haltCode).hasValue(-1);

        // Halt grace elapses → Runtime.halt(stuckExitCode) via the injected action (JVM survives).
        h.now.set(secs(190));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.haltCode).hasValue(STUCK_CODE);
    }

    /**
     * Phase-aware boundary: a run that made phase progress (throttle events) and then TRULY wedges
     * must still trip once the signal freezes for a full window — the folded-in throttle-event signal
     * must not become an "always alive" latch. This kills a mutant that treated any historical
     * throttle activity as perpetual liveness.
     */
    @Test
    void phaseProgressThenGenuineFreezeStillTrips() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeClockOverMetrics();
        // Some real forward progress first.
        for (int i = 0; i < 5; i++) {
            h.metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
            h.now.addAndGet(secs(10));
            h.watchdog.tick();
            assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);
        }
        // Now the signal freezes: no new events for a full stall window → trip.
        h.now.addAndGet(secs(120));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);
        assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);
    }
}
