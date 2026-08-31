/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Wiring/compile-confidence tests for {@link LivenessWatchdog}: the escalation ladder is
 * driven through the pure {@link LivenessWatchdog#tick()} state machine on an injected fake clock /
 * progress supplier with observable halt/interrupt/dump actions — so the whole
 * cancel→interrupt→dump→halt sequence is asserted WITHOUT scheduling a real thread or killing the JVM.
 *
 * <p>NOT the separately maintained adversarial trip/no-trip + phase-aware test;
 * these guard the mechanism (injected seams, ladder ordering, timing).
 */
final class LivenessWatchdogTest {

    private static final int STUCK_CODE = 75;

    private static long secs(long s) {
        return Duration.ofSeconds(s).toNanos();
    }

    private static final Duration NO_PROGRESS = Duration.ofMinutes(10);

    @Test
    void ladderAdvancesCancelThenInterruptDumpThenHalt() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly();

        // A first poll with no elapsed stall: stays HEALTHY, nothing fires.
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);
        assertThat(h.token.isCancelled()).isFalse();

        // (a) stall window elapses with a frozen progress signal → cooperative cancel(STUCK).
        h.now.set(secs(120));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);
        assertThat(h.token.isCancelled()).isTrue();
        assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);
        assertThat(h.interrupted).isFalse();
        assertThat(h.haltCode).hasValue(-1);

        // Still inside the interrupt grace: no escalation yet.
        h.now.set(secs(125));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);
        assertThat(h.interrupted).isFalse();

        // (b)+(c) interrupt grace elapses → interrupt the workers AND dump the forensic snapshot.
        h.now.set(secs(130));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.INTERRUPTED);
        assertThat(h.interrupted).isTrue();
        assertThat(h.dumped).isTrue();
        assertThat(h.haltCode).hasValue(-1);

        // Still inside the halt grace: no halt yet.
        h.now.set(secs(180));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.INTERRUPTED);
        assertThat(h.haltCode).hasValue(-1);

        // (d) halt grace elapses → Runtime.halt(stuckExitCode) via the injected action (JVM survives).
        h.now.set(secs(190));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.haltCode).hasValue(STUCK_CODE);
    }

    /**
     * Once tripping begins (the cooperative cancel is committed) the ladder is PURELY time-driven —
     * a post-trip dribble of progress must NOT rearm it, or a wedged unwind that produces one progress
     * tick per poll could postpone the halt backstop forever (the halt guarantee is unconditional). Do
     * not check progress before the state switch in {@code tick()}: that ordering lets a progress tick
     * short-circuit and freeze the ladder in CANCELLED/INTERRUPTED. This drives progress between
     * CANCELLED and HALT and asserts the ladder still reaches HALT.
     */
    @Test
    void progressAfterTheFirstCancelDoesNotRearmTheLadderAndHaltStillFires() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly();

        // Trip: stall window elapses with frozen progress → cooperative cancel(STUCK).
        h.now.set(secs(120));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);

        // A dribble of progress on the way through the interrupt grace must NOT reset the ladder.
        h.progress.incrementAndGet();
        h.now.set(secs(130));
        h.watchdog.tick();
        assertThat(h.watchdog.state())
                .as("post-trip progress must not freeze the ladder in CANCELLED")
                .isEqualTo(LivenessWatchdog.State.INTERRUPTED);

        // More progress through the halt grace: the halt backstop must still fire.
        h.progress.incrementAndGet();
        h.now.set(secs(190));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.haltCode).hasValue(STUCK_CODE);
    }

    /**
     * The hard {@code Runtime.halt} bypasses {@code JsonRunSummaryWriter.close()}/normal
     * finalization, so in the fully-wedged case the terminal {@code stop_reason=stuck} summary would
     * never be written. The halt path must durably record the stuck disposition on stderr (the marker an
     * external runner's classifier reads) IMMEDIATELY before halting. Captured via a logback appender.
     */
    @Test
    void haltPathEmitsAStuckSummaryMarkerBeforeHalting() {
        Logger logger =
                (Logger) LoggerFactory.getLogger(LivenessWatchdog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);
        try {
            LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly();
            h.now.set(secs(120));
            h.watchdog.tick();   // cancel
            h.now.set(secs(130));
            h.watchdog.tick();   // interrupt + dump
            h.now.set(secs(190));
            h.watchdog.tick();   // halt
            assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        String haltMarker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("list_stuck_halt"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_stuck_halt marker emitted before halt"));
        assertThat(haltMarker)
                .as("the halt-path marker must name the watchdog as the stop_source "
                        + "(the only source the halt ladder can ever reach), not leave it null")
                .contains("stop_reason=stuck")
                .contains("stop_source=liveness_watchdog")
                .contains("exit_code=" + STUCK_CODE);

        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("list_stuck_summary"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_stuck_summary marker emitted before halt"));
        assertThat(marker)
                .as("the pre-halt marker carries the stuck disposition for the runner's classifier")
                .contains("stop_reason=stuck")
                .contains("stop_source=liveness_watchdog")
                .contains("completed=false")
                .contains("exit_code=" + STUCK_CODE);
    }

    @Test
    void forwardProgressResetsTheStallClockAndNeverTrips() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly();
        // Advance well past the stall window on every poll, but bump the progress signal each time:
        // a slow-but-progressing run must never be aborted.
        for (int i = 1; i <= 20; i++) {
            h.now.set(secs(200L * i));
            h.progress.incrementAndGet();
            h.watchdog.tick();
        }
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);
        assertThat(h.token.isCancelled()).isFalse();
        assertThat(h.haltCode).hasValue(-1);
        assertThat(h.interrupted).isFalse();
    }

    @Test
    void aForeignCancelKeepsItsReasonButTheWedgeStillEscalatesToHalt() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly();
        // A --max-duration (or signal) cancel fired first and won the reason attribution.
        h.token.cancel(StopReason.MAX_DURATION);

        h.now.set(secs(120));
        h.watchdog.tick();   // stall → cancel(STUCK) is a no-op on the reason (first-writer-wins)
        assertThat(h.token.stopReason()).isEqualTo(StopReason.MAX_DURATION);
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);

        // But a wedged unwind past both graces must still be force-exited with the stuck code.
        h.now.set(secs(130));
        h.watchdog.tick();
        h.now.set(secs(190));
        h.watchdog.tick();
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
        assertThat(h.haltCode).hasValue(STUCK_CODE);
    }

    @Test
    void armWithZeroWindowIsDisarmedAndNeverCancels() throws Exception {
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (LivenessWatchdog watchdog =
                     LivenessWatchdog.arm(token, metrics, Duration.ZERO, Duration.ZERO, STUCK_CODE)) {
            assertThat(watchdog).isNotNull();
            Thread.sleep(40);
            assertThat(token.isCancelled()).isFalse();
        }
    }

    @Test
    void armedWatchdogTripsOnARealStalledSignal() {
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());   // progressSignal() stays 0
        // A tiny real window exercises the scheduled poller end-to-end (arm → daemon → tick → cancel).
        try (LivenessWatchdog ignored =
                     LivenessWatchdog.arm(token, metrics, Duration.ofMillis(30), Duration.ZERO, STUCK_CODE)) {
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(token::isCancelled);
            assertThat(token.stopReason()).isEqualTo(StopReason.STUCK);
        }
    }

    @Test
    void progressSignalAdvancesAcrossPhases() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        long base = metrics.progressSignal();
        metrics.recordPage();                 // LISTING page completion
        assertThat(metrics.progressSignal()).isGreaterThan(base);
        long afterPage = metrics.progressSignal();
        metrics.recordEntriesEmitted(500);    // objects emitted
        assertThat(metrics.progressSignal()).isGreaterThan(afterPage);
        long afterObjects = metrics.progressSignal();
        metrics.markProgress();               // merge/finalize tail tick
        assertThat(metrics.progressSignal()).isGreaterThan(afterObjects);
    }

    /**
     * Metrics-level only — NOT the separately maintained adversarial ladder-level trip/no-trip test:
     * a classified throttle/transient event (recorded only when an
     * attempt actually RETURNS, per {@code S3PageFetcher}) advances {@code progressSignal()}, so a
     * live-but-throttled run's retries count as forward progress, not a wedge.
     */
    @Test
    void progressSignalAdvancesOnAnyClassifiedThrottleOrTransientEvent() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        long base = metrics.progressSignal();
        metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        assertThat(metrics.progressSignal()).isGreaterThan(base);
        long afterTimeout = metrics.progressSignal();
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        assertThat(metrics.progressSignal()).isGreaterThan(afterTimeout);
    }

    // ---- zero-real-progress backstop -------------------------------------------------------

    /**
     * The SECOND, independent tripwire. progressSignal keeps climbing every poll (a permanent
     * 503/5xx retry storm re-arms it forever) while realProgressSignal stays FLAT (nothing commits) —
     * exactly the active-but-zero-output shape that the total-freeze tripwire can never catch. The
     * backstop must escalate HEALTHY → CANCELLED(STUCK) → INTERRUPTED → HALTED after its own window, and
     * log the DISTINCT {@code list_no_progress_abort} marker (NOT {@code list_stuck_abort}).
     */
    @Test
    void zeroRealProgressBackstopTripsWhileProgressSignalKeepsAdvancing() {
        Logger logger =
                (Logger) LoggerFactory.getLogger(LivenessWatchdog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly(NO_PROGRESS);   // backstop enabled at 10m
        try {
            // progressSignal advances every poll (throttle/retry activity) → the stall clock rearms and
            // NEVER trips; realProgress stays flat, so only the backstop can fire. Window = 600s.
            for (int i = 1; i <= 6; i++) {
                h.progress.incrementAndGet();
                h.now.set(secs(100L * i));
                h.watchdog.tick();
            }
            assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.CANCELLED);
            assertThat(h.token.stopReason()).isEqualTo(StopReason.STUCK);

            // The ladder is otherwise identical to the stall path: interrupt+dump, then halt.
            h.now.set(secs(610));
            h.watchdog.tick();
            assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.INTERRUPTED);
            h.now.set(secs(670));
            h.watchdog.tick();
            assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HALTED);
            assertThat(h.haltCode).hasValue(STUCK_CODE);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        var markers = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        String backstop = markers.stream()
                .filter(m -> m.startsWith("list_no_progress_abort"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_no_progress_abort marker emitted"));
        assertThat(backstop)
                .contains("stop_reason=stuck")
                .contains("no_real_progress_ms=")
                .contains("exit_code=" + STUCK_CODE);
        assertThat(markers)
                .as("the backstop must NOT emit the total-freeze marker — it is a distinct trigger")
                .noneMatch(m -> m.startsWith("list_stuck_abort"));
    }

    /**
     * No-false-trip: with realProgress advancing (a slow-but-progressing run — the THR-1 503 grind
     * really commits pages, just slowly), the backstop clock rearms every poll and never trips, even as
     * the fake clock jumps far past both windows on every tick.
     */
    @Test
    void realProgressAdvancingNeverTripsTheBackstop() {
        LivenessWatchdogHarness h = LivenessWatchdogHarness.fakeStallOnly(NO_PROGRESS);
        for (int i = 1; i <= 20; i++) {
            h.now.set(secs(1000L * i));       // far past both the stall AND no-progress windows
            h.progress.incrementAndGet();
            h.realProgress.incrementAndGet();  // real committed work each poll
            h.watchdog.tick();
        }
        assertThat(h.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);
        assertThat(h.token.isCancelled()).isFalse();
        assertThat(h.haltCode).hasValue(-1);
        assertThat(h.interrupted).isFalse();
    }

    /**
     * {@code realProgressSignal()} = committed work only, EXCLUDING throttle/retry activity: a
     * throttle event moves {@code progressSignal()} but NOT {@code realProgressSignal()}; a committed
     * page moves both.
     */
    @Test
    void realProgressSignalExcludesThrottleEventsButAdvancesOnCommittedWork() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        long base = metrics.realProgressSignal();
        metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
        assertThat(metrics.realProgressSignal())
                .as("throttle/retry activity is NOT real progress")
                .isEqualTo(base);
        assertThat(metrics.progressSignal())
                .as("but progressSignal DOES fold in the throttle event")
                .isGreaterThan(base);
        metrics.recordPage();
        assertThat(metrics.realProgressSignal())
                .as("a committed page IS real progress")
                .isGreaterThan(base);
    }

    /**
     * The interrupt rung's ONE achievable purpose is waking a worker parked in
     * platform-thread blocking I/O — the real {@code parquet-writer-*} (ParquetWriterPool) and
     * {@code *-encoder-*} (SortLane) lanes. Do not narrow the predicate to carriers plus
     * {@code swath-worker}/{@code swath-scan} names: those names do not exist, so the filter would
     * miss the real lanes. It must match the real lanes and still exclude the infra daemons.
     */
    @Test
    void interruptFilterMatchesTheRealPlatformIoLanes() {
        // The real blocking-I/O lanes this rung exists to wake ARE matched.
        assertThat(LivenessWatchdog.isInterruptibleWorker("parquet-writer-1")).isTrue();
        assertThat(LivenessWatchdog.isInterruptibleWorker("seg-0000-encoder-2")).isTrue();
        // The parallel range merge's threads. Omitted when the parallel path landed, which made the
        // cooperative rung a no-op for the threads that own the merge: every trip went straight to
        // Runtime.halt with no terminal summary. The exact worker name shape finalization produces.
        assertThat(LivenessWatchdog.isInterruptibleWorker("swath-sort-range-1-8")).isTrue();
        // The VT-scheduler carrier match is kept (harmless best-effort).
        assertThat(LivenessWatchdog.isInterruptibleWorker("ForkJoinPool-3-worker-7")).isTrue();
        // Infra daemons are never interrupted.
        assertThat(LivenessWatchdog.isInterruptibleWorker("swath-liveness-watchdog")).isFalse();
        assertThat(LivenessWatchdog.isInterruptibleWorker("swath-max-duration")).isFalse();
        assertThat(LivenessWatchdog.isInterruptibleWorker("swath-progress")).isFalse();
        assertThat(LivenessWatchdog.isInterruptibleWorker("swath-summary-json")).isFalse();
        // The JVM-wide common pool is deliberately NOT swept, and an unrelated/null name
        // never matches.
        assertThat(LivenessWatchdog.isInterruptibleWorker("ForkJoinPool.commonPool-worker-1")).isFalse();
        assertThat(LivenessWatchdog.isInterruptibleWorker("main")).isFalse();
        assertThat(LivenessWatchdog.isInterruptibleWorker(null)).isFalse();
    }
}
