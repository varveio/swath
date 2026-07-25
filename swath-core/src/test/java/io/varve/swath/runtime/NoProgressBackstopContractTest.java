/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

/**
 * The "no-real-progress" watchdog BACKSTOP: the END-TO-END counterpart to the pure
 * {@code tick()}-with-fake-clock unit tests in {@link LivenessWatchdogTest}. Every case here runs the
 * REAL machinery — a REAL scheduler thread, {@code System.nanoTime}, and a REAL
 * {@link RunMetrics#realProgressSignal()} supplier — so the backstop is proven to actually fire (and to
 * NOT false-trip) in wall-clock time, not just as a state-machine transition on an injected clock.
 *
 * <p><b>The contract.</b> A run that is ACTIVE (retry/throttle activity keeps the legacy
 * {@link RunMetrics#progressSignal()} climbing) but commits ZERO real pages MUST be cancelled with
 * {@link StopReason#STUCK} in BOUNDED time, so a permanently-503ing endpoint can no longer hang forever
 * with zero output. Conversely, a run that commits real pages (even slowly) MUST NOT trip (no
 * false-trip / THR-1 regression).
 *
 * <p><b>Why the JVM survives.</b> Production {@link LivenessWatchdog#arm} wires a real
 * {@link Runtime#halt} as the terminal rung; a test must NEVER reach it. Two of the three cases build
 * the watchdog through its package-private constructor seam with an INJECTED (recording) halt action but
 * otherwise-real wiring, so the halt is observable, not fatal, and the graces are set large so a halt
 * could not fire during the short window even in principle — letting us assert POSITIVELY that the
 * COOPERATIVE cancel is the rung that fired ({@code haltCode == -1}). The third case drives the genuine
 * {@link LivenessWatchdog#arm} path (real {@code Runtime.halt} wired) and closes the watchdog the instant
 * cancellation is observed — ~1s in, ~70s before the default halt grace elapses — so the test's mere
 * completion is itself proof the halt never fired.
 *
 * <p><b>Tautology avoidance / which mutant each kills.</b>
 * <ul>
 *   <li>{@link #activeButZeroRealProgressIsCancelledStuckByTheRealScheduledBackstop()} bumps ONLY
 *       {@code recordThrottleEvent} — so {@code progressSignal()} climbs forever while
 *       {@code realProgressSignal()} stays flat. Reverting the backstop (deleting the
 *       {@code || noRealProgress} arm of {@code tick()}) leaves NOTHING able to cancel — the stall
 *       tripwire is disabled and the total-freeze signal never freezes — so the token stays uncancelled
 *       and the {@code @Timeout} fires. It ALSO kills the mutant that folds throttle events INTO
 *       {@code realProgressSignal()} (i.e. {@code realProgressSignal() == progressSignal()}): then real
 *       progress would climb too and never trip. The {@code list_no_progress_abort} marker assertion pins
 *       the specific backstop trigger (vs. the total-freeze {@code list_stuck_abort}).</li>
 *   <li>{@link #attemptTimeoutStormIsCancelledStuckByTheBackstop()} runs the ATTEMPT-TIMEOUT storm shape
 *       (not just 503-forever): kills a mutant that let {@code attempt_timeout} count as real progress.</li>
 *   <li>{@link #throttledSeedProbesLeaveRealProgressFlatOnlyCompletedProbesAdvanceIt()} locks the
 *       seed-phase success-gating: {@code markProgress()} advances real progress ONLY on a COMPLETED probe
 *       (SeedStep.java:479, after {@code fetchPage} returns), so a probe storm stays flat and trips. Breaks
 *       if {@code markProgress} were ever moved to tick on a failed probe / retry / backoff.</li>
 *   <li>{@link #slowButCommittingRunNeverTripsTheRealScheduledBackstop()} bumps real progress
 *       ({@code recordPage}) every window — a backstop that trips despite committed work (or that failed
 *       to rearm {@code lastRealProgressNanos} on real progress) false-trips and fails this. This is the
 *       THR-1 "slow-but-progressing 503 grind is alive, not wedged" regression guard.</li>
 *   <li>{@link #sparseBucketWithLongButNonzeroProgressGapsNeverTrips()} is the sparse-bucket regression
 *       guard: a sparse/deep-prefix bucket commits real progress only intermittently (long, nonzero gaps); the
 *       rearm must hold across SEVERAL long gaps. A too-eager backstop false-trips and fails.</li>
 *   <li>{@link #armWiresRealProgressSignalIntoTheBackstopEndToEnd()} exercises {@code arm(...)} itself:
 *       a mutant where {@code arm} fails to pass {@code metrics::realProgressSignal} (e.g. passes
 *       {@code progressSignal} for both suppliers), or fails to arm the poller when ONLY the
 *       no-progress window is enabled, never cancels and fails the {@code @Timeout}.</li>
 * </ul>
 */
// Tagged at METHOD granularity. Five methods drive a REAL scheduled poller with wall-clock
// awaits/sleeps (latency-injecting, schedule-sensitive) and are `@Tag("deep")` — they run on
// main-merge + nightly (`-Pdeep`), not per-PR. The SAME contract is pinned PER-COMMIT by fast
// deterministic siblings that need no real clock: the fake-clock tick() tests in LivenessWatchdogTest
// (zeroRealProgressBackstopTrips… / realProgressAdvancingNeverTrips… /
// realProgressSignalExcludesThrottleEvents…) and this class's own RunMetrics-level
// throttledSeedProbes… method (untagged → fast). So the demotion is a scheduling change, not a coverage
// drop. Tier mechanics: docs/ops/dev/TESTING.md.
final class NoProgressBackstopContractTest {

    private static final int STUCK_CODE = 75;

    /**
     * A short no-progress window so the backstop fires in ~1s. Deliberately a WHOLE second (not tens of
     * ms) so the negative case can commit real progress comfortably within it without racing the poller.
     */
    private static final Duration NO_PROGRESS = Duration.ofSeconds(1);

    /**
     * A daemon thread that keeps calling {@code action} on the metrics every {@code periodMs} until
     * stopped — the "another thread simulating the failure/healthy mode" the contract calls for.
     */
    private static Thread startDriver(String name, long periodMs, AtomicBoolean stop, Runnable action) {
        Thread t = new Thread(() -> {
            while (!stop.get()) {
                action.run();
                try {
                    Thread.sleep(periodMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ---- POSITIVE: the no-real-progress case is closed --------------------------------------------

    @Tag("deep")   // real scheduled poller + wall-clock await; fast per-commit backstop: LivenessWatchdogTest fake-clock tests
    @Test
    @Timeout(30)
    void activeButZeroRealProgressIsCancelledStuckByTheRealScheduledBackstop() throws Exception {
        Logger logger =
                (Logger) LoggerFactory.getLogger(LivenessWatchdog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);

        AtomicBoolean stop = new AtomicBoolean(false);
        Thread driver = null;
        try (LivenessWatchdogHarness rig = LivenessWatchdogHarness.realScheduledBackstop()) {
            // The classic active-but-useless run: keep bumping ONLY throttle events, from another thread,
            // so progressSignal() climbs every poll (the stall tripwire would rearm forever) while
            // realProgressSignal() never moves. Only the backstop can end this.
            driver = startDriver("test-throttle-storm", 40, stop,
                    () -> rig.metrics.recordThrottleEvent(ThrottleType.SLOWDOWN));
            rig.start();

            // BOUNDED wall-clock time: the CancellationToken must become cancelled(STUCK).
            await().atMost(20, TimeUnit.SECONDS).until(rig.token::isCancelled);
            assertThat(rig.token.stopReason()).isEqualTo(StopReason.STUCK);

            // Sanity: progressSignal really did keep climbing (this is genuinely the active shape, not a
            // total freeze) while realProgressSignal stayed pinned at zero.
            assertThat(rig.metrics.progressSignal()).isGreaterThan(0L);
            assertThat(rig.metrics.realProgressSignal()).isEqualTo(0L);

            // The COOPERATIVE cancel is the rung that fired — NOT a halt (graces are 60s; ~1s has passed).
            assertThat(rig.haltCode)
                    .as("cooperative cancel(STUCK), not Runtime.halt, closes the bug")
                    .hasValue(-1);

            rig.close();   // stop the poller before reading the (now-quiescent) appender list
            stop.set(true);
            driver.join(TimeUnit.SECONDS.toMillis(5));
        } finally {
            if (driver != null) {
                stop.set(true);
            }
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
                .as("the backstop is a DISTINCT trigger — it must not masquerade as the total-freeze abort")
                .noneMatch(m -> m.startsWith("list_stuck_abort"));
    }

    /**
     * Storm SHAPE (b): an ATTEMPT-TIMEOUT storm — a self-inflicted client timeout — is just as
     * "active but useless" as a 503 storm. Each attempt-timeout is recorded as a throttle event
     * (moving {@code progressSignal()}) but commits nothing ({@code realProgressSignal()} flat), so the
     * backstop must still cancel STUCK in bounded time under a short window. Kills a mutant that special-
     * cased only {@code slowdown}/{@code server5xx} as "not real progress" and let {@code attempt_timeout}
     * count — which would let an attempt-timeout storm hang forever.
     */
    @Tag("deep")   // real scheduled poller + wall-clock await
    @Test
    @Timeout(30)
    void attemptTimeoutStormIsCancelledStuckByTheBackstop() throws Exception {
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread driver = null;
        try (LivenessWatchdogHarness rig = LivenessWatchdogHarness.realScheduledBackstop()) {
            driver = startDriver("test-attempt-timeout-storm", 40, stop,
                    () -> rig.metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT));
            rig.start();
            await().atMost(20, TimeUnit.SECONDS).until(rig.token::isCancelled);
            assertThat(rig.token.stopReason()).isEqualTo(StopReason.STUCK);
            assertThat(rig.metrics.realProgressSignal())
                    .as("an attempt-timeout storm commits no real work")
                    .isEqualTo(0L);
            assertThat(rig.haltCode).as("cooperative cancel, not halt").hasValue(-1);
        } finally {
            if (driver != null) {
                stop.set(true);
                driver.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    /**
     * Storm SHAPE (c) — the SEED-PHASE success-gating invariant, asserted at the {@link RunMetrics} level
     * (no engine boot needed). A seed run makes NO page completions; its ONLY real-progress component is
     * {@code markProgress()}, and that is called EXACTLY ONCE per COMPLETED probe — {@code SeedStep.probe}
     * (SeedStep.java:479) calls it only AFTER {@code fetcher.fetchPage(...)} RETURNS. A failed/throttled
     * probe throws out of {@code fetchPage} and never reaches that line: it records a throttle event but
     * NOT {@code markProgress()}.
     *
     * <p>So a probe storm (every probe throttled/failing) keeps {@code realProgressSignal()} pinned at
     * zero and the backstop can trip it — while a genuinely-progressing seed (probes completing) rearms it
     * and does not. This test locks that gate: EVERY throttle type leaves {@code realProgressSignal()}
     * flat, and only {@code markProgress()} (the completed-probe signal) advances it. It would BREAK if
     * {@code markProgress()} were ever moved to tick on a failed probe / retry / backoff — the exact mutant
     * that would silently defeat the backstop for a seed-phase storm.
     */
    @Test
    void throttledSeedProbesLeaveRealProgressFlatOnlyCompletedProbesAdvanceIt() {
        RunMetrics m = new RunMetrics(new SimpleMeterRegistry());
        long base = m.realProgressSignal();
        for (ThrottleType type : ThrottleType.values()) {
            m.recordThrottleEvent(type);   // a FAILED/throttled probe records this, but never markProgress()
        }
        assertThat(m.realProgressSignal())
                .as("failed/throttled probes (no markProgress) must NOT advance real progress")
                .isEqualTo(base);
        assertThat(m.progressSignal())
                .as("...yet progressSignal DOES fold in the throttle activity, so the run looks 'active'")
                .isGreaterThan(base);
        m.markProgress();   // exactly what SeedStep.probe does after a COMPLETED fetchPage
        assertThat(m.realProgressSignal())
                .as("a COMPLETED probe's markProgress is the only thing that rearms the backstop")
                .isGreaterThan(base);
    }

    // ---- NEGATIVE: no false-trip / no THR-1 regression -------------------------------------------

    @Tag("deep")   // real scheduled poller + multi-second wall-clock observation
    @Test
    @Timeout(30)
    void slowButCommittingRunNeverTripsTheRealScheduledBackstop() throws Exception {
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread driver = null;
        try (LivenessWatchdogHarness rig = LivenessWatchdogHarness.realScheduledBackstop()) {
            // Same active shape (throttle events flowing), but this run ALSO commits real pages every
            // ~200ms — comfortably inside the 1s window. realProgressSignal() advances, rearming the
            // backstop clock every poll; it must NEVER trip, even across many windows.
            driver = startDriver("test-slow-but-committing", 200, stop, () -> {
                rig.metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
                rig.metrics.recordPage();   // real committed work
            });
            rig.start();

            // Watch across several full no-progress windows.
            Thread.sleep(NO_PROGRESS.multipliedBy(4).toMillis());

            assertThat(rig.token.isCancelled())
                    .as("a slow-but-progressing run (THR-1 503 grind) is alive, not wedged")
                    .isFalse();
            assertThat(rig.token.stopReason()).isNull();
            assertThat(rig.haltCode).hasValue(-1);
            assertThat(rig.interrupted).isFalse();
            assertThat(rig.dumped).isFalse();
            assertThat(rig.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);

            // And it really was making both kinds of progress the whole time.
            assertThat(rig.metrics.realProgressSignal()).isGreaterThan(0L);
        } finally {
            stop.set(true);
            if (driver != null) {
                driver.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    /**
     * NEGATIVE regression guard. A pathological deep-prefix / sparse bucket has real, live work, but the
     * gaps between page commits are LONG. A too-eager backstop kills that healthy run. Here real progress
     * ({@code recordPage}) arrives only every ~0.7× of the window — a long but nonzero gap — and the test
     * proves the rearm holds across SEVERAL such gaps, not just one. A backstop that measures against a
     * fixed run-start instant (rather than rearming {@code lastRealProgressNanos} on each commit), or that
     * uses too tight a window, trips here and fails.
     *
     * <p>Window is a generous 2s and the gap 1.4s, leaving ~600ms of slack per gap to absorb scheduler
     * jitter without flaking; the observation spans ~5 gaps.
     */
    @Tag("deep")   // real scheduled poller + ~7s wall-clock observation across several long gaps
    @Test
    @Timeout(30)
    void sparseBucketWithLongButNonzeroProgressGapsNeverTrips() throws Exception {
        Duration window = Duration.ofSeconds(2);
        long gapMs = 1400;   // ~0.7 × window: a long gap, but the rearm keeps it under the window
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread driver = null;
        try (LivenessWatchdogHarness rig = LivenessWatchdogHarness.realScheduledBackstop(window)) {
            // Commit a real page every long gap, from another thread; NO throttle noise this time — the
            // point is purely that sparse-but-real progress rearms the backstop across many long gaps.
            driver = startDriver("test-sparse-long-gap", gapMs, stop, rig.metrics::recordPage);
            rig.start();

            Thread.sleep(gapMs * 5);   // span at least ~5 long gaps

            assertThat(rig.token.isCancelled())
                    .as("long-but-nonzero progress gaps must not false-trip (sparse-bucket guard)")
                    .isFalse();
            assertThat(rig.watchdog.state()).isEqualTo(LivenessWatchdog.State.HEALTHY);
            assertThat(rig.haltCode).hasValue(-1);
            assertThat(rig.interrupted).isFalse();
            assertThat(rig.metrics.realProgressSignal())
                    .as("several real commits landed across the long gaps")
                    .isGreaterThanOrEqualTo(3L);
        } finally {
            if (driver != null) {
                stop.set(true);
                driver.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    // ---- POSITIVE via the genuine arm(...) wiring ----------------------------------------------

    /**
     * The end-to-end {@link LivenessWatchdog#arm} path — real daemon scheduler, real {@code Runtime.halt}
     * WIRED (never reached). Proves {@code arm} correctly maps {@code metrics::realProgressSignal} into
     * the backstop and arms the poller when ONLY the no-progress window is enabled. We close the watchdog
     * the instant cancellation is observed (~1s), ~70s before the default halt grace could elapse; the
     * test completing at all is proof the JVM was never halted.
     */
    @Tag("deep")   // genuine arm(...) path: real daemon scheduler + wall-clock await
    @Test
    @Timeout(30)
    void armWiresRealProgressSignalIntoTheBackstopEndToEnd() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread driver = startDriver("test-arm-throttle-storm", 40, stop,
                () -> metrics.recordThrottleEvent(ThrottleType.SERVER_5XX));
        try (LivenessWatchdog ignored =
                     LivenessWatchdog.arm(token, metrics,
                             Duration.ZERO,   // stall DISABLED
                             NO_PROGRESS,     // only the no-real-progress backstop is armed
                             STUCK_CODE)) {
            await().atMost(20, TimeUnit.SECONDS).until(token::isCancelled);
            assertThat(token.stopReason()).isEqualTo(StopReason.STUCK);
            assertThat(metrics.realProgressSignal())
                    .as("nothing real ever committed — only the throttle storm ran")
                    .isEqualTo(0L);
        } finally {
            // try-with-resources close() shuts the real poller here (~1s in), long before the ~70s halt
            // grace — so the wired Runtime.halt is never reached.
            stop.set(true);
            driver.join(TimeUnit.SECONDS.toMillis(5));
        }
    }
}
