/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Shared harness for the {@code LivenessWatchdog*} test classes: wraps the 14-arg
 * package-private {@link LivenessWatchdog} constructor with the escalation-ladder defaults every
 * consumer shares — stuck exit code, stall window, and grace periods — and exposes only the
 * knobs the four consumer classes actually vary. Three shapes cover every consumer:
 * <ul>
 *   <li>{@link #fakeStallOnly()} / {@link #fakeStallOnly(Duration)} — a manually-ticked fake
 *       clock over synthetic {@code progress}/{@code realProgress} counters, stall tripwire live,
 *       the no-real-progress backstop optionally armed ({@code LivenessWatchdogTest});</li>
 *   <li>{@link #fakeClockOverMetrics()} — a manually-ticked fake clock over a REAL
 *       {@link RunMetrics} progress signal, stall tripwire only, the backstop disabled
 *       ({@code LivenessWatchdogPhaseAwareContractTest}, {@code LivenessWatchdogFinalizeWindowContractTest});</li>
 *   <li>{@link #realScheduledBackstop()} / {@link #realScheduledBackstop(Duration)} — a REAL
 *       daemon scheduler + REAL clock over a REAL {@link RunMetrics}, stall tripwire disabled,
 *       only the no-real-progress backstop armed ({@code NoProgressBackstopContractTest}).</li>
 * </ul>
 */
final class LivenessWatchdogHarness implements AutoCloseable {

    private static final int STUCK_CODE = 75;
    private static final Duration STALL = Duration.ofSeconds(120);
    private static final Duration GRACE_INTERRUPT = Duration.ofSeconds(10);
    private static final Duration GRACE_HALT = Duration.ofSeconds(60);
    /**
     * Graces for the real-scheduler shape: far larger than any observation window in its tests,
     * so a cooperative cancel can be positively distinguished from a later halt.
     */
    private static final Duration LARGE_GRACE = Duration.ofSeconds(60);
    private static final Duration DEFAULT_NO_PROGRESS_WINDOW = Duration.ofSeconds(1);

    /** Fake wall clock, populated only by the fake-clock shapes; {@code null} otherwise. */
    final AtomicLong now;
    /** Synthetic progress counter, populated only by {@link #fakeStallOnly}; {@code null} otherwise. */
    final AtomicLong progress;
    /** Synthetic real-progress counter, populated only by {@link #fakeStallOnly}; {@code null} otherwise. */
    final AtomicLong realProgress;
    /** Real metrics, populated by the metrics-backed shapes; {@code null} for {@link #fakeStallOnly}. */
    final RunMetrics metrics;
    final CancellationToken token = new CancellationToken();
    final AtomicInteger haltCode = new AtomicInteger(-1);
    final AtomicBoolean interrupted = new AtomicBoolean(false);
    final AtomicBoolean dumped = new AtomicBoolean(false);
    final LivenessWatchdog watchdog;
    private final ScheduledExecutorService scheduler;

    private LivenessWatchdogHarness(AtomicLong now, AtomicLong progress, AtomicLong realProgress,
                                     RunMetrics metrics, LongSupplier clock, LongSupplier progressSupplier,
                                     LongSupplier realProgressSupplier, Supplier<String> errorClassSupplier,
                                     ScheduledExecutorService scheduler, Duration stallWindow,
                                     Duration noProgressWindow, Duration graceInterrupt, Duration graceHalt) {
        this.now = now;
        this.progress = progress;
        this.realProgress = realProgress;
        this.metrics = metrics;
        this.scheduler = scheduler;
        this.watchdog = new LivenessWatchdog(
                scheduler, token, clock, progressSupplier, realProgressSupplier, errorClassSupplier,
                haltCode::set, () -> interrupted.set(true), () -> dumped.set(true),
                STUCK_CODE, stallWindow, noProgressWindow, graceInterrupt, graceHalt);
    }

    /**
     * Fake clock over synthetic {@code progress}/{@code realProgress} counters and a constant
     * {@code "stuck_unknown"} error class; stall tripwire live, no-real-progress backstop
     * DISABLED.
     */
    static LivenessWatchdogHarness fakeStallOnly() {
        return fakeStallOnly(Duration.ZERO);
    }

    /** As {@link #fakeStallOnly()} but with the no-real-progress backstop armed at {@code noProgressWindow}. */
    static LivenessWatchdogHarness fakeStallOnly(Duration noProgressWindow) {
        AtomicLong now = new AtomicLong(0);
        AtomicLong progress = new AtomicLong(0);
        AtomicLong realProgress = new AtomicLong(0);
        return new LivenessWatchdogHarness(now, progress, realProgress, null,
                now::get, progress::get, realProgress::get, () -> "stuck_unknown",
                null, STALL, noProgressWindow, GRACE_INTERRUPT, GRACE_HALT);
    }

    /** Fake clock over a REAL {@link RunMetrics} progress signal; no-real-progress backstop disabled. */
    static LivenessWatchdogHarness fakeClockOverMetrics() {
        AtomicLong now = new AtomicLong(0);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        return new LivenessWatchdogHarness(now, null, null, metrics,
                now::get, metrics::progressSignal, metrics::realProgressSignal,
                metrics::classifyStuckErrorClass,
                null, STALL, Duration.ZERO, GRACE_INTERRUPT, GRACE_HALT);
    }

    /**
     * Real daemon scheduler + real clock over a REAL {@link RunMetrics}; stall tripwire
     * disabled, only the no-real-progress backstop armed at the default 1s window.
     */
    static LivenessWatchdogHarness realScheduledBackstop() {
        return realScheduledBackstop(DEFAULT_NO_PROGRESS_WINDOW);
    }

    /** As {@link #realScheduledBackstop()} with a caller-chosen backstop window. */
    static LivenessWatchdogHarness realScheduledBackstop(Duration noProgressWindow) {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform().daemon(true).name("test-backstop-poller").unstarted(r));
        return new LivenessWatchdogHarness(null, null, null, metrics,
                System::nanoTime, metrics::progressSignal, metrics::realProgressSignal,
                metrics::classifyStuckErrorClass,
                scheduler, Duration.ZERO, noProgressWindow, LARGE_GRACE, LARGE_GRACE);
    }

    /** Starts the real scheduled poller (100ms period) — only meaningful for {@link #realScheduledBackstop}. */
    void start() {
        scheduler.scheduleAtFixedRate(watchdog::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        watchdog.close();
    }
}
