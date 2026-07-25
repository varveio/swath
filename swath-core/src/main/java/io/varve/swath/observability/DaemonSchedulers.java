/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/**
 * The daemon-scheduler lifecycle shared by {@link JsonRunSummaryWriter} and {@link
 * RunProgressReporter}: each starts a single daemon platform
 * thread ticking a periodic task at a fixed rate, and stops it with the same
 * {@code shutdownNow} + bounded-{@code awaitTermination} ladder. Both sites agree on
 * {@code daemon=true}, {@code scheduleAtFixedRate} (not {@code scheduleWithFixedDelay}) and a
 * {@value #SHUTDOWN_AWAIT_SECONDS}-second termination
 * wait — audited site-by-site before unifying. Where they genuinely differ (thread name,
 * task, the activation delay, whether the interval is validated before scheduling, what happens
 * after shutdown) stays at the call site rather than being forced in here.
 *
 * <p>{@link #create} and {@link #schedule} are split, rather than one {@code start(name, period,
 * task)} call, because every site schedules a method reference bound to the very object it is
 * constructing: the executor must exist before that object, and the object before the schedule
 * call.
 *
 * <p>{@link #stop} takes the CALLER's {@link Logger} instead of logging under its own: each
 * site's stop-timeout warning must keep firing under that class's logger name, since operator log
 * filters and per-class appender levels are keyed on it — moving it here would silently re-home
 * that identity (repo guardrail 7 for extractions).
 */
final class DaemonSchedulers {

    private static final int SHUTDOWN_AWAIT_SECONDS = 5;

    private DaemonSchedulers() {
    }

    /** A single daemon platform thread named {@code threadName}, not yet scheduled. */
    static ScheduledExecutorService create(String threadName) {
        return Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofPlatform().daemon(true).name(threadName).unstarted(r));
    }

    /** Ticks {@code task} on {@code executor} at a fixed rate every
     * {@code Math.max(1, period.toMillis())} ms, with that same initial delay. */
    static void schedule(ScheduledExecutorService executor, Duration period, Runnable task) {
        schedule(executor, period, period, task);
    }

    /**
     * As {@link #schedule(ScheduledExecutorService, Duration, Runnable)} but with an ACTIVATION
     * delay distinct from the steady cadence — for a task whose first tick is what makes a short
     * run visible at all, so waiting one whole period for it defeats the purpose (see {@link
     * RunProgressReporter}). Every other site keeps first-tick-at-one-period via the 3-arg overload.
     */
    static void schedule(ScheduledExecutorService executor, Duration activationDelay, Duration period,
            Runnable task) {
        executor.scheduleAtFixedRate(task, Math.max(1L, activationDelay.toMillis()),
                Math.max(1L, period.toMillis()), TimeUnit.MILLISECONDS);
    }

    /** Shuts {@code executor} down ({@code shutdownNow}, then up to
     * {@value #SHUTDOWN_AWAIT_SECONDS}s {@code awaitTermination}), warning via the caller's
     * {@code log} with a {@code "<timeoutMarker> timeout_seconds=<n>"} line if it doesn't finish
     * in time, and restoring the interrupt flag if this thread is interrupted while waiting. */
    static void stop(ScheduledExecutorService executor, Logger log, String timeoutMarker) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(timeoutMarker + " timeout_seconds={}", SHUTDOWN_AWAIT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
