/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THE live-progress lifecycle of a run: one ticker, spanning seed → listing → merge → writing, that
 * builds one {@link ProgressEvent} per tick and hands it to the installed {@link ProgressSink}.
 *
 * <p><b>One lifecycle, several scopes.</b> Several nested scopes each want progress running (the
 * CLI around the whole run including the seed step, {@code ListRunner} around the engine and the
 * merge). The FIRST {@link #start} owns the ticker; a start while one is already running returns a
 * JOINED handle whose {@code close()} does nothing, so the inner scope cannot stop the outer one's
 * reporting — that is what makes elapsed a single session clock, keeps the windowed rate's baseline
 * advancing exactly once per tick, and leaves the merge/publish tail covered instead of silent.
 *
 * <p><b>No terminal knowledge lives here.</b> swath-core cannot tell a TTY from a pipe and must not
 * try; whether progress is wanted, on which fd, in what shape, is the {@link ProgressSink}
 * implementor's decision.
 */
public final class RunProgressReporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RunProgressReporter.class);
    private static final Duration NON_TTY_INTERVAL = Duration.ofSeconds(30);

    /**
     * How long a run must be alive before its FIRST frame — deliberately NOT the cadence. Scheduling
     * the first tick one whole period out meant a 30s-cadence run that spent 22s seeding and then
     * finished reported nothing at all, which is precisely the run an operator stares at. A short
     * grace period keeps a sub-second run silent while making a stalled one visible quickly.
     */
    private static final Duration ACTIVATION_DELAY = Duration.ofSeconds(2);

    private final RunMetrics metrics;
    private final long startedNs;
    /** {@code null} on a JOINED handle: another reporter owns the ticker, so close() has nothing to do. */
    private final ScheduledExecutorService executor;

    private RunProgressReporter(RunMetrics metrics, long startedNs, ScheduledExecutorService executor) {
        this.metrics = metrics;
        this.startedNs = startedNs;
        this.executor = executor;
    }

    public static RunProgressReporter start(RunMetrics metrics, Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("progress interval must be positive");
        }
        RunProgressReporter reporter =
                new RunProgressReporter(metrics, System.nanoTime(), DaemonSchedulers.create("swath-progress"));
        if (!metrics.claimProgressReporter(reporter)) {
            return new RunProgressReporter(metrics, reporter.startedNs, null);   // joined: no ticker of its own
        }
        // The first frame lands after the activation grace period (capped by the cadence itself, so
        // an explicitly short --progress-interval is never delayed BEYOND it), then every interval.
        Duration activation = ACTIVATION_DELAY.compareTo(interval) < 0 ? ACTIVATION_DELAY : interval;
        DaemonSchedulers.schedule(reporter.executor, activation, interval, reporter::reportSafely);
        return reporter;
    }

    /** The default cadence (30s); also the default flush interval for {@link JsonRunSummaryWriter}. */
    public static Duration nonTtyInterval() {
        return NON_TTY_INTERVAL;
    }

    @Override
    public void close() {
        if (executor == null) {
            return;
        }
        DaemonSchedulers.stop(executor, log, "progress_reporter_stop_timeout");
        metrics.releaseProgressReporter(this);
    }

    private void reportSafely() {
        try {
            ProgressSink sink = metrics.progressSink();
            if (!sink.isEnabled()) {
                return;   // nothing renders this tick: build no event at all
            }
            sink.accept(metrics.progressEvent(Duration.ofNanos(System.nanoTime() - startedNs)));
        } catch (RuntimeException e) {
            log.warn("progress_report_failed message={}", e.getMessage());
        }
    }
}
