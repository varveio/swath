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

/** Periodic human progress line for an active list run. */
public final class RunProgressReporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RunProgressReporter.class);
    private static final Duration NON_TTY_INTERVAL = Duration.ofSeconds(30);

    private final RunMetrics metrics;
    private final long startedNs;
    private final ScheduledExecutorService executor;

    private RunProgressReporter(RunMetrics metrics, long startedNs, ScheduledExecutorService executor) {
        this.metrics = metrics;
        this.startedNs = startedNs;
        this.executor = executor;
    }

    /**
     * @param stdoutIsTerminal the caller's own per-fd {@code isatty} result -- swath-core has no
     *                         ambient terminal knowledge and must not
     *                         call {@code System.console()} itself; the CLI is the one layer that
     *                         does (via {@code TerminalCapabilities}).
     */
    public static RunProgressReporter start(RunMetrics metrics, boolean stdoutIsTerminal) {
        if (stdoutIsTerminal) {
            // TODO: replace this fallback with a JLine TTY display; the field set and cadence
            // stay contract-aligned in the interim.
        }
        return start(metrics, NON_TTY_INTERVAL);
    }

    public static RunProgressReporter start(RunMetrics metrics, Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("progress interval must be positive");
        }
        ScheduledExecutorService executor = DaemonSchedulers.create("swath-progress");
        RunProgressReporter reporter = new RunProgressReporter(metrics, System.nanoTime(), executor);
        DaemonSchedulers.schedule(executor, interval, reporter::reportSafely);
        return reporter;
    }

    /** The non-TTY default cadence (30s); also the default flush interval for {@link JsonRunSummaryWriter}. */
    public static Duration nonTtyInterval() {
        return NON_TTY_INTERVAL;
    }

    @Override
    public void close() {
        DaemonSchedulers.stop(executor, log, "progress_reporter_stop_timeout");
    }

    private void reportSafely() {
        try {
            RunMetrics.ProgressSnapshot s = metrics.snapshot(Duration.ofNanos(System.nanoTime() - startedNs));
            log.info("progress run_id={} strategy={} in_flight={} target_workers={} live_rate={} avg_rate={} total_emitted={} api_calls={} cost_usd={} oldest_pending_range={} eta={} pages={} steals={} splits={} cursor={} prefix={} rss={} heap={}",
                    s.runId(), s.strategy(), s.inFlight(), s.concurrencyTarget(),
                    s.liveKeysPerSecond(), s.keysPerSecond(), s.keys(), s.apiCalls(), s.estimatedCostUsd(),
                    s.oldestPendingRange(), s.eta(), s.pages(), s.steals(), s.splits(), s.cursor(), s.prefix(),
                    ResourceMetrics.currentRssBytes(), ResourceMetrics.currentHeapBytes());
        } catch (RuntimeException e) {
            log.warn("progress_report_failed message={}", e.getMessage());
        }
    }
}
