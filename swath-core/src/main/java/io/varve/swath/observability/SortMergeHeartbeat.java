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
 * A periodic external-facing log line for the sort-merge phase. {@link
 * RunProgressReporter} is closed at the LISTING/MERGING boundary (its try-with-resources scopes
 * only {@code pipeline.run(...)} + {@code closeLane(...)} in {@code ListRunner}), so a fully
 * enumerated run's whole merge/publish tail runs with NO periodic line on any external channel
 * (log tail or {@code summary.json} poller) even though {@code KWayMerge}'s per-pass batched
 * callback already feeds real forward progress into {@code swath.progress.units} the whole time
 * (the same signal the internal {@code LivenessWatchdog} already trusts) — an external supervisor
 * reading only the log tail sees silence and kills a healthy, still-merging run.
 *
 * <p>This is a small, dedicated heartbeat (not a re-use of {@code RunProgressReporter}, whose
 * line shape is LISTING-specific: {@code in_flight}/{@code target_workers}/{@code cursor} do not
 * apply mid-merge) started at merge kickoff and stopped at merge end, on both the success and the
 * exception path. It is a pure observability addition: it never touches merge behavior, ordering,
 * or the merge result — it only reads already-advancing counters via {@link RunMetrics}.
 */
public final class SortMergeHeartbeat implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SortMergeHeartbeat.class);

    private final RunMetrics metrics;
    private final long startedNs;
    private final ScheduledExecutorService executor;

    private SortMergeHeartbeat(RunMetrics metrics, long startedNs, ScheduledExecutorService executor) {
        this.metrics = metrics;
        this.startedNs = startedNs;
        this.executor = executor;
    }

    /** Starts at the default cadence — the same 30s non-TTY interval {@link RunProgressReporter} uses. */
    public static SortMergeHeartbeat start(RunMetrics metrics) {
        return start(metrics, RunProgressReporter.nonTtyInterval());
    }

    public static SortMergeHeartbeat start(RunMetrics metrics, Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("heartbeat interval must be positive");
        }
        ScheduledExecutorService executor = DaemonSchedulers.create("swath-sort-merge-heartbeat");
        SortMergeHeartbeat heartbeat = new SortMergeHeartbeat(metrics, System.nanoTime(), executor);
        DaemonSchedulers.schedule(executor, interval, heartbeat::reportSafely);
        return heartbeat;
    }

    /**
     * Stops the periodic tick and emits one final, synchronous line reflecting the state at close
     * time — called on both the success and the exception path, so the log always shows where the
     * merge left off, whether or not it finished cleanly. Never throws — an observability shutdown
     * must not fail the run.
     */
    @Override
    public void close() {
        DaemonSchedulers.stop(executor, log, "sort_merge_heartbeat_stop_timeout");
        reportSafely();
    }

    private void reportSafely() {
        try {
            long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
            log.info("sort_merge_progress passes={} progress_units={} elapsed_ms={}",
                    metrics.sortMergePassesCount(), metrics.progressUnits(), elapsedMs);
        } catch (RuntimeException e) {
            log.warn("sort_merge_heartbeat_report_failed message={}", e.getMessage());
        }
    }
}
