/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.Locale;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ProgressSink#LOG}: the structured {@code progress} log record, one per tick, for every
 * phase of the run — the single line schema that replaced the three that existed before (a
 * seed-window reporter, a listing reporter, and a separate {@code sort_merge_progress} heartbeat
 * whose {@code close()} also emitted an extra final line, so a log tail saw three different shapes
 * and one duplicate).
 *
 * <p>Every record carries the same run-level prefix; the tail is whatever the phase actually has
 * (see {@link ProgressEvent}). Logs under {@link RunProgressReporter}'s logger name, NOT this
 * class's own: operator log filters and per-class appender levels are keyed on that name and moving
 * the line to a new one would silently re-home its identity.
 */
final class LoggingProgressSink implements ProgressSink {

    private static final Logger log = LoggerFactory.getLogger(RunProgressReporter.class);

    private static final String COMMON =
            "progress run_id={} phase={} strategy={} elapsed_ms={} phase_elapsed_ms={} api_calls={}"
                    + " cost_usd={} retries={}";
    private static final String COMPLETION = " completed={}/{} {} percent={}";
    private static final String SEEDING = " probes={} probe_budget={} last_probe_age_ms={}";
    private static final String LISTING =
            " objects={} recovered_objects={} live_rate={} avg_rate={} pages={} in_flight={}"
                    + " target_workers={} steals={} splits={}";
    private static final String MERGING = " rows_merged={} staged_rows={} segments={} merge_passes={}";

    @Override
    public void accept(ProgressEvent e) {
        String format = COMMON;
        Object[] args = {e.runId(), e.phase().name().toLowerCase(Locale.ROOT), e.strategy(),
                e.sessionElapsed().toMillis(), e.phaseElapsed().toMillis(), e.apiCalls(),
                e.estimatedCostUsd(), e.retries()};
        if (e.completion() != null) {
            ProgressEvent.Completion c = e.completion();
            format += COMPLETION;
            args = concat(args, c.done(), c.total(), c.unit().name().toLowerCase(Locale.ROOT),
                    Math.round(c.fraction() * 100));
        }
        if (e.seeding() != null) {
            ProgressEvent.Seeding s = e.seeding();
            format += SEEDING;
            args = concat(args, s.probesCompleted(), s.probeBudget(), s.sinceLastProbe().toMillis());
        } else if (e.listing() != null) {
            ProgressEvent.Listing l = e.listing();
            format += LISTING;
            args = concat(args, l.sessionObjects(), l.recoveredObjects(), l.liveObjectsPerSecond(),
                    l.averageObjectsPerSecond(), l.pages(), l.inFlight(), l.concurrencyTarget(),
                    l.steals(), l.splits());
        } else if (e.merging() != null) {
            ProgressEvent.Merging m = e.merging();
            format += MERGING;
            args = concat(args, m.sessionRowsMerged(), m.stagedRows(), m.segments(), m.passes());
        }
        log.info(format, args);
    }

    /** Whether the record would be emitted at all — the gate that skips building the event. */
    @Override
    public boolean isEnabled() {
        return log.isInfoEnabled();
    }

    private static Object[] concat(Object[] head, Object... tail) {
        return Stream.concat(Stream.of(head), Stream.of(tail)).toArray();
    }
}
