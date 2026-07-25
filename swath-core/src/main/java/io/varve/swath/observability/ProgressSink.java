/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

/**
 * Live-progress consumer — the seam a presentation layer (today: the CLI's operator-facing stderr
 * display) uses to render a run in flight, the sibling of {@link RunSummarySink} for the terminal
 * block. swath-core owns WHAT a run is doing ({@link ProgressEvent}); every terminal question — is
 * stderr a TTY, is colour wanted, is the provider's cost knowable, how wide is the line — belongs
 * to the layer that can answer it, so no terminal flag is threaded into core.
 *
 * <p>Exactly ONE sink is installed at a time ({@link RunMetrics#setProgressSink}), and the installed
 * sink OWNS the progress channel: a presentation layer REPLACES {@link #LOG} rather than adding to
 * it, so an operator never sees the same tick rendered twice (once as a log record, once as a
 * display frame) on one stderr.
 *
 * <p>{@link #isEnabled()} is checked BEFORE the event is built, so a run with progress switched off
 * — or with the log sink installed while INFO is disabled, which is the default level — pays
 * nothing at all per tick: no snapshot, no counter walk, no rate sampling.
 *
 * <p>Implementations must not throw: a rendering failure must never disturb the run it is
 * describing.
 */
public interface ProgressSink {

    /** Discards every event and reports itself disabled, so no snapshot is built. */
    ProgressSink NONE = new ProgressSink() {

        @Override
        public void accept(ProgressEvent event) {
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    };

    /**
     * The default: one structured {@code progress} record per tick on the run's log, at INFO (so
     * {@code -v} shows it) under {@code RunProgressReporter}'s own logger name. This is the surface
     * an external supervisor tailing the log sees, and the only one a redirected/non-interactive
     * run has — which is why it stays the default rather than {@link #NONE}.
     */
    ProgressSink LOG = new LoggingProgressSink();

    /** Render one tick. Never throws. */
    void accept(ProgressEvent event);

    /**
     * Whether this sink would render anything right now — polled every tick, so it may change over
     * the life of a run (the log sink follows the level).
     */
    default boolean isEnabled() {
        return true;
    }
}
