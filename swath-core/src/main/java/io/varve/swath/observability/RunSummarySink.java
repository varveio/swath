/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

/**
 * Terminal run-summary consumer — the seam a presentation layer (today: the CLI's operator-facing
 * stderr block) uses to render a finished run WITHOUT widening the {@code ListRunner.run*}
 * signatures, all ~20 of which return {@code ListingStatistics}.
 *
 * <p>The sink is handed the very objects the {@code list_run_summary}/{@code list_run_diagnostics}
 * log lines and the JSON run report are built from, so those surfaces cannot drift apart: one
 * source, several renderings. It is invoked exactly once per run, at the same terminal points the
 * JSON sidecar writes its terminal record — including the unwound (cancelled/failed) path, where
 * {@code status} carries the attributed {@link StopReason} instead of {@link StopReason#COMPLETED}.
 *
 * <p>Implementations must not throw: the sink runs on the run's terminal path, where a rendering
 * failure would mask the disposition it is trying to report.
 */
@FunctionalInterface
public interface RunSummarySink {

    /** Discards every summary — the installed sink until a presentation layer replaces it. */
    RunSummarySink NONE = (summary, diagnostics, status) -> {
    };

    /**
     * @param summary the terminal {@link RunSummary} (the same instance the JSON report writes)
     * @param diagnostics the terminal {@link RunMetrics.RunDiagnostics}, which carries the
     *         fault counters ({@code throttleEvents}/{@code transientEvents}) the summary does not
     * @param status the run's terminal disposition — {@link StopReason#COMPLETED} for a clean run,
     *         a {@code null} {@link JsonRunSummaryWriter.TerminalStatus#reason()} for a broken pipe
     *         (a downstream close is not a failure), any other reason for a partial
     */
    void accept(RunSummary summary, RunMetrics.RunDiagnostics diagnostics,
            JsonRunSummaryWriter.TerminalStatus status);
}
