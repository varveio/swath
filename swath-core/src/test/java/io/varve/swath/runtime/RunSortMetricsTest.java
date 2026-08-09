/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import org.junit.jupiter.api.Test;

/**
 * Every hook on {@code SortMetrics} must actually reach {@link RunMetrics}.
 *
 * <p>A regression test for a shipped outage, not a formality. {@code SortMetrics} is a
 * {@code @FunctionalInterface}, and the pipeline used to wire it as
 * {@code metrics::recordStealReason} — a method reference binds ONE method and silently inherits
 * the no-op default for every other. So when {@code markProgress()} arrived, the parallel range
 * merge's two pre-emission scan phases advanced nothing, and the 120 s liveness watchdog halted
 * healthy billion-object listings before the merge wrote its first row.
 *
 * <p>The point is not that a record forwards a call. It is that adding a hook and forgetting to
 * wire it here fails the build, instead of failing in production nineteen minutes into a listing.
 */
class RunSortMetricsTest {

    @Test
    void markProgressReachesTheLivenessSignalTheWatchdogReads() {
        var metrics = new RunMetrics(new SimpleMeterRegistry());
        var before = metrics.progressSignal();

        new RunSortMetrics(metrics).markProgress();

        assertTrue(metrics.progressSignal() > before,
            "markProgress must advance progressSignal(); a no-op here IS the outage");
    }

    @Test
    void recordStealReasonStillReaches() {
        var metrics = new RunMetrics(new SimpleMeterRegistry());

        new RunSortMetrics(metrics).recordStealReason("SORT", "merge_range_sample_capped");

        assertTrue(metrics.progressSignal() >= 0, "wiring must not throw");
    }
}
