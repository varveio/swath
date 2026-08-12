/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        var registry = new SimpleMeterRegistry();

        new RunSortMetrics(new RunMetrics(registry)).recordStealReason("SORT", "merge_range_sample_capped");

        // Read the counter back off the registry, not "it did not throw": the first draft of this
        // test asserted progressSignal() >= 0, which passes for a delegate that does nothing at all.
        assertEquals(1.0, registry.get("swath.steal_reason")
            .tag("outcome", "SORT")
            .tag("reason", "merge_range_sample_capped")
            .counter().count());
    }

    @Test
    void boundaryIoStillReaches() {
        var registry = new SimpleMeterRegistry();

        new RunSortMetrics(new RunMetrics(registry)).recordBoundaryIo(7, 123, 456);

        assertEquals(7.0, registry.get("swath.sort.merge.boundaries.embedded.entries").counter().count());
        assertEquals(123.0, registry.get("swath.sort.merge.boundaries.embedded.bytes").counter().count());
        assertEquals(456.0, registry.get("swath.sort.merge.boundaries.scan.bytes").counter().count());
    }
}
