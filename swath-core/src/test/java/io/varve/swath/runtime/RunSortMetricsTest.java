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
 * <p>These tests pin the live adapter's forwarding contract to the actual run-scoped meters.
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

        new RunSortMetrics(new RunMetrics(registry)).recordStealReason("SORT", "finalization_pipeline");

        // Read the counter back off the registry, not "it did not throw": the first draft of this
        // test asserted progressSignal() >= 0, which passes for a delegate that does nothing at all.
        assertEquals(1.0, registry.get("swath.steal_reason")
            .tag("outcome", "SORT")
            .tag("reason", "finalization_pipeline")
            .counter().count());
    }
}
