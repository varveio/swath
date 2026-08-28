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

    @Test
    void pageAwareOverlapMetersReachTheLiveRegistry() {
        var registry = new SimpleMeterRegistry();
        var adapter = new RunSortMetrics(new RunMetrics(registry));

        adapter.recordPageAwareOverlapCluster();
        adapter.recordPageAwareOverlapState(2, 10);
        adapter.recordPageAwareOverlapState(3, 8);

        assertEquals(1.0, registry.get("swath.sort.merge.overlap.clusters").counter().count());
        assertEquals(3.0, registry.get("swath.sort.merge.overlap.pages.peak").gauge().value());
        assertEquals(10.0, registry.get("swath.sort.merge.overlap.rows.peak").gauge().value());
    }

    @Test
    void rangeIndexBytesStillReach() {
        var registry = new SimpleMeterRegistry();

        new RunSortMetrics(new RunMetrics(registry)).recordRangeIndexBytes(789);

        assertEquals(789.0, registry.get("swath.sort.merge.range.index.bytes").counter().count());
    }

    @Test
    void proofSpoolOperationsBytesAndTimeReach() {
        var registry = new SimpleMeterRegistry();

        new RunSortMetrics(new RunMetrics(registry)).recordProofSpool(7, 1_234, 5_000_000);

        assertEquals(7.0,
                registry.get("swath.sort.merge.proof_spool.operations").counter().count());
        assertEquals(1_234.0,
                registry.get("swath.sort.merge.proof_spool.bytes").counter().count());
        assertEquals(5.0, registry.get("swath.sort.merge.proof_spool.latency")
                .timer().totalTime(java.util.concurrent.TimeUnit.MILLISECONDS));
    }
}
