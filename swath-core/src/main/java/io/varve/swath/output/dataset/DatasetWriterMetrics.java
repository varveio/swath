/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;

/** Shared bounded-summary wiring for every parallel directory-dataset writer. */
public final class DatasetWriterMetrics {
    private DatasetWriterMetrics() {
    }

    public static void registerSummary(
            RunMetrics metrics, String format, SharedDatasetWriterPool writerPool,
            DatasetWriterResourcePlan resourcePlan) {
        if (metrics == null) {
            return;
        }
        metrics.registerDatasetWriterSummary(() -> {
            var lanes = writerPool.laneStatistics().stream()
                        .map(s -> new RunSummary.DatasetWriterLane(
                                s.lane(), s.queueCapacity(), s.queueDepth(), s.queueDepthPeak(),
                                s.waitingForWork(), s.rowsWritten(), s.finalizedBytes(), s.batchesWritten(),
                                s.activeElapsedNanos(), s.submitBlockedCount(), s.submitBlockedNanos(),
                                s.headOfLineBlockedCount(), s.headOfLineBlockedNanos(),
                                s.partsFinalized(), s.finalizeCount(), s.finalizeElapsedNanos()))
                        .toList();
            return RunSummary.DatasetWriterSummary.from(
                    format, lanes,
                    writerPool.rotationIntervalNanos(), writerPool.rotationMaxRows(),
                    writerPool.partDigestCount(), writerPool.partDigestNanos(),
                    writerPool.manifestWriteCount(), writerPool.manifestWriteNanos(),
                    resourcePlan.rowGroupTargetBytesPerWriter(),
                    resourcePlan.rowGroupAllowanceMultiplier(),
                    resourcePlan.plannedHeapBytes(), resourcePlan.heapAdmissionApplied());
        });
    }
}
