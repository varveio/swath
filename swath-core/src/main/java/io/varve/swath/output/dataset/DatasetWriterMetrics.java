/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.OutputFormat;
import java.util.Locale;

/** Shared bounded-summary wiring for every parallel directory-dataset writer. */
public final class DatasetWriterMetrics {
    private DatasetWriterMetrics() {
    }

    public static void registerSummary(
            RunMetrics metrics, OutputFormat format, SharedDatasetWriterPool writerPool) {
        if (metrics == null) {
            return;
        }
        String summaryFormat = switch (format) {
            case PARQUET, TSV, JSONL -> format.name().toLowerCase(Locale.ROOT);
            case TABLE -> throw new IllegalArgumentException("parallel dataset format required");
        };
        metrics.registerDatasetWriterSummary(() -> RunSummary.DatasetWriterSummary.from(
                summaryFormat,
                writerPool.laneStatistics().stream()
                        .map(s -> new RunSummary.DatasetWriterLane(
                                s.lane(), s.queueCapacity(), s.queueDepth(), s.queueDepthPeak(),
                                s.waitingForWork(), s.rowsWritten(), s.finalizedBytes(), s.batchesWritten(),
                                s.activeElapsedNanos(), s.submitBlockedCount(), s.submitBlockedNanos(),
                                s.headOfLineBlockedCount(), s.headOfLineBlockedNanos(),
                                s.partsFinalized(), s.finalizeCount(), s.finalizeElapsedNanos()))
                        .toList()));
    }
}
