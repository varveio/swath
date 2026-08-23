/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.ParquetWriterMemoryPlan;
import io.varve.swath.output.parquet.PartWriter;
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
        metrics.registerDatasetWriterSummary(() -> {
            var lanes = writerPool.laneStatistics().stream()
                        .map(s -> new RunSummary.DatasetWriterLane(
                                s.lane(), s.queueCapacity(), s.queueDepth(), s.queueDepthPeak(),
                                s.waitingForWork(), s.rowsWritten(), s.finalizedBytes(), s.batchesWritten(),
                                s.activeElapsedNanos(), s.submitBlockedCount(), s.submitBlockedNanos(),
                                s.headOfLineBlockedCount(), s.headOfLineBlockedNanos(),
                                s.partsFinalized(), s.finalizeCount(), s.finalizeElapsedNanos()))
                        .toList();
            Long bufferBytesPerWriter = format == OutputFormat.PARQUET
                    ? PartWriter.ROW_GROUP_BYTES : null;
            Long plannedHeapBytes = format == OutputFormat.PARQUET
                    ? ParquetWriterMemoryPlan.plannedHeapBytes(lanes.size()) : null;
            return RunSummary.DatasetWriterSummary.from(
                    summaryFormat, lanes, bufferBytesPerWriter, plannedHeapBytes);
        });
    }
}
