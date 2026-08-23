/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.dataset.DatasetWriterObserver;
import io.varve.swath.output.dataset.DatasetWriterPool;
import io.varve.swath.output.dataset.DatasetWriterPoolConfig;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import java.nio.file.Path;
import java.util.function.LongSupplier;
import org.apache.parquet.schema.MessageType;

/** Parquet construction facade over the format-neutral dataset writer machinery. */
public final class ParquetWriterPool implements DatasetWriterPool {
    private final SharedDatasetWriterPool delegate;

    public ParquetWriterPool(Path dir, MessageType schema, String argsHash,
                             int writers, long targetBytes, int queueCapacity) {
        this(dir, schema, argsHash, writers, targetBytes, queueCapacity, ParquetWriterPoolConfig.DEFAULT);
    }

    public ParquetWriterPool(Path dir, MessageType schema, String argsHash,
                             int writers, long targetBytes, int queueCapacity,
                             ParquetWriterPoolConfig config) {
        delegate = new SharedDatasetWriterPool(dir, new ParquetDatasetFormat(schema), argsHash,
                writers, targetBytes, queueCapacity, sharedConfig(config));
        registerLaneSummary(config.metrics(), delegate);
    }

    ParquetWriterPool(Path dir, MessageType schema, String argsHash,
                      int writers, long targetBytes, int queueCapacity,
                      ParquetWriterPoolConfig config, LongSupplier nanoClock) {
        delegate = new SharedDatasetWriterPool(dir, new ParquetDatasetFormat(schema), argsHash,
                writers, targetBytes, queueCapacity, sharedConfig(config), nanoClock);
        registerLaneSummary(config.metrics(), delegate);
    }

    private static DatasetWriterPoolConfig sharedConfig(ParquetWriterPoolConfig config) {
        return new DatasetWriterPoolConfig("parquet", config.bucket(), config.partListener(),
                config.existingParts(), config.rotationIntervalNanos(), config.rotationMaxRows(),
                observer(config.metrics()));
    }

    private static DatasetWriterObserver observer(RunMetrics metrics) {
        if (metrics == null) {
            return DatasetWriterObserver.NONE;
        }
        return new DatasetWriterObserver() {
            @Override public void recordLaneWork(long elapsedNanos) { metrics.recordParquetWrite(elapsedNanos); }
            @Override public void recordRotation(String reason) { metrics.recordParquetRotation(reason); }
            @Override public Object startFinalize() { return metrics.startParquetFinalizeTimer(); }
            @Override public void recordFinalize(Object sample) {
                metrics.recordParquetFinalizeLatency((Timer.Sample) sample);
            }
            @Override public void recordPart(String result) { metrics.recordParquetPart(result); }
            @Override public void markProgress() { metrics.markProgress(); }
        };
    }

    private static void registerLaneSummary(RunMetrics metrics, SharedDatasetWriterPool writerPool) {
        if (metrics == null) {
            return;
        }
        metrics.registerParquetWriterSummary(() -> RunSummary.ParquetWriterSummary.from(
                writerPool.laneStatistics().stream()
                        .map(s -> new RunSummary.ParquetWriterLane(
                                s.lane(), s.queueCapacity(), s.queueDepth(), s.queueDepthPeak(),
                                s.waitingForWork(), s.rowsWritten(), s.finalizedBytes(), s.batchesWritten(),
                                s.activeElapsedNanos(), s.submitBlockedCount(), s.submitBlockedNanos(),
                                s.headOfLineBlockedCount(), s.headOfLineBlockedNanos(),
                                s.partsFinalized(), s.finalizeCount(), s.finalizeElapsedNanos()))
                        .toList()));
    }

    @Override public void submit(PageBatch batch) throws OutputException, InterruptedException {
        delegate.submit(batch);
    }

    @Override public long committedPartCount() { return delegate.committedPartCount(); }

    @Override public long committedBytes() { return delegate.committedBytes(); }

    @Override public void close() throws OutputException { delegate.close(); }

    @Override public void abort() { delegate.abort(); }
}
