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
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.dataset.DatasetDataSyncMetrics;
import io.varve.swath.output.dataset.DatasetFormat;
import io.varve.swath.output.dataset.DatasetWriterMetrics;
import io.varve.swath.output.dataset.DatasetWriterObserver;
import io.varve.swath.output.dataset.DatasetWriterPool;
import io.varve.swath.output.dataset.DatasetWriterPoolConfig;
import io.varve.swath.output.dataset.LaneRouting;
import io.varve.swath.output.dataset.DatasetWriterResourcePlan;
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
        this(createDelegate(dir, argsHash, writers, targetBytes, queueCapacity,
                config, new ParquetDatasetFormat(schema, config.writebackBytes()), null),
                config.metrics(), writers);
    }

    ParquetWriterPool(Path dir, MessageType schema, String argsHash,
                      int writers, long targetBytes, int queueCapacity,
                      ParquetWriterPoolConfig config, LongSupplier nanoClock) {
        this(createDelegate(dir, argsHash, writers, targetBytes, queueCapacity,
                config, new ParquetDatasetFormat(schema, config.writebackBytes()), nanoClock),
                config.metrics(), writers);
    }

    private ParquetWriterPool(SharedDatasetWriterPool delegate, RunMetrics metrics, int writers) {
        this.delegate = delegate;
        registerSummary(metrics, writers);
    }

    static ParquetWriterPool withDataForcer(
            Path dir, MessageType schema, String argsHash, int writers, long targetBytes,
            int queueCapacity, ParquetWriterPoolConfig config,
            SyncableLocalOutputFile.DataForcer dataForcer) {
        DatasetFormat format = new ParquetDatasetFormat(schema, config.writebackBytes())
                .withDataForcer(dataForcer);
        return new ParquetWriterPool(createDelegate(dir, argsHash, writers,
                targetBytes, queueCapacity, config, format, null), config.metrics(), writers);
    }

    private static SharedDatasetWriterPool createDelegate(
            Path dir, String argsHash, int writers, long targetBytes,
            int queueCapacity, ParquetWriterPoolConfig config, DatasetFormat format,
            LongSupplier nanoClock) {
        DatasetWriterPoolConfig shared = sharedConfig(config);
        if (nanoClock == null) {
            return new SharedDatasetWriterPool(dir, format, argsHash,
                    writers, targetBytes, queueCapacity, shared);
        }
        return new SharedDatasetWriterPool(dir, format, argsHash,
                writers, targetBytes, queueCapacity, shared, nanoClock);
    }

    private void registerSummary(RunMetrics metrics, int writers) {
        var resourcePlan = new DatasetWriterResourcePlan(
                PartWriter.ROW_GROUP_BYTES,
                ParquetWriterMemoryPlan.ROW_GROUP_ALLOWANCE_MULTIPLIER,
                ParquetWriterMemoryPlan.plannedHeapBytes(writers),
                writers > ParquetWriterMemoryPlan.RELEASE_ENVELOPE_MAX_WRITERS);
        DatasetWriterMetrics.registerSummary(metrics, OutputFormat.PARQUET.name().toLowerCase(),
                delegate, resourcePlan);
    }

    private static DatasetWriterPoolConfig sharedConfig(ParquetWriterPoolConfig config) {
        return new DatasetWriterPoolConfig("parquet", config.bucket(), config.partListener(),
                config.existingParts(), config.rotationIntervalNanos(), config.rotationMaxRows(),
                observer(config.metrics()), LaneRouting.STICKY);
    }

    private static DatasetWriterObserver observer(RunMetrics metrics) {
        if (metrics == null) {
            return DatasetWriterObserver.NONE;
        }
        DatasetDataSyncMetrics syncMetrics =
                new DatasetDataSyncMetrics(metrics, "parquet", DatasetDataSyncMetrics.Classification.PARQUET);
        return new DatasetWriterObserver() {
            @Override public void recordLaneWork(long elapsedNanos) { metrics.recordParquetWrite(elapsedNanos); }
            @Override public void recordRotation(String reason) { metrics.recordParquetRotation(reason); }
            @Override public Object startFinalize() { return metrics.startParquetFinalizeTimer(); }
            @Override public void recordFinalize(Object sample) {
                metrics.recordParquetFinalizeLatency((Timer.Sample) sample);
            }
            @Override public void recordPeriodicSync(long elapsedNanos, long bytes) {
                syncMetrics.recordSync(elapsedNanos, bytes);
            }
            @Override public void recordPeriodicSyncResidual(long bytes) {
                syncMetrics.recordResidual(bytes);
            }
            @Override public void recordPart(String result) { metrics.recordParquetPart(result); }
            @Override public void markProgress() { metrics.markProgress(); }
        };
    }

    @Override public void submit(PageBatch batch) throws OutputException, InterruptedException {
        delegate.submit(batch);
    }

    @Override public long committedPartCount() { return delegate.committedPartCount(); }

    @Override public long committedBytes() { return delegate.committedBytes(); }

    @Override public void close() throws OutputException { delegate.close(); }

    @Override public void abort() { delegate.abort(); }
}
