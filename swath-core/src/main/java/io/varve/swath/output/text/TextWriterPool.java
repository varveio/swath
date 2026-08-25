/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.dataset.DatasetFormat;
import io.varve.swath.output.dataset.DatasetWriterMetrics;
import io.varve.swath.output.dataset.DatasetWriterObserver;
import io.varve.swath.output.dataset.DatasetWriterPool;
import io.varve.swath.output.dataset.DatasetWriterPoolConfig;
import io.varve.swath.output.dataset.DatasetWriterResourcePlan;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import io.varve.swath.output.parquet.PartListener;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/** Text facade over the shared dataset writer implementation. */
public final class TextWriterPool implements DatasetWriterPool {
    private final SharedDatasetWriterPool delegate;

    public TextWriterPool(TextWriterPoolConfig config) {
        this(createDelegate(config, textFormat(config)));
    }

    private TextWriterPool(SharedDatasetWriterPool delegate) {
        this.delegate = delegate;
    }

    static TextWriterPool withDataForcer(
            TextWriterPoolConfig config, TextDatasetFormat.DataForcer dataForcer) {
        return new TextWriterPool(createDelegate(
                config, textFormat(config).withDataForcer(dataForcer)));
    }

    private static TextDatasetFormat textFormat(TextWriterPoolConfig config) {
        return new TextDatasetFormat(config.format(), config.compression(), config.escape(),
                config.writebackBytes());
    }

    private static SharedDatasetWriterPool createDelegate(
            TextWriterPoolConfig config, DatasetFormat format) {
        DatasetWriterPoolConfig poolConfig = new DatasetWriterPoolConfig(
                "text", config.bucket(), PartListener.NONE, List.of(),
                config.rotationIntervalNanos(), config.rotationMaxRows(), observer(config));
        SharedDatasetWriterPool delegate = new SharedDatasetWriterPool(
                config.directory(), format, config.argsHash(),
                config.writers(), config.targetBytes(), config.queueCapacity(), poolConfig);
        DatasetWriterMetrics.registerSummary(config.metrics(),
                config.format().name().toLowerCase(Locale.ROOT), delegate,
                DatasetWriterResourcePlan.NONE);
        if (config.metrics() != null && config.format() == OutputFormat.TSV) {
            config.metrics().recordStealReason("OUTPUT", "tsv_byte_encoder");
            config.metrics().recordStealReason("OUTPUT",
                    config.escape() ? "tsv_escape_on" : "tsv_raw_output");
        }
        return delegate;
    }

    private static DatasetWriterObserver observer(TextWriterPoolConfig config) {
        RunMetrics metrics = config.metrics();
        if (metrics == null) {
            return DatasetWriterObserver.NONE;
        }
        String format = config.format().name().toLowerCase(Locale.ROOT);
        AtomicBoolean syncEngaged = new AtomicBoolean();
        return new DatasetWriterObserver() {
            @Override public void recordLaneWork(long elapsedNanos) { metrics.recordTextDatasetWrite(elapsedNanos); }
            @Override public void recordRotation(String reason) { metrics.recordTextDatasetRotation(reason); }
            @Override public Object startFinalize() { return metrics.startTextDatasetFinalizeTimer(); }
            @Override public void recordFinalize(Object sample) {
                metrics.recordTextDatasetFinalizeLatency((Timer.Sample) sample);
            }
            @Override public void recordPeriodicSync(long elapsedNanos, long bytes) {
                metrics.recordDatasetDataSync(format, elapsedNanos, bytes);
                if (syncEngaged.compareAndSet(false, true)) {
                    metrics.recordStealReason("OUTPUT", "data_sync");
                    metrics.recordStealReason("OUTPUT",
                            config.compression() == TextCompression.NONE
                                    ? "data_sync_text_uncompressed"
                                    : "data_sync_text_compressed");
                }
            }
            @Override public void recordPeriodicSyncResidual(long bytes) {
                metrics.recordDatasetDataSyncResidual(format, bytes);
            }
            @Override public void recordPart(String result) { metrics.recordTextDatasetPart(result); }
            @Override public void markProgress() { metrics.markProgress(); }
        };
    }

    @Override public void submit(PageBatch batch) throws OutputException, InterruptedException { delegate.submit(batch); }
    @Override public long committedPartCount() { return delegate.committedPartCount(); }
    @Override public long committedBytes() { return delegate.committedBytes(); }
    @Override public void close() throws OutputException { delegate.close(); }
    @Override public void abort() { delegate.abort(); }
}
