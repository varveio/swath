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
import io.varve.swath.output.dataset.DatasetWriterMetrics;
import io.varve.swath.output.dataset.DatasetWriterObserver;
import io.varve.swath.output.dataset.DatasetWriterPool;
import io.varve.swath.output.dataset.DatasetWriterPoolConfig;
import io.varve.swath.output.dataset.DatasetWriterResourcePlan;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import io.varve.swath.output.parquet.PartListener;
import java.util.List;
import java.util.Locale;

/** Text facade over the shared dataset writer implementation. */
public final class TextWriterPool implements DatasetWriterPool {
    private final SharedDatasetWriterPool delegate;

    public TextWriterPool(TextWriterPoolConfig config) {
        TextDatasetFormat format = new TextDatasetFormat(
                config.format(), config.compression(), config.escape());
        DatasetWriterPoolConfig poolConfig = new DatasetWriterPoolConfig(
                "text", config.bucket(), PartListener.NONE, List.of(),
                config.rotationIntervalNanos(), config.rotationMaxRows(), observer(config.metrics()));
        delegate = new SharedDatasetWriterPool(config.directory(), format, config.argsHash(),
                config.writers(), config.targetBytes(), config.queueCapacity(), poolConfig);
        DatasetWriterMetrics.registerSummary(config.metrics(),
                config.format().name().toLowerCase(Locale.ROOT), delegate,
                DatasetWriterResourcePlan.NONE);
    }

    private static DatasetWriterObserver observer(RunMetrics metrics) {
        if (metrics == null) {
            return DatasetWriterObserver.NONE;
        }
        return new DatasetWriterObserver() {
            @Override public void recordLaneWork(long elapsedNanos) { metrics.recordTextDatasetWrite(elapsedNanos); }
            @Override public void recordRotation(String reason) { metrics.recordTextDatasetRotation(reason); }
            @Override public Object startFinalize() { return metrics.startTextDatasetFinalizeTimer(); }
            @Override public void recordFinalize(Object sample) {
                metrics.recordTextDatasetFinalizeLatency((Timer.Sample) sample);
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
