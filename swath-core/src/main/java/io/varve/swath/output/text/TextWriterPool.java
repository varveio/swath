/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import io.varve.swath.error.OutputException;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.dataset.DatasetWriterPool;
import io.varve.swath.output.dataset.DatasetWriterPoolConfig;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import io.varve.swath.output.parquet.PartListener;
import java.util.List;

/** Text facade over the shared dataset writer implementation. */
public final class TextWriterPool implements DatasetWriterPool {
    private final SharedDatasetWriterPool delegate;

    public TextWriterPool(TextWriterPoolConfig config) {
        TextDatasetFormat format = new TextDatasetFormat(
                config.format(), config.compression(), config.escape());
        DatasetWriterPoolConfig poolConfig = new DatasetWriterPoolConfig(
                "text", config.bucket(), PartListener.NONE, List.of(),
                config.rotationIntervalNanos(), config.rotationMaxRows(), null);
        delegate = new SharedDatasetWriterPool(config.directory(), format, config.argsHash(),
                config.writers(), config.targetBytes(), config.queueCapacity(), poolConfig);
    }

    @Override public void submit(PageBatch batch) throws OutputException, InterruptedException { delegate.submit(batch); }
    @Override public long committedPartCount() { return delegate.committedPartCount(); }
    @Override public long committedBytes() { return delegate.committedBytes(); }
    @Override public void close() throws OutputException { delegate.close(); }
    @Override public void abort() { delegate.abort(); }
}
