/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.error.OutputException;
import io.varve.swath.model.PageBatch;

/** Format-neutral bounded dataset writer lifecycle. */
public interface DatasetWriterPool extends AutoCloseable {
    void submit(PageBatch batch) throws OutputException, InterruptedException;
    long committedPartCount();
    long committedBytes();
    @Override void close() throws OutputException;
    void abort();
}
