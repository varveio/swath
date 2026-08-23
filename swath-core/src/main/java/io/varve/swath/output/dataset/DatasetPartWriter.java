/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Path;

/** Format-specific state for one open dataset part. */
public interface DatasetPartWriter {
    Path path();
    long rows();
    long bufferedDataSize();
    void write(ListEntry entry) throws IOException;
    void close() throws IOException;
    void discard() throws IOException;

    /** Byte-exact MD5 of the physical part, available only after {@link #close()} returns. */
    String md5();

    /** CPU time spent maintaining {@link #md5()} on the physical-write path. */
    long digestNanos();
}
