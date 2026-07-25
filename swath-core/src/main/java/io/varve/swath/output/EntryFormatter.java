/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.ListEntry;
import java.io.Closeable;
import java.io.IOException;

/**
 * Sealed output-formatter SPI. One instance per sink/part.
 * {@code close()} flushes (Parquet writes its footer ⇒ the part becomes
 * COMPLETE-able, I6).
 */
public sealed interface EntryFormatter extends Closeable
        permits ParquetFormatter, JsonlFormatter, TsvFormatter, AlignedFormatter {

    void writeHeader() throws IOException;

    void write(ListEntry e) throws IOException;

    @Override
    void close() throws IOException;
}
