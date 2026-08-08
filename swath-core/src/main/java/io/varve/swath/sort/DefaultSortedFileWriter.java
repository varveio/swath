/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartWriter;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The default final-file writer: the plain existing high-level {@link PartWriter} path (canonical
 * schema, standard row groups, finalize + fsync on close). An alternate {@link
 * SortedFileWriterFactory} supplies the stamped {@code SortedParquetWriter} (footer stamp + small
 * row groups).
 */
final class DefaultSortedFileWriter implements SortedFileWriter {

    private final PartWriter delegate;

    DefaultSortedFileWriter(Path path) throws IOException {
        this.delegate = new PartWriter(path, ParquetSchema.canonical());
    }

    @Override
    public void write(ListEntry e) throws IOException {
        delegate.write(e);
    }

    @Override
    public long rows() {
        return delegate.rows();
    }

    @Override
    public long dataSize() {
        return delegate.bufferedDataSize();
    }

    /**
     * No-op, and genuinely so: this writer emits no sortedness stamp at all, so it has no
     * {@code file_index} to correct. Stated explicitly rather than inherited from a {@code default},
     * because a silent inherited no-op is what let two decorators drop the call unnoticed.
     */
    @Override
    public void setFileIndex(int fileIndex) {
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
