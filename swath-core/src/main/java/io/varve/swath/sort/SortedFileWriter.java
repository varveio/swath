/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;

/**
 * Writes the <b>final</b> sorted output file. Implemented by the stamped {@code SortedParquetWriter}
 * (footer sortedness stamp + small seek-friendly row groups) and by {@link
 * SortedFileWriterFactory#DEFAULT}, the plain existing part-writer path. {@link #close()} finalizes
 * + fsyncs the file.
 */
public interface SortedFileWriter extends AutoCloseable {

    void write(ListEntry e) throws IOException;

    /** Rows written so far. */
    long rows();

    /** Uncompressed buffered bytes — the {@code final-file-bytes} roll signal (§2). */
    long dataSize();

    /**
     * Marks this file as the LAST file of a multi-file (or single-file) sorted output, to be called
     * — if at all — after the caller knows no further file will be opened and before {@link #close()}.
     * The stamped {@code SortedParquetWriter} records
     * this as an additive footer key so the replay server can prove a resolved file set is COMPLETE
     * (self-describing, no sidecar) even after a crash mid-publish leaves only a prefix of the files.
     * Default no-op: writers that don't stamp completeness (e.g. the unstamped {@link
     * SortedFileWriterFactory#DEFAULT}) simply ignore it.
     */
    default void markFinal() {
    }

    /** Finalize (footer) and fsync. */
    @Override
    void close() throws IOException;
}
