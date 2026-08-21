/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.util.Optional;

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

    /**
     * Sets this file's 1-based position in the output's roll sequence, overriding whatever the
     * factory was given. Honoured any time before {@link #close()}, exactly like {@link #markFinal()}.
     *
     * <p>The parallel range merge is the caller that needs it: a range writes its parts before it can
     * know how many parts the ranges BELOW it produced, so global indices are assigned once every
     * range has drained and the full ordered part list is known.
     *
     * <p><b>Deliberately abstract, unlike {@link #markFinal()}.</b> A {@code default} no-op here is a
     * trap: every DECORATOR of this interface then silently swallows the call, and the completeness
     * stamp degrades to the range-local one with no compile error and no test failure — which is
     * exactly what happened, in two separate decorators at once, while every direct-construction test
     * passed. Forcing each implementation to say what it does is the only guard that scales to the
     * next decorator someone adds.
     */
    void setFileIndex(int fileIndex);

    /**
     * Immutable publish metadata, available only after a successful durable {@link #close()}.
     * Writers that cannot capture it inline return empty and leave the publisher's existing
     * validation/readback path in force.
     */
    default Optional<FinalPartMetadata> finalMetadata() {
        return Optional.empty();
    }

    /** Finalize (footer) and fsync. */
    @Override
    void close() throws IOException;
}
