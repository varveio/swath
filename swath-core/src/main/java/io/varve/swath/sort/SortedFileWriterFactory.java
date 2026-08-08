/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Creates a {@link SortedFileWriter} for one final output file. {@link SortTransform} writes through
 * this factory so an alternate writer implementation can be swapped in without touching the
 * merge/publish logic. {@link #DEFAULT} is the plain part-writer path (no stamp, standard row groups);
 * the stamped, small-row-group implementation replaces it for real --sort runs.
 */
public interface SortedFileWriterFactory {

    /** Plain, unstamped part-writer path; a stamped implementation replaces it for real --sort runs. */
    SortedFileWriterFactory DEFAULT = (path, fileIndex) -> new DefaultSortedFileWriter(path);

    /**
     * Return a factory whose mutable decorator state is scoped to one ordered output sequence.
     * A serial publish has one sequence across all of its rolled files; a parallel range merge has
     * one sequence per range. Stateless factories may return {@code this}.
     */
    default SortedFileWriterFactory forOutputSequence() {
        return this;
    }

    /**
     * @param fileIndex this file's 1-based position in the final output's roll sequence — known at
     *                   open time, unlike "is this the last file" which is only known when the merge
     *                   completes (see {@link SortedFileWriter#markFinal()}).
     */
    SortedFileWriter create(Path path, int fileIndex) throws IOException;
}
