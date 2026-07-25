/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;

/**
 * A sorted forward-only source of {@link ListEntry} feeding the {@link StreamingMerger}. Both
 * {@link SegmentReader} (over a Parquet segment) and {@link PageRunCursor} (over a node's in-memory
 * {@link PageBlock} run) implement it, so the one merge core drives both the disk merge phase and
 * the in-memory seal path.
 *
 * <p>The head record is pre-loaded so {@link #peek()} is a pure, non-throwing getter (usable from a
 * {@link java.util.PriorityQueue} comparator during the merge); {@link #next()} performs the read
 * that advances to the following record and may fail.
 */
interface EntryStream extends AutoCloseable {

    /** True iff a head record is available. */
    boolean hasNext();

    /** The current head without consuming it, or {@code null} if exhausted. Never throws. */
    ListEntry peek();

    /** Return the current head and advance to the next record (the read that may fail). */
    ListEntry next() throws IOException;

    @Override
    void close() throws IOException;
}
