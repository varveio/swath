/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

/**
 * How {@link SharedDatasetWriterPool#submit} picks a lane for a page.
 *
 * <p>{@link #STICKY} sends every page of a node to {@code nodeId % numWriters}, so a node's pages
 * occupy a contiguous run of one lane's size-rotated parts (which finalize in order). That is what
 * makes the {@code durable_cursor} resume model sound, so any sink with a part listener needs it.
 * Its cost is head-of-line blocking: the single dispatcher waits on one full lane while others
 * idle, which on a billion-object TSV run held the 16 lanes at ~60% busy behind a full-queue
 * median wait of ~110 ms per page.
 *
 * <p>{@link #SPILL} keeps sticky placement while the sticky lane has queue space, and only when
 * that queue is full sends the page to the lane with the shortest queue instead of blocking. A
 * node's pages therefore stay contiguous in the common case and spread across lanes and parts
 * only under pressure. Sinks that offer no resume contract (the text datasets) lose nothing by it.
 */
public enum LaneRouting {
    STICKY,
    SPILL
}
