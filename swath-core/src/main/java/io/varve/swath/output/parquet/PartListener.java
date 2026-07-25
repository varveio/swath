/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.util.Map;

/**
 * Notified when a writer lane <b>finalizes</b> a part (footer fsynced). This is
 * the exactly-once durability commit point (algorithms.md §4.5, I6): the
 * checkpoint records the finalized {@code part_file} and advances each contained
 * node's {@code durable_cursor} <i>in one transaction</i>, after the footer is on
 * disk. A crash between the footer fsync and this commit leaves the part
 * un-recorded ⇒ resume discards it and re-lists its tail (no finalized row lost or
 * duplicated). Kept checkpoint-agnostic so the writer pool doesn't depend on the
 * checkpoint package; the runtime adapts the event onto {@code partFinalized}.
 */
@FunctionalInterface
public interface PartListener {

    /** No-op listener for non-checkpointed runs ({@code --checkpoint none} / direct tests). */
    PartListener NONE = e -> { };

    void onFinalized(PartFinalizedEvent e) throws Exception;

    /**
     * @param writerId    the lane id ({@code node_id % numWriters})
     * @param fileName    the finalized part's file name (within the output dir)
     * @param rows        rows in the part
     * @param bytes       part size on disk
     * @param nodeMaxKeys per node id, the highest key whose pages this part made durable
     */
    record PartFinalizedEvent(int writerId, String fileName, long rows, long bytes,
                              Map<Long, byte[]> nodeMaxKeys) {
    }
}
