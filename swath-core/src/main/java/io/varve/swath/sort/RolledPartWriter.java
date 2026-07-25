/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * The shared publish-roll loop behind both merge paths' final streaming pass: drain a merged sorted
 * cursor into a rolled sequence of files, starting a fresh file each time the current one reaches the
 * roll threshold, and feeding merge-progress in batches of {@link KWayMerge#PROGRESS_BATCH_ROWS}
 * (never per-row — §3.2). The serial publish ({@link SortTransform}) and the off-by-default parallel
 * range-merge ({@link ParallelRangeMerge}) share this identical control loop, roll math, and progress
 * cadence; they differ in exactly two ways, both parameters here:
 *
 * <ul>
 *   <li>{@code fileFactory} — where each rolled file is opened (the serial path writes final
 *       {@code part-*} tmps; each parallel range writes its own range-local {@code prange-*} tmps).</li>
 *   <li>{@code markFinalOnLast} — whether this path stamps global completeness. The serial path
 *       ({@code true}) force-publishes one valid empty file for an empty listing and marks the last
 *       file final, so the output self-describes as complete. The parallel path ({@code false})
 *       deliberately does NEITHER: an empty range produces zero parts and no file is ever marked
 *       final — the ranges carry range-local {@code file_index}es and no {@code file_final}, a
 *       documented limitation of the off-by-default path (see {@link ParallelRangeMerge}'s class javadoc).</li>
 * </ul>
 */
final class RolledPartWriter {

    /** Opens the next rolled file, already registered with the caller's own output bookkeeping. */
    @FunctionalInterface
    interface FileFactory {
        SortedFileWriter open() throws IOException;
    }

    private RolledPartWriter() {
    }

    /**
     * Drain {@code merged} into rolled files opened by {@code fileFactory}, rolling to a fresh file
     * whenever the current one reaches {@code finalFileBytes}; returns the total row count. Feeds
     * {@code progressCallback} one batch per {@link KWayMerge#PROGRESS_BATCH_ROWS} rows written, with
     * any remainder flushed once the cursor is fully drained.
     */
    static long drain(SortedCursor merged, long finalFileBytes, FileFactory fileFactory,
                      boolean markFinalOnLast, LongConsumer progressCallback) throws IOException {
        long totalRows = 0;
        long batchRows = 0;
        SortedFileWriter writer = null;
        try {
            while (merged.hasNext()) {
                if (writer == null || shouldRoll(writer, finalFileBytes)) {
                    if (writer != null) {
                        writer.close();
                    }
                    writer = fileFactory.open();
                }
                writer.write(merged.next());
                totalRows++;
                // §3.2: batched merge-progress feed (never per-row) — see KWayMerge.PROGRESS_BATCH_ROWS.
                if (++batchRows >= KWayMerge.PROGRESS_BATCH_ROWS) {
                    progressCallback.accept(batchRows);
                    batchRows = 0;
                }
            }
            if (markFinalOnLast) {
                if (writer == null) {
                    // Empty listing: still publish one valid, self-describing empty sorted file.
                    writer = fileFactory.open();
                }
                // Whichever writer is open here — whether the loop just drained naturally or the
                // empty-listing branch above opened the only file — is genuinely the LAST file of this
                // publish: no further roll happens after this point. Mark it before close() (which is
                // when the footer, and so the stamp, is actually written). An exceptional exit below
                // (finally) deliberately skips this: an aborted run's last-written file is NOT the true
                // final file and must not claim to be.
                writer.markFinal();
                writer.close();
                writer = null;
            } else if (writer != null) {
                writer.close();
                writer = null;
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
            if (batchRows > 0) {
                progressCallback.accept(batchRows);
            }
        }
        return totalRows;
    }

    private static boolean shouldRoll(SortedFileWriter writer, long finalFileBytes) {
        return finalFileBytes != Long.MAX_VALUE
                && writer.rows() > 0
                && writer.dataSize() >= finalFileBytes;
    }
}
