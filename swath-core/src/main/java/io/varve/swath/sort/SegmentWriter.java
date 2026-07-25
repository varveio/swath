/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Flushes a sorted stream as ONE internally-sorted Parquet staging segment. Two entry
 * points share the same {@link SegmentParquetSink} ({@code segmentRowGroupBytes} row groups —
 * default 1&nbsp;MB, small on purpose so a merge with many open segment streams stays
 * memory-bounded — finalize + fsync):
 *
 * <ul>
 *   <li>{@link #flush} — the listing-phase seal: k-way merges a {@link SealedBuffer}'s per-node page
 *       runs straight into the segment, and returns the per-node max keys the checkpoint
 *       needs. Emits {@code SORT.segment_flushed} and, when the byte gate forced the seal,
 *       {@code SORT.buffer_byte_gated}.</li>
 *   <li>{@link #writeIntermediate} — a merge-phase cascade intermediate: writes an already-merged
 *       {@link SortedCursor} to a fresh segment. No per-node tracking, no counters (the merge phase
 *       has its own).</li>
 * </ul>
 */
final class SegmentWriter {

    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final long segmentRowGroupBytes;

    SegmentWriter(Comparator<ListEntry> comparator, DuplicateHook hook, SortMetrics metrics,
                 long segmentRowGroupBytes) {
        this.comparator = comparator;
        this.hook = hook;
        this.metrics = metrics;
        this.segmentRowGroupBytes = segmentRowGroupBytes;
    }

    /** Merge the sealed buffer's per-node runs into a segment at {@code path}. */
    SegmentResult flush(SealedBuffer buffer, Path path) throws IOException {
        long rows;
        try (SortedCursor cursor = buffer.merge(comparator, hook, metrics)) {
            rows = writeCursor(cursor, path);
        }
        metrics.recordStealReason("SORT", "segment_flushed");
        if (buffer.trigger() == SealTrigger.BYTE_GATE) {
            metrics.recordStealReason("SORT", "buffer_byte_gated");
        }
        return new SegmentResult(path, rows, Files.size(path), buffer.perNodeMaxKeys());
    }

    /** Write an already-sorted stream to a fresh intermediate segment; the caller closes {@code sorted}. */
    long writeIntermediate(SortedCursor sorted, Path path) throws IOException {
        return writeCursor(sorted, path);
    }

    private long writeCursor(SortedCursor cursor, Path path) throws IOException {
        long rows = 0;
        try (SegmentParquetSink sink = new SegmentParquetSink(path, segmentRowGroupBytes)) {
            while (cursor.hasNext()) {
                sink.write(cursor.next());
                rows++;
            }
        }
        return rows;
    }
}
