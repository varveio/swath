/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Single ordered router over decoded reader slots. Disjoint pages move directly to encoders;
 * overlap clusters use the same {@link PageRowMerger} as the existing page-aware merge.
 */
final class MergeRouter {
    static final int BATCH_ROWS = 4_096;
    static final int MAX_CLUSTER_PAGES = 64;

    record Result(long rows, long pagesForwarded, int parts) {
    }

    private final SegmentReaderSlots readers;
    private final PartEncoders encoders;
    private final PipelinePartSizer sizer;
    private final Comparator<ListEntry> comparator;
    private final SortMetrics metrics;
    private final PipelineFailure failure;
    private final PriorityQueue<Head> frontier = new PriorityQueue<>((left, right) -> {
        int order = Arrays.compareUnsigned(left.page.firstKeyUnsafe(), right.page.firstKeyUnsafe());
        return order != 0 ? order : Integer.compare(left.segment, right.segment);
    });
    private final PartBatcher batcher;
    private long rows;
    private long pagesForwarded;

    MergeRouter(SegmentReaderSlots readers, PartEncoders encoders, PipelinePartSizer sizer,
            Comparator<ListEntry> comparator, SortMetrics metrics, PipelineFailure failure) {
        this.readers = readers;
        this.encoders = encoders;
        this.sizer = sizer;
        this.comparator = comparator;
        this.metrics = metrics;
        this.failure = failure;
        this.batcher = new PartBatcher();
    }

    Result route(int segments) throws IOException {
        for (int segment = 0; segment < segments; segment++) {
            PageBlock page = readers.next(segment);
            if (page != null) {
                frontier.add(new Head(segment, page));
            }
        }
        while (!frontier.isEmpty()) {
            failure.check();
            Head head = frontier.poll();
            advance(head.segment);
            if (frontier.isEmpty()
                    || Arrays.compareUnsigned(head.page.lastKeyUnsafe(),
                            frontier.peek().page.firstKeyUnsafe()) < 0) {
                batcher.offer(new PipelineBatch.WholePage(head.page));
                rows = Math.addExact(rows, head.page.count());
                pagesForwarded++;
                metrics.recordPipelinePagesForwarded(1);
            } else {
                mergeCluster(head);
            }
        }
        int parts = batcher.finish();
        return new Result(rows, pagesForwarded, parts);
    }

    private void mergeCluster(Head first) throws IOException {
        metrics.recordStealReason("SORT", "pipeline_cluster_merge");
        PageRowMerger cluster = new PageRowMerger(comparator);
        cluster.add(first.segment, first.page, 0);
        byte[] ceiling = first.page.lastKeyUnsafe();
        int clusterPages = 1;
        long clusterRows = first.page.count();
        while (!frontier.isEmpty()
                && Arrays.compareUnsigned(frontier.peek().page.firstKeyUnsafe(), ceiling) <= 0) {
            if (clusterPages >= MAX_CLUSTER_PAGES) {
                metrics.recordStealReason("SORT", "merge_decoded_residency_exhausted");
                throw new MergeMemoryExhaustedException(
                        "pipeline overlap cluster exceeds " + MAX_CLUSTER_PAGES + " decoded pages");
            }
            Head overlapping = frontier.poll();
            advance(overlapping.segment);
            cluster.add(overlapping.segment, overlapping.page, 0);
            clusterPages++;
            clusterRows = Math.addExact(clusterRows, overlapping.page.count());
            if (Arrays.compareUnsigned(overlapping.page.lastKeyUnsafe(), ceiling) > 0) {
                ceiling = overlapping.page.lastKeyUnsafe();
            }
        }

        ArrayList<ListEntry> batch = new ArrayList<>(BATCH_ROWS);
        long logicalBytes = 0;
        while (cluster.hasNext()) {
            failure.check();
            ListEntry entry = cluster.next();
            batch.add(entry);
            logicalBytes = Math.addExact(logicalBytes, PageBlock.estimatedBytes(entry));
            if (batch.size() == BATCH_ROWS) {
                batcher.offer(new PipelineBatch.Rows(batch, logicalBytes));
                batch = new ArrayList<>(BATCH_ROWS);
                logicalBytes = 0;
            }
        }
        if (!batch.isEmpty()) {
            batcher.offer(new PipelineBatch.Rows(batch, logicalBytes));
        }
        rows = Math.addExact(rows, clusterRows);
        metrics.recordPipelineCluster(clusterPages, clusterRows);
    }

    private void advance(int segment) {
        PageBlock next = readers.next(segment);
        if (next != null) {
            frontier.add(new Head(segment, next));
        }
    }

    private record Head(int segment, PageBlock page) {
    }

    /** Holds one payload so a roll decision can compare its last raw key with the next first key. */
    private final class PartBatcher {
        private PipelineBatch.Payload pending;
        private long partLogicalBytes;
        private long partRows;
        private long sequence;
        private int ordinal;
        private int batchesInPart;

        void offer(PipelineBatch.Payload next) {
            MergeCancellation.check();
            if (pending != null) {
                boolean close = sizer.shouldClose(partLogicalBytes, partRows)
                        && KeyBytes.compareUnsigned(pending.lastKey(), next.firstKey()) != 0;
                dispatch(pending, close, false);
                if (close) {
                    ordinal++;
                    batchesInPart = 0;
                    partLogicalBytes = 0;
                    partRows = 0;
                }
            }
            pending = next;
            partLogicalBytes = Math.addExact(partLogicalBytes, next.logicalBytes());
            partRows = Math.addExact(partRows, next.rowCount());
        }

        int finish() {
            MergeCancellation.check();
            if (pending == null) {
                pending = PipelineBatch.Empty.INSTANCE;
            }
            dispatch(pending, true, true);
            pending = null;
            return ordinal + 1;
        }

        private void dispatch(PipelineBatch.Payload payload, boolean last, boolean mergeEnd) {
            PipelineBatch batch = new PipelineBatch(sequence++, ordinal, batchesInPart == 0,
                    last, mergeEnd, payload);
            encoders.submit(batch);
            batchesInPart++;
        }
    }
}
