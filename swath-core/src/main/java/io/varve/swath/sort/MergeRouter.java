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

    record Result(long rows, long pagesForwarded, int parts) {
    }

    private final SegmentReaderSlots readers;
    private final PartEncoders encoders;
    private final PipelinePartSizer sizer;
    private final Comparator<ListEntry> comparator;
    private final SortMetrics metrics;
    private final PipelineFailure failure;
    private final DecodedPageBudget decodedBudget;
    private final PriorityQueue<Head> frontier = new PriorityQueue<>((left, right) -> {
        int order = Arrays.compareUnsigned(left.page.firstKeyUnsafe(), right.page.firstKeyUnsafe());
        return order != 0 ? order : Integer.compare(left.segment, right.segment);
    });
    private final PartBatcher batcher;
    private long rows;
    private long pagesForwarded;

    MergeRouter(SegmentReaderSlots readers, PartEncoders encoders, PipelinePartSizer sizer,
            Comparator<ListEntry> comparator, long mergeBudgetBytes, SortMetrics metrics,
            PipelineFailure failure) {
        this.readers = readers;
        this.encoders = encoders;
        this.sizer = sizer;
        this.comparator = comparator;
        this.metrics = metrics;
        this.failure = failure;
        this.decodedBudget = new DecodedPageBudget(mergeBudgetBytes, metrics);
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
            // Advance before testing disjointness so the runner-up includes this segment's successor;
            // otherwise a same-segment overlap could be forwarded out of order.
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
        long reserved = decodedBudget.reserve(first.page);
        long clusterPages = 0;
        long clusterRows = 0;
        try {
            cluster.add(first.segment, first.page, reserved);
            reserved = 0;
            clusterPages++;
            clusterRows = first.page.count();

            ArrayList<ListEntry> batch = new ArrayList<>(BATCH_ROWS);
            long logicalBytes = 0;
            while (cluster.hasNext()) {
                while (!frontier.isEmpty()
                        && Arrays.compareUnsigned(frontier.peek().page.firstKeyUnsafe(),
                                cluster.nextKey()) <= 0) {
                    Head overlapping = frontier.poll();
                    long pageBytes = decodedBudget.reserve(overlapping.page);
                    try {
                        advance(overlapping.segment);
                        cluster.add(overlapping.segment, overlapping.page, pageBytes);
                        pageBytes = 0;
                        clusterPages++;
                        clusterRows = Math.addExact(clusterRows, overlapping.page.count());
                    } finally {
                        decodedBudget.release(pageBytes);
                    }
                }
                failure.check();
                ListEntry entry = cluster.next();
                decodedBudget.release(cluster.releasedBytes());
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
        } finally {
            decodedBudget.release(reserved);
            decodedBudget.release(cluster.releaseAllBytes());
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

    /**
     * Retains one payload because the soft target alone cannot close a part: the next payload's first
     * raw key is needed to prove the pending payload's last equal-key group will remain indivisible.
     */
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
                // A managed dataset always has one durable final file, including an empty listing.
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
