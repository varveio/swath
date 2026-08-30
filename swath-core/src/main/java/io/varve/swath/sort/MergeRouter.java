/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;

/**
 * Single ordering and part-boundary owner over page references. It performs only page-range work:
 * disjoint references remain individual items, while a transitive overlap component becomes one
 * cluster item for an encoder to decode and merge. Every consumed ref moves into exactly one plan.
 */
final class MergeRouter {
    record Result(long rows, long pagesForwarded, long refs, int parts) {
    }

    private final SegmentHeaderCursors cursors;
    private final Consumer<PartPlan> plans;
    private final PipelinePartSizer sizer;
    private final SortMetrics metrics;
    private final PipelineFailure failure;
    private final PriorityQueue<PageRef> frontier = new PriorityQueue<>((left, right) -> {
        int order = Arrays.compareUnsigned(left.minKey(), right.minKey());
        return order != 0 ? order : Integer.compare(left.segmentId(), right.segmentId());
    });
    private final PartPlanner partPlanner = new PartPlanner();
    private long rows;
    private long refs;
    private long pagesForwarded;

    MergeRouter(SegmentHeaderCursors cursors, Consumer<PartPlan> plans,
            PipelinePartSizer sizer, SortMetrics metrics, PipelineFailure failure) {
        this.cursors = cursors;
        this.plans = plans;
        this.sizer = sizer;
        this.metrics = metrics;
        this.failure = failure;
    }

    /** Drain all header cursors and close the final complete plan, including the empty-input plan. */
    Result route(int segments) {
        for (int segment = 0; segment < segments; segment++) {
            PageRef ref = cursors.next(segment);
            if (ref != null) {
                frontier.add(ref);
            }
        }
        while (!frontier.isEmpty()) {
            failure.check();
            PageRef first = frontier.poll();
            advance(first.segmentId());
            PartPlan.Item item;
            if (frontier.isEmpty()
                    || Arrays.compareUnsigned(first.maxKey(), frontier.peek().minKey()) < 0) {
                item = new PartPlan.Page(first);
                pagesForwarded++;
                metrics.recordPipelinePagesForwarded(1);
            } else {
                item = collectCluster(first);
            }
            partPlanner.offer(item);
        }
        int parts = partPlanner.finish();
        return new Result(rows, pagesForwarded, refs, parts);
    }

    /** Close one transitive page-range component without loading or comparing any row. */
    private PartPlan.Cluster collectCluster(PageRef first) {
        metrics.recordStealReason("SORT", "pipeline_cluster_merge");
        ArrayList<PageRef> cluster = new ArrayList<>();
        cluster.add(first);
        byte[] high = first.maxKey();
        long clusterRows = first.count();
        while (!frontier.isEmpty()
                && Arrays.compareUnsigned(frontier.peek().minKey(), high) <= 0) {
            PageRef overlapping = frontier.poll();
            cluster.add(overlapping);
            clusterRows = Math.addExact(clusterRows, overlapping.count());
            if (Arrays.compareUnsigned(overlapping.maxKey(), high) > 0) {
                high = overlapping.maxKey();
            }
            advance(overlapping.segmentId());
        }
        metrics.recordPipelineCluster(cluster.size(), clusterRows);
        return new PartPlan.Cluster(cluster);
    }

    private void advance(int segment) {
        PageRef next = cursors.next(segment);
        if (next != null) {
            frontier.add(next);
        }
    }

    /** Accumulate complete plans and preserve raw-key-atomic boundaries between adjacent items. */
    private final class PartPlanner {
        private final ArrayList<PartPlan.Item> items = new ArrayList<>();
        private long logicalBytes;
        private long partRows;
        private int ordinal;

        void offer(PartPlan.Item item) {
            MergeCancellation.check();
            if (!items.isEmpty() && sizer.shouldClose(logicalBytes, partRows)
                    && KeyBytes.compareUnsigned(lastKey(items.getLast()), firstKey(item)) != 0) {
                dispatch(false);
            }
            items.add(item);
            for (PageRef ref : item.refs()) {
                logicalBytes = Math.addExact(logicalBytes, ref.rawPayloadLength());
                partRows = Math.addExact(partRows, ref.count());
                rows = Math.addExact(rows, ref.count());
                refs++;
            }
        }

        int finish() {
            MergeCancellation.check();
            dispatch(true);
            return ordinal;
        }

        private void dispatch(boolean mergeEnd) {
            PartPlan plan = new PartPlan(ordinal, items, true, true, mergeEnd,
                    logicalBytes, partRows);
            plans.accept(plan);
            ordinal++;
            items.clear();
            logicalBytes = 0;
            partRows = 0;
        }
    }

    private static byte[] firstKey(PartPlan.Item item) {
        return item.refs().getFirst().minKey();
    }

    private static byte[] lastKey(PartPlan.Item item) {
        List<PageRef> itemRefs = item.refs();
        byte[] high = itemRefs.getFirst().maxKey();
        for (int i = 1; i < itemRefs.size(); i++) {
            if (Arrays.compareUnsigned(itemRefs.get(i).maxKey(), high) > 0) {
                high = itemRefs.get(i).maxKey();
            }
        }
        return high;
    }
}
