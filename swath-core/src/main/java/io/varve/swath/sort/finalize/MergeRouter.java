/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.spill.PageRef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;

/**
 * Single ordering and part-boundary owner over page references. It performs only page-range work:
 * disjoint references remain individual items, while a transitive overlap component becomes one
 * cluster item for an encoder to decode and merge. Every consumed ref moves into exactly one plan.
 * Plans contain coordinates rather than bodies so the router can expose independent parts to a
 * shared encoder pool without retaining decoded part-sized buffers. For calibrated byte sizing it
 * exposes exactly one initial plan, waits for its durable size, and only then routes the remaining
 * plans; this bounds calibration lag to the intentionally conservative warm-up part. Heap
 * admission may lower the usual 16,384-reference cap, which this router enforces for both ordinary
 * plans and indivisible overlap clusters.
 */
final class MergeRouter {
    /** Independent counts used to reconcile header refs, source rows, and completed parts. */
    record Result(long rows, long pagesForwarded, long refs, int parts) {
    }

    private final PageRunHeaderStreams cursors;
    private final Consumer<PartPlan> plans;
    private final PartSizer sizer;
    private final SortMetrics metrics;
    private final FinalizationFailure failure;
    private final Runnable awaitFirstCompletion;
    private final int maxPlanRefs;
    private final PriorityQueue<PageRef> frontier = new PriorityQueue<>((left, right) -> {
        int order = Arrays.compareUnsigned(left.minKey(), right.minKey());
        return order != 0 ? order : Integer.compare(left.segmentId(), right.segmentId());
    });
    private final PartPlanner partPlanner = new PartPlanner();
    private long rows;
    private long refs;
    private long pagesForwarded;

    /**
     * Bind the sole frontier and boundary owner to a bounded plan consumer. Encoder completion is
     * consulted only for first-part calibration; it never influences merge ordering.
     */
    MergeRouter(PageRunHeaderStreams cursors, Consumer<PartPlan> plans,
            PartSizer sizer, SortMetrics metrics, FinalizationFailure failure,
            Runnable awaitFirstCompletion, int maxPlanRefs) {
        if (maxPlanRefs < 1 || maxPlanRefs > FinalizationPlanner.MAX_PIPELINE_PLAN_REFS) {
            throw new IllegalArgumentException("pipeline plan reference cap is out of bounds");
        }
        this.cursors = cursors;
        this.plans = plans;
        this.sizer = sizer;
        this.metrics = metrics;
        this.failure = failure;
        this.awaitFirstCompletion = awaitFirstCompletion;
        this.maxPlanRefs = maxPlanRefs;
    }

    /**
     * Drain all header cursors and close the final complete plan, including the empty-input plan.
     * Every frontier removal advances that same segment exactly once, which makes ref conservation
     * independent of whether the ref becomes a whole page or joins an overlap cluster.
     */
    Result route(int segments) throws MergeMemoryExhaustedException {
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
                metrics.recordStealReason("SORT", "pipeline_whole_page_merge");
            } else {
                item = collectCluster(first);
            }
            partPlanner.offer(item);
        }
        int parts = partPlanner.finish();
        return new Result(rows, pagesForwarded, refs, parts);
    }

    /**
     * Close one transitive page-range component without loading or comparing any row. Transitivity
     * matters: stopping at the first pairwise overlap could separate a later page whose rows still
     * interleave with the growing component and create overlapping output parts.
     */
    private PartPlan.Cluster collectCluster(PageRef first)
            throws MergeMemoryExhaustedException {
        metrics.recordStealReason("SORT", "pipeline_cluster_merge");
        ArrayList<PageRef> cluster = new ArrayList<>();
        cluster.add(first);
        byte[] high = first.maxKey();
        long clusterRows = first.count();
        while (!frontier.isEmpty()
                && Arrays.compareUnsigned(frontier.peek().minKey(), high) <= 0) {
            if (cluster.size() == maxPlanRefs) {
                metrics.recordStealReason("SORT", "pipeline_plan_ref_capped");
                throw new MergeMemoryExhaustedException(
                        "transitive overlap cluster exceeds pipeline plan ref cap: cap="
                                + maxPlanRefs);
            }
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

    /** Replace one consumed frontier head, retaining at most one heap entry per live segment. */
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
        private int partRefs;
        private int ordinal;

        /**
         * Close before adding an item when either soft geometry or the hard reference cap engages.
         * The cap is independent of {@code final-file-bytes}, so a nominal single-file merge may
         * still produce several strictly adjacent files rather than retaining O(total pages) refs.
         * Admission may lower the usual cap, but does not weaken the raw-key boundary rule.
         */
        void offer(PartPlan.Item item) {
            MergeCancellation.check();
            int itemRefs = item.refs().size();
            if (itemRefs > maxPlanRefs) {
                throw new IllegalStateException("overlap cluster exceeds pipeline plan ref cap: refs="
                        + itemRefs + " cap=" + maxPlanRefs);
            }
            boolean refCapped = partRefs > maxPlanRefs - itemRefs;
            if (!items.isEmpty() && (sizer.shouldClose(logicalBytes, partRows) || refCapped)
                    // Items are already disjoint, but equal raw keys must remain atomic even if a
                    // future routing item shape weakens that construction-time guarantee.
                    && KeyBytes.compareUnsigned(lastKey(items.getLast()), firstKey(item)) != 0) {
                if (refCapped) {
                    metrics.recordStealReason("SORT", "pipeline_plan_ref_capped");
                }
                dispatch(false);
            }
            items.add(item);
            partRefs = Math.addExact(partRefs, itemRefs);
            for (PageRef ref : item.refs()) {
                logicalBytes = Math.addExact(logicalBytes, ref.rawPayloadLength());
                partRows = Math.addExact(partRows, ref.count());
                rows = Math.addExact(rows, ref.count());
                refs++;
            }
        }

        /** Dispatch the terminal plan even when the input is empty so the final footer stamp exists. */
        int finish() {
            MergeCancellation.check();
            dispatch(true);
            return ordinal;
        }

        /**
         * Transfer an immutable plan, reset all per-part counters, and perform the one warm-up wait.
         * Clearing after {@link PartPlan}'s defensive copy prevents later routing from mutating work
         * already visible to an encoder.
         */
        private void dispatch(boolean mergeEnd) {
            PartPlan plan = new PartPlan(ordinal, items, mergeEnd,
                    logicalBytes, partRows);
            plans.accept(plan);
            ordinal++;
            items.clear();
            logicalBytes = 0;
            partRows = 0;
            partRefs = 0;
            if (ordinal == 1 && !mergeEnd && sizer.needsCalibrationWarmup()) {
                awaitFirstCompletion.run();
                failure.check();
            }
        }
    }

    /** Return the routing minimum without cloning; plan refs are immutable internal coordinates. */
    private static byte[] firstKey(PartPlan.Item item) {
        return item.refs().getFirst().minKey();
    }

    /**
     * Compute an item's true high bound instead of trusting list order inside an overlap cluster.
     * Cluster refs are ordered by minima, which does not imply their maxima are monotone.
     */
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
