/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.spill.PageRef;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.function.Consumer;

/**
 * Single ordering and part-boundary owner over page references. It performs only page-range work:
 * disjoint references remain individual items, while a transitive overlap component becomes one
 * cluster item for an encoder to decode and merge. Every consumed ref moves into exactly one plan.
 * Plans contain coordinates rather than bodies so the router can expose independent parts to a
 * shared encoder pool without retaining decoded part-sized buffers. For calibrated byte sizing it
 * exposes exactly one initial plan, waits for its durable size, and only then routes the remaining
 * plans; this bounds calibration lag to the intentionally conservative warm-up part. Heap admission
 * may lower the usual 16,384-reference cap, which bounds both an ordinary plan and the references
 * this router keeps in heap while closing an overlap component.
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
    private final Path stagingDir;
    private final PriorityQueue<PageRef> frontier = new PriorityQueue<>((left, right) -> {
        int order = Arrays.compareUnsigned(left.minKey(), right.minKey());
        return order != 0 ? order : Integer.compare(left.segmentId(), right.segmentId());
    });
    private final PartPlanner partPlanner = new PartPlanner();
    private long rows;
    private long refs;
    private long pagesForwarded;
    private int clusters;

    /**
     * Bind the sole frontier and boundary owner to a bounded plan consumer. Encoder completion is
     * consulted only for first-part calibration; it never influences merge ordering.
     */
    MergeRouter(PageRunHeaderStreams cursors, Consumer<PartPlan> plans,
            PartSizer sizer, SortMetrics metrics, FinalizationFailure failure,
            Runnable awaitFirstCompletion, int maxPlanRefs, Path stagingDir) {
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
        this.stagingDir = stagingDir;
    }

    /**
     * Drain all header cursors and close the final complete plan, including the empty-input plan.
     * Every frontier removal advances that same segment exactly once, which makes ref conservation
     * independent of whether the ref becomes a whole page or joins an overlap cluster.
     */
    Result route(int segments) throws IOException {
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
     * interleave with the growing component and create overlapping output parts. A component may be
     * arbitrarily wide—a single broad page can overlap every page of another segment—so collection
     * spills its references once it outgrows the heap-resident cap instead of refusing the input.
     */
    private PartPlan.Cluster collectCluster(PageRef first) throws IOException {
        metrics.recordStealReason("SORT", "pipeline_cluster_merge");
        byte[] high = first.maxKey();
        long clusterRows = first.count();
        long clusterBytes = first.rawPayloadLength();
        boolean spilled = false;
        try (ClusterRefs.Builder cluster =
                     new ClusterRefs.Builder(maxPlanRefs, stagingDir, clusters++)) {
            cluster.add(first);
            while (!frontier.isEmpty()
                    && Arrays.compareUnsigned(frontier.peek().minKey(), high) <= 0) {
                MergeCancellation.check();
                PageRef overlapping = frontier.poll();
                cluster.add(overlapping);
                if (!spilled && cluster.spilled()) {
                    spilled = true;
                    metrics.recordStealReason("SORT", "pipeline_cluster_spilled");
                }
                clusterRows = Math.addExact(clusterRows, overlapping.count());
                clusterBytes = Math.addExact(clusterBytes, overlapping.rawPayloadLength());
                if (Arrays.compareUnsigned(overlapping.maxKey(), high) > 0) {
                    high = overlapping.maxKey();
                }
                advance(overlapping.segmentId());
            }
            metrics.recordPipelineCluster(cluster.count(), clusterRows);
            return new PartPlan.Cluster(cluster.build(), first.minKey(), high,
                    cluster.count(), clusterRows, clusterBytes);
        }
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
        private long partRefs;
        private int ordinal;

        /**
         * Close before adding an item when either soft geometry or the hard reference cap engages.
         * The cap is independent of {@code final-file-bytes}, so a nominal single-file merge may
         * still produce several strictly adjacent files rather than retaining O(total pages) refs.
         * An overlap component wider than the cap is indivisible, so it deterministically takes a
         * part of its own: it closes the part before it and caps the part after it.
         */
        void offer(PartPlan.Item item) {
            MergeCancellation.check();
            long itemRefs = item.refCount();
            boolean refCapped = partRefs > maxPlanRefs - itemRefs;
            if (!items.isEmpty() && (sizer.shouldClose(logicalBytes, partRows) || refCapped)
                    // Items are already disjoint, but equal raw keys must remain atomic even if a
                    // future routing item shape weakens that construction-time guarantee.
                    && KeyBytes.compareUnsigned(items.getLast().lastKey(), item.firstKey()) != 0) {
                if (refCapped) {
                    metrics.recordStealReason("SORT", "pipeline_plan_ref_capped");
                }
                dispatch(false);
            }
            items.add(item);
            partRefs = Math.addExact(partRefs, itemRefs);
            logicalBytes = Math.addExact(logicalBytes, item.logicalBytes());
            partRows = Math.addExact(partRows, item.rows());
            rows = Math.addExact(rows, item.rows());
            refs = Math.addExact(refs, itemRefs);
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
         * already visible to an encoder. A plan that never reaches the encoder queue still owns its
         * spilled references, so a failed handoff releases them here rather than in staging debris.
         */
        private void dispatch(boolean mergeEnd) {
            PartPlan plan = new PartPlan(ordinal, items, mergeEnd,
                    logicalBytes, partRows);
            try {
                plans.accept(plan);
            } catch (Throwable handoffFailure) {
                try {
                    plan.discard();
                } catch (IOException discardFailure) {
                    handoffFailure.addSuppressed(discardFailure);
                }
                throw handoffFailure;
            }
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
}
