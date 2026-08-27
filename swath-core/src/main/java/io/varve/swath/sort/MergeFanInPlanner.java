/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The merge fan-in planner — "how wide can this merge pass be" — extracted out of
 * {@link SortTransform} so its tmp/rename/publish state machine doesn't also carry this arithmetic
 * cluster. Composes the STATIC budget-bounded fan-in ({@link SortConfig#effectiveFanIn()}) with two
 * RUNTIME clamps — the process fd limit ({@link MergeFdBudget}) and the largest encoded page-run
 * record observed in segment trailers — and owns cascade-predicted / fan-in-clamped observability
 * (log warnings +
 * {@code SORT.merge_fanin_clamped}/{@code _fd_clamped}/{@code _mem_clamped}/{@code
 * merge_cascade_predicted} counters), so a caller only ever needs {@link #plan(List)}.
 *
 * <p>The arithmetic itself (the pure, injectable fd/memory composition) stays in
 * {@link MergeFdBudget}; this class composes it against a real {@link SortConfig} + segment list,
 * and owns the metrics/logging around that decision, out of {@link SortTransform}. {@code
 * SortTransformFanInClampTest} and {@code MergeFdBudgetTest} pin this behavior byte-for-byte.
 */
final class MergeFanInPlanner {

    private static final Logger log = LoggerFactory.getLogger(MergeFanInPlanner.class);

    private final SortConfig config;
    private final SortMetrics metrics;
    private final IntSupplier softFdLimitSupplier;

    MergeFanInPlanner(SortConfig config, SortMetrics metrics, IntSupplier softFdLimitSupplier) {
        this.config = config;
        this.metrics = metrics;
        this.softFdLimitSupplier = softFdLimitSupplier;
    }

    /**
     * The runtime-clamped merge fan-in for {@code segments} — {@link #clampedMergeFanIn} plus the
     * cascade-predicted warning ({@link #warnIfCascadePredicted}) that its result can trigger. The one
     * entry point the serial merge path needs. The parallel range-merge path does not
     * call this: it computes its own PER-RANGE width ({@link ParallelRangeMerge#perRangeFanIn}), which
     * divides both the memory budget and the process fd budget by the range count and prices a stream
     * by staging format — reusing {@link #maxPageRunRecordLen} for the page-run case. It calls
     * {@link #warnIfCascadePredicted} directly for the warning alone.
     */
    int plan(List<PageRunSegmentDescriptor> segments) {
        int clamped = clampedMergeFanIn(segments);
        warnIfCascadePredicted(segments.size(), clamped);
        return clamped;
    }

    /**
     * Note once, before the merge starts, that {@code segments >
     * effectiveFanIn} will force a cascade (multi-pass) merge — the same condition {@link
     * KWayMerge#merge} discovers pass-by-pass, surfaced up front instead of only inferable
     * afterward from the summary's {@code segments}/{@code passes} fields. {@code
     * predictedPasses} mirrors {@link KWayMerge#merge}'s own reduction exactly (repeatedly divide
     * by {@code effectiveFanIn}, plus the final streaming pass), so it is not a rough
     * log-approximation but the actual pass count the cascade below will run. Logged at DEBUG —
     * expected on a large sort and already recorded as a steal reason, not a fault.
     */
    void warnIfCascadePredicted(int segments, int effectiveFanIn) {
        if (segments <= effectiveFanIn) {
            return;
        }
        int predictedPasses = predictedPasses(segments, effectiveFanIn);
        metrics.recordStealReason("SORT", "merge_cascade_predicted");
        log.debug("sort_merge_cascade_predicted segments={} effective_fan_in={} predicted_passes={} "
                + "advice=a larger heap (-Xmx) or a higher swath.sort.merge-budget-bytes raises "
                + "effective_fan_in and can avoid the extra pass(es)",
                segments, effectiveFanIn, predictedPasses);
    }

    /**
     * The runtime-clamped merge fan-in. Takes the MIN of (a) the static budget-bounded
     * {@link SortConfig#effectiveFanIn()}, (b) the fd clamp (this process's soft open-file limit minus
     * {@link MergeFdBudget#FD_HEADROOM}), and (c) a per-stream price no smaller than either the
     * configured estimate or the largest encoded record from the page-run trailers — never below 2.
     * Fires {@code SORT.merge_fanin_clamped} (with the fd/mem sub-reason) whenever the runtime clamp
     * reduces the fan-in below the static estimate; {@link #plan} then lets {@link
     * #warnIfCascadePredicted} note the cascade if the clamp forces the fan-in below the segment
     * count.
     */
    private int clampedMergeFanIn(List<PageRunSegmentDescriptor> segments) {
        int staticFanIn = config.effectiveFanIn();
        int softFdLimit = softFdLimitSupplier.getAsInt();
        int recordSizedFanIn = recordSizedFanIn(segments);
        int clamped = MergeFdBudget.clampedFanIn(staticFanIn, softFdLimit, MergeFdBudget.FD_HEADROOM,
                recordSizedFanIn);
        if (clamped < staticFanIn) {
            int fdBound = MergeFdBudget.fdBoundedFanIn(softFdLimit, MergeFdBudget.FD_HEADROOM);
            metrics.recordStealReason("SORT", "merge_fanin_clamped");
            if (fdBound < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_fd_clamped");
            }
            if (recordSizedFanIn < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_mem_clamped");
            }
            log.debug("sort_merge_fanin_clamped static_fan_in={} fd_bound={} record_sized_fan_in={} "
                    + "clamped_fan_in={} soft_fd_limit={} segments={}",
                    staticFanIn, fdBound, recordSizedFanIn, clamped, softFdLimit, segments.size());
        }
        return clamped;
    }

    /**
     * Runtime refinement of the configured per-stream estimate. A page-run trailer exposes the
     * largest encoded record, so a record larger than {@link SortConfig#mergePerStreamBytes()} must
     * tighten the fan-in before any reader allocates it. Encoded size is not a complete resident-heap
     * measurement (decoding and legal overlap add working state), so the trailer can tighten but never
     * relax the configured estimate. Returns {@link Integer#MAX_VALUE} when no trailer size is
     * available, leaving the static estimate in force.
     */
    private int recordSizedFanIn(List<PageRunSegmentDescriptor> segments) {
        long maxRecordLen = PageRunSegmentDescriptor.maxRecordLen(segments);
        if (maxRecordLen <= 0) {
            return Integer.MAX_VALUE;
        }
        long perStreamPrice = Math.max(config.mergePerStreamBytes(), maxRecordLen);
        long bound = config.mergeBudgetBytes() / perStreamPrice;
        return (int) Math.min(Integer.MAX_VALUE, Math.max(2L, bound));
    }

    /**
     * The largest framed record across {@code segments}, read O(1) per segment from the page-run
     * trailer — an O(1) encoded-record allocation guard used to tighten the configured stream price,
     * not the complete decoded heap of one stream. Empty input returns {@code -1}; an unreadable
     * trailer is an input failure and is never silently reinterpreted as an unknown estimate.
     *
     * <p>Package-private and shared: {@link ParallelRangeMerge} needs the same quantity to size its
     * PER-RANGE fan-in, and computing it there independently would be the same scan written twice.
     */
    static long maxPageRunRecordLen(List<Path> segments) throws IOException {
        if (segments.isEmpty()) {
            return -1;
        }
        for (Path seg : segments) {
            if (!SortTransform.isPageRunSegment(seg)) {
                throw new IOException("unsupported sort staging segment: " + seg);
            }
        }
        return PageRunSegmentDescriptor.maxRecordLen(PageRunSegmentDescriptor.readAll(segments));
    }

    /** Number of {@link KWayMerge#merge} passes (cascade passes + the final streaming pass). */
    private static int predictedPasses(int segments, int effectiveFanIn) {
        int passes = 1;   // the final streaming pass
        int remaining = segments;
        while (remaining > effectiveFanIn) {
            remaining = (remaining + effectiveFanIn - 1) / effectiveFanIn;
            passes++;
        }
        return passes;
    }
}
