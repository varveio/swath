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
 * RUNTIME clamps — the process fd limit ({@link MergeFdBudget}) and the EXACT per-segment page-run
 * memory bound — and owns the cascade-predicted / fan-in-clamped observability (log warnings +
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
     * entry point the serial merge path needs. The off-by-default parallel range-merge path does not
     * call this: it computes its own PER-RANGE width ({@link ParallelRangeMerge#perRangeFanIn}), which
     * divides both the memory budget and the process fd budget by the range count and prices a stream
     * by staging format — reusing {@link #maxPageRunRecordLen} for the page-run case. It calls
     * {@link #warnIfCascadePredicted} directly for the warning alone.
     */
    int plan(List<Path> segments) {
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
     * {@link MergeFdBudget#FD_HEADROOM}), and (c) the EXACT per-segment memory bound from the page-run
     * trailers ({@code mergeBudgetBytes / max(maxRecordLen)}) when the input is page-run — never below 2.
     * Fires {@code SORT.merge_fanin_clamped} (with the fd/mem sub-reason) whenever the runtime clamp
     * reduces the fan-in below the static estimate; {@link #plan} then lets {@link
     * #warnIfCascadePredicted} note the cascade if the clamp forces the fan-in below the segment
     * count.
     */
    private int clampedMergeFanIn(List<Path> segments) {
        int staticFanIn = config.effectiveFanIn();
        int softFdLimit = softFdLimitSupplier.getAsInt();
        int exactMemoryFanIn = exactMemoryFanIn(segments);
        int clamped = MergeFdBudget.clampedFanIn(staticFanIn, softFdLimit, MergeFdBudget.FD_HEADROOM,
                exactMemoryFanIn);
        if (clamped < staticFanIn) {
            int fdBound = MergeFdBudget.fdBoundedFanIn(softFdLimit, MergeFdBudget.FD_HEADROOM);
            metrics.recordStealReason("SORT", "merge_fanin_clamped");
            if (fdBound < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_fd_clamped");
            }
            if (exactMemoryFanIn < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_mem_clamped");
            }
            log.debug("sort_merge_fanin_clamped static_fan_in={} fd_bound={} exact_mem_fan_in={} "
                    + "clamped_fan_in={} soft_fd_limit={} segments={}",
                    staticFanIn, fdBound, exactMemoryFanIn, clamped, softFdLimit, segments.size());
        }
        return clamped;
    }

    /**
     * Exact memory bound: {@code mergeBudgetBytes / max(maxRecordLen over the page-run segments)}
     * — the actual per-open-stream heap a merge holds, read O(1) per segment from the page-run trailer
     * ({@link PageRunSegmentReader#readTrailer}). Returns {@link Integer#MAX_VALUE} (so the static
     * estimate governs) when the input is not page-run (columnar Parquet fixtures), is empty, or a
     * trailer read fails — the static bound is an acceptable, correct fallback.
     */
    private int exactMemoryFanIn(List<Path> segments) {
        long maxRecordLen = maxPageRunRecordLen(segments);
        if (maxRecordLen <= 0) {
            return Integer.MAX_VALUE;
        }
        long bound = config.mergeBudgetBytes() / maxRecordLen;
        return (int) Math.min(Integer.MAX_VALUE, Math.max(2L, bound));
    }

    /**
     * The largest framed record across {@code segments}, read O(1) per segment from the page-run
     * trailer — the EXACT heap one open page-run merge stream holds, and so the right divisor for any
     * merge-memory bound over page-run input. Returns {@code -1} when the input is empty, is not
     * all page-run (columnar Parquet, or a mixed set), or a trailer cannot be read; every caller
     * then falls back to its own static estimate, which is correct and merely conservative.
     *
     * <p>Package-private and shared: {@link ParallelRangeMerge} needs the same quantity to size its
     * PER-RANGE fan-in, and computing it there independently would be the same scan written twice.
     */
    static long maxPageRunRecordLen(List<Path> segments) {
        if (segments.isEmpty()) {
            return -1;
        }
        long maxRecordLen = 0;
        try {
            for (Path seg : segments) {
                if (!SortTransform.isPageRunSegment(seg)) {
                    return -1;
                }
                maxRecordLen = Math.max(maxRecordLen, PageRunSegmentReader.readTrailer(seg).maxRecordLen());
            }
        } catch (IOException e) {
            log.debug("could not read a page-run trailer for the exact merge-memory bound; "
                    + "falling back to the static estimate", e);
            return -1;
        }
        return maxRecordLen > 0 ? maxRecordLen : -1;
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
