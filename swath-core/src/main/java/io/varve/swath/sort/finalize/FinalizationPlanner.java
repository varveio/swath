/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.spill.PageRef;
import io.varve.swath.sort.spill.PageRunCatalog;
import io.varve.swath.sort.spill.PageRunDescriptor;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource planning for cascade fan-in and reference-routed finalization. */
final class FinalizationPlanner {

    private static final Logger log = LoggerFactory.getLogger(FinalizationPlanner.class);

    static final int MAX_PIPELINE_PLAN_REFS = 16_384;
    static final int MIN_PIPELINE_PLAN_REFS = 256;

    private final SortConfig config;
    private final SortMetrics metrics;
    private final IntSupplier softFdLimitSupplier;

    FinalizationPlanner(SortRun run) {
        this(run.config(), run.metrics(), run.softFdLimitSupplier());
    }

    FinalizationPlanner(SortConfig config, SortMetrics metrics, IntSupplier softFdLimitSupplier) {
        this.config = config;
        this.metrics = metrics;
        this.softFdLimitSupplier = softFdLimitSupplier;
    }

    /**
     * Clamp cascade inputs while reserving descriptors for the requested output writers, lowering
     * that reservation as far as a single writer before refusing a merge the narrower output would
     * complete.
     */
    int pipelineFanIn(PageRunCatalog catalog, int encoderCount)
            throws CascadeCapacityExhaustedException {
        if (encoderCount < 1) {
            throw new IllegalArgumentException("pipeline encoder count must be positive");
        }
        return runtimeFanIn(catalog, encoderCount);
    }

    /** Admit encoder lanes and their bounded reference and decoded-page residency. */
    PipelinePlan pipelineParallelism(int requested, PageRunCatalog catalog)
            throws MergeMemoryExhaustedException {
        if (requested < 1) {
            throw new IllegalArgumentException("pipeline encoder count must be positive");
        }
        int segments = catalog.descriptors().size();
        long readPageBytes = readPageBytes(catalog);
        long retainedPageBytes = retainedPageBytes(catalog);
        int refBytes = PageRef.retainedBytes(catalog.maxKeyLength());
        long cursorRefs = saturatedMultiply(segments, PageRunHeaderStreams.QUEUE_DEPTH + 2L);
        long usableFds = usableFdBudget();
        long byFd = usableFds == Long.MAX_VALUE
                ? Long.MAX_VALUE : Math.max(0L, usableFds - segments);
        if (byFd == 0) {
            metrics.recordStealReason("SORT", "pipeline_encoders_fd_floor_exhausted");
            throw new MergeMemoryExhaustedException(
                    "minimum pipeline lane does not fit descriptor budget: usable_fds="
                            + usableFds + ", segments=" + segments + ", reason=fd_exhausted");
        }
        int fdAdmitted = (int) Math.max(1L, Math.min(requested, byFd));
        int admitted = fdAdmitted;
        long requestedPlanRefs = pipelinePlanRefs(catalog);
        long admittedPlanRefs = -1;
        while (admittedPlanRefs < 0 && admitted > 0) {
            admittedPlanRefs = largestFittingPlanRefs(admitted, cursorRefs, refBytes,
                    readPageBytes, retainedPageBytes, requestedPlanRefs);
            if (admittedPlanRefs < 0) {
                admitted--;
            }
        }
        if (admitted == 0) {
            metrics.recordStealReason("SORT", "pipeline_encoder_heap_floor_exhausted");
            throw new MergeMemoryExhaustedException(
                    "minimum pipeline lane does not fit retained-page residency: read_page_bytes="
                            + readPageBytes + ", retained_page_bytes=" + retainedPageBytes
                            + ", merge_budget_bytes=" + config.mergeBudgetBytes());
        }
        boolean planRefCapped = admittedPlanRefs < requestedPlanRefs;
        if (planRefCapped) {
            metrics.recordStealReason("SORT", "pipeline_plan_ref_capped");
            log.warn("sort_pipeline_plan_refs_capped requested={} effective={} encoders={} "
                            + "ref_bytes={} merge_budget_bytes={}",
                    requestedPlanRefs, admittedPlanRefs, admitted, refBytes,
                    config.mergeBudgetBytes());
        }
        PipelineClampReason reason = PipelineClampReason.NONE;
        if (admitted < requested) {
            reason = fdAdmitted < requested && admitted == fdAdmitted
                    ? PipelineClampReason.FD_CLAMPED : PipelineClampReason.HEAP_CLAMPED;
        }
        long fixedBytes = fixedBytes(admitted, cursorRefs, refBytes,
                readPageBytes, admittedPlanRefs);
        long clusterPool = fixedBytes >= config.mergeBudgetBytes()
                ? 0L : config.mergeBudgetBytes() - fixedBytes;
        long clusterBudget = clusterPool / admitted;
        int planRefLimit = planRefCapped
                ? Math.toIntExact(admittedPlanRefs) : MAX_PIPELINE_PLAN_REFS;
        return new PipelinePlan(admitted, reason, PageRunHeaderStreams.QUEUE_DEPTH,
                refBytes, readPageBytes, retainedPageBytes, clusterBudget,
                planRefLimit);
    }

    private long largestFittingPlanRefs(int encoders, long cursorRefs, int refBytes,
            long readPageBytes, long retainedPageBytes, long requestedPlanRefs) {
        long floor = Math.min(requestedPlanRefs, MIN_PIPELINE_PLAN_REFS);
        if (!heapFits(encoders, cursorRefs, refBytes,
                readPageBytes, retainedPageBytes, floor)) {
            return -1;
        }
        long low = floor;
        long high = requestedPlanRefs;
        while (low < high) {
            long candidate = low + (high - low + 1) / 2;
            if (heapFits(encoders, cursorRefs, refBytes,
                    readPageBytes, retainedPageBytes, candidate)) {
                low = candidate;
            } else {
                high = candidate - 1;
            }
        }
        return low;
    }

    private boolean heapFits(int encoders, long cursorRefs, int refBytes,
            long readPageBytes, long retainedPageBytes, long planRefs) {
        long fixedBytes = fixedBytes(encoders, cursorRefs, refBytes, readPageBytes, planRefs);
        long clusterBytes = saturatedMultiply(encoders, retainedPageBytes);
        return saturatedAdd(fixedBytes, clusterBytes) <= config.mergeBudgetBytes();
    }

    /**
     * The residency that does not scale with cluster width: header-cursor references, every plan
     * reference wave the pipeline can hold at once, one transient positional read body per encoder,
     * and one open writer per encoder.
     *
     * <p>Two of those waves belong to the router, not one. {@code MergeRouter#route} closes a whole
     * overlap component before offering it, so the component's references and the references already
     * accumulated in the current part are live simultaneously; each is capped at {@code planRefs}.
     * The remaining {@code (QUEUE_DEPTH + 1)} waves per encoder are the queued and executing plans.
     */
    private long fixedBytes(int encoders, long cursorRefs, int refBytes,
            long readPageBytes, long planRefs) {
        long retainedPlanRefs = saturatedMultiply(planRefs,
                saturatedAdd(2L, saturatedMultiply(PartEncoders.QUEUE_DEPTH + 1L, encoders)));
        long routerBytes = saturatedMultiply(
                saturatedAdd(cursorRefs, retainedPlanRefs), refBytes);
        long writers = saturatedMultiply(encoders,
                PartEncoders.writerHeapEstimateBytes(config.finalRowGroupBytes()));
        long reads = saturatedMultiply(encoders, readPageBytes);
        return saturatedAdd(saturatedAdd(routerBytes, writers), reads);
    }

    long pipelinePlanRefs(PageRunCatalog catalog) {
        long records = catalog.totalRecords();
        if (records <= 0) {
            return 0;
        }
        long averageRecordBytes = Math.max(1L, stagedBytes(catalog) / records);
        long estimated = ceilDiv(
                PartSizer.initialLogicalTarget(config.finalFileBytes()), averageRecordBytes);
        return Math.min(records, Math.min(MAX_PIPELINE_PLAN_REFS, estimated));
    }

    private static long readPageBytes(PageRunCatalog catalog) {
        long maximum = 1;
        for (PageRunDescriptor descriptor : catalog.descriptors()) {
            if (descriptor.trailer().totalRecords() > 0) {
                maximum = Math.max(maximum, descriptor.trailer().maxRecordLen());
            }
        }
        return maximum;
    }

    private static long retainedPageBytes(PageRunCatalog catalog) {
        long maximum = 1;
        for (PageRunDescriptor descriptor : catalog.descriptors()) {
            if (descriptor.trailer().totalRecords() > 0) {
                maximum = Math.max(maximum, DecodedPageBudget.retainedPageUpperBound(
                        descriptor.maxRawPayloadLength(), descriptor.trailer().maxRecordLen()));
            }
        }
        return maximum;
    }

    private int runtimeFanIn(PageRunCatalog catalog, int outputWriters)
            throws CascadeCapacityExhaustedException {
        int staticFanIn = config.effectiveFanIn();
        int softFdLimit = softFdLimitSupplier.getAsInt();
        int recordSizedFanIn = recordSizedFanIn(catalog);
        int writers = feasibleWriterReservation(outputWriters, staticFanIn, softFdLimit,
                recordSizedFanIn);
        int headroom = saturatedHeadroom(writers);
        int clamped = FileDescriptorBudget.clampedFanIn(staticFanIn, softFdLimit,
                headroom, recordSizedFanIn);
        int segments = catalog.descriptors().size();
        if (clamped < 2) {
            if (segments >= 2) {
                metrics.recordStealReason("SORT", "merge_fanin_floor_exhausted");
                throw new CascadeCapacityExhaustedException(
                        "cascade cannot open the minimum two streams under the current budget: "
                                + "capacity=" + clamped + ", segments=" + segments
                                + ", soft_fd_limit=" + softFdLimit
                                + ", record_sized_fan_in=" + recordSizedFanIn
                                + ", reserved_writers=" + writers);
            }
            // A single source segment never opens a cascade group (reduceToFanIn's size > fanIn
            // guard skips it), so an unusable width here can never actually be exercised — but
            // CascadeReducer's own fanIn >= 2 constructor invariant must still be satisfied.
            clamped = 2;
        } else if (writers < outputWriters) {
            metrics.recordStealReason("SORT", "merge_fanin_writer_reservation_degraded");
            log.debug("sort_merge_fanin_writer_reservation_degraded requested_writers={} "
                            + "reserved_writers={} clamped_fan_in={} soft_fd_limit={} segments={}",
                    outputWriters, writers, clamped, softFdLimit, segments);
        }
        if (clamped < staticFanIn) {
            int fdBound = FileDescriptorBudget.fdBoundedFanIn(softFdLimit, headroom);
            metrics.recordStealReason("SORT", "merge_fanin_clamped");
            if (fdBound < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_fd_clamped");
            }
            if (recordSizedFanIn < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_mem_clamped");
            }
            log.debug("sort_merge_fanin_clamped static_fan_in={} fd_bound={} "
                            + "record_sized_fan_in={} clamped_fan_in={} soft_fd_limit={} segments={}",
                    staticFanIn, fdBound, recordSizedFanIn, clamped, softFdLimit, segments);
        }
        warnIfCascadePredicted(segments, clamped);
        return clamped;
    }

    /**
     * The largest output-writer reservation that still leaves a usable two-stream cascade width, or
     * {@code 1} when no reservation does. The requested encoder count is a ceiling rather than a
     * reservation the cascade must honor — {@link #pipelineParallelism} admits the lanes that
     * actually run, from the survivors this pass leaves — so under a tight descriptor budget the
     * inputs get the descriptors back instead of the merge being refused for output width nothing
     * has committed to yet. Fan-in is non-increasing in the reservation, so walking down returns the
     * largest feasible count; returning {@code 1} when none is feasible makes the refusal above
     * report capacity at the minimum any output needs.
     */
    private int feasibleWriterReservation(int requestedWriters, int staticFanIn, int softFdLimit,
            int recordSizedFanIn) {
        for (int writers = requestedWriters; writers > 1; writers--) {
            if (FileDescriptorBudget.clampedFanIn(staticFanIn, softFdLimit,
                    saturatedHeadroom(writers), recordSizedFanIn) >= 2) {
                return writers;
            }
        }
        return 1;
    }

    private static int saturatedHeadroom(int outputWriters) {
        long adjusted = (long) FileDescriptorBudget.FD_HEADROOM + outputWriters - 1L;
        return (int) Math.min(Integer.MAX_VALUE, adjusted);
    }

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
     * The record-size-bounded fan-in ceiling, honestly reported: {@code 0} or {@code 1} means even
     * the minimum two-stream cascade width does not fit, not an artificially floored {@code 2}.
     */
    private int recordSizedFanIn(PageRunCatalog catalog) {
        long perStreamPrice = Math.max(config.mergePerStreamBytes(), catalog.maxRecordLen());
        long bound = config.mergeBudgetBytes() / Math.max(1L, perStreamPrice);
        return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, bound));
    }

    private static int predictedPasses(int segments, int effectiveFanIn) {
        int passes = 1;
        int remaining = segments;
        while (remaining > effectiveFanIn) {
            remaining = (remaining + effectiveFanIn - 1) / effectiveFanIn;
            passes++;
        }
        return passes;
    }

    private long usableFdBudget() {
        int softLimit = softFdLimitSupplier.getAsInt();
        return softLimit < 0
                ? Long.MAX_VALUE
                : Math.max(0L, (long) softLimit - FileDescriptorBudget.FD_HEADROOM);
    }

    private static long stagedBytes(PageRunCatalog catalog) {
        long total = 0;
        for (PageRunDescriptor descriptor : catalog.descriptors()) {
            long bytes = Math.max(0L, descriptor.fileSize());
            total = total > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : total + bytes;
        }
        return total;
    }

    private static long saturatedMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static long ceilDiv(long dividend, long divisor) {
        if (dividend == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return 1L + (dividend - 1L) / divisor;
    }

    enum PipelineClampReason {
        NONE("none"),
        FD_CLAMPED("fd_clamped"),
        HEAP_CLAMPED("heap_clamped");

        private final String logValue;

        PipelineClampReason(String logValue) {
            this.logValue = logValue;
        }

        String logValue() {
            return logValue;
        }
    }

    record PipelinePlan(int encoders, PipelineClampReason reason, int cursorDepth,
                        int refBytes, long readPageBytes, long retainedPageBytes,
                        long clusterBudgetBytes, int planRefLimit) {
        PipelinePlan {
            if (encoders < 1 || cursorDepth < 1 || refBytes < 1 || readPageBytes < 1
                    || retainedPageBytes < 1 || clusterBudgetBytes < 1
                    || planRefLimit < 1 || planRefLimit > MAX_PIPELINE_PLAN_REFS) {
                throw new IllegalArgumentException("pipeline resource plan must be positive");
            }
        }
    }
}
