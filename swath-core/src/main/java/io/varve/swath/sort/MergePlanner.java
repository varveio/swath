/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single planning owner for serial fan-in and parallel range/resource decisions.
 *
 * <p>It composes the static merge budget with descriptor-derived record sizes, the process FD
 * budget, the proof-spool reservation, output-writer reservations, staged-size admission, and the
 * selected distinct/row-weighted boundary policy. Runtime execution, seek-plan construction, worker
 * lifecycle, and proof remain outside this class.
 */
final class MergePlanner {

    private static final Logger log = LoggerFactory.getLogger(MergePlanner.class);

    /** At the supported 16-range maximum this retains 1,024 candidates per range. */
    static final int MAX_BOUNDARY_CANDIDATES = 16_384;
    /** One shared temporary proof spool descriptor, independent of the range count. */
    static final int PROOF_SPOOL_FDS = 1;

    private final SortConfig config;
    private final SortMetrics metrics;
    private final IntSupplier softFdLimitSupplier;
    private final MergeDiskPolicy diskPolicy;

    MergePlanner(SortRun run) {
        this(run.config(), run.metrics(), run.softFdLimitSupplier(), run.mergeDiskPolicy());
    }

    MergePlanner(SortConfig config, SortMetrics metrics, IntSupplier softFdLimitSupplier) {
        this(config, metrics, softFdLimitSupplier, MergeDiskPolicy.disabled());
    }

    MergePlanner(SortConfig config, SortMetrics metrics, IntSupplier softFdLimitSupplier,
            MergeDiskPolicy diskPolicy) {
        this.config = config;
        this.metrics = metrics;
        this.softFdLimitSupplier = softFdLimitSupplier;
        this.diskPolicy = diskPolicy;
    }

    /**
     * Apply filesystem admission after heap/FD planning. Disk is a distinct pass because reducing
     * {@code R} changes only the exact proof extent; it must not obscure the earlier clamp reason.
     */
    EffectiveRanges admitDisk(EffectiveRanges resourcePlan, PageRunCatalog catalog,
            Path stagingDir, Path outputDir) throws MergeDiskExhaustedException {
        if (diskPolicy.bypassedByCaller()) {
            metrics.recordStealReason("SORT", "merge_disk_policy_bypassed");
            return resourcePlan;
        }
        metrics.recordStealReason("SORT", "merge_disk_policy_enforced");
        MergeDiskPolicy.Snapshot space = diskPolicy.snapshot(stagingDir, outputDir);
        MergeDiskPlan.Decision decision = MergeDiskPlan.decide(
                resourcePlan.ranges(), catalog.descriptors().size(), stagedBytes(catalog), space);
        if (decision.refused()) {
            metrics.recordStealReason("SORT", "merge_disk_exhausted");
            String reason = MergeDiskPlan.refusalReason(decision.reservation(), space);
            log.error("sort_merge_disk_refused reason=\"{}\" error_class=sort_disk_exhausted "
                            + "stop_reason=sort_disk_exhausted resumable=true",
                    reason);
            throw new MergeDiskExhaustedException(reason);
        }
        if (decision.ranges() < resourcePlan.ranges()) {
            metrics.recordStealReason("SORT", "merge_range_disk_limited");
            log.warn("sort_merge_range_clamped requested={} effective={} segments={} reason=disk_limited "
                            + "requested_proof_spool_bytes={} effective_proof_spool_bytes={}",
                    resourcePlan.ranges(), decision.ranges(), catalog.descriptors().size(),
                    PageRunProofSpool.logicalBytes(resourcePlan.ranges(), catalog.descriptors().size()),
                    decision.ranges() > 1 ? decision.reservation().proofBytes() : 0L);
            return new EffectiveRanges(decision.ranges(), ClampReason.DISK_LIMITED);
        }
        return resourcePlan;
    }

    /** Fresh usable-space sample immediately before proof-file creation and zero-fill. */
    void recheckDiskBeforeProof(int ranges, PageRunCatalog catalog,
            Path stagingDir, Path outputDir) throws MergeDiskExhaustedException {
        if (diskPolicy.bypassedByCaller()) {
            return;
        }
        MergeDiskPolicy.Snapshot space = diskPolicy.snapshot(stagingDir, outputDir);
        MergeDiskPlan.Decision decision = MergeDiskPlan.decide(
                ranges, catalog.descriptors().size(), stagedBytes(catalog), space);
        if (decision.ranges() != ranges) {
            metrics.recordStealReason("SORT", "merge_disk_recheck_refused");
            String reason = MergeDiskPlan.refusalReason(
                    MergeDiskPlan.reservation(stagedBytes(catalog),
                            PageRunProofSpool.logicalBytes(ranges, catalog.descriptors().size())),
                    space);
            log.error("sort_merge_disk_recheck_refused reason=\"{}\" "
                            + "error_class=sort_disk_exhausted stop_reason=sort_disk_exhausted "
                            + "resumable=true ranges={}", reason, ranges);
            throw new MergeDiskExhaustedException(reason);
        }
    }

    /** Runtime-clamped serial fan-in plus its exact predicted-cascade signal. */
    int serialFanIn(PageRunCatalog catalog) throws MergeMemoryExhaustedException {
        return runtimeFanIn(catalog, 1);
    }

    /** Clamp cascade inputs while reserving descriptors for every concurrently open pipeline output. */
    int pipelineFanIn(PageRunCatalog catalog, int encoderCount)
            throws MergeMemoryExhaustedException {
        if (encoderCount < 1) {
            throw new IllegalArgumentException("pipeline encoder count must be positive");
        }
        return runtimeFanIn(catalog, encoderCount);
    }

    private int runtimeFanIn(PageRunCatalog catalog, int outputWriters)
            throws MergeMemoryExhaustedException {
        requireDecodedPageFits(catalog);
        int staticFanIn = config.effectiveFanIn();
        int softFdLimit = softFdLimitSupplier.getAsInt();
        int recordSizedFanIn = recordSizedFanIn(catalog);
        int headroom = saturatedHeadroom(outputWriters);
        int clamped = MergeFdBudget.clampedFanIn(staticFanIn, softFdLimit,
                headroom, recordSizedFanIn);
        if (clamped < staticFanIn) {
            int fdBound = MergeFdBudget.fdBoundedFanIn(softFdLimit, headroom);
            metrics.recordStealReason("SORT", "merge_fanin_clamped");
            if (fdBound < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_fd_clamped");
            }
            if (recordSizedFanIn < staticFanIn) {
                metrics.recordStealReason("SORT", "merge_fanin_mem_clamped");
            }
            log.debug("sort_merge_fanin_clamped static_fan_in={} fd_bound={} record_sized_fan_in={} "
                            + "clamped_fan_in={} soft_fd_limit={} segments={}",
                    staticFanIn, fdBound, recordSizedFanIn, clamped, softFdLimit,
                    catalog.descriptors().size());
        }
        warnIfCascadePredicted(catalog.descriptors().size(), clamped);
        return clamped;
    }

    private static int saturatedHeadroom(int outputWriters) {
        long adjusted = (long) MergeFdBudget.FD_HEADROOM + outputWriters - 1L;
        return (int) Math.min(Integer.MAX_VALUE, adjusted);
    }

    /** Exact pass-count warning shared by serial and admitted parallel planning. */
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

    private int recordSizedFanIn(PageRunCatalog catalog) {
        long perStreamPrice = perStreamBytes(catalog);
        if (perStreamPrice <= 0) {
            return Integer.MAX_VALUE;
        }
        long bound = config.mergeBudgetBytes() / perStreamPrice;
        return (int) Math.min(Integer.MAX_VALUE, Math.max(2L, bound));
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

    /** Choose boundaries using this run's configured policy and metrics sink. */
    List<byte[]> boundaries(PageRunCatalog catalog, BoundaryCandidates candidates,
            int desiredRanges) throws IOException {
        return boundaries(catalog.descriptors(), candidates, desiredRanges,
                config.mergeBoundaryPolicy(), metrics);
    }

    /** Test/characterization entry point preserving the default distinct policy. */
    static List<byte[]> boundaries(List<PageRunSegmentDescriptor> segments,
            BoundaryCandidates candidates, int desiredRanges, SortMetrics metrics) throws IOException {
        return boundaries(segments, candidates, desiredRanges, MergeBoundaryPolicy.DISTINCT, metrics);
    }

    /** The one boundary-selection implementation for both distinct and row-weighted policy. */
    static List<byte[]> boundaries(List<PageRunSegmentDescriptor> segments,
            BoundaryCandidates distinct, int desiredRanges,
            MergeBoundaryPolicy policy, SortMetrics metrics) throws IOException {
        boolean embedded = false;
        boolean scanned = false;
        for (PageRunSegmentDescriptor segment : segments) {
            SampleSource source = sampleKeys(segment, distinct, metrics);
            embedded |= source == SampleSource.EMBEDDED;
            scanned |= source == SampleSource.SCAN;
        }
        if (distinct.capped()) {
            metrics.recordStealReason("SORT", "merge_boundary_global_capped");
        }
        if (embedded && scanned) {
            metrics.recordStealReason("SORT", "merge_boundary_source_mixed");
        } else if (embedded) {
            metrics.recordStealReason("SORT", "merge_boundary_source_embedded");
        } else {
            metrics.recordStealReason("SORT", "merge_boundary_source_scan");
        }
        if (distinct.size() < 2) {
            return null;
        }
        List<byte[]> candidates = distinct.sortedKeys();
        if (policy == MergeBoundaryPolicy.ROWS) {
            List<byte[]> weighted = RowWeightedBoundaries.select(
                    segments, candidates, desiredRanges, metrics);
            if (weighted != null) {
                return weighted;
            }
        }
        return distinctBoundaries(candidates, desiredRanges);
    }

    private static List<byte[]> distinctBoundaries(List<byte[]> candidates, int desiredRanges) {
        int ranges = Math.min(desiredRanges, candidates.size());
        List<byte[]> boundaries = new ArrayList<>();
        byte[] last = null;
        for (int j = 1; j < ranges; j++) {
            int index = (int) ((long) j * candidates.size() / ranges);
            byte[] key = candidates.get(index);
            if (last == null || KeyBytes.compareUnsigned(key, last) > 0) {
                boundaries.add(key);
                last = key;
            }
        }
        return boundaries.isEmpty() ? null : boundaries;
    }

    private static SampleSource sampleKeys(PageRunSegmentDescriptor descriptor,
            BoundaryCandidates distinct, SortMetrics metrics) throws IOException {
        PageRunBoundarySample.ReadResult embedded = descriptor.sample();
        if (embedded.valid()) {
            if (embedded.totalRecords() > PageRunBoundarySample.MAX_ENTRIES) {
                metrics.recordStealReason("SORT", "merge_range_sample_capped");
            }
            metrics.recordBoundaryIo(embedded.entryCount(), embedded.bytesRead(), 0);
            metrics.markProgress();
            return SampleSource.EMBEDDED;
        }
        recordFallback(descriptor.extension().status(), metrics);

        long stride = PageRunBoundarySample.stride(embedded.totalRecords());
        if (stride > 1) {
            metrics.recordStealReason("SORT", "merge_range_sample_capped");
        }
        try (PageFrontierReader frontier = new PageFrontierReader(descriptor.path(), metrics)) {
            for (long page = 0; frontier.hasPage(); page++) {
                if (page % stride == 0) {
                    distinct.add(frontier.minKey().clone());
                }
                metrics.markProgress();
                frontier.advance();
            }
        }
        long fixedTailStart = descriptor.fileSize() - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long framedRecordBytes = descriptor.trailerStart() >= descriptor.headerBytes()
                        && descriptor.trailerStart() <= fixedTailStart
                ? descriptor.trailerStart() - descriptor.headerBytes()
                : 0;
        metrics.recordBoundaryIo(0, embedded.bytesRead(), framedRecordBytes);
        return SampleSource.SCAN;
    }

    private static void recordFallback(PageRunPageIndex.Status status, SortMetrics metrics) {
        switch (status) {
            case ABSENT -> metrics.recordStealReason("SORT", "merge_boundary_fallback_absent");
            case UNKNOWN -> metrics.recordStealReason("SORT", "merge_boundary_fallback_unknown");
            case INVALID_LENGTH ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_length");
            case INVALID_COUNT ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_count");
            case INVALID_CRC ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_crc");
            case INVALID_ORDER ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_order");
            case INVALID_BOUNDS ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_bounds");
            case INVALID_OFFSET ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_offset");
            case INVALID_CUMULATIVE ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_cumulative");
            case SKIPPED -> throw new AssertionError("parallel boundary sampling was skipped");
            case EMBEDDED, EMBEDDED_MINIMA_ONLY ->
                    throw new AssertionError("valid sample cannot fall back");
        }
    }

    int perRangeFanIn(int ranges, PageRunCatalog catalog) {
        return perRangeFanIn(ranges, perStreamBytes(catalog), ranges,
                catalog.descriptors().size());
    }

    private int perRangeFanIn(int ranges, long perStreamBytes, long openPartBudget,
                              int segments) {
        long proofBytes = PageRunProofSpool.logicalBytes(ranges, segments);
        long streamBudget = Math.max(0L, config.mergeBudgetBytes() - proofBytes);
        long perRangeBudget = streamBudget / ranges;
        long budgetBound = perRangeBudget / perStreamBytes;
        long fdBound = streamFdBudget(openPartBudget) / (long) ranges;
        return (int) Math.min(config.fanIn(), Math.max(2L, Math.min(budgetBound, fdBound)));
    }

    int openOutputPartLimit(int ranges, int perRangeFanIn) {
        long usable = usableFdBudget();
        if (usable == Long.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        long inputReservation = (long) ranges * perRangeFanIn + PROOF_SPOOL_FDS;
        return (int) Math.max(0L, Math.min(Integer.MAX_VALUE, usable - inputReservation));
    }

    EffectiveRanges effectiveRanges(int requested, PageRunCatalog catalog)
            throws MergeMemoryExhaustedException {
        requireDecodedPageFits(catalog);
        int segments = catalog.descriptors().size();
        if (requested <= 1 || segments <= 0) {
            return new EffectiveRanges(Math.max(1, requested), ClampReason.NONE);
        }
        if (stagedBytes(catalog) < config.minParallelStagedBytes()) {
            return new EffectiveRanges(1, ClampReason.BELOW_STAGED_FLOOR);
        }
        long perStream = perStreamBytes(catalog);
        if (streamFdBudget(1) < 2) {
            return new EffectiveRanges(1, ClampReason.FD_EXHAUSTED);
        }
        long byStreamBudget = config.mergeBudgetBytes() / perStream / segments;
        // The fixed proof spool is physically allocated and completely mapped. Charge its exact
        // range×segment extent to the same configured merge-phase resource budget as the open
        // streams, rather than discovering an unsafe plan only after worker startup.
        long combinedRangePrice = combinedRangePrice(perStream, segments);
        long byBudget = combinedRangePrice == Long.MAX_VALUE
                ? 0
                : config.mergeBudgetBytes() / combinedRangePrice;
        long usableFds = usableFdBudget();
        long byFd = usableFds == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : Math.max(0L, usableFds - PROOF_SPOOL_FDS) / (segments + 1L);
        int candidate = (int) Math.max(1L, Math.min(requested, Math.min(byBudget, byFd)));
        int beforeCascadeClamp = candidate;
        while (candidate > 1
                && perRangeFanIn(candidate, perStream, candidate, segments) < segments) {
            candidate--;
        }
        ClampReason reason = ClampReason.NONE;
        if (candidate < requested) {
            boolean cascadeBinding = candidate < beforeCascadeClamp;
            boolean fdBinding = byFd < requested && byFd <= byBudget;
            boolean proofBinding = byBudget < requested && byBudget < byStreamBudget
                    && byBudget < byFd;
            if (cascadeBinding) {
                reason = ClampReason.WOULD_CASCADE;
            } else if (fdBinding) {
                reason = candidate == 1 ? ClampReason.FD_EXHAUSTED : ClampReason.FD_LIMITED;
            } else if (proofBinding) {
                reason = ClampReason.PROOF_BUDGET_LIMITED;
            } else {
                reason = ClampReason.WOULD_CASCADE;
            }
        }
        return new EffectiveRanges(candidate, reason);
    }

    private void requireDecodedPageFits(PageRunCatalog catalog)
            throws MergeMemoryExhaustedException {
        long minimumWidth = Math.min(2L, catalog.descriptors().size());
        long required;
        try {
            required = Math.multiplyExact(minimumWidth, perStreamBytes(catalog));
        } catch (ArithmeticException overflow) {
            required = Long.MAX_VALUE;
        }
        if (catalog.maxRawPayloadLength() <= config.mergeBudgetBytes()
                && required <= config.mergeBudgetBytes()) {
            return;
        }
        metrics.recordStealReason("SORT", "merge_decoded_page_budget_exhausted");
        throw new MergeMemoryExhaustedException(
                "minimum merge width does not fit decoded-page residency: page_bytes="
                        + catalog.maxRawPayloadLength() + ", merge_budget_bytes="
                        + config.mergeBudgetBytes() + ", required_bytes=" + required
                        + ", minimum_streams=" + minimumWidth);
    }

    private static long combinedRangePrice(long perStreamBytes, int segments) {
        try {
            long streamAndProofSlot = Math.addExact(
                    perStreamBytes, (long) PageRunProofSpool.slotBytes());
            return Math.multiplyExact(streamAndProofSlot, segments);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private long perStreamBytes(PageRunCatalog catalog) {
        long encodedAndDecoded;
        try {
            // One stream normally retains the decoded current page's record body and raw payload
            // plus its already-advanced successor frontier body. The runtime overlap guard below
            // charges every additional retained page in a legal overlap cluster.
            long twoEncoded = Math.multiplyExact(
                    2L, Math.max(0L, catalog.maxRecordLen()));
            encodedAndDecoded = Math.addExact(twoEncoded, catalog.maxRawPayloadLength());
            encodedAndDecoded = Math.addExact(encodedAndDecoded,
                    2L * PageBlockCodec.PERSISTED_DICTIONARY_COORDINATE_BYTES);
        } catch (ArithmeticException overflow) {
            encodedAndDecoded = Long.MAX_VALUE;
        }
        return Math.max(config.mergePerStreamBytes(), encodedAndDecoded);
    }

    private static long stagedBytes(PageRunCatalog catalog) {
        long total = 0;
        for (PageRunSegmentDescriptor descriptor : catalog.descriptors()) {
            long bytes = Math.max(0L, descriptor.fileSize());
            total = total > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : total + bytes;
        }
        return total;
    }

    private long usableFdBudget() {
        int softLimit = softFdLimitSupplier.getAsInt();
        if (softLimit < 0) {
            return Long.MAX_VALUE;
        }
        return Math.max(0L, (long) softLimit - MergeFdBudget.FD_HEADROOM);
    }

    private long streamFdBudget(long openPartBudget) {
        long usable = usableFdBudget();
        return usable == Long.MAX_VALUE ? Long.MAX_VALUE
                : Math.max(0L, usable - openPartBudget - PROOF_SPOOL_FDS);
    }

    enum ClampReason {
        NONE("none"),
        BELOW_STAGED_FLOOR("below_staged_floor"),
        FD_EXHAUSTED("fd_exhausted"),
        FD_LIMITED("fd_limited"),
        PROOF_BUDGET_LIMITED("proof_budget_limited"),
        DISK_LIMITED("disk_limited"),
        WOULD_CASCADE("would_cascade");

        private final String logValue;

        ClampReason(String logValue) {
            this.logValue = logValue;
        }

        String logValue() {
            return logValue;
        }
    }

    record EffectiveRanges(int ranges, ClampReason reason) {
        EffectiveRanges {
            if (ranges < 1) {
                throw new IllegalArgumentException("ranges must be >= 1");
            }
        }
    }

    /** Whole-run bounded bottom-hash sample over distinct page minima. */
    static final class BoundaryCandidates {
        private static final Comparator<ScoredKey> BY_SCORE = (first, second) -> {
            int byHash = Long.compareUnsigned(first.score(), second.score());
            return byHash != 0 ? byHash : KeyBytes.compareUnsigned(first.key(), second.key());
        };

        private final TreeSet<byte[]> byKey = new TreeSet<>(KeyBytes::compareUnsigned);
        private final TreeSet<ScoredKey> byScore = new TreeSet<>(BY_SCORE);
        private final int maxCandidates;
        private boolean capped;

        BoundaryCandidates() {
            this(MAX_BOUNDARY_CANDIDATES);
        }

        BoundaryCandidates(int maxCandidates) {
            if (maxCandidates < 1) {
                throw new IllegalArgumentException("maxCandidates must be >= 1");
            }
            this.maxCandidates = maxCandidates;
        }

        void add(byte[] key) {
            if (byKey.contains(key)) {
                return;
            }
            ScoredKey candidate = new ScoredKey(score(key), key);
            if (byScore.size() == maxCandidates
                    && BY_SCORE.compare(candidate, byScore.last()) >= 0) {
                capped = true;
                return;
            }
            byte[] retained = key.clone();
            byKey.add(retained);
            byScore.add(new ScoredKey(candidate.score(), retained));
            if (byScore.size() > maxCandidates) {
                ScoredKey removed = byScore.pollLast();
                byKey.remove(removed.key());
                capped = true;
            }
        }

        int size() {
            return byKey.size();
        }

        boolean capped() {
            return capped;
        }

        List<byte[]> sortedKeys() {
            return new ArrayList<>(byKey);
        }

        private static long score(byte[] key) {
            long hash = 0xcbf29ce484222325L;
            for (byte value : key) {
                hash = (hash ^ (value & 0xFFL)) * 0x100000001b3L;
            }
            hash ^= hash >>> 33;
            hash *= 0xff51afd7ed558ccdL;
            hash ^= hash >>> 33;
            hash *= 0xc4ceb9fe1a85ec53L;
            return hash ^ (hash >>> 33);
        }

        private record ScoredKey(long score, byte[] key) {
        }
    }

    private enum SampleSource {
        EMBEDDED,
        SCAN
    }
}
