/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** The page-run storage seam shared by serial and range-scoped k-way merges. */
final class PageRunMergeIo implements KWayMerge.SegmentIo<Path> {

    private final SortRun run;
    private final PageRunSegmentWriter segmentWriter;
    private final Path stagingDir;
    private final String intermediatePrefix;
    private final KeyRange scope;
    private final Map<Path, PageRunSegmentDescriptor> descriptors;
    private final Consumer<RangeScopedPageFrontier> frontierRegistration;
    private final int range;
    private final PageRunSeekPlan seekPlan;
    private final PageRunZoneVerifier.RangeBuilder proofBuilder;
    private final int maxRawPayloadLength;
    private final long decodedPageBudgetBytes;
    private long maxEncodedRecordBytes;
    private final List<Path> intermediates = new ArrayList<>();
    private int sequence;

    PageRunMergeIo(SortRun run, PageRunSegmentWriter segmentWriter, Path stagingDir,
                   String intermediatePrefix, KeyRange scope,
                   Map<Path, PageRunSegmentDescriptor> descriptors,
                   Consumer<RangeScopedPageFrontier> frontierRegistration,
                   int range, PageRunSeekPlan seekPlan,
                   PageRunZoneVerifier.RangeBuilder proofBuilder) {
        this.run = run;
        this.segmentWriter = segmentWriter;
        this.stagingDir = stagingDir;
        this.intermediatePrefix = intermediatePrefix;
        this.scope = scope;
        this.descriptors = descriptors;
        this.frontierRegistration = frontierRegistration;
        this.range = range;
        this.seekPlan = seekPlan;
        this.proofBuilder = proofBuilder;
        long configuredPrice = run.config().mergePerStreamBytes();
        int decodedLimit = (int) Math.min(PageBlock.MAX_RAW_PAYLOAD_BYTES, configuredPrice);
        long encodedMaximum = 0;
        boolean unknownDecodedMaximum = false;
        for (PageRunSegmentDescriptor descriptor : descriptors.values()) {
            encodedMaximum = Math.max(encodedMaximum, descriptor.trailer().maxRecordLen());
            decodedLimit = Math.max(decodedLimit, (int) Math.min(
                    PageBlock.MAX_RAW_PAYLOAD_BYTES, descriptor.trailer().maxRecordLen()));
            if (descriptor.hasDecodedPageMaximum()) {
                decodedLimit = Math.max(decodedLimit, descriptor.maxRawPayloadLength());
            } else {
                unknownDecodedMaximum = true;
            }
        }
        if (unknownDecodedMaximum) {
            decodedLimit = (int) Math.min(
                    PageBlock.MAX_RAW_PAYLOAD_BYTES, run.config().mergeBudgetBytes());
        }
        this.maxRawPayloadLength = decodedLimit;
        this.maxEncodedRecordBytes = encodedMaximum;
        if (scope == null || seekPlan == null) {
            this.decodedPageBudgetBytes = run.config().mergeBudgetBytes();
        } else {
            long proofBytes = PageRunProofSpool.logicalBytes(
                    seekPlan.ranges(), descriptors.size());
            long streamBudget = Math.max(0L, run.config().mergeBudgetBytes() - proofBytes);
            this.decodedPageBudgetBytes = streamBudget / seekPlan.ranges();
        }
    }

    @Override
    public EntryStream open(Path segment) throws IOException {
        return new PageRunSegmentReader(openPageFrontier(segment), run.comparator(), run.metrics());
    }

    @Override
    public Path writeIntermediate(SortedCursor sorted) throws IOException {
        Path destination = stagingDir.resolve(
                StagingNames.cascadeIntermediate(intermediatePrefix, sequence++));
        segmentWriter.writeIntermediate(sorted, destination, maxRawPayloadLength);
        intermediates.add(destination);
        return destination;
    }

    @Override
    public void delete(Path segment) throws IOException {
        Files.deleteIfExists(segment);
    }

    @Override
    public boolean supportsPageFrontier(Path segment) {
        return run.inputProfile().pageFrontierAllowed();
    }

    @Override
    public PageFrontierStream openFrontier(Path segment) throws IOException {
        return openPageFrontier(segment);
    }

    @Override
    public long decodedPageBudgetBytes(List<Path> segments) throws IOException {
        // Original descriptors already carry maxRecordLen. Cascade intermediates are trusted files
        // produced earlier in this merge, but regrouping can widen their headers; read only their
        // validated fixed trailers before KWayMerge allocates the first frontier body.
        for (Path segment : segments) {
            if (descriptors.containsKey(segment)) {
                continue;
            }
            try (PageRunSegmentIo intermediate = PageRunSegmentIo.open(segment, run.metrics())) {
                maxEncodedRecordBytes = Math.max(
                        maxEncodedRecordBytes, intermediate.maxRecordLen);
            }
        }
        long encodedReservation;
        try {
            encodedReservation = Math.multiplyExact(maxEncodedRecordBytes, segments.size());
        } catch (ArithmeticException overflow) {
            encodedReservation = Long.MAX_VALUE;
        }
        if (encodedReservation > decodedPageBudgetBytes) {
            run.metrics().recordStealReason("SORT", "merge_decoded_residency_exhausted");
            throw new MergeMemoryExhaustedException(
                    "encoded frontier bodies exceed the per-merger merge budget before open: "
                            + "frontier_bytes=" + encodedReservation + ", budget_bytes="
                            + decodedPageBudgetBytes + ", streams=" + segments.size());
        }
        return decodedPageBudgetBytes - encodedReservation;
    }

    List<Path> intermediates() {
        return intermediates;
    }

    private PageFrontierStream openPageFrontier(Path segment) throws IOException {
        PageRunSegmentDescriptor descriptor = descriptors.get(segment);
        int segmentRawPayloadLength = descriptor == null || !descriptor.hasDecodedPageMaximum()
                ? (descriptor == null ? maxRawPayloadLength : PageBlock.MAX_RAW_PAYLOAD_BYTES)
                : descriptor.maxRawPayloadLength();
        if (scope == null) {
            return new PageFrontierReader(segment, run.metrics(), segmentRawPayloadLength);
        }
        PageRunSeekPlan.SegmentPlan segmentPlan = seekPlan == null ? null : seekPlan.segment(segment);
        PageFrontierReader frontier = segmentPlan == null
                ? new PageFrontierReader(segment, run.metrics(), segmentRawPayloadLength)
                : new PageFrontierReader(
                        segment, run.metrics(), segmentPlan, range, segmentRawPayloadLength);
        // decodedPageBudgetBytes(group) preflighted every intermediate before frontier allocation;
        // retain this fold as a defensive check for direct/open-order test seams.
        maxEncodedRecordBytes = Math.max(maxEncodedRecordBytes, frontier.maxRecordLen());
        long totalPages = frontier.totalRecords();
        if (descriptor != null) {
            run.metrics().recordStealReason("SORT", "merge_scoped_frontier_validated_trailer");
        } else {
            run.metrics().recordStealReason("SORT", "merge_scoped_frontier_trailer_reread");
        }
        try {
            PageRunZoneVerifier.Tracker tracker = segmentPlan == null
                    ? null : proofBuilder.open(segmentPlan, frontier, run.metrics());
            long startOrdinal = segmentPlan == null ? 0 : segmentPlan.start(range).pageOrdinal();
            if (segmentPlan != null && startOrdinal > 0) {
                run.metrics().recordStealReason("SORT", "merge_range_index_seek");
            } else if (segmentPlan != null && !descriptor.extension().valid()) {
                run.metrics().recordStealReason("SORT", "merge_range_index_absent");
            }
            RangeScopedPageFrontier scoped = new RangeScopedPageFrontier(
                    frontier, scope.lo(), scope.hi(), totalPages, startOrdinal, run.metrics(), tracker);
            frontierRegistration.accept(scoped);
            return scoped;
        } catch (IOException | RuntimeException e) {
            try {
                frontier.close();
            } catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }
}
