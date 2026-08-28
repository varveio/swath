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
    }

    @Override
    public EntryStream open(Path segment) throws IOException {
        return new PageRunSegmentReader(openPageFrontier(segment), run.comparator(), run.metrics());
    }

    @Override
    public Path writeIntermediate(SortedCursor sorted) throws IOException {
        Path destination = stagingDir.resolve(
                StagingNames.cascadeIntermediate(intermediatePrefix, sequence++));
        segmentWriter.writeIntermediate(sorted, destination);
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

    List<Path> intermediates() {
        return intermediates;
    }

    private PageFrontierStream openPageFrontier(Path segment) throws IOException {
        if (scope == null) {
            return new PageFrontierReader(segment, run.metrics());
        }
        PageRunSegmentDescriptor descriptor = descriptors.get(segment);
        PageRunSeekPlan.SegmentPlan segmentPlan = seekPlan == null ? null : seekPlan.segment(segment);
        PageFrontierReader frontier = segmentPlan == null
                ? new PageFrontierReader(segment, run.metrics())
                : new PageFrontierReader(segment, run.metrics(), segmentPlan, range);
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
