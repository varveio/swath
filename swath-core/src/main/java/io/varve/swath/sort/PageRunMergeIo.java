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
    private final List<Path> intermediates = new ArrayList<>();
    private int sequence;

    PageRunMergeIo(SortRun run, PageRunSegmentWriter segmentWriter, Path stagingDir,
                   String intermediatePrefix, KeyRange scope,
                   Map<Path, PageRunSegmentDescriptor> descriptors,
                   Consumer<RangeScopedPageFrontier> frontierRegistration) {
        this.run = run;
        this.segmentWriter = segmentWriter;
        this.stagingDir = stagingDir;
        this.intermediatePrefix = intermediatePrefix;
        this.scope = scope;
        this.descriptors = descriptors;
        this.frontierRegistration = frontierRegistration;
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
        long totalPages;
        if (descriptor != null) {
            run.metrics().recordStealReason("SORT", "merge_scoped_frontier_validated_trailer");
            totalPages = descriptor.trailer().totalRecords();
        } else {
            run.metrics().recordStealReason("SORT", "merge_scoped_frontier_trailer_reread");
            try (PageRunSegmentIo segmentIo =
                         PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
                totalPages = PageRunTrailer.read(segmentIo).totalRecords();
            }
        }
        PageFrontierReader frontier = new PageFrontierReader(segment, run.metrics());
        try {
            RangeScopedPageFrontier scoped = new RangeScopedPageFrontier(
                    frontier, scope.lo(), scope.hi(), totalPages, run.metrics());
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
