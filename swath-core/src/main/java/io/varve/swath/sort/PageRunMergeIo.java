/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Page-run storage seam used by cascade passes. */
final class PageRunMergeIo implements KWayMerge.SegmentIo<Path> {

    private final SortRun run;
    private final PageRunSegmentWriter segmentWriter;
    private final Path stagingDir;
    private final StagingReconciliation stagingAuthority;
    private final String intermediatePrefix;
    private final int maxRawPayloadLength;
    private final List<Path> intermediates = new ArrayList<>();
    private int sequence;

    PageRunMergeIo(SortRun run, PageRunSegmentWriter segmentWriter, Path stagingDir,
            StagingReconciliation stagingAuthority, String intermediatePrefix) {
        this.run = run;
        this.segmentWriter = segmentWriter;
        this.stagingDir = stagingDir;
        this.stagingAuthority = stagingAuthority;
        this.intermediatePrefix = intermediatePrefix;
        maxRawPayloadLength = (int) Math.min(
                PageBlock.MAX_RAW_PAYLOAD_BYTES, run.config().mergeBudgetBytes());
    }

    @Override
    public EntryStream open(Path segment) throws IOException {
        return new PageRunSegmentReader(
                segment, run.comparator(), run.metrics(), maxRawPayloadLength);
    }

    @Override
    public Path writeIntermediate(SortedCursor sorted) throws IOException {
        Path destination = stagingDir.resolve(
                StagingNames.cascadeIntermediate(intermediatePrefix, sequence++));
        stagingAuthority.requireOwnedStagingAuthority(stagingDir);
        segmentWriter.writeIntermediate(sorted, destination, maxRawPayloadLength);
        intermediates.add(destination);
        return destination;
    }

    @Override
    public void delete(Path segment) throws IOException {
        stagingAuthority.deleteDisposable(segment);
    }

    List<Path> intermediates() {
        return intermediates;
    }
}
