/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedCursor;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageBlockCodec;
import io.varve.swath.sort.spill.PageRunSegmentIo;
import io.varve.swath.sort.spill.PageRunSegmentWriter;
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
    public KWayMerge.PageStream openPages(Path segment) throws IOException {
        return new PageStream(PageRunSegmentIo.openUsingPersistedMaximum(
                segment, run.metrics()));
    }

    @Override
    public long decodedPageBudgetBytes(List<KWayMerge.PageStream> streams) throws IOException {
        long encodedFrontiers = 0;
        for (KWayMerge.PageStream stream : streams) {
            encodedFrontiers = Math.addExact(
                    encodedFrontiers, stream.frontierRetainedBytes());
        }
        long available = run.config().mergeBudgetBytes() - encodedFrontiers;
        if (available <= 0) {
            run.metrics().recordStealReason("SORT", "merge_decoded_residency_exhausted");
            throw new MergeMemoryExhaustedException(
                    "encoded cascade page frontiers exhaust the merge budget: frontier_bytes="
                            + encodedFrontiers + ", merge_budget_bytes="
                            + run.config().mergeBudgetBytes());
        }
        return available;
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

    /** One CRC-valid current page plus its independently validated successor advance. */
    private static final class PageStream implements KWayMerge.PageStream {
        private final PageRunSegmentIo io;
        private PageRunSegmentIo.Page current;

        PageStream(PageRunSegmentIo io) {
            this.io = io;
        }

        @Override
        public void initialize() throws IOException {
            advance();
        }

        @Override
        public long frontierRetainedBytes() {
            return Math.addExact(
                    io.maxRecordLen, PageBlockCodec.PERSISTED_DICTIONARY_COORDINATE_BYTES);
        }

        @Override
        public boolean hasPage() {
            return current != null;
        }

        @Override
        public byte[] minKey() {
            return current.header().minKey();
        }

        @Override
        public byte[] maxKey() {
            return current.header().maxKey();
        }

        @Override
        public PageBlock decodeCurrentPage() {
            return current.decode(io.path());
        }

        @Override
        public void advance() throws IOException {
            current = io.nextPage();
        }

        @Override
        public void close() throws IOException {
            io.close();
        }
    }
}
