/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedEntryCursor;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageBlockFormat;
import io.varve.swath.sort.spill.PageRunReader;
import io.varve.swath.sort.spill.PageRunWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Page-run storage seam used by cascade passes. */
final class PageRunCascadeStore implements CascadeReducer.SegmentIo<Path> {

    private final SortRun run;
    private final PageRunWriter segmentWriter;
    private final Path stagingDir;
    private final StagingReconciliation stagingAuthority;
    private final String intermediatePrefix;
    private final int maxRawPayloadLength;
    private final List<Path> intermediates = new ArrayList<>();
    private int sequence;

    PageRunCascadeStore(SortRun run, PageRunWriter segmentWriter, Path stagingDir,
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
    public CascadeReducer.PageStream openPages(Path segment) throws IOException {
        return new PageStream(PageRunReader.openUsingPersistedMaximum(
                segment, run.metrics()));
    }

    @Override
    public long decodedPageBudgetBytes(List<CascadeReducer.PageStream> streams) throws IOException {
        long encodedFrontiers = 0;
        for (CascadeReducer.PageStream stream : streams) {
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
    public Path writeIntermediate(SortedEntryCursor sorted) throws IOException {
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
    private static final class PageStream implements CascadeReducer.PageStream {
        private final PageRunReader io;
        private PageRunReader.Page current;

        PageStream(PageRunReader io) {
            this.io = io;
        }

        @Override
        public void initialize() throws IOException {
            advance();
        }

        @Override
        public long frontierRetainedBytes() {
            return Math.addExact(
                    io.maxRecordLen, PageBlockFormat.PERSISTED_DICTIONARY_COORDINATE_BYTES);
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
