/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Decode-free {@link PageFrontierStream} over one {@link PageRunSegmentWriter} page-run segment.
 * Reads one framed {@link PageBlock} record at a time and exposes the current page's
 * {@code [minKey, maxKey]} and {@code count} by structurally validating the body without decoding
 * rows. {@link #decodeCurrentPage()} runs the deferred {@link PageBlock#deserialize} on the retained
 * body bytes when (and only when) the merger emits the page; its cursor verifies payload exhaustion
 * and decoded first/last keys as it emits rows, without a second decode pass.
 *
 * <p>Shares {@link PageRunSegmentReader}'s physical-integrity guarantees, single-sourced in
 * {@link PageRunSegmentIo}: header/trailer magic validated on open, every framed record's length
 * bounded against {@code maxRecordLen} before allocation and CRC32C-verified on read, and an
 * unconditional end-of-stream cross-check of the summed per-page entry counts against the trailer's
 * declared {@code totalEntries} — even when {@link #advance()} never loads a page. Parsing the
 * leading min/max/count from the body is decode-free of ROWS, not of bytes: the body is still fully
 * read and CRC-verified — only the expensive row materialization is deferred.
 *
 * <p>Both page-run read paths — this decode-free frontier and {@link PageRunSegmentReader}'s
 * entry-typed stream — share {@link PageRunSegmentIo#nextPage()}'s intra-segment min-monotonicity
 * guard rather than each enforcing it separately, so neither can silently misorder a segment whose
 * pages regress. Page-range OVERLAP stays perfectly legal (the merger's key-merge fallback handles
 * it): only min REGRESSION is rejected.
 * A parallel original positioned from {@link PageRunSeekPlan} defers its single-reader tail check
 * to the stronger cross-range {@link PageRunZoneVerifier}; serial and intermediate readers keep the
 * existing local check unchanged.
 *
 * <p>This is a sibling of {@link PageRunSegmentReader}, not a replacement: the entry-typed
 * {@link EntryStream} reader still serves the Parquet-equivalence path and any direct entry consumer.
 * The framing/IO/validation is single-sourced in {@link PageRunSegmentIo}; this reader keeps only the
 * frontier-typed tail (decode-free min/max/count + retained body for the deferred decode).
 */
final class PageFrontierReader implements PageFrontierStream {

    private final PageRunSegmentIo io;
    private final boolean deferCompletenessToZoneProof;
    private long recordsLeft;
    private long seenEntries;

    private byte[] currentBody;
    private byte[] currentMin;
    private byte[] currentMax;
    private int currentCount;
    private PageRunSegmentIo.PagePosition currentPosition;

    /**
     * Open {@code path}, validate the header magic/version and the trailing magic (truncation check),
     * read the fixed trailer tail for the record/entry counts and {@code maxRecordLen}, then load the
     * first page's frontier. {@code metrics} carries the {@code SORT.page_run_min_regression}
     * engagement counter into the run summary.
     */
    PageFrontierReader(Path path, SortMetrics metrics) throws IOException {
        this(path, metrics, null, -1);
    }

    /** Open an original parallel input at its pre-worker planned seam. */
    PageFrontierReader(Path path, SortMetrics metrics, PageRunSeekPlan.SegmentPlan plan,
                       int range) throws IOException {
        this.io = PageRunSegmentIo.open(path, metrics);
        this.deferCompletenessToZoneProof = plan != null;
        try {
            PageRunPageIndex.IndexEntry target = plan == null ? null : plan.readTarget(io, range);
            if (target != null) {
                io.seekToPage(target);
                this.recordsLeft = io.totalRecords - target.pageOrdinal();
                this.seenEntries = target.cumulativeEntries();
            } else {
                this.recordsLeft = io.totalRecords;
            }
            advance();
        } catch (IOException | RuntimeException e) {
            io.close();
            throw e;
        }
    }

    @Override
    public boolean hasPage() {
        return currentBody != null;
    }

    @Override
    public byte[] minKey() {
        return currentMin;
    }

    @Override
    public byte[] maxKey() {
        return currentMax;
    }

    @Override
    public int count() {
        return currentCount;
    }

    @Override
    public PageBlock decodeCurrentPage() throws IOException {
        if (currentBody == null) {
            throw io.fail("decodeCurrentPage() with no current page");
        }
        try {
            return PageBlock.deserialize(currentBody, io.path());
        } catch (RuntimeException e) {
            throw io.corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "malformed page body", e);
        }
    }

    @Override
    public void advance() throws IOException {
        if (recordsLeft == 0) {
            // Completeness cross-check: fires even when no page was ever loaded — e.g. a one-page
            // segment whose totalRecords was bit-flipped to 0.
            if (!deferCompletenessToZoneProof) {
                io.checkComplete(seenEntries);
            }
            currentBody = null;
            currentMin = null;
            currentMax = null;
            currentCount = 0;
            currentPosition = null;
            return;
        }
        recordsLeft--;

        // The min-monotonicity guard lives inside nextPage() (the shared IO layer), so this reader
        // and the entry-typed PageRunSegmentReader can never disagree about what a legal segment is.
        PageRunSegmentIo.Page page = io.nextPage();
        PageRunSegmentIo.FrontierFields fields = page.fields();
        this.currentBody = page.body();
        this.currentMin = fields.minKey();
        this.currentMax = fields.maxKey();
        this.currentCount = fields.count();
        this.currentPosition = page.position();
        this.seenEntries += fields.count();
    }

    PageRunSegmentIo.PagePosition currentPosition() {
        return currentPosition;
    }

    long framedBytesRead() {
        return io.framedBytesRead();
    }

    long totalRecords() {
        return io.totalRecords;
    }

    long nextFrameOffset() throws IOException {
        return io.nextFrameOffset();
    }

    PageRunPageIndex.Cursor indexCursor(PageRunPageIndex.ReadResult extension,
                                        long payloadOffset, int entryCount) {
        return PageRunPageIndex.cursor(io, extension, payloadOffset, entryCount);
    }

    @Override
    public void close() throws IOException {
        io.close();
    }
}
