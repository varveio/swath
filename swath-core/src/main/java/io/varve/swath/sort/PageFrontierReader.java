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
 * {@code [minKey, maxKey]} and {@code count} by parsing ONLY the record body's leading fields — the
 * front-coded row payload is never decoded here. {@link #decodeCurrentPage()} runs the deferred
 * {@link PageBlock#deserialize} on the retained body bytes when (and only when) the merger emits the
 * page.
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
 *
 * <p>This is a sibling of {@link PageRunSegmentReader}, not a replacement: the entry-typed
 * {@link EntryStream} reader still serves the Parquet-equivalence path and any direct entry consumer.
 * The framing/IO/validation is single-sourced in {@link PageRunSegmentIo}; this reader keeps only the
 * frontier-typed tail (decode-free min/max/count + retained body for the deferred decode).
 */
final class PageFrontierReader implements PageFrontierStream {

    private final PageRunSegmentIo io;
    private long recordsLeft;
    private long seenEntries;

    private byte[] currentBody;
    private byte[] currentMin;
    private byte[] currentMax;
    private int currentCount;

    /**
     * Open {@code path} with no metrics recorder (tests / direct readers): as
     * {@link #PageFrontierReader(Path, SortMetrics)} with {@link SortMetrics#NO_OP}.
     */
    PageFrontierReader(Path path) throws IOException {
        this(path, SortMetrics.NO_OP);
    }

    /**
     * Open {@code path}, validate the header magic/version and the trailing magic (truncation check),
     * read the fixed trailer tail for the record/entry counts and {@code maxRecordLen}, then load the
     * first page's frontier. {@code metrics} carries the {@code SORT.page_run_min_regression}
     * engagement counter into the run summary.
     */
    PageFrontierReader(Path path, SortMetrics metrics) throws IOException {
        this.io = PageRunSegmentIo.open(path, metrics);
        try {
            this.recordsLeft = io.totalRecords;
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
        return PageBlock.deserialize(currentBody);
    }

    @Override
    public void advance() throws IOException {
        if (recordsLeft == 0) {
            // Completeness cross-check: fires even when no page was ever loaded — e.g. a one-page
            // segment whose totalRecords was bit-flipped to 0.
            io.checkComplete(seenEntries);
            currentBody = null;
            currentMin = null;
            currentMax = null;
            currentCount = 0;
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
        this.seenEntries += fields.count();
    }

    @Override
    public void close() throws IOException {
        io.close();
    }
}
