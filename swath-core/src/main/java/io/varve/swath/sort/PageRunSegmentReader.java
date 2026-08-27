/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Streaming {@link EntryStream} over one {@link PageRunSegmentWriter} page-run segment — a
 * <b>genuinely sorted run</b>, which is exactly what the generic entry-typed {@link StreamingMerger}
 * assumes of every input it is handed. Path-backed production merges use the page-frontier route;
 * this seam remains useful to embedded and direct callers. It never materializes a whole segment:
 * only the current page group is in heap.
 *
 * <p><b>Why a page-run segment isn't sorted by plain concatenation.</b> Pages are stored in
 * non-decreasing {@code minKey} order ({@link PageRunSegmentWriter#flush()} sorts each buffer's
 * pages on their first key), but adjacent pages may legally OVERLAP in range: two interleaved node
 * runs flush to pages {@code [a..m]} then {@code [c..z]} — mins ascend, ranges overlap — so
 * file-order concatenation yields {@code a, m, c, z}, which is NOT a sorted run. Page-range overlap
 * is legal, contract-normal output; the <em>reader</em> is what has to resolve it, so the trusting
 * {@link StreamingMerger} never sees it.
 *
 * <p><b>Resolution: the same machinery {@link PageAwareMerger} already uses for a whole merge.</b>
 * This reader is a {@link PageAwareMerger} over a SINGLE {@link PageFrontierReader} for this
 * segment, so:
 * <ul>
 *   <li><b>Disjoint pages (the common case) — decode-once fast path.</b> When the next page's
 *       {@code minKey} is strictly greater (unsigned) than the current page's {@code maxKey}, the
 *       page is decoded once and streamed whole in file order ({@code
 *       SORT.page_run_entry_whole_page}). No key comparison per entry, no heap.</li>
 *   <li><b>Overlapping pages — key-merged.</b> Otherwise the overlapping pages are decoded and
 *       merged at the key level under the full comparator (the merger's existing overlap
 *       fallback), so the emitted stream is sorted by construction ({@code
 *       SORT.page_run_entry_overlap_keymerge}; 0 on a segment whose pages are range-disjoint).</li>
 * </ul>
 * The inner merger's own {@code page_whole_emitted}/{@code page_overlap_keymerge} counters are
 * re-labelled to the two above so those two keep meaning "the MERGE saw (interleaved) pages across
 * its inputs", undiluted by this route's intra-segment reads — see {@link #relabelled} for the
 * re-labelling itself. The inner merger is given {@link DuplicateHook#NO_OP} because the outer
 * merger fires the hook on the merged output — reporting an adjacent duplicate here too would
 * double-count it.
 *
 * <p><b>Intra-segment min-monotonicity guard.</b> Every page advance goes through
 * {@link PageRunSegmentIo#nextPage()}, the single page-advance primitive shared with
 * {@link PageFrontierReader}, which rejects a page whose {@code minKey} REGRESSES below the
 * previous page's (unsigned) as segment corruption ({@link SegmentCorruptionException},
 * {@code error_class=page_run_min_regression}) after bumping {@code SORT.page_run_min_regression}.
 * Overlapping-but-ascending pages and equal mins stay legal — they are resolved by the key-merge
 * above, not rejected.
 *
 * <p><b>Fail-fast, no per-record counter.</b> A bad magic/version, a per-record CRC32C mismatch, a
 * short read, or a missing/torn trailer throws {@link IOException} — never a silent skip (the
 * merger's {@link UncheckedIOException} wrapper is unwrapped here so this stream keeps
 * {@link EntryStream}'s checked-{@code IOException} contract and a {@link SegmentCorruptionException}
 * stays typed). There is deliberately no per-record metric — billions of records would make it too
 * hot — so the thrown exception is the observability. The framing/IO/validation is single-sourced
 * in {@link PageRunSegmentIo}; the end-of-stream completeness cross-check
 * ({@code seenEntries == totalEntries}) is the frontier reader's, run unconditionally as its pages
 * are drained.
 *
 * <p><b>Memory.</b> On the fast path this holds the frontier's retained successor page body plus
 * the page being streamed — whose cursor pins the whole {@link PageBlock}, so both its compressed
 * payload and its lazily-decoded payload cache stay live; an overlap event additionally holds the
 * decoded pages it is key-merging, exactly as {@link PageAwareMerger} does on the all-page-run
 * route. {@link SortConfig#mergePerStreamBytes()} is an advisory
 * per-stream ESTIMATE (the merge fan-in denominator), not a bound this footprint is clamped to:
 * page size plus decompression expansion can exceed it.
 */
final class PageRunSegmentReader implements EntryStream {

    /** This route's engagement counters (SORT category) — see the class javadoc. */
    static final String ENTRY_WHOLE_PAGE = "page_run_entry_whole_page";
    static final String ENTRY_OVERLAP_KEYMERGE = "page_run_entry_overlap_keymerge";

    private final PageAwareMerger merger;
    private ListEntry head;

    /**
     * Open {@code path} with no metrics recorder (tests / direct readers): as
     * {@link #PageRunSegmentReader(Path, Comparator, SortMetrics)} with the
     * {@link ListEntryComparator} and {@link SortMetrics#NO_OP}.
     */
    PageRunSegmentReader(Path path) throws IOException {
        this(path, SortMetrics.NO_OP);
    }

    /**
     * Open {@code path} under the {@link ListEntryComparator}. {@code metrics} carries the
     * {@code SORT.page_run_min_regression} guard counter and this route's page counters.
     */
    PageRunSegmentReader(Path path, SortMetrics metrics) throws IOException {
        this(path, new ListEntryComparator(), metrics);
    }

    /**
     * Open {@code path}, validate the header magic/version and the trailing magic (completeness /
     * truncation check), then position at the first entry of the segment's sorted run. {@code comparator}
     * is the order the merge itself uses — the same one that resolves overlapping pages here, so the
     * run this stream presents and the order the caller merges under can never disagree.
     */
    PageRunSegmentReader(Path path, Comparator<ListEntry> comparator, SortMetrics metrics)
            throws IOException {
        // The frontier reader carries the RAW metrics (its page advances fire the guard counter
        // under its own name); only the merger's two page counters are re-labelled to this route's.
        this(new PageFrontierReader(path, metrics), comparator, metrics);
    }

    /**
     * As {@link #PageRunSegmentReader(Path, Comparator, SortMetrics)}, but over a frontier the
     * caller supplies and this reader then OWNS (closing this closes it). The seam exists for
     * {@link ParallelRangeMerge}'s page skip: a {@link RangeScopedPageFrontier} steps over the pages
     * that cannot reach the range without decoding them, and everything below this constructor —
     * the {@link PageAwareMerger}, the disjoint-page fast path, the overlap key-merge, the
     * min-monotonicity guard — is unchanged and cannot tell the difference, because a filtered
     * frontier is still a frontier presenting pages in non-decreasing {@code minKey} order.
     */
    PageRunSegmentReader(PageFrontierStream frontier, Comparator<ListEntry> comparator,
                         SortMetrics metrics) throws IOException {
        try {
            this.merger = new PageAwareMerger(List.of(frontier), comparator, DuplicateHook.NO_OP,
                    relabelled(metrics));
        } catch (UncheckedIOException e) {
            throw e.getCause();   // PageAwareMerger's constructor already closed the frontier stream
        }
        try {
            this.head = readNext();
        } catch (IOException | RuntimeException e) {
            try {
                merger.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);   // never mask the read failure that got us here
            }
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return head != null;
    }

    @Override
    public ListEntry peek() {
        return head;
    }

    @Override
    public ListEntry next() throws IOException {
        ListEntry cur = head;
        head = readNext();
        return cur;
    }

    private ListEntry readNext() throws IOException {
        try {
            return merger.hasNext() ? merger.next() : null;
        } catch (UncheckedIOException e) {
            // Restore EntryStream's checked contract: a SegmentCorruptionException stays typed and
            // greppable for ListRunner's error_class walk instead of hiding under an unchecked wrapper.
            throw e.getCause();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            merger.close();   // closes the single frontier stream it owns
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * The inner single-segment {@link PageAwareMerger}'s two page counters, re-labelled to this route's
     * (see the class javadoc): the merge-level {@code page_whole_emitted}/{@code page_overlap_keymerge}
     * must keep meaning "the MERGE saw (interleaved) pages across its inputs", so an intra-segment page
     * resolution done inside one input stream is counted under its own reason instead. Anything else the
     * merger might emit passes through unchanged.
     *
     * <p>The re-labelled reasons are passed as STRING LITERALS, not via the {@link #ENTRY_WHOLE_PAGE}
     * /{@link #ENTRY_OVERLAP_KEYMERGE} constants: {@code scripts/ci/check-instrumentation-drift.sh}
     * statically reconciles {@code recordStealReason} call sites against the §5 registry table and can
     * only resolve literals — passing a constant makes the counter a "ghost row" and fails the build.
     * The constants remain for {@code equals} comparisons and for tests to reference by name.
     */
    private static SortMetrics relabelled(SortMetrics metrics) {
        return (outcome, reason) -> {
            // "SORT" is a literal, not the forwarded `outcome`, for the same drift-guard reason: both
            // arguments must be literals for the guard to resolve the counter. PageAwareMerger only ever
            // emits these two reasons under the SORT outcome, so pinning it here is faithful, not a guess.
            if (PageAwareMerger.WHOLE_PAGE_EMITTED.equals(reason)) {
                metrics.recordStealReason("SORT", "page_run_entry_whole_page");
            } else if (PageAwareMerger.OVERLAP_KEYMERGE.equals(reason)) {
                metrics.recordStealReason("SORT", "page_run_entry_overlap_keymerge");
            } else {
                metrics.recordStealReason(outcome, reason);
            }
        };
    }

    /**
     * The completeness trailer of a page-run segment: the actual segment key bounds plus the record /
     * entry counts and the max framed record length. This is the seam {@code SortedFileIndex}
     * consumes for {@code bounds} ({@code segMinKey}/{@code segMaxKey} are exact keys, no truncated-stats
     * hazard) and the runtime merge fan-in planner consumes {@code maxRecordLen} as an encoded-record
     * refinement of its configured per-stream estimate.
     */
    record Trailer(byte[] segMinKey, byte[] segMaxKey, long totalRecords, long totalEntries, long maxRecordLen) {
    }

    /**
     * Read just the trailer of {@code path} without decoding any record: validate the header and the
     * trailing magic (fail-fast on a truncated/corrupt segment) via {@link PageRunSegmentIo#open},
     * then seek straight to {@code trailerStart} to read the key bounds — O(1) regardless of how many
     * records the segment holds (no per-record length-prefix walk).
     */
    static Trailer readTrailer(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path)) {
            return readTrailer(io);
        }
    }

    /** Read the trailer bounds from an already-open, validated segment. */
    static Trailer readTrailer(PageRunSegmentIo io) throws IOException {
        long fixedTailStart = io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        byte[] segMin = readLenPrefixedKey(io, io.trailerStart, fixedTailStart);
        byte[] segMax = readLenPrefixedKey(io, io.trailerStart + 2 + segMin.length,
                fixedTailStart);
        if (io.totalRecords == 0 && (segMin.length != 0 || segMax.length != 0)) {
            throw io.fail("empty segment has non-empty trailer bounds");
        }
        return new Trailer(segMin, segMax, io.totalRecords, io.totalEntries, io.maxRecordLen);
    }

    private static byte[] readLenPrefixedKey(PageRunSegmentIo io, long pos, long limit)
            throws IOException {
        if (pos < io.trailerStart || pos > limit - 2) {
            throw io.fail("trailer key prefix exceeds trailer bounds");
        }
        int len = io.readAt(pos, 2).getShort() & 0xFFFF;
        if (len > limit - pos - 2) {
            throw io.fail("trailer key exceeds trailer bounds");
        }
        return io.readAt(pos + 2, len).array();
    }
}
