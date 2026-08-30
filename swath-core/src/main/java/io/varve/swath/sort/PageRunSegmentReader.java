/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

/**
 * Streaming {@link EntryStream} over one {@link PageRunSegmentWriter} page-run segment — a
 * <b>genuinely sorted run</b>, which is exactly what the generic entry-typed {@link StreamingMerger}
 * assumes of every input it is handed. Path-backed production merges use the page-frontier route;
 * this seam remains useful to embedded and direct callers. It never materializes a whole segment:
 * only the current page group is in heap.
 *
 * <p>Page-run v2 pages carry a persisted ordering mode and must be range-disjoint. OBJECTS requires
 * a strict seam ({@code previous.maxKey < next.minKey}); VERSIONS permits equality so all rows for
 * one raw key need not fit in one page. The writer checks this before completion, and the reader
 * independently rejects a corrupt overlap before emitting its page.
 *
 * <p><b>Resolution: the same machinery {@link PageAwareMerger} already uses for a whole merge.</b>
 * This reader is a {@link PageAwareMerger} over a SINGLE {@link PageFrontierReader} for this
 * segment, so:
 * <ul>
 *   <li><b>Disjoint pages (the common case) — decode-once fast path.</b> When the next page's
 *       {@code minKey} is strictly greater (unsigned) than the current page's {@code maxKey}, the
 *       page is decoded once and streamed whole in file order ({@code
 *       SORT.page_run_entry_whole_page}). There is no merge heap; the persisted-page cursor still
 *       compares adjacent rows to prove that the body is internally ordered.</li>
 *   <li><b>Equal VERSIONS boundary — key-merged.</b> When adjacent pages touch at one raw key, they
 *       are decoded and merged under the full comparator ({@code
 *       SORT.page_run_entry_overlap_keymerge}). True range overlap is corruption.</li>
 * </ul>
 * The inner merger is scoped as {@link MergeScope#INTRA_SEGMENT}, so it emits the two route-specific
 * counters directly. The outer merge owns duplicate reporting; doing it inside this reader as well
 * would double-count adjacent equals.
 *
 * <p><b>Intra-segment ordering guards.</b> Every page advance goes through
 * {@link PageRunSegmentIo#nextPage()}, the single page-advance primitive shared with
 * {@link PageFrontierReader}, which rejects a page whose {@code minKey} REGRESSES below the
 * previous page's (unsigned) as segment corruption ({@link SegmentCorruptionException},
 * {@code error_class=page_run_min_regression}) after bumping {@code SORT.page_run_min_regression}.
 * It then enforces the mode-aware range seam and reports {@code page_run_page_overlap} on failure.
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
 * the page being streamed — whose cursor pins the whole {@link PageBlock} and therefore its one
 * record-body owner plus any lazily-decoded compressed payload; there is no second stored-payload
 * array. A legal VERSIONS equality event additionally holds the decoded pages it is key-merging,
 * exactly as {@link PageAwareMerger} does on the all-page-run
 * route. {@link SortConfig#mergePerStreamBytes()} is an advisory
 * per-stream ESTIMATE (the merge fan-in denominator), not a bound this footprint is clamped to:
 * page size plus decompression expansion can exceed it.
 */
final class PageRunSegmentReader implements EntryStream {

    private final PageAwareMerger merger;
    private ListEntry head;

    /**
     * Present a genuinely sorted entry stream over a frontier the caller supplies and this reader
     * then owns (closing this closes it). The seam lets
     * {@link ParallelRangeMerge}'s page skip: a {@link RangeScopedPageFrontier} steps over the pages
     * that cannot reach the range without decoding them, and everything below this constructor —
     * the {@link PageAwareMerger}, the disjoint-page fast path, the VERSIONS equality merge, and the
     * ordering guards — is unchanged and cannot tell the difference, because a filtered
     * frontier is still a frontier presenting pages in non-decreasing {@code minKey} order.
     */
    PageRunSegmentReader(PageFrontierStream frontier, Comparator<ListEntry> comparator,
                         SortMetrics metrics) throws IOException {
        try {
            this.merger = new PageAwareMerger(
                    List.of(frontier), comparator, MergeScope.INTRA_SEGMENT, metrics);
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

}
