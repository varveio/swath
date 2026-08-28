/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Page-aware k-way merge of N page-run segments into one sorted {@link SortedCursor} —
 * selected for ANY merge group, whether an intermediate cascade pass or the final pass, whenever
 * every input exposes a {@link PageFrontierStream} (page-run staging / cascade intermediates); a
 * mixed or Parquet input keeps the entry-typed {@link StreamingMerger}. Output is byte-identical,
 * entry-for-entry, to the {@link StreamingMerger} on the same input. Adjacent duplicate reporting is
 * applied once around either merger by {@link DuplicateReporting}.
 *
 * <p><b>Decode-free frontier.</b> Each segment is a {@link PageFrontierStream} positioned at its
 * current page, whose {@code [minKey, maxKey]} is known without decoding any row (only the record
 * body's leading fields are parsed; see {@link PageFrontierReader}). Because a page is internally
 * ordered under the full §0.3 comparator, its first entry's key equals its {@code minKey}, so
 * the segment with the smallest current {@code minKey} holds the globally-next entry.
 *
 * <p><b>Page-whole fast path.</b> {@link #plan()} pops the minimum-{@code minKey} segment {@code m},
 * snapshots its current page, and <em>advances {@code m}'s frontier to its next page before
 * deciding</em> — so the frontier heap then holds every OTHER segment's current page <b>and {@code
 * m}'s own next page</b>. If the smallest {@code minKey} remaining in that heap is strictly greater
 * (unsigned) than {@code m}'s current-page {@code maxKey}, then no page IN THE FRONTIER — cross-segment
 * or {@code m}'s own successor — starts within the current page's range, so the whole page is globally
 * next: it is decoded once and streamed in order. {@link MergeScope#CROSS_SEGMENT} emits {@code
 * SORT.page_whole_emitted}; the intra-segment reader uses the same path and emits
 * {@code SORT.page_run_entry_whole_page} instead.
 *
 * <p><b>What the strict check does and does not prove.</b> It establishes disjointness only
 * <em>relative to the frontier</em>: it compares {@code pageMax} against each segment's CURRENT page —
 * for {@code m} that is its immediate NEXT page, never the pages after it. It is therefore <b>not</b> a
 * self-standing intra-segment monotonicity guard. The frontier is a sound lower bound on a segment's
 * <em>remaining</em> keys only because page {@code minKey}s are non-decreasing WITHIN a segment: given
 * that ascent, a segment's current-page {@code minKey} bounds everything it has left, and the strict
 * check is a true global-disjointness test. That ascent is a writer-side precondition
 * ({@link PageRunSegmentWriter#flush()} sorts pages by first key) — and one that is
 * <b>verified at read time</b> by {@link PageRunSegmentIo#nextPage()} (the page-advance primitive behind
 * {@link PageFrontierReader#advance()} AND {@link PageRunSegmentReader}, so the entry-typed
 * {@link StreamingMerger} route is guarded identically), which fails the segment as corrupt
 * ({@link SegmentCorruptionException}, {@code error_class=page_run_min_regression}, counter
 * {@code SORT.page_run_min_regression}) if a page's {@code minKey} regresses. Without that read-time guard a regressed page min would let this
 * fast path emit a page ahead of keys that sort earlier and misorder the output silently, with {@code
 * page_overlap_keymerge} firing only afterwards — an alarm, not a proof. Page-range OVERLAP is a
 * different thing entirely and stays legal: it is what the key-merge fallback below exists for.
 *
 * <p><b>Overlap fallback (key-level merge).</b> If that strict condition fails — some other page (or
 * {@code m}'s own next page) begins at/inside {@code m}'s current-page range — the pages interleave
 * and emitting {@code m}'s page whole could misorder. The merger instead decodes the involved pages
 * and merges their entries by comparator (an entry heap), pulling any frontier page whose range
 * overlaps the active region into the merge as needed. {@link MergeScope#CROSS_SEGMENT} emits
 * {@code SORT.page_overlap_keymerge}; {@link MergeScope#INTRA_SEGMENT} emits {@code
 * SORT.page_run_entry_overlap_keymerge}.
 * On a well-formed OBJECTS run every page is range-disjoint, so this counter is 0 — a nonzero value is
 * a loud invariant alarm (mis-ordered/overlapping pages), never a silent misorder. Correctness beats
 * the optimization: a page is emitted whole only when strictly {@code maxKey <} every other {@code
 * minKey}; any equal-key boundary, version tiebreak, or ambiguity falls back to the key-merge.
 */
final class PageAwareMerger implements SortedCursor, LogicalMergeCompletion {

    private final Comparator<ListEntry> comparator;
    private final MergeScope scope;
    private final SortMetrics metrics;
    private final MergeRunSink runSink;
    private final List<PageFrontierStream> allStreams;
    /** Per-merger share of the process merge budget; checked before compressed payload allocation. */
    private final long decodedPageBudgetBytes;
    private long residentDecodedBytes;

    /** Segments whose CURRENT (undecoded) page is still a frontier candidate, keyed by unsigned minKey. */
    private final PriorityQueue<Seg> frontier;
    /** Decoded pages currently being key-merged (overlap fallback), keyed by head entry. */
    private final PriorityQueue<DecodedPage> active;
    /** Monotone upper bound (max page maxKey) over the current key-merge event; null when active is empty. */
    private byte[] ceiling;
    /** Decoded entries retained across all pages in the active overlap cluster. */
    private long activeRows;

    /** A single decoded page being streamed whole (fast path); null otherwise. */
    private PageBlockCursor wholeCursor;
    private long wholeDecodedBytes;

    private ListEntry pending;
    private final MergeRunTracker sourceRuns;
    private boolean logicalMergeComplete;
    private boolean closed;

    PageAwareMerger(List<PageFrontierStream> streams, Comparator<ListEntry> comparator,
                    MergeScope scope, SortMetrics metrics) {
        this(streams, comparator, scope, metrics, MergeRunSink.NO_OP);
    }

    PageAwareMerger(List<PageFrontierStream> streams, Comparator<ListEntry> comparator,
                    MergeScope scope, SortMetrics metrics, MergeRunSink runSink) {
        this(streams, comparator, scope, metrics, runSink, Long.MAX_VALUE);
    }

    PageAwareMerger(List<PageFrontierStream> streams, Comparator<ListEntry> comparator,
                    MergeScope scope, SortMetrics metrics, MergeRunSink runSink,
                    long decodedPageBudgetBytes) {
        this.comparator = comparator;
        this.scope = scope;
        this.metrics = metrics;
        this.runSink = runSink;
        this.allStreams = streams;
        this.decodedPageBudgetBytes = decodedPageBudgetBytes;
        this.frontier = new PriorityQueue<>((a, b) -> Arrays.compareUnsigned(a.minKey(), b.minKey()));
        this.active = new PriorityQueue<>((a, b) -> comparator.compare(a.head, b.head));
        this.sourceRuns = new MergeRunTracker(streams.size());
        for (int i = 0; i < streams.size(); i++) {
            PageFrontierStream s = streams.get(i);
            if (s.hasPage()) {
                frontier.add(new Seg(i, s));
            }
        }
        // computeNext() decodes the first page group and can throw (CRC/magic mismatch); if it does,
        // the object never finishes constructing, so close() is unreachable — release every already-open
        // frontier stream here rather than leaking them (mirrors PageFrontierReader's constructor guard).
        try {
            this.pending = computeNext();
        } catch (RuntimeException e) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                if (closeFailure != e) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return pending != null;
    }

    @Override
    public ListEntry next() {
        if (pending == null) {
            throw new NoSuchElementException();
        }
        ListEntry result = pending;
        pending = computeNext();
        return result;
    }

    private ListEntry computeNext() {
        try {
            while (true) {
                MergeCancellation.check();
                // (1) Streaming a whole page (fast path)?
                if (wholeCursor != null) {
                    if (wholeCursor.hasNext()) {
                        return wholeCursor.next();
                    }
                    wholeCursor = null;
                    releaseDecoded(wholeDecodedBytes);
                    wholeDecodedBytes = 0;
                }
                // (2) Key-merging overlapping pages?
                if (!active.isEmpty()) {
                    closeActiveUnderFrontier();
                    DecodedPage dp = active.poll();
                    ListEntry e = dp.head;
                    dp.advanceHead();
                    activeRows--;
                    if (dp.head != null) {
                        active.add(dp);
                    } else {
                        releaseDecoded(dp.decodedBytes);
                        dp.decodedBytes = 0;
                    }
                    if (active.isEmpty()) {
                        ceiling = null;   // event resolved; the next plan() starts a fresh one
                    }
                    recordSource(dp.source);
                    return e;
                }
                // (3) Plan the next page group.
                if (frontier.isEmpty()) {
                    logicalMergeComplete = true;
                    return null;
                }
                plan();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("page-aware merge read failed", e);
        }
    }

    /**
     * Pop the minimum-{@code minKey} segment, advance its frontier, then decide whole-emit vs
     * key-merge on the unified strict-disjointness check (see the class javadoc). Sets up either
     * {@link #wholeCursor} (fast path) or seeds {@link #active} (overlap fallback).
     */
    private void plan() throws IOException {
        Seg m = frontier.poll();
        byte[] pageMax = m.maxKey().clone();   // snapshot: m.advance() invalidates the reader's buffer
        PageBlock page = m.decodeCurrentPage();
        // Reserve the current body-as-extra plus its decompression target BEFORE advance() can
        // allocate the successor body. The per-stream base already reserves one frontier body;
        // this reservation is what makes the transient current+successor overlap safe.
        long reserved = reserveDecoded(page);
        try {
            m.advance();
            if (m.stream.hasPage()) {
                frontier.add(m);   // m's NEXT page re-enters contention
            }

            // Whole iff NOTHING remaining in the frontier — neither another segment's current page
            // nor m's own next page — starts at/inside this page's range.
            boolean whole = frontier.isEmpty()
                    || Arrays.compareUnsigned(frontier.peek().minKey(), pageMax) > 0;
            if (whole) {
                recordEngagement(true);
                recordSource(m.source);
                wholeCursor = page.cursor();
                wholeDecodedBytes = reserved;
                reserved = 0;
            } else {
                recordEngagement(false);
                metrics.recordStealReason("SORT", "merge_overlap_cluster");
                metrics.recordPageAwareOverlapCluster();
                addActive(m.source, page, reserved);
                reserved = 0;
                ceiling = pageMax;
            }
        } finally {
            releaseDecoded(reserved);
        }
    }

    /**
     * Ensure {@link #active} is closed under overlap: pull in every frontier segment whose current
     * page starts at/inside the active region ({@code minKey <= ceiling}, unsigned) so the entry heap
     * holds the true global minimum. {@code ceiling} is kept monotone non-decreasing within an event
     * (a pulled page can only widen it) — over-pulling a page slightly early is harmless and stays
     * byte-identical, but never under-pulls a page that could interleave.
     */
    private void closeActiveUnderFrontier() throws IOException {
        while (!frontier.isEmpty()
                && Arrays.compareUnsigned(frontier.peek().minKey(), ceiling) <= 0) {
            Seg f = frontier.poll();
            byte[] fMax = f.maxKey().clone();
            PageBlock page = f.decodeCurrentPage();
            long reserved = reserveDecoded(page);
            try {
                f.advance();
                if (f.stream.hasPage()) {
                    frontier.add(f);
                }
                addActive(f.source, page, reserved);
                reserved = 0;
                if (Arrays.compareUnsigned(fMax, ceiling) > 0) {
                    ceiling = fMax;
                }
            } finally {
                releaseDecoded(reserved);
            }
        }
    }

    /** Emit the scope-specific literal at the point that chooses the page algorithm path. */
    private void recordEngagement(boolean whole) {
        switch (scope) {
            case CROSS_SEGMENT -> {
                if (whole) {
                    metrics.recordStealReason("SORT", "page_whole_emitted");
                } else {
                    metrics.recordStealReason("SORT", "page_overlap_keymerge");
                }
            }
            case INTRA_SEGMENT -> {
                if (whole) {
                    metrics.recordStealReason("SORT", "page_run_entry_whole_page");
                } else {
                    metrics.recordStealReason("SORT", "page_run_entry_overlap_keymerge");
                }
            }
        }
    }

    /** Add one decoded page to the active cluster and publish only cheap peak observations. */
    private void addActive(int source, PageBlock page, long decodedBytes) {
        active.add(new DecodedPage(source, page.cursor(), page.count(), decodedBytes));
        activeRows += page.count();
        metrics.recordPageAwareOverlapState(active.size(), activeRows);
    }

    /**
     * Reserve the decoded page's retained record body plus any separate decompression target before
     * {@link PageBlock#cursor()} can allocate it. The per-merger base budget has already reserved one
     * encoded frontier body per open stream; after advance, this page owner is an additional retained
     * body alongside that stream's successor frontier.
     */
    private long reserveDecoded(PageBlock page) throws MergeMemoryExhaustedException {
        long decodedBytes = (long) page.retainedRecordBytes()
                + (page.codec() == PageCodec.NONE ? 0L : page.rawPayloadLength());
        long next;
        try {
            next = Math.addExact(residentDecodedBytes, decodedBytes);
        } catch (ArithmeticException overflow) {
            next = Long.MAX_VALUE;
        }
        if (next > decodedPageBudgetBytes) {
            metrics.recordStealReason("SORT", "merge_decoded_residency_exhausted");
            throw new MergeMemoryExhaustedException(
                    "decoded-page retained residency exceeds the per-merger merge budget: resident_bytes="
                            + residentDecodedBytes + ", next_page_bytes=" + decodedBytes
                            + ", budget_bytes=" + decodedPageBudgetBytes);
        }
        residentDecodedBytes = next;
        return decodedBytes;
    }

    private void releaseDecoded(long decodedBytes) {
        residentDecodedBytes -= decodedBytes;
        if (residentDecodedBytes < 0) {
            throw new IllegalStateException("decoded-page residency accounting underflow");
        }
    }

    /** Track source runs with an int comparison only; no row key comparison or allocation is added. */
    private void recordSource(int source) {
        sourceRuns.emittedFrom(source);
    }

    @Override
    public void completeLogicalMerge() {
        logicalMergeComplete = true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        RuntimeException failure = validateDecodedPages();
        for (PageFrontierStream s : allStreams) {
            try {
                s.close();
            } catch (IOException e) {
                failure = append(failure,
                        new UncheckedIOException("closing page frontier stream failed", e));
            } catch (RuntimeException e) {
                failure = append(failure, e);
            }
        }
        if (logicalMergeComplete && failure == null) {
            long copyable = 0;
            long interleaved = 0;
            for (int source = 0; source < allStreams.size(); source++) {
                int runs = sourceRuns.count(source);
                if (runs == 1) {
                    copyable++;
                } else if (runs > 1) {
                    interleaved++;
                }
            }
            try {
                runSink.accept(copyable, interleaved);
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * A range cutoff may stop above this merger while it still owns decoded pages. Drain only those
     * cursors — never untouched frontier pages — so their payload/bounds validation cannot be skipped.
     * Direct cursor drains deliberately bypass source-run, duplicate, engagement, and progress signals.
     */
    private RuntimeException validateDecodedPages() {
        RuntimeException failure = null;
        if (wholeCursor != null) {
            try {
                wholeCursor.drainAndValidate();
            } catch (RuntimeException e) {
                failure = append(failure, e);
            } finally {
                releaseDecoded(wholeDecodedBytes);
                wholeDecodedBytes = 0;
            }
        }
        for (DecodedPage page : active) {
            try {
                page.drainAndValidate();
            } catch (RuntimeException e) {
                failure = append(failure, e);
            } finally {
                releaseDecoded(page.decodedBytes);
                page.decodedBytes = 0;
            }
        }
        return failure;
    }

    private static RuntimeException append(RuntimeException first, RuntimeException next) {
        if (first == null) {
            return next;
        }
        if (next != first) {
            first.addSuppressed(next);
        }
        return first;
    }

    /** One page-run segment at its current (undecoded) frontier page. Only mutated while OUT of the heap. */
    private static final class Seg {
        private final int source;
        private final PageFrontierStream stream;

        Seg(int source, PageFrontierStream stream) {
            this.source = source;
            this.stream = stream;
        }

        byte[] minKey() {
            return stream.minKey();
        }

        byte[] maxKey() {
            return stream.maxKey();
        }

        PageBlock decodeCurrentPage() throws IOException {
            return stream.decodeCurrentPage();
        }

        void advance() throws IOException {
            stream.advance();
        }
    }

    /** A decoded page mid-emission in the key-merge fallback: its cursor plus the peeked head entry. */
    private static final class DecodedPage {
        private final int source;
        private final PageBlockCursor cursor;
        private long decodedBytes;
        private ListEntry head;

        DecodedPage(int source, PageBlockCursor cursor, int count, long decodedBytes) {
            this.source = source;
            this.cursor = cursor;
            this.decodedBytes = decodedBytes;
            this.head = cursor.hasNext() ? cursor.next() : null;   // a page always has >= 1 entry
            if (count < 1 || head == null) {
                throw new IllegalArgumentException("decoded page must contain at least one row");
            }
        }

        void advanceHead() {
            head = cursor.hasNext() ? cursor.next() : null;
        }

        void drainAndValidate() {
            cursor.drainAndValidate();
        }
    }
}
