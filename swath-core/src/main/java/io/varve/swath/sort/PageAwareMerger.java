/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Page-aware k-way merge of N page-run segments into one sorted {@link SortedCursor} —
 * selected for ANY merge group, whether an intermediate cascade pass or the final pass, whenever
 * every input exposes a {@link PageFrontierStream} (page-run staging / cascade intermediates); a
 * mixed or Parquet input keeps the entry-typed {@link StreamingMerger}. Output is byte-identical, entry-for-entry, to the {@link StreamingMerger}
 * on the same input (same sorted order, same {@link DuplicateHook} firing on adjacent equals).
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
 * next: it is decoded once and streamed in order, firing {@code SORT.page_whole_emitted}.
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
 * overlaps the active region into the merge as needed, and fires {@code SORT.page_overlap_keymerge}.
 * On a well-formed OBJECTS run every page is range-disjoint, so this counter is 0 — a nonzero value is
 * a loud invariant alarm (mis-ordered/overlapping pages), never a silent misorder. Correctness beats
 * the optimization: a page is emitted whole only when strictly {@code maxKey <} every other {@code
 * minKey}; any equal-key boundary, version tiebreak, or ambiguity falls back to the key-merge.
 */
final class PageAwareMerger implements SortedCursor {

    /**
     * The CANONICAL values of this merger's two engagement counters ({@code SORT} category).
     *
     * <p>{@link #plan()} does NOT emit these constants — it passes the same values as inline string
     * literals, because {@code scripts/ci/check-instrumentation-drift.sh} statically reconciles every
     * {@code recordStealReason} call site against the §5 registry table and can only resolve literals.
     * So the literal is the emitter and this is the name others match against; the two are duplicated
     * BY NECESSITY.
     *
     * <p>They exist because {@link PageRunSegmentReader} runs this same merger over a SINGLE segment to
     * make its entry stream a genuinely sorted run and re-labels these two reasons to its own
     * route's counters — the cross-segment merge signal and the intra-segment read signal must stay
     * distinguishable post-hoc (a nonzero {@code page_overlap_keymerge} means the MERGE hit interleaved
     * pages across its inputs, which is an invariant alarm; expected 0 in production).
     *
     * <p>That re-labelling is an {@code equals} match against these constants, so if a literal in
     * {@code plan()} ever drifts from the constant beside it, the re-label would SILENTLY stop matching
     * and the counter would break with nothing failing. {@code PageAwareMergerCounterNamesTest} pins the
     * two together behaviourally — it asserts the reasons this merger actually emits equal these
     * constants. Change one, change both, or that test goes red.
     */
    static final String WHOLE_PAGE_EMITTED = "page_whole_emitted";
    static final String OVERLAP_KEYMERGE = "page_overlap_keymerge";

    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final List<PageFrontierStream> allStreams;

    /** Segments whose CURRENT (undecoded) page is still a frontier candidate, keyed by unsigned minKey. */
    private final PriorityQueue<Seg> frontier;
    /** Decoded pages currently being key-merged (overlap fallback), keyed by head entry. */
    private final PriorityQueue<DecodedPage> active;
    /** Monotone upper bound (max page maxKey) over the current key-merge event; null when active is empty. */
    private byte[] ceiling;

    /** A single decoded page being streamed whole (fast path); null otherwise. */
    private PageBlock.Cursor wholeCursor;

    private ListEntry previousEmitted;
    private ListEntry pending;
    private boolean closed;

    PageAwareMerger(List<PageFrontierStream> streams, Comparator<ListEntry> comparator,
                    DuplicateHook hook, SortMetrics metrics) {
        this.comparator = comparator;
        this.hook = hook;
        this.metrics = metrics;
        this.allStreams = streams;
        this.frontier = new PriorityQueue<>((a, b) -> Arrays.compareUnsigned(a.minKey(), b.minKey()));
        this.active = new PriorityQueue<>((a, b) -> comparator.compare(a.head, b.head));
        for (PageFrontierStream s : streams) {
            if (s.hasPage()) {
                frontier.add(new Seg(s));
            }
        }
        // computeNext() decodes the first page group and can throw (CRC/magic mismatch); if it does,
        // the object never finishes constructing, so close() is unreachable — release every already-open
        // frontier stream here rather than leaking them (mirrors PageFrontierReader's constructor guard).
        try {
            this.pending = computeNext();
        } catch (RuntimeException e) {
            close();
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
                // (1) Streaming a whole page (fast path)?
                if (wholeCursor != null) {
                    if (wholeCursor.hasNext()) {
                        return emit(wholeCursor.next());
                    }
                    wholeCursor = null;
                }
                // (2) Key-merging overlapping pages?
                if (!active.isEmpty()) {
                    closeActiveUnderFrontier();
                    DecodedPage dp = active.poll();
                    ListEntry e = dp.head;
                    dp.advanceHead();
                    if (dp.head != null) {
                        active.add(dp);
                    }
                    if (active.isEmpty()) {
                        ceiling = null;   // event resolved; the next plan() starts a fresh one
                    }
                    return emit(e);
                }
                // (3) Plan the next page group.
                if (frontier.isEmpty()) {
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
        m.advance();
        if (m.stream.hasPage()) {
            frontier.add(m);   // m's NEXT page re-enters contention (and the intra-segment check below)
        }

        // Whole iff NOTHING remaining in the frontier — neither another segment's current page nor m's
        // own next page — starts at/inside this page's [.., pageMax] range (strict, unsigned).
        boolean whole = frontier.isEmpty()
                || Arrays.compareUnsigned(frontier.peek().minKey(), pageMax) > 0;

        if (whole) {
            metrics.recordStealReason("SORT", "page_whole_emitted");   // literal: the CI drift guard resolves only literals
            wholeCursor = page.cursor();
        } else {
            metrics.recordStealReason("SORT", "page_overlap_keymerge");   // literal: the CI drift guard resolves only literals
            active.add(new DecodedPage(page.cursor()));
            ceiling = pageMax;
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
            f.advance();
            if (f.stream.hasPage()) {
                frontier.add(f);
            }
            active.add(new DecodedPage(page.cursor()));
            if (Arrays.compareUnsigned(fMax, ceiling) > 0) {
                ceiling = fMax;
            }
        }
    }

    private ListEntry emit(ListEntry e) {
        if (previousEmitted != null && comparator.compare(previousEmitted, e) == 0) {
            hook.onDuplicate(previousEmitted, e);
        }
        previousEmitted = e;
        return e;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        List<IOException> failures = new ArrayList<>();
        for (PageFrontierStream s : allStreams) {
            try {
                s.close();
            } catch (IOException e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            UncheckedIOException wrapped =
                    new UncheckedIOException("closing page frontier streams failed", failures.get(0));
            for (int i = 1; i < failures.size(); i++) {
                wrapped.addSuppressed(failures.get(i));
            }
            throw wrapped;
        }
    }

    /** One page-run segment at its current (undecoded) frontier page. Only mutated while OUT of the heap. */
    private static final class Seg {
        private final PageFrontierStream stream;

        Seg(PageFrontierStream stream) {
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
        private final PageBlock.Cursor cursor;
        private ListEntry head;

        DecodedPage(PageBlock.Cursor cursor) {
            this.cursor = cursor;
            this.head = cursor.hasNext() ? cursor.next() : null;   // a page always has >= 1 entry
        }

        void advanceHead() {
            head = cursor.hasNext() ? cursor.next() : null;
        }
    }
}
