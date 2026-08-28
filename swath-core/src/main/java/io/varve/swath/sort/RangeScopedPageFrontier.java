/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.io.IOException;

/**
 * A range-scoped view of one page-run frontier: a
 * {@link PageFrontierStream} view of one page-run segment restricted to the pages that can hold a
 * key in {@code [lo, hi)}. Pages outside the range are stepped over <b>without decoding their
 * rows</b> — {@link #decodeCurrentPage()} is never called on them — which is what makes a range cost
 * a fraction of the segment instead of a full pass.
 *
 * <p><b>The overlap test is conservative, and correctness does not depend on it.</b> A page whose
 * frontier is {@code [min, max]} is kept iff {@code max >= lo} and {@code min < hi} (either bound
 * {@code null} = unbounded). Equality on {@code max == lo} keeps the page, because a key equal to
 * {@code lo} belongs to THIS range (inclusive lo). Every kept page is still trimmed per-row by
 * {@link RangeFilteredCursor} sitting above the whole range merge, so the skip is a pure performance
 * layer: being too generous costs time, and the only thing that would cost correctness — skipping a
 * page that could hold an in-range key — is exactly what the test above cannot do.
 *
 * <p><b>Why both sides can now be bounded.</b> Pages are stored in non-decreasing
 * {@code minKey} order ({@link PageRunSegmentWriter}), so once {@code min >= hi} every later page
 * also has {@code min >= hi}: the scan stops there and the rest of the segment is never read at all.
 * {@code maxKey}, by contrast, is NOT monotone, so a safe head seek uses the type-2 index's
 * monotone {@code prefixMax}, never a sampled page max or minimum. The selected entry remains an
 * untrusted hint: the reader verifies its next frame and the coordinator proves the complete
 * header-to-trailer physical-zone chain before publication. Legacy/type-1 inputs retain the header
 * start and therefore the former prefix walk.
 *
 * <p>The framed body is still read and CRC-verified for every page this range owns or steps over.
 * Pages physically bypassed by a seek belong to an earlier range's proof zone and are verified
 * there, so the optimization removes duplicate range reads without adding a separate integrity pass.
 *
 * <p><b>Whole-input proof.</b> Pre-worker planning supplies every zone bound, including repeated
 * starts as explicit empty zones. This frontier reports the actual pages, entries, framed bytes,
 * minima, maxima, and index samples in its owned interval. The coordinator checks all ranges tile
 * each segment exactly and match its trailer before returning their still-open writers; a mismatch
 * closes writers and sweeps temporary output through the ordinary failure path.
 */
final class RangeScopedPageFrontier implements PageFrontierStream {

    private final PageFrontierReader inner;
    private final byte[] lo;   // inclusive, or null for -inf
    private final byte[] hi;   // exclusive, or null for +inf

    /**
     * Ticked once per page stepped over in {@link #skipToOverlapping()}.
     *
     * <p>The prefix walk reads and CRC-verifies roughly {@code r/R} of the staged bytes for range
     * {@code r} before the merge emits its first row, and it emitted NOTHING while doing so. The
     * liveness watchdog's total-freeze tripwire is a 120 s default, so on a billion-object listing
     * this phase alone halted the JVM — the run was healthy and the signal was simply absent. The
     * serial merge has no equivalent phase (it opens a bare frontier and reads one page per
     * segment), which is why this only ever bit the parallel path.
     */
    private final SortMetrics metrics;

    /** Pages in the whole segment (trailer {@code totalRecords}) — the denominator for the signal. */
    private final long totalPages;
    private final long startOrdinal;
    private final PageRunZoneVerifier.Tracker zoneTracker;

    /** Pages whose rows this range may decode, and pages read-then-stepped-over. */
    private long pagesKept;
    private long pagesSkipped;

    /** True once the scan has passed {@code hi}; the underlying stream is left un-drained. */
    private boolean pastRange;

    RangeScopedPageFrontier(PageFrontierReader inner, byte[] lo, byte[] hi, long totalPages,
                            long startOrdinal, SortMetrics metrics,
                            PageRunZoneVerifier.Tracker zoneTracker) throws IOException {
        this.inner = inner;
        this.lo = lo;
        this.hi = hi;
        this.totalPages = totalPages;
        this.startOrdinal = startOrdinal;
        this.metrics = metrics;
        this.zoneTracker = zoneTracker;
        try {
            observeLoadedPage();
            skipToOverlapping();
        } catch (IOException | RuntimeException e) {
            // The constructor does IO (it walks the prefix), so a corrupt/truncated segment throws
            // here — before the caller holds a reference it could close. Own the failure like every
            // other reader in this package and release the stream we were handed.
            try {
                inner.close();
            } catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    /** Step over non-overlapping pages until the frontier holds one this range can use, or it ends. */
    private void skipToOverlapping() throws IOException {
        while (inner.hasPage()) {
            if (beyondRange()) {
                // mins are non-decreasing: no later page can overlap either. Stop reading the segment.
                // This page WAS read (the frontier loaded and CRC-verified it before we could compare
                // its min), so it counts as skipped, not unread -- otherwise a segment lying wholly
                // above hi would report a page as never-read despite having been read in full.
                pagesSkipped++;
                // Read and CRC-verified like any other, so it owes the watchdog a tick too. One page
                // cannot move the 120 s window on its own, but a skipped tick on the path that
                // ACCOUNTS for the page is the same class of gap this change exists to close.
                metrics.markProgress();
                pastRange = true;
                return;
            }
            if (overlaps()) {
                pagesKept++;
                return;
            }
            pagesSkipped++;
            // Every stepped-over page is a full read plus a CRC32C verify, so this is real work and
            // the watchdog is entitled to see it. Ticking per page rather than per batch costs an
            // AtomicLong increment against an I/O-bound loop, and removes any question of whether the
            // cadence clears the stall window.
            metrics.markProgress();
            // ...and this walk must be abandonable. When one range fails, the coordinator cancels its
            // siblings and joins them before cleanup; a sibling that polls nothing walks its whole
            // prefix first, so the join outlives the stall window, the watchdog re-traps the run, and
            // the ORIGINAL classified failure is replaced by stuck_unknown. Polling here is what makes
            // that cancel cooperative -- the constructor already closes the stream on the way out.
            MergeCancellation.check();
            inner.advance();
            observeLoadedPage();
        }
    }

    private void observeLoadedPage() throws IOException {
        if (zoneTracker == null) {
            return;
        }
        if (inner.hasPage()) {
            zoneTracker.observe(inner.currentPosition(), inner.minKey(), inner.maxKey(), inner.count());
        } else {
            zoneTracker.exhausted(inner.nextFrameOffset());
        }
    }

    /** {@code min >= hi}: this page and every later one start at or above the range's end. */
    private boolean beyondRange() {
        return hi != null && KeyBytes.compareUnsigned(inner.minKey(), hi) >= 0;
    }

    /** {@code max >= lo}: the page can still reach into this range (its min is already {@code < hi}). */
    private boolean overlaps() {
        return lo == null || KeyBytes.compareUnsigned(inner.maxKey(), lo) >= 0;
    }

    @Override
    public boolean hasPage() {
        return !pastRange && inner.hasPage();
    }

    @Override
    public byte[] minKey() {
        return inner.minKey();
    }

    @Override
    public byte[] maxKey() {
        return inner.maxKey();
    }

    @Override
    public int count() {
        return inner.count();
    }

    @Override
    public PageBlock decodeCurrentPage() throws IOException {
        return inner.decodeCurrentPage();
    }

    @Override
    public void advance() throws IOException {
        if (pastRange) {
            return;
        }
        inner.advance();
        observeLoadedPage();
        skipToOverlapping();
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            inner.close();
        } catch (IOException e) {
            failure = e;
        } finally {
            if (zoneTracker != null) {
                zoneTracker.finish();
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    long pagesKept() {
        return pagesKept;
    }

    /** Pages this range READ and stepped over — the below-{@code lo} prefix. */
    long pagesSkipped() {
        return pagesSkipped;
    }

    /**
     * Pages this range never read at all, because the scan stopped at {@code hi} (see
     * {@link #beyondRange()}). This is the LARGER saving and, for range 0 ({@code lo == null}, which
     * makes {@link #overlaps()} always true), the ONLY one — so a signal that counted just
     * {@link #pagesSkipped()} would report range 0 as having skipped nothing while it in fact read
     * roughly {@code 1/R} of each segment, and post-analysis could not tell whether the skip helped.
     */
    long pagesUnread() {
        return Math.max(0, totalPages - startOrdinal - pagesKept - pagesSkipped);
    }

    long pagesSeekedOver() {
        return startOrdinal;
    }

    long bytesRead() {
        return inner.framedBytesRead();
    }
}
