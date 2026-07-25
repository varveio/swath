/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * Disengage-on-low-adoption gate for {@link SpeculativeReadahead}, owned by a single engaged
 * {@link RangeScanner#runRange} range. Tracks the recent per-page adoption outcome — was the next
 * contiguous page served from a speculative (concurrent) fetch, or did the scanner fall back to a
 * plain serial fetch ({@code guess_gap}) — over a fixed <b>tumbling</b> window of pages, and reports
 * when the adopted fraction over the last full window fell at/below a floor.
 *
 * <p>This is the surgical guard for a range that <i>engaged</i> (drained {@code engageAfterFullPages}
 * full serial pages) but whose density-reflected guesses are not paying off — the signature: ~25%
 * adoption with heavy {@code discarded_overlap} — readahead there is pure overhead (extra LIST calls,
 * RSS) while the plain serial scan would have absorbed the dense stretch fine. When
 * the window's adopted fraction is at/below {@link #minAdoption} the scanner disengages speculation and
 * reverts to serial; a genuinely sustained drain that resumes can re-engage on a fresh streak.
 *
 * <p>The window is <b>tumbling</b> (reset every {@code window} pages), not sliding: a single boolean
 * verdict is latched at each window boundary and the counters reset, so evaluation is O(1) per page and
 * carries no stale history across a boundary. {@code window <= 0} disables the gate entirely (the
 * verdict is never set).
 *
 * <p>Not thread-safe: every method is called on the owning worker thread inside {@code runRange}.
 */
final class AdoptionWindow {

    private final int window;
    private final double minAdoption;

    private int pages;
    private int adopted;
    private boolean belowFloor;

    AdoptionWindow(int window, double minAdoption) {
        this.window = window;
        this.minAdoption = minAdoption;
    }

    /**
     * Record one engaged-range page outcome. {@code adoptedPage} is {@code true} when the page was
     * served from speculation (an {@code adopted_page}), {@code false} when the scanner fell back to a
     * serial fetch ({@code guess_gap}). At each {@code window}-page boundary the adopted fraction is
     * evaluated once and the counters reset.
     */
    void record(boolean adoptedPage) {
        if (window <= 0) {
            return;   // gate disabled
        }
        pages++;
        if (adoptedPage) {
            adopted++;
        }
        if (pages >= window) {
            belowFloor = adopted <= minAdoption * window;
            pages = 0;
            adopted = 0;
        }
    }

    /**
     * {@code true} once the most recently completed window's adopted fraction was at/below the floor —
     * the range should disengage speculation. Stays {@code false} until the first full window completes.
     */
    boolean shouldDisengage() {
        return belowFloor;
    }
}
