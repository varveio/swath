/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * Configuration for intra-range speculative readahead, threaded into {@link RangeScanner}. When
 * {@link #enabled()} is false (the shipped default — the {@code readahead} engine toggle is opt-in),
 * {@link RangeScanner} behaves as pure serial pagination, with zero {@code READAHEAD.*}
 * counters. See {@link SpeculativeReadahead} for the mechanism and {@code algorithms.md} §3.4 for
 * the engagement/tuning rationale behind every default below.
 *
 * @param enabled          whether readahead may engage at all (the {@code readahead} toggle)
 * @param k                the number of concurrent guess-ahead cursors. Default {@value #DEFAULT_K}
 * @param maxBufferedPages the adoption-buffer cap in pages (bounded memory; in-flight + buffered ≤ K
 *                         and buffered ≤ this)
 * @param engageAfterFullPages consecutive full, truncated, un-split serial pages a range must drain
 *                         before readahead engages. Default {@value #DEFAULT_ENGAGE_AFTER_FULL_PAGES};
 *                         see {@link #DEFAULT_ENGAGE_AFTER_FULL_PAGES} before raising it
 * @param disengageWindow  the tumbling window, counted in <i>scanner pages processed while
 *                         engaged</i>, over which the adopted-fraction is evaluated; below {@link
 *                         #disengageMinAdoption} the range disengages and reverts to plain serial (a
 *                         later sustained drain can re-engage). Default {@value
 *                         #DEFAULT_DISENGAGE_WINDOW}. {@code <= 0} disables disengagement
 * @param disengageMinAdoption the adopted-fraction floor (adopted / window pages) at or below which
 *                         an engaged range disengages. Default {@value #DEFAULT_DISENGAGE_MIN_ADOPTION}
 * @param minEngageRemainingPages <b>disabled by default</b> ({@value
 *                         #DEFAULT_MIN_ENGAGE_REMAINING_PAGES}): the estimated remaining pages
 *                         {@code (cursor, hi]} must exceed for the range to engage at all, via {@code
 *                         StealMath#estRemaining} scoped to the current streak's own start key.
 *                         Deferred engagements count {@code READAHEAD.engage_deferred_est_remaining}
 *                         (the streak is not reset). Retained for explicit-config use in tests; see
 *                         {@link #DEFAULT_MIN_ENGAGE_REMAINING_PAGES} for why it stays off by default
 * @param placementFactor  reserved placement-tuning knob (advisory). Guess spacing is
 *                         density-reflected off the observed one-page key span and needs no spacing
 *                         fraction; retained for forward compatibility
 * @param fetchBudgetMillis the wall-clock budget — covering every internal retry — a single
 *                         speculative fetch through the isolated off-gauge fetcher may run before
 *                         {@link SpeculativeReadahead} cancels it as a {@code speculative_fault}.
 *                         Default {@value #DEFAULT_FETCH_BUDGET_MILLIS}
 */
public record ReadaheadConfig(
        boolean enabled,
        int k,
        int maxBufferedPages,
        int engageAfterFullPages,
        double placementFactor,
        long fetchBudgetMillis,
        int disengageWindow,
        double disengageMinAdoption,
        double minEngageRemainingPages) {

    /** Default {@link #k} — measured a 7.3× aggregate-pages/s win on a real dense tail. */
    public static final int DEFAULT_K = 8;
    /**
     * Default {@link #engageAfterFullPages}. {@link RangeScanner} resets the streak to 0 whenever
     * {@code hi} is narrowed by a steal or self-split. Do not raise this to reduce engagement count:
     * under normal work-stealing an owner is narrowed frequently, so a longer streak requirement
     * makes engagement rare for every bucket rather than targeting collapsed tails alone. See
     * {@code algorithms.md} §3.4.
     */
    public static final int DEFAULT_ENGAGE_AFTER_FULL_PAGES = 6;
    /**
     * Default {@link #disengageWindow}: {@code 2 × DEFAULT_K} — two replenish cohorts' worth of
     * adoption outcomes, long enough to be statistically meaningful and short enough to render a
     * verdict within a typical engagement. See {@code algorithms.md} §3.4.
     */
    public static final int DEFAULT_DISENGAGE_WINDOW = 2 * DEFAULT_K;
    /**
     * Default {@link #disengageMinAdoption}, sitting with wide margin between a transient stretch's
     * adoption rate (~0.25) and a healthy sustained drain's (~0.7–0.8). See {@code algorithms.md} §3.4.
     */
    public static final double DEFAULT_DISENGAGE_MIN_ADOPTION = 0.40;
    /**
     * Default {@link #minEngageRemainingPages} — disabled. See {@code algorithms.md} §3.4 for why
     * this floor stays off by default; the duration discipline in force is {@link
     * #DEFAULT_ENGAGE_AFTER_FULL_PAGES}.
     */
    public static final double DEFAULT_MIN_ENGAGE_REMAINING_PAGES = 0.0;
    /** Guess spacing as a fraction of one observed page (biased below the boundary → overlap-trim). */
    public static final double DEFAULT_PLACEMENT_FACTOR = 0.95;
    /**
     * Default {@link #fetchBudgetMillis} — comfortably above a healthy RTT and the worker's own
     * per-attempt timeout plus a couple of backoff retries, so a recovering fetch isn't discarded
     * prematurely while a persistent throttle storm still costs only a small fixed bound per guess.
     */
    public static final long DEFAULT_FETCH_BUDGET_MILLIS = 30_000L;

    /** Readahead off — the shipped default; {@link RangeScanner} stays purely serial. */
    public static ReadaheadConfig off() {
        return new ReadaheadConfig(false, DEFAULT_K, DEFAULT_K, DEFAULT_ENGAGE_AFTER_FULL_PAGES,
                DEFAULT_PLACEMENT_FACTOR, DEFAULT_FETCH_BUDGET_MILLIS,
                DEFAULT_DISENGAGE_WINDOW, DEFAULT_DISENGAGE_MIN_ADOPTION, DEFAULT_MIN_ENGAGE_REMAINING_PAGES);
    }

    /** Readahead on with the default {@code K}, buffer cap {@code K}, and placement tuning. */
    public static ReadaheadConfig on() {
        return new ReadaheadConfig(true, DEFAULT_K, DEFAULT_K, DEFAULT_ENGAGE_AFTER_FULL_PAGES,
                DEFAULT_PLACEMENT_FACTOR, DEFAULT_FETCH_BUDGET_MILLIS,
                DEFAULT_DISENGAGE_WINDOW, DEFAULT_DISENGAGE_MIN_ADOPTION, DEFAULT_MIN_ENGAGE_REMAINING_PAGES);
    }

    /** Readahead on with an explicit {@code K} (buffer cap = {@code K}) — for tests/tuning. */
    public static ReadaheadConfig onWithK(int k) {
        return new ReadaheadConfig(true, k, k, DEFAULT_ENGAGE_AFTER_FULL_PAGES, DEFAULT_PLACEMENT_FACTOR,
                DEFAULT_FETCH_BUDGET_MILLIS, DEFAULT_DISENGAGE_WINDOW, DEFAULT_DISENGAGE_MIN_ADOPTION,
                DEFAULT_MIN_ENGAGE_REMAINING_PAGES);
    }

    /** Readahead on with an explicit {@code K} and fetch budget — for tests exercising the budget. */
    public static ReadaheadConfig onWithKAndBudget(int k, long fetchBudgetMillis) {
        return new ReadaheadConfig(true, k, k, DEFAULT_ENGAGE_AFTER_FULL_PAGES, DEFAULT_PLACEMENT_FACTOR,
                fetchBudgetMillis, DEFAULT_DISENGAGE_WINDOW, DEFAULT_DISENGAGE_MIN_ADOPTION,
                DEFAULT_MIN_ENGAGE_REMAINING_PAGES);
    }
}
