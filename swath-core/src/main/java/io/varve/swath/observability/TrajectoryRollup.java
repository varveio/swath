/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;

/**
 * A fixed-size ({@code bins}) time-bin rollup of in-flight concurrency + progress rate, folded on the
 * SAME "sample on every transition" seam {@link RunMetrics#incrementInFlight()}/{@link
 * RunMetrics#decrementInFlight()} use (pure observation — no extra store/API call). Bounded memory
 * regardless of run length: once the run's elapsed time would need more bins than {@code bins} at the
 * CURRENT bin width, every adjacent pair of bins is merged (halving the live bin count) and the bin
 * width doubles — the classic ring/doubling-bucket downsample. A window between two transitions is
 * attributed to the bin containing the window's END instant (a documented approximation — this is a
 * diagnostic rollup, not a correctness measurement).
 *
 * <p>Extracted from {@code RunMetrics}. The bin count ({@link #TRAJECTORY_BINS}) now lives here
 * (relocated); {@code RunMetrics} keeps a delegating alias for its own pinned-surface callers. The
 * width/threshold constants below are this rollup's own.
 */
final class TrajectoryRollup {

    /** Fixed bin count — this rollup's own dimension constant (relocated from {@code RunMetrics}). */
    static final int TRAJECTORY_BINS = 30;

    private static final long INITIAL_BIN_NANOS = 1_000_000_000L;   // 1s
    /** "serial" for {@code serial_frac}/{@code collapse_at_frac}: at or below this many in-flight. */
    private static final double SERIAL_THRESHOLD = 2.0;

    private final int bins;
    private final ReentrantLock lock = new ReentrantLock();
    private long binWidthNanos = INITIAL_BIN_NANOS;
    private int binsUsed = 0;
    private final double[] inFlightArea;
    private final long[] dtNanos;
    private final long[] keysDelta;
    private long lastKeys = 0L;

    TrajectoryRollup(int bins) {
        this.bins = bins;
        this.inFlightArea = new double[bins];
        this.dtNanos = new long[bins];
        this.keysDelta = new long[bins];
    }

    /**
     * Fold one in-flight-transition window into the fixed-size trajectory rollup — see the class
     * javadoc for the bounded ring/doubling-bucket scheme. Best-effort under contention: if this call
     * races {@link RunMetrics#markRunStarted()} resetting the run clock (a negative {@code
     * elapsedNanos}) it is simply dropped, never a crash — this is a diagnostic rollup, not a
     * correctness path. The zero-work early-return guard below runs BEFORE the lock is even attempted,
     * so a benign/degenerate call never pays for the acquisition at all.
     *
     * <p><b>A global {@link ReentrantLock} on every in-flight transition (two per LIST call — the
     * increment at dispatch and the decrement at completion — across ALL workers) is deliberately left
     * as a plain lock, not a lock-free
     * rewrite.</b> The contention arithmetic: even a wide run (W ≈ 64–200 workers) at a healthy
     * per-worker page rate (~10 pages/s, i.e. ~20 in-flight transitions/s each) is only ≈ 1,280–4,000
     * lock ACQUISITIONS per second in
     * aggregate — many orders of magnitude below where a {@code ReentrantLock} becomes a measurable
     * bottleneck (single-digit millions/sec uncontended on any modern JVM). Each critical section is
     * O(1) (a handful of array/field writes) except the rare bin-doubling case
     * ({@link #mergeBinsForDoubling}, O(30) array copies), which itself only fires
     * O(log(run-duration)) times over the whole run's lifetime (each doubling covers 2x the wall-clock
     * span the last one did). This is nowhere near hot enough to justify a lock-free bin-doubling
     * rewrite's added complexity for a purely diagnostic rollup — simplicity wins here.
     */
    void record(long elapsedNanos, long windowNanos, long valueDuringWindow, long keysNow) {
        if (elapsedNanos < 0L || windowNanos <= 0L) {
            return;
        }
        lock.lock();
        try {
            while (elapsedNanos / binWidthNanos >= bins) {
                mergeBinsForDoubling();
            }
            int idx = (int) Math.min(bins - 1, elapsedNanos / binWidthNanos);
            inFlightArea[idx] += (double) valueDuringWindow * windowNanos;
            dtNanos[idx] += windowNanos;
            keysDelta[idx] += Math.max(0L, keysNow - lastKeys);
            lastKeys = Math.max(lastKeys, keysNow);
            binsUsed = Math.max(binsUsed, idx + 1);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Halves the live bin count by merging adjacent pairs (bin {@code i} = old bins {@code 2i}/
     * {@code 2i+1}) and doubles the bin width — the classic ring-doubling downsample that keeps the
     * trajectory rollup at a fixed {@code bins}-bin memory footprint no matter how long the run runs.
     * Always called with {@link #lock} already held.
     */
    private void mergeBinsForDoubling() {
        int half = bins / 2;
        for (int i = 0; i < half; i++) {
            inFlightArea[i] = inFlightArea[2 * i] + inFlightArea[2 * i + 1];
            dtNanos[i] = dtNanos[2 * i] + dtNanos[2 * i + 1];
            keysDelta[i] = keysDelta[2 * i] + keysDelta[2 * i + 1];
        }
        for (int i = half; i < bins; i++) {
            inFlightArea[i] = 0.0;
            dtNanos[i] = 0L;
            keysDelta[i] = 0L;
        }
        binWidthNanos *= 2;
        binsUsed = Math.min(bins, (binsUsed + 1) / 2);
    }

    /**
     * Assembles the {@code trajectory} JSON block from the bounded bin rollup — the per-bin
     * {@code inFlight}/{@code progressRate} arrays (one entry per bin actually used, never
     * zero-padded) plus the derived scalars: {@code serialFrac} (fraction of total wall-time at
     * {@code <= SERIAL_THRESHOLD} in-flight), {@code collapseAtFrac} (the fractional bin index where
     * the run's TRAILING low-concurrency run began — {@code -1.0} if the run never permanently
     * collapsed, i.e. its last bin is still above the threshold), {@code peakWorkers} (passed in as a
     * SUPPLIER over {@link RunMetrics#peakInFlight()}, reused not duplicated, and read at the
     * record-construction point AFTER the locked bin snapshot + scalar derivation — the same sequence
     * point the facade's pre-extraction code read it), and {@code finalWorkers} (the last bin's average
     * in-flight, rounded).
     */
    RunSummary.TrajectorySummary buildSummary(IntSupplier peakWorkers) {
        double[] inFlightAvg;
        double[] rate;
        long[] dt;
        int used;
        lock.lock();
        try {
            used = binsUsed;
            inFlightAvg = new double[used];
            rate = new double[used];
            dt = new long[used];
            for (int i = 0; i < used; i++) {
                dt[i] = dtNanos[i];
                inFlightAvg[i] = dt[i] > 0L ? inFlightArea[i] / dt[i] : 0.0;
                rate[i] = dt[i] > 0L ? keysDelta[i] / (dt[i] / 1_000_000_000.0) : 0.0;
            }
        } finally {
            lock.unlock();
        }
        long totalDt = 0L;
        long serialDt = 0L;
        for (int i = 0; i < used; i++) {
            totalDt += dt[i];
            if (inFlightAvg[i] <= SERIAL_THRESHOLD) {
                serialDt += dt[i];
            }
        }
        double serialFrac = totalDt > 0L ? (double) serialDt / totalDt : 0.0;
        int collapseIdx = used;   // sentinel: never collapsed
        for (int i = used - 1; i >= 0; i--) {
            if (inFlightAvg[i] <= SERIAL_THRESHOLD) {
                collapseIdx = i;
            } else {
                break;
            }
        }
        double collapseAtFrac = (collapseIdx >= used || used == 0) ? -1.0 : (double) collapseIdx / used;
        int finalWorkers = used > 0 ? (int) Math.round(inFlightAvg[used - 1]) : 0;
        return new RunSummary.TrajectorySummary(
                inFlightAvg, rate, serialFrac, collapseAtFrac, peakWorkers.getAsInt(), finalWorkers);
    }
}
