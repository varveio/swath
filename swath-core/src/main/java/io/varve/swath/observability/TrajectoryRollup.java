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
    private final long[] workerSuccessCount;
    private final long[] workerKeysFetched;
    private final long[] workerLatencySampleCount;
    private final long[] workerLatencySumNanos;
    private final long[] workerLatencyMinNanos;
    private final long[] latencyInflatedCount;
    private final int[] aimdTargetMin;
    private final int[] aimdTargetMax;
    private final int[] aimdTargetLast;
    private final long[] latencyBaselineLastNanos;
    private final long[] latencyEwmaLastNanos;
    private long lastKeys = 0L;

    TrajectoryRollup(int bins) {
        this.bins = bins;
        this.inFlightArea = new double[bins];
        this.dtNanos = new long[bins];
        this.keysDelta = new long[bins];
        this.workerSuccessCount = new long[bins];
        this.workerKeysFetched = new long[bins];
        this.workerLatencySampleCount = new long[bins];
        this.workerLatencySumNanos = new long[bins];
        this.workerLatencyMinNanos = new long[bins];
        this.latencyInflatedCount = new long[bins];
        this.aimdTargetMin = new int[bins];
        this.aimdTargetMax = new int[bins];
        this.aimdTargetLast = new int[bins];
        this.latencyBaselineLastNanos = new long[bins];
        this.latencyEwmaLastNanos = new long[bins];
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
     * Record one successful slot-gated worker page at the same bounded time-bin resolution as the
     * in-flight trajectory. These are store-completion observations, deliberately upstream of
     * checkpointing, filtering and output; they are diagnostic only and never feed the controller.
     */
    void recordAimdWorkerSuccess(long elapsedNanos, int keysOnPage, long latencyNanos,
                                 int targetT, long baselineNanos, long ewmaNanos,
                                 boolean latencyInflated, boolean latencySampled) {
        if (elapsedNanos < 0L || keysOnPage < 0 || targetT <= 0) {
            return;
        }
        lock.lock();
        try {
            while (elapsedNanos / binWidthNanos >= bins) {
                mergeBinsForDoubling();
            }
            int idx = (int) Math.min(bins - 1, elapsedNanos / binWidthNanos);
            workerSuccessCount[idx]++;
            workerKeysFetched[idx] += keysOnPage;
            aimdTargetMin[idx] = aimdTargetMin[idx] == 0
                    ? targetT : Math.min(aimdTargetMin[idx], targetT);
            aimdTargetMax[idx] = Math.max(aimdTargetMax[idx], targetT);
            aimdTargetLast[idx] = targetT;
            latencyBaselineLastNanos[idx] = baselineNanos;
            latencyEwmaLastNanos[idx] = ewmaNanos;
            if (latencySampled && latencyNanos > 0L) {
                workerLatencySampleCount[idx]++;
                workerLatencySumNanos[idx] += latencyNanos;
                workerLatencyMinNanos[idx] = workerLatencyMinNanos[idx] == 0L
                        ? latencyNanos : Math.min(workerLatencyMinNanos[idx], latencyNanos);
                if (latencyInflated) {
                    latencyInflatedCount[idx]++;
                }
            }
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
            workerSuccessCount[i] = workerSuccessCount[2 * i] + workerSuccessCount[2 * i + 1];
            workerKeysFetched[i] = workerKeysFetched[2 * i] + workerKeysFetched[2 * i + 1];
            workerLatencySampleCount[i] =
                    workerLatencySampleCount[2 * i] + workerLatencySampleCount[2 * i + 1];
            workerLatencySumNanos[i] =
                    workerLatencySumNanos[2 * i] + workerLatencySumNanos[2 * i + 1];
            workerLatencyMinNanos[i] = minPresent(
                    workerLatencyMinNanos[2 * i], workerLatencyMinNanos[2 * i + 1]);
            latencyInflatedCount[i] = latencyInflatedCount[2 * i] + latencyInflatedCount[2 * i + 1];
            aimdTargetMin[i] = minPresent(aimdTargetMin[2 * i], aimdTargetMin[2 * i + 1]);
            aimdTargetMax[i] = Math.max(aimdTargetMax[2 * i], aimdTargetMax[2 * i + 1]);
            aimdTargetLast[i] = lastPresent(aimdTargetLast[2 * i], aimdTargetLast[2 * i + 1]);
            latencyBaselineLastNanos[i] = lastPresent(
                    latencyBaselineLastNanos[2 * i], latencyBaselineLastNanos[2 * i + 1]);
            latencyEwmaLastNanos[i] = lastPresent(
                    latencyEwmaLastNanos[2 * i], latencyEwmaLastNanos[2 * i + 1]);
        }
        for (int i = half; i < bins; i++) {
            inFlightArea[i] = 0.0;
            dtNanos[i] = 0L;
            keysDelta[i] = 0L;
            workerSuccessCount[i] = 0L;
            workerKeysFetched[i] = 0L;
            workerLatencySampleCount[i] = 0L;
            workerLatencySumNanos[i] = 0L;
            workerLatencyMinNanos[i] = 0L;
            latencyInflatedCount[i] = 0L;
            aimdTargetMin[i] = 0;
            aimdTargetMax[i] = 0;
            aimdTargetLast[i] = 0;
            latencyBaselineLastNanos[i] = 0L;
            latencyEwmaLastNanos[i] = 0L;
        }
        binWidthNanos *= 2;
        binsUsed = Math.min(bins, (binsUsed + 1) / 2);
    }

    private static int minPresent(int left, int right) {
        return left == 0 ? right : right == 0 ? left : Math.min(left, right);
    }

    private static long minPresent(long left, long right) {
        return left == 0L ? right : right == 0L ? left : Math.min(left, right);
    }

    private static int lastPresent(int left, int right) {
        return right != 0 ? right : left;
    }

    private static long lastPresent(long left, long right) {
        return right != 0L ? right : left;
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
        double[] workerPageRate;
        double[] workerKeyRate;
        double[] workerLatencyMinMs;
        double[] workerLatencyMeanMs;
        double[] targetMin;
        double[] targetMax;
        double[] targetLast;
        double[] baselineLastMs;
        double[] ewmaLastMs;
        double[] inflatedFrac;
        long[] dt;
        int used;
        lock.lock();
        try {
            used = binsUsed;
            inFlightAvg = new double[used];
            rate = new double[used];
            workerPageRate = new double[used];
            workerKeyRate = new double[used];
            workerLatencyMinMs = new double[used];
            workerLatencyMeanMs = new double[used];
            targetMin = new double[used];
            targetMax = new double[used];
            targetLast = new double[used];
            baselineLastMs = new double[used];
            ewmaLastMs = new double[used];
            inflatedFrac = new double[used];
            dt = new long[used];
            for (int i = 0; i < used; i++) {
                dt[i] = dtNanos[i];
                inFlightAvg[i] = dt[i] > 0L ? inFlightArea[i] / dt[i] : 0.0;
                rate[i] = dt[i] > 0L ? keysDelta[i] / (dt[i] / 1_000_000_000.0) : 0.0;
                double seconds = dt[i] / 1_000_000_000.0;
                workerPageRate[i] = seconds > 0.0 ? workerSuccessCount[i] / seconds : 0.0;
                workerKeyRate[i] = seconds > 0.0 ? workerKeysFetched[i] / seconds : 0.0;
                long latencySamples = workerLatencySampleCount[i];
                workerLatencyMinMs[i] = latencySamples > 0L
                        ? workerLatencyMinNanos[i] / 1_000_000.0 : -1.0;
                workerLatencyMeanMs[i] = latencySamples > 0L
                        ? workerLatencySumNanos[i] / (double) latencySamples / 1_000_000.0 : -1.0;
                targetMin[i] = aimdTargetMin[i] > 0 ? aimdTargetMin[i] : -1.0;
                targetMax[i] = aimdTargetMax[i] > 0 ? aimdTargetMax[i] : -1.0;
                targetLast[i] = aimdTargetLast[i] > 0 ? aimdTargetLast[i] : -1.0;
                baselineLastMs[i] = latencyBaselineLastNanos[i] > 0L
                        ? latencyBaselineLastNanos[i] / 1_000_000.0 : -1.0;
                ewmaLastMs[i] = latencyEwmaLastNanos[i] > 0L
                        ? latencyEwmaLastNanos[i] / 1_000_000.0 : -1.0;
                inflatedFrac[i] = latencySamples > 0L
                        ? latencyInflatedCount[i] / (double) latencySamples : -1.0;
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
                inFlightAvg, rate, workerPageRate, workerKeyRate, workerLatencyMinMs,
                workerLatencyMeanMs, targetMin, targetMax, targetLast, baselineLastMs,
                ewmaLastMs, inflatedFrac, serialFrac, collapseAtFrac,
                peakWorkers.getAsInt(), finalWorkers);
    }
}
