/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded-memory sampler of {@code (cumulative keys emitted, elapsed wall nanos, in-flight
 * count)} triples, folded on the SAME already-serialized point {@link
 * RunMetrics#recordEntriesEmitted(long)} uses — the single consumer-stage thread's per-page
 * cumulative-keys-emitted bump (see that method's javadoc: exactly one consumer stage runs per
 * run). This buys the tail-occupancy metric its correctness for free: no extra locking beyond
 * what that already-serialized call site needs for a cross-thread-visible snapshot, and no
 * per-key work (one sample attempt per PAGE, not per key).
 *
 * <p><b>Why this exists.</b> No whole-run average can screen for a serial tail: a bucket whose
 * final slice of keys drains on a single worker while the rest of the run stayed wide can still
 * report a healthy-looking lifetime {@code avg_in_flight} (a real run averaged 11.3 in-flight
 * while its last ~45% of keys drained on ONE worker). This sampler answers a narrower, END-of-run
 * question instead: what did concurrency and wall-time look like specifically over the window in
 * which the LAST N% of the run's keys were emitted — see {@link RunMetrics}'s {@code
 * swath.tail_occupancy.avg_in_flight}/{@code swath.tail_occupancy.wall_share} gauges (tagged
 * {@code pct=5|10}).
 *
 * <p><b>Bounded memory via stride-gated decimation, not reservoir sampling.</b> Total emits are
 * unknown until the run ends, so the two windows are derived POST-HOC from bounded samples rather
 * than tracked live: every {@code stride}-th call to {@link #record} is kept, into a fixed {@code
 * capacity}-slot ring. Once the ring fills, it is compacted by deterministically dropping every
 * other sample (indices {@code 0, 2, 4, ...} survive) and the stride doubles — the same
 * ring/doubling-bucket downsample {@link TrajectoryRollup} uses for its time bins, applied here to
 * a flat sample buffer instead. No randomness, fully deterministic given the call sequence.
 * Precision from up to {@code capacity} samples is an approximation (documented, not a
 * correctness measurement): the window boundary is the first surviving sample whose cumulative
 * emit count reaches the window's start, never interpolated between two samples, and the
 * in-flight average over the window is an unweighted mean of the samples that fall inside it
 * (not time-weighted, unlike {@link TrajectoryRollup}'s bins) — adequate for screening a serial
 * tail, not for a precision measurement.
 *
 * <p><b>Resume semantics: the windows are defined over THIS process's listed keys, not the
 * checkpoint's whole-bucket total.</b> A {@code --sort --resume} reattach backfills {@code
 * swath.entries.emitted} with the pre-crash durable row count ({@code
 * RunMetrics#recordRecoveredObjects}) before this process lists a single key of its own — without
 * accounting for that jump, {@code totalEmitted} would already sit near the checkpoint's total the
 * instant the first REAL sample lands, collapsing both windows onto this process's entire (usually
 * short) relisted span and reporting a wall share near 1.0 regardless of {@code pct}. {@link
 * #recordBaseline} folds that backfilled count into {@code baseline} instead, and every window is
 * derived over {@code totalListed = totalEmitted - baseline} — i.e. the honest, THIS-process-only
 * denominator for a screening instrument that can only ever sample what it itself observed.
 */
final class TailOccupancySampler {

    /** Fixed sample-buffer capacity — plenty of precision for a screening signal (documented above). */
    static final int DEFAULT_CAPACITY = 4096;

    /** The two reporting windows this metric is defined over — the LAST 5% and LAST 10% of emitted keys. */
    static final int[] WINDOW_PERCENTS = {5, 10};

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final long[] emitIndex;
    private final long[] elapsedNanos;
    private final int[] inFlight;
    private int count;
    private long stride = 1L;
    private long callsSeen;
    /** Backfilled keys folded in by {@link #recordBaseline} — see the class javadoc's resume note. */
    private long baseline;

    TailOccupancySampler(int capacity) {
        this.capacity = capacity;
        this.emitIndex = new long[capacity];
        this.elapsedNanos = new long[capacity];
        this.inFlight = new int[capacity];
    }

    /**
     * Stride-gate one candidate sample. {@code elapsedNanosNow} negative (a call that races {@link
     * RunMetrics#markRunStarted()} resetting the run clock) is dropped, never recorded — same
     * best-effort discipline as {@link TrajectoryRollup#record}: this is a diagnostic sampler, not a
     * correctness path.
     */
    void record(long emitIndexNow, long elapsedNanosNow, int inFlightNow) {
        if (elapsedNanosNow < 0L) {
            return;
        }
        lock.lock();
        try {
            long seq = callsSeen++;
            if (seq % stride != 0L) {
                return;
            }
            if (count == capacity) {
                compactAndDoubleStride();
            }
            emitIndex[count] = emitIndexNow;
            elapsedNanos[count] = elapsedNanosNow;
            inFlight[count] = inFlightNow;
            count++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Folds a {@code --sort --resume} reattach's backfilled row count into {@link #baseline} — see
     * the class javadoc's resume-semantics note. Called from {@code
     * RunMetrics#recordRecoveredObjects} alongside its own {@code entries.emitted} bump, so {@code
     * baseline} always matches the portion of {@code entriesEmitted} this sampler never itself
     * observed a sample for.
     */
    void recordBaseline(long keys) {
        if (keys > 0L) {
            lock.lock();
            try {
                baseline += keys;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Halves the live sample count by keeping only the even indices ({@code 0, 2, 4, ...} — a
     * deterministic decimation, never random) and doubles the stride, so future samples keep the
     * same effective spacing along the call sequence the retained ones already have. Always called
     * with {@link #lock} already held.
     */
    private void compactAndDoubleStride() {
        int half = capacity / 2;
        for (int i = 0; i < half; i++) {
            emitIndex[i] = emitIndex[2 * i];
            elapsedNanos[i] = elapsedNanos[2 * i];
            inFlight[i] = inFlight[2 * i];
        }
        count = half;
        stride *= 2;
    }

    /**
     * The window's time-unweighted mean in-flight count, over the samples whose cumulative
     * emit-index falls in the last {@code pct}% of THIS PROCESS's listed keys ({@code totalEmitted
     * - baseline} — see the class javadoc's resume note). {@code NaN} when no sample has landed
     * yet, or the listed-so-far/elapsed totals aren't positive (mirrors the {@code -1}/{@code NaN}
     * "unobserved" idiom the other pull-based gauges use).
     */
    double avgInFlightForWindow(int pct, long totalEmitted, long totalElapsedNanos) {
        WindowStats w = windowStats(pct, totalEmitted, totalElapsedNanos);
        return w == null ? Double.NaN : w.avgInFlight();
    }

    /**
     * The window's share of the run's total wall time — a serial tail is a SMALL key share (5% or
     * 10%, by definition) paired with a LARGE wall-time share. {@code NaN} under the same
     * unobserved idiom as {@link #avgInFlightForWindow}.
     */
    double wallShareForWindow(int pct, long totalEmitted, long totalElapsedNanos) {
        WindowStats w = windowStats(pct, totalEmitted, totalElapsedNanos);
        return w == null ? Double.NaN : w.wallShareFrac();
    }

    private record WindowStats(double avgInFlight, double wallShareFrac) {
    }

    /**
     * Snapshots the live sample buffer (and {@link #baseline}) under {@link #lock}, then derives
     * the window boundary and its two scalars OUTSIDE the lock (cheap, bounded by {@code capacity},
     * but no reason to hold a lock other readers/writers might want). The window START is the first
     * surviving sample whose {@code emitIndex} reaches {@code baseline + totalListed * (1 -
     * pct/100)} — where {@code totalListed = totalEmitted - baseline} is THIS process's own listed
     * key count (see the class javadoc's resume note) — never interpolated (see the class javadoc's
     * documented-approximation note).
     */
    private WindowStats windowStats(int pct, long totalEmitted, long totalElapsedNanos) {
        long[] localEmitIndex;
        long[] localElapsed;
        int[] localInFlight;
        long localBaseline;
        int n;
        lock.lock();
        try {
            n = count;
            localBaseline = baseline;
            if (n == 0) {
                return null;
            }
            localEmitIndex = Arrays.copyOf(emitIndex, n);
            localElapsed = Arrays.copyOf(elapsedNanos, n);
            localInFlight = Arrays.copyOf(inFlight, n);
        } finally {
            lock.unlock();
        }
        long totalListed = totalEmitted - localBaseline;
        if (totalListed <= 0L || totalElapsedNanos <= 0L) {
            return null;
        }
        double windowStartEmit = localBaseline + totalListed * (1.0 - pct / 100.0);
        int idx = 0;
        while (idx < n - 1 && localEmitIndex[idx] < windowStartEmit) {
            idx++;
        }
        double sum = 0.0;
        int windowCount = 0;
        for (int i = idx; i < n; i++) {
            sum += localInFlight[i];
            windowCount++;
        }
        double avgInFlight = sum / windowCount;
        double wallShareFrac = (totalElapsedNanos - localElapsed[idx]) / (double) totalElapsedNanos;
        wallShareFrac = Math.max(0.0, Math.min(1.0, wallShareFrac));
        return new WindowStats(avgInFlight, wallShareFrac);
    }
}
