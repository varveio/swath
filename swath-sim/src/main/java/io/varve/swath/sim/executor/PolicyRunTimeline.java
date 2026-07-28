/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import java.util.Locale;

/**
 * <b>When</b> a run did what — the phase shape a counter total cannot express.
 *
 * <p>Counters say how often a mechanism fired over a whole run; they cannot say that every firing
 * happened in the first two seconds and the remaining thirty were one worker draining alone. That
 * difference is the whole subject of a parallel listing, so a run record carries three instants —
 * the seed's end, the <b>last</b> split of any kind, and the run's own end — and the two quantities
 * that only mean something relative to them: how many keys came out after the last split, and how
 * many ranges were being drained while they did.
 *
 * <p>The tail is defined as the interval after the last split because that is the moment the fleet
 * lost its last chance to divide: no mechanism creates work after it, so whatever parallelism exists
 * at that instant is the parallelism the rest of the run gets. A healthy run's tail is a short
 * flush of already-divided work; a run that cannot divide has a tail that is most of its duration
 * and an occupancy near one.
 *
 * <p>Everything here is accumulated in constant space by {@link Recorder}: the tail's start is not
 * known until the run ends, so rather than retain a sample per page the recorder snapshots its
 * running totals at every split and keeps only the latest snapshot. A run therefore pays no memory
 * for this, which is what lets it stay on during a sweep, where the event trace is off.
 *
 * <p><b>The end of the run is quiescence, not the kernel's last event.</b> The kernel has no
 * cancellation, so a park timer that has already been retired still costs a dispatch when it fires —
 * and a dispatch moves the clock. The longest such timer is the <b>steal-attempt-slot backstop</b>
 * ({@code idleStealAttemptParkNanos}, one second under the engine's own defaults): the park of a worker
 * that found the fleet's single steal-attempt slot busy. It is armed hundreds of times in a run that
 * steals at all, so after the last range completes and every worker has retired, the clock keeps
 * advancing for as long as the newest of those timers has left to run — measured at 0.75–1.0 s on these
 * fixtures, and bounded by that one second rather than by the 5–50 ms idle backoff ladder. Traced after
 * quiescence, the residue is <em>only</em> retired parks: no call timeout, no retry, no decision, no
 * key. A phase record measured against the kernel's last event would therefore credit the tail with
 * dead time no worker spent, so both instants are carried and their difference is named as an artifact
 * of the kernel rather than a result. <b>A comparison between runs — a sweep's ranking, a variant's
 * verdict — belongs on {@link #endNanos}</b>; the kernel's own duration carries a per-run constant that
 * has nothing to do with the policies being compared.
 *
 * @param seedCompletedNanos      the instant the seed phase handed its ranges to the fleet
 * @param lastSplitNanos          the instant of the last published split child, owner-side or thief;
 *                                equal to {@code seedCompletedNanos} when nothing ever split
 * @param endNanos                the instant the run went quiescent — its last range completed
 * @param kernelEndNanos          the kernel's own last event, retired park timers included
 * @param keysEmitted             keys emitted over the whole run
 * @param keysInTail              keys emitted after {@code lastSplitNanos}
 * @param rangeNanosInTail        the integral of concurrently-drained ranges over the tail, in
 *                                range-nanoseconds — the numerator of the tail's mean occupancy
 * @param rangeNanos              the same integral over the whole run after the seed
 * @param serialNanos             time after the seed during which at most one range was being drained
 * @param maxConcurrentRanges     the most ranges ever being drained at one instant
 */
public record PolicyRunTimeline(
        long seedCompletedNanos,
        long lastSplitNanos,
        long endNanos,
        long kernelEndNanos,
        long keysEmitted,
        long keysInTail,
        long rangeNanosInTail,
        long rangeNanos,
        long serialNanos,
        int maxConcurrentRanges) {

    /** The share of the run's duration that came after the last split. */
    public double tailFraction() {
        return endNanos <= 0L ? 0.0 : (double) (endNanos - lastSplitNanos) / endNanos;
    }

    /** Keys emitted per virtual second over the whole run. */
    public double keysPerVirtualSecond() {
        return endNanos <= 0L ? 0.0 : keysEmitted / (endNanos / 1e9);
    }

    /** Keys emitted per virtual second during the tail. */
    public double tailKeysPerVirtualSecond() {
        long tailNanos = endNanos - lastSplitNanos;
        return tailNanos <= 0L ? 0.0 : keysInTail / (tailNanos / 1e9);
    }

    /**
     * Ranges being drained at the average instant of the tail — the achieved occupancy, which for a
     * fleet that claims one range per worker is how many workers still had work.
     */
    public double meanTailOccupancy() {
        long tailNanos = endNanos - lastSplitNanos;
        return tailNanos <= 0L ? 0.0 : (double) rangeNanosInTail / tailNanos;
    }

    /**
     * Ranges being drained at the average instant after the seed — the fleet's achieved parallelism,
     * whose ceiling is the worker count.
     */
    public double meanOccupancy() {
        long nanos = endNanos - seedCompletedNanos;
        return nanos <= 0L ? 0.0 : (double) rangeNanos / nanos;
    }

    /**
     * The share of the post-seed run during which at most one range was being drained — the fleet
     * running serially, whatever its worker count says.
     */
    public double serialFraction() {
        long nanos = endNanos - seedCompletedNanos;
        return nanos <= 0L ? 0.0 : (double) serialNanos / nanos;
    }

    /** The phase shape as one line, for a run record. */
    public String describe() {
        return String.format(Locale.ROOT,
                "phases: seed_end=%.3fs last_split=%.3fs quiesced=%.3fs kernel_end=%.3fs "
                        + "tail_fraction=%.3f%n"
                        + "rates: keys_per_virtual_second=%.0f tail_keys_per_virtual_second=%.0f "
                        + "keys_in_tail=%d%n"
                        + "occupancy: mean_ranges=%.2f mean_tail_ranges=%.2f serial_fraction=%.3f "
                        + "max_concurrent_ranges=%d%n",
                seedCompletedNanos / 1e9, lastSplitNanos / 1e9, endNanos / 1e9,
                kernelEndNanos / 1e9, tailFraction(),
                keysPerVirtualSecond(), tailKeysPerVirtualSecond(), keysInTail,
                meanOccupancy(), meanTailOccupancy(), serialFraction(), maxConcurrentRanges);
    }

    /**
     * Accumulates a timeline as the run happens, in constant space.
     *
     * <p>Two running totals — keys emitted, and the integral of concurrently-drained ranges — are
     * snapshotted at each split. The last such snapshot is the tail's start, so the tail's own totals
     * are a subtraction at the end rather than a scan over retained samples.
     */
    static final class Recorder {

        private long seedCompletedNanos;
        private long lastSplitNanos;
        private long quiescedNanos = -1L;
        private long keysEmitted;
        private long keysAtLastSplit;
        private long rangeNanos;
        private long rangeNanosAtLastSplit;
        private long serialNanos;
        private int concurrentRanges;
        private int maxConcurrentRanges;
        private long lastOccupancyChangeNanos;

        /** The seed phase is over: its cut set is published and the fleet may start. */
        void seedCompleted(long nowNanos) {
            seedCompletedNanos = nowNanos;
            lastSplitNanos = nowNanos;
            lastOccupancyChangeNanos = nowNanos;
        }

        /** A split child was published, owner-side or by a thief: the tail cannot start before this. */
        void splitPublished(long nowNanos) {
            lastSplitNanos = nowNanos;
            keysAtLastSplit = keysEmitted;
            rangeNanosAtLastSplit = integralAt(nowNanos);
        }

        /** A page committed {@code keys} to the output. */
        void keysCommitted(long keys) {
            keysEmitted += keys;
        }

        /** A range was claimed by a worker, or released by one. */
        void occupancyChanged(long nowNanos, int ranges) {
            rangeNanos = integralAt(nowNanos);
            serialNanos = serialNanosAt(nowNanos);
            lastOccupancyChangeNanos = nowNanos;
            concurrentRanges = ranges;
            maxConcurrentRanges = Math.max(maxConcurrentRanges, ranges);
        }

        /** The last outstanding range completed: the fleet is done, whatever the kernel does next. */
        void quiesced(long nowNanos) {
            quiescedNanos = nowNanos;
        }

        /**
         * Closes the timeline at the kernel's last event.
         *
         * <p>A run that never went quiescent — one that ended stuck on a retry ceiling, or was cut off
         * by the event cap — has no completion instant to measure against, so its phase end falls back
         * to the kernel's. That makes {@code endNanos == kernelEndNanos} the signature of a run that
         * did not finish, and it is exactly the case where the record's own {@code completed()} is
         * false: the fallback is a stated definition for an unfinished run, not a second meaning for a
         * finished one.
         */
        PolicyRunTimeline finish(long kernelEndNanos) {
            long endNanos = quiescedNanos < 0L ? kernelEndNanos : quiescedNanos;
            return new PolicyRunTimeline(seedCompletedNanos, lastSplitNanos, endNanos, kernelEndNanos,
                    keysEmitted, keysEmitted - keysAtLastSplit,
                    integralAt(endNanos) - rangeNanosAtLastSplit, integralAt(endNanos),
                    serialNanosAt(endNanos), maxConcurrentRanges);
        }

        private long integralAt(long nowNanos) {
            return rangeNanos + (long) concurrentRanges * elapsed(nowNanos);
        }

        private long serialNanosAt(long nowNanos) {
            return concurrentRanges <= 1 ? serialNanos + elapsed(nowNanos) : serialNanos;
        }

        private long elapsed(long nowNanos) {
            return Math.max(0L, nowNanos - lastOccupancyChangeNanos);
        }
    }
}
