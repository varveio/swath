/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import java.util.Locale;

/**
 * Phase shape that whole-run counters cannot show.
 *
 * <p>The tail begins at the last published split. {@link Recorder} records its aggregates in constant
 * space by snapshotting running totals at each split.
 *
 * <p>{@link #endNanos} is quiescence and is the duration for comparing runs. {@link #kernelEndNanos}
 * can be later because retired park timers still dispatch after quiescence; unfinished runs use the
 * kernel end as their phase end.
 *
 * @param seedCompletedNanos      the instant the seed phase handed its ranges to the fleet
 * @param lastSplitNanos          the instant of the last published split child, owner-side or thief;
 *                                equal to {@code seedCompletedNanos} when nothing ever split
 * @param endNanos                the phase end: quiescence when reached, otherwise the kernel end
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

    /** Tail duration divided by the phase-end duration. */
    public double tailFraction() {
        return endNanos <= 0L ? 0.0 : (double) (endNanos - lastSplitNanos) / endNanos;
    }

    /** Whole-run keys per virtual second. */
    public double keysPerVirtualSecond() {
        return endNanos <= 0L ? 0.0 : keysEmitted / (endNanos / 1e9);
    }

    /** Tail keys per virtual second. */
    public double tailKeysPerVirtualSecond() {
        long tailNanos = endNanos - lastSplitNanos;
        return tailNanos <= 0L ? 0.0 : keysInTail / (tailNanos / 1e9);
    }

    /** Tail range-nanoseconds divided by tail duration. */
    public double meanTailOccupancy() {
        long tailNanos = endNanos - lastSplitNanos;
        return tailNanos <= 0L ? 0.0 : (double) rangeNanosInTail / tailNanos;
    }

    /** Post-seed range-nanoseconds divided by post-seed duration. */
    public double meanOccupancy() {
        long nanos = endNanos - seedCompletedNanos;
        return nanos <= 0L ? 0.0 : (double) rangeNanos / nanos;
    }

    /** Post-seed time with at most one draining range, divided by post-seed duration. */
    public double serialFraction() {
        long nanos = endNanos - seedCompletedNanos;
        return nanos <= 0L ? 0.0 : (double) serialNanos / nanos;
    }

    /** Formats the phase shape for a run record. */
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

    /** Accumulates phase aggregates in constant space. */
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

        /** Records the seed-to-fleet handoff. */
        void seedCompleted(long nowNanos) {
            seedCompletedNanos = nowNanos;
            lastSplitNanos = nowNanos;
            lastOccupancyChangeNanos = nowNanos;
        }

        /** Sets the tail start and snapshots its aggregate baselines. */
        void splitPublished(long nowNanos) {
            lastSplitNanos = nowNanos;
            keysAtLastSplit = keysEmitted;
            rangeNanosAtLastSplit = integralAt(nowNanos);
        }

        /** Adds committed output keys. */
        void keysCommitted(long keys) {
            keysEmitted += keys;
        }

        /** Updates the draining-range integral before changing occupancy. */
        void occupancyChanged(long nowNanos, int ranges) {
            rangeNanos = integralAt(nowNanos);
            serialNanos = serialNanosAt(nowNanos);
            lastOccupancyChangeNanos = nowNanos;
            concurrentRanges = ranges;
            maxConcurrentRanges = Math.max(maxConcurrentRanges, ranges);
        }

        /** Records quiescence, independently of later retired timer events. */
        void quiesced(long nowNanos) {
            quiescedNanos = nowNanos;
        }

        /** Closes at quiescence, or at the kernel end when the run did not quiesce. */
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
