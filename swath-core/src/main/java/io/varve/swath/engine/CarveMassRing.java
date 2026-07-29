/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A fixed-size ring of the last {@link #SIZE} realized owner-split child masses (keys emitted by
 * completion), and its window average — the carve brake's signal ({@code
 * docs/internals/algorithms.md} §3.3, the serial-tail over-carving campaign). {@link
 * ConfettiFeedbackGate} owns one instance and feeds it from {@code recordCompletion} alongside the
 * existing confetti tally, with no new callback or synchronization point.
 *
 * <p>{@link #SIZE} is fixed at {@code ConfettiFeedbackGate#MIN_SAMPLE} (8) — one knob, not two: the
 * brake's warmup (at least {@code MIN_SAMPLE} tagged completions) and its window size are the same
 * number, so the window is always either empty of signal (pre-warmup) or exactly full.
 *
 * <p><b>Thread-safety mirrors {@link ConfettiFeedbackGate}'s own AtomicLong counters, not a lock.</b>
 * {@code recordCompletion} is called by whichever worker drains a tagged child, so — like {@code
 * taggedTotal}/{@code taggedConfetti} — concurrent callers race this ring for genuinely different
 * completions. {@link #record} draws each caller a distinct, monotonically increasing slot index
 * via a single {@code getAndIncrement()} (no read-modify-write gap two callers can land in
 * together — the same "the type is the proof" argument {@code ConfettiFeedbackGate#consumeProbeSlot}
 * makes for its own atomic), so two concurrent {@link #record} calls never write the same array
 * slot for the same sequence value; only a wrap (two calls {@link #SIZE} apart) reuses a slot, and
 * that reuse IS the ring's intended eviction, not a race. {@link #windowAverage} can read a slot
 * mid-write relative to another slot's write (a torn read across the window), tolerated exactly as
 * {@link ConfettiFeedbackGate.Snapshot}'s own javadoc documents for its three independent counters:
 * it only nudges which decision sees a mass transition, never correctness.
 */
final class CarveMassRing {

    /** The window size, and {@code ConfettiFeedbackGate#MIN_SAMPLE} — see this class's javadoc. */
    static final int SIZE = 8;

    private final AtomicLongArray slots = new AtomicLongArray(SIZE);
    private final AtomicLong writeSeq = new AtomicLong();

    /** Record one realized mass, evicting the oldest sample once the ring is full. */
    void record(long mass) {
        long seq = writeSeq.getAndIncrement();
        slots.set((int) (seq % SIZE), mass);
    }

    /**
     * The average of every slot, or {@link Double#NaN} until at least {@link #SIZE} masses have
     * been recorded (pre-warmup: too few samples to average, not a plausible-looking zero).
     */
    double windowAverage() {
        if (writeSeq.get() < SIZE) {
            return Double.NaN;
        }
        long sum = 0;
        for (int i = 0; i < SIZE; i++) {
            sum += slots.get(i);
        }
        return (double) sum / SIZE;
    }
}
