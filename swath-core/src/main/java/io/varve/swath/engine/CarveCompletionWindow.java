/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * A synchronized, coherent completion window: the last {@link #CAPACITY} (realized mass, hasSplit)
 * pairs of completed owner-split children — the carve brake's signal (campaign memo §5), redesigned
 * per the codex consult's E-20 amendment (punch-list rows 24/25 against the earlier {@code
 * CarveMassRing}).
 *
 * <p><b>Coherent, not merely atomic (row 24 — the ring's non-linearizable publication defect).</b>
 * {@code CarveMassRing} wrote each sample to an independent {@code AtomicLongArray} slot and read the
 * window by summing across slots with no shared lock, so a reader could observe a torn mix: some
 * slots from one generation, some from another, with no instant at which that exact combination of
 * values ever really existed. {@link #record} and {@link #windowAverage} instead share ONE monitor
 * (plain {@code synchronized} methods): every write and every read is a single indivisible critical
 * section, so a reader always sees a state that corresponds to some real serialization of the actual
 * call order — never a blend that never existed. Updates happen once per COMPLETED tagged owner-split
 * child (not the hot page-commit path), so contention over this one lock is negligible.
 *
 * <p><b>Split-aware effective mass (row 25 — the ring's split-blindness).</b> The ring averaged raw
 * {@code keysEmitted} regardless of whether a child itself split further; the confetti gate's OWN
 * classifier never counts a split child as evidence of a thinning tail (a healthy intermediate node
 * routinely finishes with a small own tally purely because it shed its own further tail(s) onward —
 * see {@link ConfettiFeedbackGate}'s javadoc), but the ring's plain average let exactly that same
 * small tally drag the brake's window down anyway. {@link #windowAverage} instead computes, per
 * entry, {@code effectiveMass = hasSplit ? max(mass, K*maxKeys) : mass} — a child that split further
 * is never treated as negative evidence, floored at the admitting threshold itself rather than
 * counted at its (irrelevant) own tally.
 *
 * <p><b>K is resolved at READ time, not baked into storage.</b> {@link #record} stores the raw
 * {@code (mass, hasSplit)} pair exactly as observed; {@link #windowAverage} takes {@code k} as a
 * parameter and applies it while averaging, so ONE window instance serves whichever {@code
 * carve_brake} mode the run is actually configured with, with no re-recording needed if the
 * effective K changes conceptually (e.g. a future race arm).
 *
 * <p><b>Zero warmup.</b> The prior warmup ({@link ConfettiFeedbackGate#MIN_SAMPLE}, tied to the
 * confetti gate's own rate signal) does not apply here: {@link #windowAverage} is a prefix average
 * from the FIRST completion onward (n = however many have arrived, capped at {@link #CAPACITY}),
 * {@link Double#NaN} only when nothing has completed yet. The codex cross-read (E-20) found the
 * failing #78 reps' damaging carves reached this exact gate position with a signal already present
 * (a thin, unsplit first completion) but read {@code NaN} purely because warmup had not yet been
 * satisfied — dropping the warmup, not reordering the gate, closes that gap.
 */
final class CarveCompletionWindow {

    /** The window capacity — the number of most-recent completions the average is taken over. */
    static final int CAPACITY = 8;

    private final long[] masses = new long[CAPACITY];
    private final boolean[] hasSplit = new boolean[CAPACITY];
    /** Total completions ever recorded (uncapped) — also this window's own next write slot, mod {@link #CAPACITY}. */
    private long count;

    /** Record one completed tagged owner-split child's realized mass and split status. */
    synchronized void record(long mass, boolean split) {
        int slot = (int) (count % CAPACITY);
        masses[slot] = mass;
        hasSplit[slot] = split;
        count++;
    }

    /**
     * The split-aware effective-mass average over the last {@code min(count, CAPACITY)} completions,
     * at threshold {@code k * maxKeys}, or {@link Double#NaN} iff nothing has completed yet (zero
     * warmup: defined from the very first completion, never gated on a sample-count floor).
     *
     * @param k       the run's {@code carve_brake} mode multiplier ({@code CarveBrakeMode#k()})
     * @param maxKeys the run's page size — the effective-mass floor's unit
     */
    synchronized double windowAverage(long k, int maxKeys) {
        if (count == 0) {
            return Double.NaN;
        }
        int n = (int) Math.min(count, CAPACITY);
        double floor = (double) k * maxKeys;
        double sum = 0;
        for (int i = 0; i < n; i++) {
            sum += hasSplit[i] ? Math.max(masses[i], floor) : masses[i];
        }
        return sum / n;
    }

    /** Test/diagnostic accessor: how many completions this window has ever recorded (uncapped). */
    synchronized long count() {
        return count;
    }
}
