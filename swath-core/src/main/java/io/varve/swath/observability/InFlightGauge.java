/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * The CAS'd {@code (value, lastTransitionNanos, areaNanos)} in-flight-concurrency triple that backs
 * both {@link RunMetrics#currentInFlight()} and the time-weighted {@code swath.in_flight.avg} area
 * integral. Sample-on-change: every {@link RunMetrics#incrementInFlight()}/{@link
 * RunMetrics#decrementInFlight()} transition folds {@code value * elapsed-since-last-transition} into
 * the running integral, so there is no polling thread. {@code nanoClock} is a seam (default {@code
 * System.nanoTime}) so a test can drive deterministic transitions on a fake clock.
 *
 * <p>Extracted from {@code RunMetrics}. The peak high-water mark and the trajectory rollup are
 * folded on the SAME transition seam but owned by {@code RunMetrics}/{@code TrajectoryRollup} — this
 * class returns the winning transition's {@code (value, now, window, valueDuringWindow)} tuple so the
 * facade can drive those without a second measurement.
 */
final class InFlightGauge {

    private final LongSupplier nanoClock;
    private final AtomicReference<InFlightGaugeState> state =
            new AtomicReference<>(new InFlightGaugeState(0L, 0L, 0L));

    /** {@code value} held since {@code lastTransitionNanos}; {@code areaNanos} is the accumulated value*dt integral up to that instant. */
    private record InFlightGaugeState(long value, long lastTransitionNanos, long areaNanos) {
    }

    /**
     * The result of one committed transition: the new in-flight {@code value}, the {@code nowNanos}
     * the winning CAS attempt read, the {@code windowNanos} the prior value was held for, and that
     * prior {@code valueDuringWindow}. The facade reuses this exact tuple to fold the trajectory
     * rollup, never a second measurement.
     */
    record Transition(long value, long nowNanos, long windowNanos, long valueDuringWindow) {
    }

    InFlightGauge(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /**
     * Fold {@code delta} into the gauge's count AND fold {@code value * (now - lastTransitionNanos)}
     * into the running area integral in the SAME CAS loop, with {@link #state}'s CAS'd value the SOLE
     * source of truth for the count (no other field mutated in or after this method). Two invariants
     * the loop must hold:
     * <ol>
     *   <li>The new count derives from {@code prev.value() + delta} read AT CAS time, never an absolute
     *       value captured before the loop — otherwise two concurrent transitions can commit out of
     *       order (a delayed count=1 landing after a newer count=2), silently undercounting the tail
     *       until the next transition.</li>
     *   <li>{@code now} is re-read from {@code nanoClock} on every attempt AND clamped
     *       {@code newLast = max(prev.lastTransitionNanos, now)} — a CAS loser that read {@code now}
     *       once could otherwise commit a {@code lastTransitionNanos} older than the state it replaces,
     *       corrupting the area integral with a clamped-to-zero window and losing that window's
     *       contribution. CAS commit order, not read order, is authoritative for happens-before.</li>
     * </ol>
     */
    Transition recordTransition(long delta) {
        InFlightGaugeState prev;
        InFlightGaugeState next;
        long now;
        long elapsed;
        do {
            now = nanoClock.getAsLong();
            prev = state.get();
            long newLast = Math.max(prev.lastTransitionNanos(), now);
            elapsed = newLast - prev.lastTransitionNanos();
            long area = prev.areaNanos() + elapsed * prev.value();
            next = new InFlightGaugeState(prev.value() + delta, newLast, area);
        } while (!state.compareAndSet(prev, next));
        return new Transition(next.value(), now, elapsed, prev.value());
    }

    /**
     * The current in-flight count — {@link #state}'s {@code value} is the single source of truth (no
     * separate {@code AtomicLong} shadow copy: a shadow copy updated outside the CAS is itself a
     * stale-overwrite hazard for a reader, see {@link #recordTransition}'s javadoc).
     */
    long current() {
        return state.get().value();
    }

    /**
     * The time-weighted average in-flight listing count since {@code runStartNanos} — the running
     * area integral (folded on every transition) plus the tail since the gauge's last transition,
     * divided by elapsed wall time since run start. {@code 0.0} before the run starts (zero-elapsed
     * guard, same idiom as the other ratio getters).
     */
    double average(long runStartNanos) {
        InFlightGaugeState s = state.get();
        long now = nanoClock.getAsLong();
        long tailElapsed = Math.max(0L, now - s.lastTransitionNanos());
        long totalArea = s.areaNanos() + tailElapsed * s.value();
        long totalElapsed = now - runStartNanos;
        return totalElapsed > 0L ? (double) totalArea / (double) totalElapsed : 0.0;
    }

    /**
     * Reset the integration window to {@code nowNanos}, carrying the live count forward (a resumed
     * run's warm-start reopen work is real, not a gap) and zeroing the accumulated area. Driven from
     * {@link RunMetrics#markRunStarted()}.
     */
    void reset(long nowNanos) {
        state.set(new InFlightGaugeState(current(), nowNanos, 0L));
    }
}
