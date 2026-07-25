/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Avg/peak in-flight tracking — the {@link InFlightGauge} seam (extracted from {@code RunMetrics};
 * split from {@code RunMetricsContractTest}). Moved verbatim, no assertion changes.
 */
final class RunMetricsInFlightContractTest {

    @Test
    void avgInFlightIsTheTimeWeightedAverageOverAControlledClock() {
        // 1 worker in flight for 2 ticks, then
        // 3 workers for 1 tick -> area = 1*2 + 3*1 = 5 over 3 elapsed ticks -> avg = 5/3, deterministic
        // against a fake clock (no wall-clock flakiness). peak_in_flight would only ever read 3 here
        // (saturated/blind to the fact 1 was in flight for most of the run) -- avg_in_flight is what
        // actually moves.
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();

        metrics.incrementInFlight();               // t=0: 0 -> 1
        clock[0] = 2L;                              // 1 worker held for 2 ticks
        metrics.incrementInFlight();                // t=2: 1 -> 2
        metrics.incrementInFlight();                // t=2: 2 -> 3 (no elapsed time at this instant)
        clock[0] = 3L;                              // 3 workers held for 1 tick

        assertThat(metrics.avgInFlight()).isCloseTo(5.0 / 3.0, within(1e-9));
        assertThat(metrics.peakInFlight()).isEqualTo(3L);
    }

    @Test
    void avgInFlightIsZeroBeforeAnyTransition() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();
        clock[0] = 5L;

        // No worker was ever in flight: the integral is 0 over a non-zero elapsed window.
        assertThat(metrics.avgInFlight()).isEqualTo(0.0);
    }

    @Test
    void avgInFlightAccountsForTheTailSinceTheLastTransition() {
        // A value held from its last transition to "now" (not yet followed by another transition)
        // must still be folded in -- avgInFlight() is queryable mid-run, not just at a transition.
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();

        metrics.incrementInFlight();   // t=0: 0 -> 1
        clock[0] = 4L;                  // held at 1 worker for 4 ticks, no further transition

        assertThat(metrics.avgInFlight()).isCloseTo(1.0, within(1e-9));
    }

    @Test
    void gaugeCountSurvivesATransitionThatCompletesInsideAnotherTransitionsClockRead() {
        // recordInFlightTransition() must fold the +-1 delta into the SAME CAS as the area
        // integral, never take an externally captured absolute newValue (e.g. from a separate
        // inFlight.incrementAndGet()) -- doing so would let two concurrent transitions commit
        // their gauge state out of order: whichever call's *stale captured* value happens to CAS
        // last wins, even if it captured an earlier count, landing the gauge's `value` on 1 (A's
        // stale captured count) after two increments instead of 2. Real thread interleavings of
        // that race are non-deterministic, so this test forces the exact interleaving
        // deterministically instead of relying on real threads: the fake clock's first read
        // during "A"'s transition synchronously runs a second, nested transition ("B") to
        // completion before returning -- i.e. "B finishes entirely inside A's transition window",
        // the precondition for the race. Folding the delta against `prev.value()` read at CAS
        // time self-corrects regardless of interleaving.
        long[] clock = {0L};
        boolean[] armed = {false};
        RunMetrics[] holder = new RunMetrics[1];
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> {
            if (armed[0]) {
                armed[0] = false;
                holder[0].incrementInFlight(); // "B": a whole second transition, nested inside A's
            }
            return clock[0];
        });
        holder[0] = metrics;
        metrics.markRunStarted();

        armed[0] = true;
        metrics.incrementInFlight(); // "A": triggers B's full transition mid-flight (see clock above)

        clock[0] = 5L; // advance past both (same-instant) transitions to read the tail integral
        assertThat(metrics.avgInFlight()).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void staleClockReadDuringAConcurrentCommitIsClampedRatherThanRewindingTheGauge() {
        // `now` must be re-read on every CAS attempt, and the CAS'd `next` must floor the new
        // lastTransitionNanos at `max(prev.lastTransitionNanos, now)` -- reading `now` ONCE before
        // the CAS loop and using it directly with no floor would let a thread whose own reading of
        // `now` is stale relative to another thread's ALREADY-COMMITTED, LARGER lastTransitionNanos
        // commit a SMALLER lastTransitionNanos over it, rewinding the gauge's clock backward and
        // corrupting every subsequent tail-elapsed computation in avgInFlight(). Real nanoTime
        // reads can't be forced to go backward deterministically, so this constructs the
        // equivalent scenario directly through the nanoClock seam: "B" (a nested transition,
        // triggered as a side effect of "A"'s own clock read) commits at an ADVANCED clock value;
        // "A"'s own now-read then resolves to a STALE, smaller value once B returns and the clock
        // is wound back for the test. This passes ONLY when both the re-read and the floor are
        // present -- a per-attempt-but-unclamped read would still rewind the clock to the stale
        // value.
        //
        // A LITERAL multi-iteration CAS retry is not independently expressible with this seam:
        // nanoClock is the only injection point, and it is read BEFORE `prev`, so by the time this
        // test's hook lets B's whole transition run to completion, `prev` already reflects it and A's
        // own CAS succeeds on its very first attempt -- there is no window to inject a mutation
        // between `prev`'s read and the CAS itself. The clamp is applied to whatever (prev, now) pair
        // reaches the CAS regardless of attempt number, so exercising it on a first-and-only attempt
        // (as here) exercises the identical code path a multi-iteration retry would.
        long[] clock = {0L};
        boolean[] armed = {false};
        RunMetrics[] holder = new RunMetrics[1];
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> {
            if (armed[0]) {
                armed[0] = false;
                clock[0] = 20L;                  // "B" observes an ADVANCED clock ...
                holder[0].incrementInFlight();   // ... and commits lastTransitionNanos=20
                clock[0] = 10L;                  // ... then "A"'s own read resolves STALE, at 10
            }
            return clock[0];
        });
        holder[0] = metrics;
        metrics.markRunStarted();

        armed[0] = true;
        metrics.incrementInFlight();   // "A": now=10 (stale) is read while B commits last=20 first

        // Without the clamp: A's commit would rewind lastTransitionNanos to 10 (past B's already-
        // committed 20), so the tail below would double-count the [10,20) window inside a 15-tick
        // (10..25) elapsed instead of the correct 5-tick (20..25) one -- avgInFlight = 30/25 = 1.2.
        // With the clamp: lastTransitionNanos stays at max(20, 10) = 20 (never moves backward), A's
        // own transition credits zero elapsed (correct -- no time has passed since B's commit), and
        // the tail is exactly 25-20=5 ticks at value=2 -> avgInFlight = 10/25 = 0.4.
        clock[0] = 25L;
        assertThat(metrics.avgInFlight()).isCloseTo(0.4, within(1e-9));
    }
}
