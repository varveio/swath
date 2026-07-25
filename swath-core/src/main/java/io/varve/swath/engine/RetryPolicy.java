/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * How a transient-retry loop ({@link TransientRetryFetcher} on the seed/sequential path,
 * {@code GaugedFetcher} on the engine path) disposes a fetch that stays wedged past
 * {@link TransientRetryFetcher#MAX_TRANSIENT_RETRIES} transient faults — every {@code ThrottleException}
 * kind counts, voting and non-voting alike, accumulated within the fetch and never reset.
 *
 * <p>The choice is made once at CLI wiring time from whether a real {@code LivenessWatchdog} is armed:
 * <ul>
 *   <li>{@link #RIDE_OUT} — a real watchdog owns storm death (crash-only resumable exit-75), so the
 *       over-cap transient no longer cancels the run: it RETRIES INDEFINITELY (raised backoff ceiling,
 *       recording {@code storm_ride_out}) until the store heals or the watchdog ends the run.</li>
 *   <li>{@link #BOUNDED} — BOTH watchdog windows are disabled ({@code arm()} returned the no-op), so
 *       nothing else could ever stop an unbounded loop. Cap exhaustion
 *       cancels the run {@code STUCK} (resumable, bounded) rather than ride out forever with no
 *       backstop — closing the infinite-retry hole.</li>
 * </ul>
 * The token-less (embedded) wiring is bounded independently of this policy — with no
 * {@code CancellationToken} to attribute a stop, cap exhaustion always escapes the fatal throttle.
 */
public enum RetryPolicy {
    /** A real LivenessWatchdog is armed and owns storm death → over-cap transients ride out indefinitely. */
    RIDE_OUT,
    /** No watchdog backstop (both windows disabled) → over-cap exhaustion cancels resumable STUCK (bounded). */
    BOUNDED
}
