/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * What a run produced: the simulated wall time, how much work the kernel did to get it, why it
 * stopped, the trace, and the counters actors incremented along the way.
 *
 * @param wallNanos       the virtual instant the run ended at — the headline result
 * @param eventsProcessed events dispatched — the kernel's own cost, not the modelled system's, and
 *                        inclusive of every event an actor invalidated when it ran (see
 *                        {@link SimKernel}'s {@code maxEvents} note)
 * @param stopReason      why the run ended
 * @param log             the trace (empty unless the scenario asked for one)
 * @param counters        run counters, defensively copied into name order so a printed result is
 *                        stable and a later run cannot mutate an earlier one's numbers
 */
public record SimRunResult(
        long wallNanos,
        long eventsProcessed,
        SimStopReason stopReason,
        SimEventLog log,
        SortedMap<String, Long> counters) {

    public SimRunResult {
        counters = Collections.unmodifiableSortedMap(new TreeMap<>(counters));
    }

    /** The named counter, or zero if no actor ever incremented it. */
    public long counter(String name) {
        return counters.getOrDefault(name, 0L);
    }
}
