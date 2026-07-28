/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.sim.kernel.SimEventLog;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Readings a run's event trace can answer that its counters cannot — which <em>range</em> a number
 * belongs to. A counter says the fleet spent half the run serial; only the trace says which range it
 * was serial on, and a diagnosis without that address is a magnitude.
 *
 * <p>Shared rather than copied because more than one instrument asks: the real-listing decomposition
 * names the ranges that held the fleet, and the probe:page-ratio leg has to find the one range a tail
 * is made of before it can assert anything about it. Two walks over the same claim/complete stream
 * that disagreed would be two different runs being described.
 */
final class SimTrace {

    /** The two trace kinds that move occupancy, and so the whole of the reconstruction's input. */
    private static final Set<String> OCCUPANCY_KINDS = Set.of("range.claim", "range.complete");

    /** The pseudo-range a serial span with nothing being drained at all is attributed to. */
    static final long FLEET_IDLE = -1L;

    private SimTrace() {
    }

    /**
     * Serial nanoseconds by the range that was holding the fleet, rebuilt from the trace's claim and
     * complete records. A span counts when at most one range is being drained; a span at zero — every
     * worker parked between a completion and the next claim — is attributed to {@link #FLEET_IDLE},
     * because "nobody was working" and "one range was working alone" are different diagnoses and the
     * timeline's own serial fraction merges them.
     *
     * <p>The window opens at the seed's end, and an occupancy record from before it <b>primes</b> the
     * live set rather than being dropped: a range claimed before the window is still holding the fleet
     * inside it, and dropping its claim would attribute its whole span to {@link #FLEET_IDLE} or to
     * whoever claimed next. The window's start and the running cursor are separate for the same reason
     * — one is a property of the run, the other of the walk, and sharing a variable between them makes
     * the pre-window filter mean something different after the first entry.
     */
    static Map<Long, Long> serialNanosByNode(PolicyRunResult result) {
        Map<Long, Long> serialNanos = new LinkedHashMap<>();
        Set<Long> live = new LinkedHashSet<>();
        long windowStart = result.timeline().seedCompletedNanos();
        long since = windowStart;
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (!OCCUPANCY_KINDS.contains(entry.kind())) {
                continue;
            }
            if (entry.atNanos() < windowStart) {
                applyOccupancy(live, entry);
                continue;
            }
            if (live.size() <= 1) {
                serialNanos.merge(live.isEmpty() ? FLEET_IDLE : live.iterator().next(),
                        entry.atNanos() - since, Long::sum);
            }
            applyOccupancy(live, entry);
            since = entry.atNanos();
        }
        if (live.size() <= 1) {
            serialNanos.merge(live.isEmpty() ? FLEET_IDLE : live.iterator().next(),
                    result.timeline().endNanos() - since, Long::sum);
        }
        return serialNanos;
    }

    /** One occupancy record applied to the live set: a claim adds its range, a completion removes it. */
    private static void applyOccupancy(Set<Long> live, SimEventLog.Entry entry) {
        if ("range.claim".equals(entry.kind())) {
            live.add(nodeOf(entry));
        } else {
            live.remove(nodeOf(entry));
        }
    }

    /** The {@code node=} id a trace entry carries. */
    static long nodeOf(SimEventLog.Entry entry) {
        return Long.parseLong(field(entry, "node="));
    }

    /**
     * The value of the {@code name=} field of a trace entry's detail string. Absence is an error rather
     * than an empty reading: every caller here is parsing a format its own emitter writes, and a
     * silently missing field is a reader that has drifted from the emitter and is printing nothing.
     */
    static String field(SimEventLog.Entry entry, String name) {
        for (String field : entry.detail().split("\\|")) {
            if (field.startsWith(name)) {
                return field.substring(name.length());
            }
        }
        throw new AssertionError("trace entry " + entry.kind() + " carries no " + name + ": "
                + entry.detail());
    }
}
