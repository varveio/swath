/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A shared resource with {@code capacity} servers and a first-come-first-served queue — the second of
 * the two shapes a per-unit cost can take in this simulator, and the one an independent per-unit
 * delay cannot express.
 *
 * <p><b>Why both shapes exist.</b> Charging every page an independent delay says the cost of a page
 * is a property of that page. Routing every page through a server with a queue says the cost of a
 * page is a property of how many other pages are in flight. The two agree exactly when the resource
 * is idle and diverge without bound when it is not — so a policy that bursts is scored as free under
 * the first model and as self-throttling under the second. Which one is right is a question about
 * measured data, not about the kernel; the kernel's job is to make sure both can be stated.
 *
 * <p>Occupancy is instrumented on the submitting context, so a run's results carry whether the
 * resource was actually contended rather than leaving a reader to assume:
 *
 * <ul>
 *   <li>{@code <name>.submitted} — requests offered to the resource.</li>
 *   <li>{@code <name>.queued} — those that found every server busy and had to wait.</li>
 *   <li>{@code <name>.wait_nanos} — total time spent waiting, charged when a waiter is admitted, so
 *       a request still queued when the run ends contributes nothing.</li>
 *   <li>{@code <name>.busy_nanos} — total service time, charged <b>at service start</b>, not at
 *       completion. On a run that quiesced the two are the same number. On a run cut short by a
 *       duration or event ceiling this counter therefore <b>overstates</b>: it includes the whole
 *       service time of a request that was still in flight when the ceiling hit. That is deliberate —
 *       charging at completion would instead undercount by silently dropping in-flight work, and of
 *       the two biases an explicit over-count on a truncated run is the one a reader can correct
 *       for. Read it as "service committed", not "service elapsed".</li>
 * </ul>
 *
 * <p>Not thread-safe, and not required to be: every method runs inside an event body.
 */
public final class FifoServer {

    private record Waiting(int actorId, long serviceNanos, long enqueuedAtNanos, SimAction onComplete) {
    }

    private final String name;
    private final int capacity;
    private final Deque<Waiting> waiting = new ArrayDeque<>();
    private int busy;

    /**
     * @param name     the counter prefix and event-kind prefix this resource's activity is recorded under
     * @param capacity how many requests it serves at once ({@code 1} = a single serial writer)
     */
    public FifoServer(String name, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got " + capacity);
        }
        this.name = name;
        this.capacity = capacity;
    }

    /**
     * Requests {@code serviceNanos} of this resource on behalf of the calling actor, running
     * {@code onComplete} for that same actor once the service finishes. A request that finds a free
     * server starts immediately; otherwise it waits behind everything already queued.
     */
    public void submit(SimContext ctx, long serviceNanos, SimAction onComplete) {
        if (serviceNanos < 0) {
            throw new IllegalArgumentException("serviceNanos must be >= 0, got " + serviceNanos);
        }
        ctx.count(name + ".submitted", 1);
        if (busy < capacity) {
            start(ctx, ctx.actorId(), serviceNanos, onComplete);
            return;
        }
        ctx.count(name + ".queued", 1);
        waiting.add(new Waiting(ctx.actorId(), serviceNanos, ctx.nowNanos(), onComplete));
    }

    /** Requests in the queue right now — the depth a saturation assertion reads. */
    public int queueDepth() {
        return waiting.size();
    }

    /**
     * Whether nothing is in service and nothing is waiting. A resource is shared for the length of a
     * run, so a caller starting a new run checks this rather than inheriting the residue of a run
     * that was cut short by a duration or event ceiling.
     */
    public boolean isIdle() {
        return busy == 0 && waiting.isEmpty();
    }

    private void start(SimContext ctx, int actorId, long serviceNanos, SimAction onComplete) {
        busy++;
        // Charged at service START, so this reads as "service committed" -- it over-counts by the
        // in-flight request's full service time on a run cut short by a ceiling. See the class javadoc
        // for why that bias is preferred to the under-count charging at completion would produce.
        ctx.count(name + ".busy_nanos", serviceNanos);
        ctx.scheduleFor(actorId, serviceNanos, name + ".complete", completion -> {
            busy--;
            // Admit the next waiter FIRST, so it starts at the instant this server freed up rather
            // than at whatever instant the completed request's own continuation happens to reach.
            Waiting next = waiting.poll();
            if (next != null) {
                completion.count(name + ".wait_nanos", completion.nowNanos() - next.enqueuedAtNanos());
                start(completion, next.actorId(), next.serviceNanos(), next.onComplete());
            }
            onComplete.run(completion);
        });
    }
}
