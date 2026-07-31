/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A capacity-bounded FIFO service resource for modelling contended work.
 *
 * <ul>
 *   <li>{@code <name>.submitted}: all requests.</li>
 *   <li>{@code <name>.queued}: requests that found all servers busy.</li>
 *   <li>{@code <name>.wait_nanos}: wait charged on admission; requests still queued at the end
 *       contribute nothing.</li>
 *   <li>{@code <name>.busy_nanos}: service committed at start, so it overstates completed service in
 *       truncated runs.</li>
 * </ul>
 *
 * <p>Callbacks retain the submitting actor's identity. Event-serialized use makes thread safety
 * unnecessary.
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
        // Charge committed service now; a truncated run may not execute the completion event.
        ctx.count(name + ".busy_nanos", serviceNanos);
        ctx.scheduleFor(actorId, serviceNanos, name + ".complete", completion -> {
            busy--;
            // Admit the next waiter at the release instant, before this request's callback runs.
            Waiting next = waiting.poll();
            if (next != null) {
                completion.count(name + ".wait_nanos", completion.nowNanos() - next.enqueuedAtNanos());
                start(completion, next.actorId(), next.serviceNanos(), next.onComplete());
            }
            onComplete.run(completion);
        });
    }
}
