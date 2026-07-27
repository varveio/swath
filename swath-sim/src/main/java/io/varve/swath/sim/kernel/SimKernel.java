/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import io.varve.swath.sim.model.EngineTimeBudgets;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * The discrete-event kernel: a virtual clock, a queue of future events, and a single-threaded loop
 * that repeatedly pops the earliest event, jumps the clock to it, and runs it. Nothing happens
 * between events, so the time in which nothing happens costs nothing to simulate — that is the whole
 * speed argument, and it is why a run's wall time is a modelled quantity rather than a measured one.
 *
 * <h2>The total order, and why it is the interesting part</h2>
 * Events are ordered by {@code (atNanos, sequence)}, where {@code sequence} is a global counter
 * stamped at schedule time. That is a strict total order over every event ever scheduled, so there
 * is never a tie for the kernel to break arbitrarily, and the dispatch order is a pure function of
 * the scenario and the seed.
 *
 * <p>Two actors can nonetheless sit at the same instant, and the order between them is then "whoever
 * scheduled first". This is not a technicality to be smoothed over: it is the mechanism by which a
 * simulated race has a winner and a loser. An actor that reads shared state in one event and acts on
 * it in a later one is exposed to every event another actor manages to run in between — precisely
 * the widened read window a real engine has between snapshotting a victim's cursor and re-validating
 * it under a compare-and-set, and precisely why the loser's re-validation legitimately fails.
 * Conversely, everything one event body does is atomic with respect to every other actor (see
 * {@link SimAction}), which is how a lock hold is expressed. Between those two shapes the kernel can
 * state any interleaving a real executor's locking discipline permits, without a lock, a thread, or
 * a memory model.
 *
 * <h2>No framework</h2>
 * The kernel is deliberately this small and fused to the models it drives. A general-purpose
 * simulation library brings its own clock, its own process abstraction and its own randomness, all
 * of which stand between a scenario and the closed-form invariants this module asserts in the modes
 * where they are exact.
 */
public final class SimKernel implements SimContext {

    /** No actor — the id an event scheduled by the run's bootstrap, rather than by an actor, carries. */
    public static final int NO_ACTOR = -1;

    /** The kind stamped on the automatic trace entry every dispatched event produces. */
    private static final String DISPATCH_DETAIL = "";

    private record ScheduledEvent(long atNanos, long sequence, int actorId, String kind, SimAction action)
            implements Comparable<ScheduledEvent> {

        @Override
        public int compareTo(ScheduledEvent other) {
            int byTime = Long.compare(atNanos, other.atNanos);
            return byTime != 0 ? byTime : Long.compare(sequence, other.sequence);
        }
    }

    private final PriorityQueue<ScheduledEvent> queue = new PriorityQueue<>();
    private final Map<Long, SimRng> streams = new HashMap<>();
    private final TreeMap<String, Long> counters = new TreeMap<>();
    private final long baseSeed;
    private final EngineTimeBudgets budgets;
    private final SimEventLog log;
    private final long maxEvents;

    private long nowNanos;
    private long sequence;
    private long eventsProcessed;
    private int currentActorId = NO_ACTOR;
    private boolean running;

    /**
     * @param baseSeed  the run's one seed; every draw stream is derived from it
     * @param budgets   the run's declared engine time budgets; {@link EngineTimeBudgets#maxDurationNanos()}
     *                  is enforced here, the rest are read by the actors that model them
     * @param log       the trace to append to ({@link SimEventLog#disabled()} to record nothing)
     * @param maxEvents the runaway guard: dispatch at most this many events, {@code > 0}
     */
    public SimKernel(long baseSeed, EngineTimeBudgets budgets, SimEventLog log, long maxEvents) {
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive, got " + maxEvents);
        }
        this.baseSeed = baseSeed;
        this.budgets = budgets;
        this.log = log;
        this.maxEvents = maxEvents;
    }

    /**
     * Seeds the schedule before the run starts — the bootstrap that gives each actor its first event.
     * Legal only before {@link #run()}; an actor schedules through its {@link SimContext} instead.
     */
    public void scheduleBootstrap(long atNanos, int actorId, String kind, SimAction action) {
        if (running) {
            throw new IllegalStateException("bootstrap events must be scheduled before run(); a running "
                    + "actor schedules through its SimContext");
        }
        enqueue(atNanos, actorId, kind, action);
    }

    /**
     * Runs to quiescence, to the declared max duration, or to the event cap — whichever comes first.
     *
     * <p>The max-duration check is made on the event's own instant <em>before</em> dispatching it, so
     * a run stopped by its budget stops at the budget rather than at some instant past it: the clock
     * never advances beyond a ceiling the scenario declared. One kernel runs once.
     */
    public SimRunResult run() {
        if (running) {
            throw new IllegalStateException("a SimKernel runs exactly once");
        }
        running = true;
        long maxDuration = budgets.maxDurationNanos();
        SimStopReason stopReason = SimStopReason.QUIESCED;
        while (!queue.isEmpty()) {
            if (eventsProcessed >= maxEvents) {
                stopReason = SimStopReason.EVENT_CAP;
                break;
            }
            ScheduledEvent next = queue.peek();
            if (maxDuration != EngineTimeBudgets.UNBOUNDED_DURATION && next.atNanos() > maxDuration) {
                nowNanos = maxDuration;
                stopReason = SimStopReason.MAX_DURATION;
                break;
            }
            queue.poll();
            nowNanos = next.atNanos();
            currentActorId = next.actorId();
            eventsProcessed++;
            log.append(nowNanos, currentActorId, next.kind(), DISPATCH_DETAIL);
            next.action().run(this);
        }
        currentActorId = NO_ACTOR;
        return new SimRunResult(nowNanos, eventsProcessed, stopReason, log, counters);
    }

    @Override
    public long nowNanos() {
        return nowNanos;
    }

    @Override
    public int actorId() {
        return currentActorId;
    }

    @Override
    public SimRng rng(SimRngStream stream) {
        return streamFor(currentActorId, stream);
    }

    @Override
    public void schedule(long delayNanos, String kind, SimAction action) {
        scheduleFor(currentActorId, delayNanos, kind, action);
    }

    @Override
    public void scheduleFor(int actorId, long delayNanos, String kind, SimAction action) {
        if (delayNanos < 0) {
            throw new IllegalArgumentException("cannot schedule into the past, delay " + delayNanos);
        }
        enqueue(nowNanos + delayNanos, actorId, kind, action);
    }

    @Override
    public void record(String kind, String detail) {
        log.append(nowNanos, currentActorId, kind, detail);
    }

    @Override
    public void count(String counter, long delta) {
        counters.merge(counter, delta, Long::sum);
    }

    @Override
    public EngineTimeBudgets budgets() {
        return budgets;
    }

    /**
     * The stream of one (actor, purpose) pair, created on first use and cached for the run. Caching
     * is what makes it a <em>stream</em> rather than a sequence of identically-seeded generators
     * handing out the same first value over and over.
     */
    private SimRng streamFor(int actorId, SimRngStream stream) {
        long key = ((long) actorId << 32) ^ (stream.ordinal() & 0xFFFFFFFFL);
        return streams.computeIfAbsent(key, ignored -> SimRng.forStream(baseSeed, actorId, stream));
    }

    private void enqueue(long atNanos, int actorId, String kind, SimAction action) {
        queue.add(new ScheduledEvent(atNanos, sequence++, actorId, kind, action));
    }
}
