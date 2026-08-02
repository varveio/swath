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
 * A single-threaded discrete-event kernel. Events dispatch in {@code (atNanos, sequence)} order;
 * each event body is atomic with respect to other actors, and work it schedules runs only after the
 * body returns.
 */
public final class SimKernel implements SimContext {

    /** No actor — the id an event scheduled by the run's bootstrap, rather than by an actor, carries. */
    public static final int NO_ACTOR = -1;

    /**
     * Reserved for fleet-wide draws, so their values do not depend on which worker triggered them.
     * Distinct from {@link #NO_ACTOR} so fleet streams cannot collide with rejected out-of-dispatch
     * draws.
     */
    public static final int FLEET_ACTOR = -2;

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
     * @param maxEvents the runaway guard: dispatch at most this many events, {@code > 0}. Counted as
     *                  events <b>dispatched</b>, which includes any an actor invalidates when it runs:
     *                  the kernel has no cancellation, so a model that arms a timer against an
     *                  in-flight operation retires the loser by ignoring it in its own body, and that
     *                  dispatch is charged here like any other. A budget therefore has to be sized
     *                  against the events a scenario schedules, not against the ones that turn out to
     *                  matter; an executor that arms such timers reports how many were invalidated (see
     *                  its own {@code .stale} counter) so the difference is measured rather than
     *                  guessed at.
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

    /**
     * {@inheritDoc}
     *
     * <p>Rejected outside a dispatch. There is no actor to attribute the draw to there, so the tape it
     * would mint is keyed on {@link #NO_ACTOR} — a tape shared by every such caller, whose values then
     * depend on which of them ran first. Failing loudly turns that into a defect at its first use
     * rather than a reproducibility hole discovered when two runs disagree.
     */
    @Override
    public SimRng rng(SimRngStream stream) {
        if (currentActorId == NO_ACTOR) {
            throw new IllegalStateException("rng(" + stream + ") was called outside an event body, where "
                    + "there is no actor to own the tape; a fleet-wide instrument draws on FLEET_ACTOR "
                    + "through SimRng.forStream instead");
        }
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
        long atNanos;
        try {
            atNanos = Math.addExact(nowNanos, delayNanos);
        } catch (ArithmeticException overflow) {
            // A wrapped instant is worse than a rejected one: it lands negative, sorts ahead of every
            // legitimate event, and drags the virtual clock backwards when it is dispatched.
            throw new IllegalArgumentException("scheduling " + delayNanos + "ns past now (" + nowNanos
                    + "ns) overflows the nanosecond clock", overflow);
        }
        enqueue(atNanos, actorId, kind, action);
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
        if (atNanos < 0) {
            throw new IllegalArgumentException("an event instant must be >= 0, got " + atNanos);
        }
        queue.add(new ScheduledEvent(atNanos, sequence++, actorId, kind, action));
    }
}
