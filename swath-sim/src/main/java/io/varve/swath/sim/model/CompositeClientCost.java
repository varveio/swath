/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.FifoServer;
import io.varve.swath.sim.kernel.SimAction;
import io.varve.swath.sim.kernel.SimContext;
import io.varve.swath.sim.kernel.SimRngStream;

/**
 * The measured form: a page's client-side cost is <b>not one thing</b>, so this charges it as the
 * three-plus-one stages measurement actually found, each in the shape its own data supports.
 *
 * <ol>
 *   <li><b>The fetch worker's own per-page work</b> — response unmarshalling, page-model conversion
 *       and the residual around them. Independent per page and flat in concurrency, so it is charged
 *       on the receiving actor's own timeline, in parallel with every other worker's. It dominates:
 *       roughly six to ten milliseconds a page against everything else's fractions of one.</li>
 *   <li><b>The checkpoint commit</b> — a single serial writer every page waits on before it may emit
 *       (the durability ordering the engine guarantees). Charged as a one-capacity queue, so its cost
 *       to a page is its own service time <em>plus</em> whatever was already queued: the measured
 *       per-page wait grows several-fold with concurrency while the service time barely moves, which
 *       is the signature of contention and not of a per-page cost.</li>
 *   <li><b>The consumer stage</b> — one sink, serial, the same shape. Its service rate is a real
 *       ceiling on how fast the client can absorb pages, independently corroborated against a
 *       measured plateau.</li>
 *   <li><b>An optional offload pool</b> — encode work a sink hands to its own threads, measured to be
 *       parallel and off the page's critical path. Charged to a multi-lane queue that nothing waits
 *       on, so it consumes capacity and can saturate, but never delays the page that caused it.</li>
 * </ol>
 *
 * <h2>Serial or overlapping? Serial for 1–3, parallel for 4 — and why</h2>
 * The measured spans are non-additive by construction (several of them nest), so arithmetic over them
 * cannot settle this; the ordering has to come from what the engine does. A page arrives on a worker,
 * that worker converts it, and only then does the durability commit begin — I/O the same worker
 * awaits before emitting, because emitting first would break the commit-before-emit ordering the
 * checkpoint contract rests on. The emit then goes to a bounded pipeline whose single consumer sets
 * the rate at which pages can leave. Each of those three is genuinely downstream of the last on that
 * page's own timeline, so they are charged in series. The offload pool is the one measured exception:
 * its work happens on other threads, after dispatch, and the dispatch cost alone is what the page
 * pays.
 *
 * <p>The alternative — charging the three in parallel and taking the maximum — was rejected on the
 * consequence rather than the aesthetics: it would let a simulated fleet emit pages faster than the
 * consumer stage can retire them, which is precisely the impossible strategy a client-cost term
 * exists to prevent the simulator from discovering.
 *
 * <h2>Sampling rather than averaging, where the data allows</h2>
 * Two stages have a mean several times their median. For those the term carries a measured quantile
 * ladder and this model draws through {@link SimRngStream#CLIENT_COST} per page (see
 * {@link SampledClientCostTerm}); the rest are charged at their mean, which is all that was published
 * for them. Sampling is what makes a bursty policy pay for its bursts instead of paying the average
 * twice.
 *
 * <h2>What this model may not be validated against</h2>
 * The instrument that replays a fixture over HTTP saturates several times below the real client, flat
 * across a sixteen-fold range of concurrency. High-concurrency behaviour produced here must therefore
 * not be checked against that instrument's page rates while its own ceiling stands: agreement would
 * mean the sim had reproduced the instrument, and disagreement would mean nothing.
 *
 * <p>Stateful (three queues), so one instance belongs to one run — {@link #requireReadyForNewRun()}
 * refuses a carried-over one.
 */
public final class CompositeClientCost implements ClientCostModel {

    /** The event kind the per-page worker-side charge is traced under. */
    public static final String WORKER_CHARGE_EVENT = "client_cost.worker";

    /** The serial checkpoint writer's resource name (its counters and completion events). */
    public static final String CHECKPOINT_RESOURCE = "client_cost.checkpoint";

    /** The serial consumer stage's resource name. */
    public static final String SINK_RESOURCE = "client_cost.sink";

    /** The parallel offload pool's resource name. */
    public static final String OFFLOAD_RESOURCE = "client_cost.offload";

    private final SampledClientCostTerm worker;
    private final SampledClientCostTerm checkpoint;
    private final SampledClientCostTerm sink;
    private final SampledClientCostTerm offload;
    private final FifoServer checkpointServer = new FifoServer(CHECKPOINT_RESOURCE, 1);
    private final FifoServer sinkServer = new FifoServer(SINK_RESOURCE, 1);
    private final FifoServer offloadServer;

    /**
     * @param worker     the fetch worker's own per-page cost (the dominant, parallel term)
     * @param checkpoint the serial checkpoint writer's per-page service time
     * @param sink       the serial consumer stage's per-page service time
     * @param offload    the optional offload pool's per-page cost, or {@code null} for a sink with no
     *                   pool behind it
     * @param offloadLanes how many pages the offload pool encodes at once (ignored when there is none)
     */
    public CompositeClientCost(SampledClientCostTerm worker, SampledClientCostTerm checkpoint,
                               SampledClientCostTerm sink, SampledClientCostTerm offload, int offloadLanes) {
        if (worker == null) {
            throw new MissingSimDependencyException("worker-side per-page client cost term");
        }
        if (checkpoint == null) {
            throw new MissingSimDependencyException("checkpoint writer per-page service term");
        }
        if (sink == null) {
            throw new MissingSimDependencyException("consumer stage per-page service term");
        }
        this.worker = worker;
        this.checkpoint = checkpoint;
        this.sink = sink;
        this.offload = offload;
        this.offloadServer = offload == null ? null : new FifoServer(OFFLOAD_RESOURCE, offloadLanes);
    }

    /**
     * The headline term: the worker-side per-page cost. It is the term a result is read against —
     * it dominates the composite by an order of magnitude — and it carries the provenance every stage
     * here shares.
     */
    @Override
    public ClientCostTerm term() {
        return worker.term();
    }

    /** The serial checkpoint writer's term. */
    public ClientCostTerm checkpointTerm() {
        return checkpoint.term();
    }

    /** The serial consumer stage's term. */
    public ClientCostTerm sinkTerm() {
        return sink.term();
    }

    /** The parallel offload pool's term, or {@code null} when the sink has no pool behind it. */
    public ClientCostTerm offloadTerm() {
        return offload == null ? null : offload.term();
    }

    @Override
    public void chargePage(SimContext ctx, int keys, SimAction onComplete) {
        long workerNanos = worker.drawNanos(keys, ctx.rng(SimRngStream.CLIENT_COST));
        ctx.count(CHARGES_COUNTER, 1);
        ctx.count(NANOS_COUNTER, workerNanos);
        ctx.schedule(workerNanos, WORKER_CHARGE_EVENT, converted -> {
            long commitNanos = checkpoint.drawNanos(keys, converted.rng(SimRngStream.CLIENT_COST));
            converted.count(NANOS_COUNTER, commitNanos);
            checkpointServer.submit(converted, commitNanos, committed -> {
                long sinkNanos = sink.drawNanos(keys, committed.rng(SimRngStream.CLIENT_COST));
                committed.count(NANOS_COUNTER, sinkNanos);
                if (offloadServer != null) {
                    // Dispatched at the same instant the page reaches the consumer stage, and never
                    // waited on: the pool's work is measured to be parallel and off this page's
                    // critical path. It still occupies lanes, so a pool that cannot keep up shows up
                    // as a growing queue on its own counters rather than as free work.
                    long offloadNanos = offload.drawNanos(keys, committed.rng(SimRngStream.CLIENT_COST));
                    committed.count(NANOS_COUNTER, offloadNanos);
                    offloadServer.submit(committed, offloadNanos, encoded -> {
                    });
                }
                sinkServer.submit(committed, sinkNanos, onComplete);
            });
        });
    }

    @Override
    public void requireReadyForNewRun() {
        requireIdle(checkpointServer, CHECKPOINT_RESOURCE);
        requireIdle(sinkServer, SINK_RESOURCE);
        if (offloadServer != null) {
            requireIdle(offloadServer, OFFLOAD_RESOURCE);
        }
    }

    /** Pages waiting for the consumer stage right now — the depth a saturation assertion reads. */
    public int sinkQueueDepth() {
        return sinkServer.queueDepth();
    }

    /** Pages waiting for the checkpoint writer right now. */
    public int checkpointQueueDepth() {
        return checkpointServer.queueDepth();
    }

    private static void requireIdle(FifoServer server, String name) {
        if (!server.isIdle()) {
            throw new IllegalStateException("this " + name + " still holds work from an earlier run ("
                    + server.queueDepth() + " queued); a contended resource belongs to one run");
        }
    }
}
