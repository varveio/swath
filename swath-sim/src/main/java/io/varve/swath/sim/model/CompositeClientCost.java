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
 * Charges the measured client cost: worker work, checkpoint commit, and sink service are serial on a
 * page's critical path; optional sink offload is dispatched in parallel and is never awaited.
 *
 * <p>Stages with a measured quantile ladder draw once per page from {@link SimRngStream#CLIENT_COST};
 * other stages use their published service cost. The worker term is the headline provenance. This
 * stateful model belongs to one run; {@link #requireReadyForNewRun()} requires every queue to be idle.
 */
public final class CompositeClientCost implements ClientCostModel {

    /** The event kind the per-page worker-side charge is traced under. */
    public static final String WORKER_CHARGE_EVENT = "client_cost.worker";

    /** The serial checkpoint writer's resource name. */
    public static final String CHECKPOINT_RESOURCE = "client_cost.checkpoint";

    /** The serial consumer stage's resource name. */
    public static final String SINK_RESOURCE = "client_cost.sink";

    /** The optional parallel offload pool's resource name. */
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

    /** The headline worker-side term and provenance for this composite. */
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
                    // Dispatch is off the page path; its finite pool still records its own pressure.
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
