/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import io.varve.swath.sim.model.ClientCostModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import io.varve.swath.sim.model.MissingSimDependencyException;
import java.util.List;

/**
 * Everything a simulated run needs except the store handle: the seed, the workload, the physics, and
 * the declared budgets. Two runs of one scenario at one seed against one store produce identical
 * results, which is the property the whole design exists to provide.
 *
 * <p>Every input is required. There is no partially-specified scenario, because each of the defaults
 * a scenario could plausibly fall back to is a claim about the world that would then go unstated —
 * most sharply the client-cost model, whose absence is rejected as a
 * {@link MissingSimDependencyException} rather than treated as zero.
 *
 * @param seed          the run's one seed; every draw is derived from it
 * @param workerCount   how many workers run concurrently
 * @param pageSize      keys requested per store call (a full page for the real protocol is 1000)
 * @param ranges        the work, in claim order; workers take them first-come-first-served
 * @param latency       per-call service times
 * @param clientCost    what a page costs the client after it arrives, and in which form
 * @param budgets       the engine time budgets this run declares
 * @param recordEventLog whether to retain the full trace — on for a determinism or invariant check,
 *                      off for a sweep leg, where the trace dominates the cost
 * @param maxEvents     the runaway guard
 */
public record SimScenario(
        long seed,
        int workerCount,
        int pageSize,
        List<KeyRange> ranges,
        LatencyModel latency,
        ClientCostModel clientCost,
        EngineTimeBudgets budgets,
        boolean recordEventLog,
        long maxEvents) {

    /** The event cap a scenario gets when it does not choose one; ample for any in-repo fixture. */
    public static final long DEFAULT_MAX_EVENTS = 100_000_000L;

    public SimScenario {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive, got " + workerCount);
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive, got " + pageSize);
        }
        if (ranges == null || ranges.isEmpty()) {
            throw new IllegalArgumentException("a scenario must list at least one range");
        }
        if (latency == null) {
            throw new MissingSimDependencyException("latency model (per-call-class service times)");
        }
        if (clientCost == null) {
            throw new MissingSimDependencyException("client cost model (per-page client service cost, "
                    + "and whether it is independent or contended)");
        }
        if (budgets == null) {
            throw new MissingSimDependencyException("engine time budgets (probe budget, pacing windows, "
                    + "max duration)");
        }
        ranges = List.copyOf(ranges);
    }

    /**
     * This scenario at {@code newWorkerCount} workers, everything else held fixed — the one axis a
     * concurrency-scaling sweep varies.
     */
    public SimScenario withWorkerCount(int newWorkerCount) {
        return new SimScenario(seed, newWorkerCount, pageSize, ranges, latency, clientCost, budgets,
                recordEventLog, maxEvents);
    }

    /** This scenario at {@code newSeed}, everything else held fixed. */
    public SimScenario withSeed(long newSeed) {
        return new SimScenario(newSeed, workerCount, pageSize, ranges, latency, clientCost, budgets,
                recordEventLog, maxEvents);
    }
}
