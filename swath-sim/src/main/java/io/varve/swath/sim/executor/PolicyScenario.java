/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.model.ClientCostModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import io.varve.swath.sim.model.MissingSimDependencyException;

/**
 * Everything a <b>policy</b> run needs except the store handle. The sibling of the synthetic driver's
 * scenario, and different from it in exactly one structural way that matters: its workload is not a
 * list of ranges.
 *
 * <p><b>Why the workload cannot be a range list here.</b> Under real policies nobody knows the ranges
 * in advance. The seed planner decides how the keyspace is first cut, from probes it issues against
 * this very store, and from then on the set of ranges is rewritten continuously by owner-side splits
 * and by thieves carving tails off drainers. Handing such a run a pre-declared range list would be
 * declaring the answer to the first question the run exists to ask. The workload is therefore "this
 * bucket, under this seed mode", and everything after that is the policies' own doing.
 *
 * <p>Every input is required, for the same reason the synthetic scenario requires its own: a default
 * would be an unstated claim about the world. The client-cost model in particular is refused rather
 * than defaulted to zero.
 *
 * @param seed           the run's one seed; every draw is derived from it
 * @param workerCount    the fleet size, which is also the adaptive controller's ceiling
 * @param pageSize       keys per listing call (a full page for the real protocol is 1000)
 * @param scanPrefix     the listing prefix, {@code null} or empty for the whole bucket
 * @param seedMode       how the keyspace is cut before any worker starts
 * @param toggles        the engine ablation namespace the policies are constructed with
 * @param latency        per-call service times
 * @param clientCost     what a page costs the client after it arrives, and in which form
 * @param budgets        the engine time budgets this run declares
 * @param storeServerCapacity how many calls the modelled store serves at once, or {@code 0} for a store
 *                       that never queues. Zero is the honest default: measurement of the live system
 *                       never reached a store ceiling, so a queueing store would be modelling a system
 *                       nobody has. A positive value is the deliberate exception — the shape that makes
 *                       a call's completion instant depend on how many other calls are in flight
 * @param recordEventLog whether to retain the full trace — on for a determinism or invariant check, off
 *                       for a sweep leg, where the trace dominates the cost
 * @param maxEvents      the runaway guard, counted in events dispatched
 */
public record PolicyScenario(
        long seed,
        int workerCount,
        int pageSize,
        byte[] scanPrefix,
        SimSeedMode seedMode,
        EngineToggles toggles,
        LatencyModel latency,
        ClientCostModel clientCost,
        EngineTimeBudgets budgets,
        int storeServerCapacity,
        boolean recordEventLog,
        long maxEvents) {

    /** The event cap a scenario gets when it does not choose one; ample for any in-repo fixture. */
    public static final long DEFAULT_MAX_EVENTS = 100_000_000L;

    /** How the keyspace is cut before the fleet starts. */
    public enum SimSeedMode {
        /** One range over the whole keyspace: every cut after this is a policy decision at run time. */
        NONE,
        /** The shallow structure descent: bounded probes, then a tiling cut set. */
        SHALLOW
    }

    public PolicyScenario {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive, got " + workerCount);
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive, got " + pageSize);
        }
        if (seedMode == null) {
            throw new MissingSimDependencyException("seed mode (how the keyspace is cut before the "
                    + "fleet starts)");
        }
        if (toggles == null) {
            throw new MissingSimDependencyException("engine toggles (the ablation namespace the "
                    + "policies are constructed with)");
        }
        if (latency == null) {
            throw new MissingSimDependencyException("latency model (per-call-class service times)");
        }
        if (clientCost == null) {
            throw new MissingSimDependencyException("client cost model (per-page client service cost, "
                    + "and whether it is independent, contended, or a composite of both)");
        }
        if (budgets == null) {
            throw new MissingSimDependencyException("engine time budgets (probe budget, attempt "
                    + "timeouts, pacing windows, adaptive-concurrency windows, max duration)");
        }
        if (storeServerCapacity < 0) {
            throw new IllegalArgumentException("storeServerCapacity must be >= 0 (0 = a store that "
                    + "never queues), got " + storeServerCapacity);
        }
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive, got " + maxEvents);
        }
        scanPrefix = scanPrefix == null ? new byte[0] : scanPrefix.clone();
    }

    @Override
    public byte[] scanPrefix() {
        return scanPrefix.clone();
    }

    /** This scenario at {@code newWorkerCount} workers, everything else held fixed. */
    public PolicyScenario withWorkerCount(int newWorkerCount) {
        return new PolicyScenario(seed, newWorkerCount, pageSize, scanPrefix, seedMode, toggles, latency,
                clientCost, budgets, storeServerCapacity, recordEventLog, maxEvents);
    }

    /** This scenario at {@code newSeed}, everything else held fixed. */
    public PolicyScenario withSeed(long newSeed) {
        return new PolicyScenario(newSeed, workerCount, pageSize, scanPrefix, seedMode, toggles,
                latency, clientCost, budgets, storeServerCapacity, recordEventLog, maxEvents);
    }

    /**
     * This scenario with a fresh client-cost model — the substitution a sweep must make on every leg,
     * because a composite or contended model owns queues that belong to exactly one run.
     */
    public PolicyScenario withClientCost(ClientCostModel newClientCost) {
        return new PolicyScenario(seed, workerCount, pageSize, scanPrefix, seedMode, toggles, latency,
                newClientCost, budgets, storeServerCapacity, recordEventLog, maxEvents);
    }

    /** This scenario with the trace turned on or off — on for a determinism check, off for a sweep. */
    public PolicyScenario withEventLog(boolean record) {
        return new PolicyScenario(seed, workerCount, pageSize, scanPrefix, seedMode, toggles, latency,
                clientCost, budgets, storeServerCapacity, record, maxEvents);
    }
}
