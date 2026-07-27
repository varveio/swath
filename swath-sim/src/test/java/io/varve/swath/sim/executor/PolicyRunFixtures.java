/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.model.CallClass;
import io.varve.swath.sim.model.ClientCostModel;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import io.varve.swath.sim.model.LatencyModel;
import io.varve.swath.sim.model.MeasuredClientCost;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Scenario shapes the policy-run tests share, so each test states only what it varies. */
final class PolicyRunFixtures {

    /** A store that answers a page in 30 ms and a one-key probe in 8 ms — a plausible remote store. */
    static final LatencyModel REMOTE_LATENCY = perClass(
            TimeUnit.MILLISECONDS.toNanos(30), TimeUnit.MILLISECONDS.toNanos(8));

    /**
     * The regime measured on a real deployment's own serial tail: a full page answered in 110 ms and a
     * one-key probe in 35 ms. Belongs with {@link #MEASURED_TAIL_PAGE_SIZE} — the two were measured
     * together and neither means anything alone, since what a footrace turns on is how many keys a
     * victim drains while a thief is probing, which is the page's <em>latency</em> and its <em>size</em>
     * at once.
     */
    static final LatencyModel MEASURED_TAIL_LATENCY = perClass(
            TimeUnit.MILLISECONDS.toNanos(110), TimeUnit.MILLISECONDS.toNanos(35));

    /** The page size {@link #MEASURED_TAIL_LATENCY} was measured at: a full listing page. */
    static final int MEASURED_TAIL_PAGE_SIZE = 1_000;

    private PolicyRunFixtures() {
    }

    static LatencyModel perClass(long pageNanos, long probeNanos) {
        Map<CallClass, Long> byClass = new EnumMap<>(CallClass.class);
        byClass.put(CallClass.WORKER_PAGE, pageNanos);
        byClass.put(CallClass.PIVOT_PROBE, probeNanos);
        byClass.put(CallClass.STRUCTURE_PROBE, probeNanos);
        byClass.put(CallClass.SEED_PROBE, probeNanos);
        return ConstantLatencyModel.perClass(byClass);
    }

    /** The measured client cost, in the shape it was measured: a composite of named stages. */
    static ClientCostModel measuredCost() {
        return MeasuredClientCost.composite(MeasuredClientCost.SinkKind.TEXT);
    }

    /** A deliberately zeroed cost, for a run whose point is a closed form rather than a prediction. */
    static ClientCostModel zeroedCost(String why) {
        return new IidClientCost(ClientCostTerm.zeroedForExactMode(why));
    }

    /**
     * A scenario that seeds nothing: one range over the whole keyspace, so every cut is a run-time
     * policy decision. The shape that isolates splitting and stealing from the seed descent.
     */
    static PolicyScenario unseededScenario(int workers, int pageSize, LatencyModel latency,
                                           ClientCostModel cost) {
        return new PolicyScenario(20260727L, workers, pageSize, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, latency, cost,
                EngineTimeBudgets.engineDefaults(),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);
    }

    /** The default policy scenario: today's engine, its own budgets, a shallow seed. */
    static PolicyScenario scenario(int workers, int pageSize, LatencyModel latency, ClientCostModel cost) {
        return new PolicyScenario(20260727L, workers, pageSize, new byte[0],
                PolicyScenario.SimSeedMode.SHALLOW, EngineToggles.DEFAULT, latency, cost,
                EngineTimeBudgets.engineDefaults(),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);
    }
}
