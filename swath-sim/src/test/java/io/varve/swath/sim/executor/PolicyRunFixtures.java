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
     * The regime a deployment's own serial tail was timed at: a full page answered in 110 ms and a
     * one-key probe in 35 ms. Belongs with {@link #MEASURED_TAIL_PAGE_SIZE} — the two were measured
     * together and neither means anything alone, since what a footrace turns on is how many keys a
     * victim drains while a thief is probing, which is the page's <em>latency</em> and its <em>size</em>
     * at once.
     *
     * <p><b>This is a bench regime, not the live store's call-class profile.</b> Its probe:page ratio
     * is 35/110 = 0.32; the live store's is 121/223 = 0.54 ({@link #LIVE_S3_LATENCY}), and that ratio —
     * not either absolute — is what sets how many owner pages a steal attempt's window is worth. The
     * synthetic benches keep running here because their pinned tables were taken here and each states
     * the regime it was taken at; nothing about the numbers they pin transfers to a run at the live
     * ratio, and the real-listing instruments run at {@link #LIVE_S3_LATENCY} for exactly that reason.
     */
    static final LatencyModel MEASURED_TAIL_LATENCY = perClass(
            TimeUnit.MILLISECONDS.toNanos(110), TimeUnit.MILLISECONDS.toNanos(35));

    /**
     * The live store's own call-class profile, measured on the {@code nara-1950-census} live and replay
     * profile on 2026-07-28 (serial-tail campaign, experiment E-11): a full page answered in 223 ms, a
     * one-key pivot probe in 121 ms, and a delimited probe — structure or seed descent — in 223 ms. Same
     * page size as the regime above ({@link #MEASURED_TAIL_PAGE_SIZE}).
     *
     * <p><b>The load-bearing property is the ratio between the call classes, not the absolutes.</b> A
     * thief snapshots a victim's cursor, spends probes placing a pivot, and loses if the victim drained
     * past it meanwhile — so the window it has to win in, measured in the owner's pages, is probe cost
     * over page cost times the probes an attempt issues. At the live 121/223 that window is ≈ 2 owner
     * pages, which is what the engine was measured losing every tail race inside; at
     * {@link #MEASURED_TAIL_LATENCY}'s 35/110 it is under one page, and a fleet modelled there wins
     * races the engine loses.
     *
     * <p><b>Flat, deliberately.</b> The measured structure probe carries a per-common-prefix term on top
     * of its base (223 ms + 2 ms/cp on the profile above); no {@link LatencyModel} can express it, since
     * the cost model is not told a probe's fan-out. Modelling it is a disclosed non-goal of this profile
     * rather than an oversight — it needs a change to what {@code SimListingView} reports.
     */
    static final LatencyModel LIVE_S3_LATENCY = perClass(TimeUnit.MILLISECONDS.toNanos(223),
            TimeUnit.MILLISECONDS.toNanos(121), TimeUnit.MILLISECONDS.toNanos(223));

    /** The page size both measured regimes were taken at: a full listing page. */
    static final int MEASURED_TAIL_PAGE_SIZE = 1_000;

    private PolicyRunFixtures() {
    }

    static LatencyModel perClass(long pageNanos, long probeNanos) {
        return perClass(pageNanos, probeNanos, probeNanos);
    }

    /**
     * A profile that prices a delimited probe apart from a one-key one — the shape a real store has,
     * where a rollup pays for the server-side walk and a pivot probe is round trip alone. The seed
     * descent's probes are delimited too, so they are priced with the structure probes.
     */
    static LatencyModel perClass(long pageNanos, long pivotNanos, long structureNanos) {
        Map<CallClass, Long> byClass = new EnumMap<>(CallClass.class);
        byClass.put(CallClass.WORKER_PAGE, pageNanos);
        byClass.put(CallClass.PIVOT_PROBE, pivotNanos);
        byClass.put(CallClass.STRUCTURE_PROBE, structureNanos);
        byClass.put(CallClass.SEED_PROBE, structureNanos);
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
