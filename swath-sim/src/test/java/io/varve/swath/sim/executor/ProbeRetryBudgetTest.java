/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.KeyspaceFixtures.SubtreeMass;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.CallClass;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A declared probe-retry ceiling is the ceiling the run uses.
 *
 * <p>Every time budget here is a stated input rather than a constant, because a budget only means
 * something relative to the latencies it bounds — and an input that is silently clamped is worse than
 * one that is missing, since the run still produces a number and the number is answering a different
 * question. The retry ceiling is the easiest of them to clamp by accident: the first attempt's timeout
 * path and the retried attempt's are separate handlers, and a retried probe that fail-fasts
 * unconditionally agrees with the engine's default of one retry while disagreeing with every other
 * value.
 *
 * <p>The store here answers a structure probe more slowly than the probe's own attempt budget, so every
 * one of them times out and the ceiling is the only thing that decides how many are issued: one initial
 * attempt plus the cap.
 */
class ProbeRetryBudgetTest {

    /** Pages and pivot probes answer promptly; a structure probe never answers in time. */
    private static final LatencyModel STRUCTURE_PROBES_NEVER_ANSWER = structureProbesNeverAnswer();

    @Test
    void aDeclaredProbeRetryCeilingIsSpentInFullAndCountedOnEveryAttempt() {
        PolicyRunResult oneRetry = run(1);
        PolicyRunResult threeRetries = run(3);

        assertThat(oneRetry.completed()).as(oneRetry::describe).isTrue();
        assertThat(threeRetries.completed()).as(threeRetries::describe).isTrue();

        // Two structure-probe calls per cascade that reaches one under the engine's default, four under
        // a ceiling of three: the initial attempt plus the ceiling, in both cases.
        assertThat(callsPerCascade(threeRetries))
                .as("a scenario that declares three retries gets three")
                .isEqualTo(2.0 * callsPerCascade(oneRetry));
        assertThat(callsPerCascade(oneRetry)).isEqualTo(2.0);

        // The victim-streak tally that eventually suppresses structure probing counts the retried
        // attempts too: one per timed-out call, not one per cascade.
        assertThat(threeRetries.counter("STRUCTURE.probe_timed_out"))
                .as("the evidence a suppression is made of must include the retries")
                .isEqualTo(threeRetries.counter("store.calls.structure_probe"));
        assertThat(oneRetry.counter("STRUCTURE.probe_timed_out"))
                .isEqualTo(oneRetry.counter("store.calls.structure_probe"));
    }

    /** Structure-probe calls per steal cascade that issued one at all. */
    private static double callsPerCascade(PolicyRunResult result) {
        long calls = result.counter("store.calls.structure_probe");
        long cascades = result.counter("RETRY.probe_retry_cap_failfast");
        assertThat(cascades).as("the run must reach a structure probe, or this measures nothing")
                .isPositive();
        return (double) calls / cascades;
    }

    private static PolicyRunResult run(int probeRetryCap) {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 400, SubtreeMass.HEAVY_TAILED));
        PolicyScenario scenario = new PolicyScenario(20260727L, 8, 100, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, STRUCTURE_PROBES_NEVER_ANSWER,
                PolicyRunFixtures.zeroedCost("a retry ceiling is about how many calls, not their cost"),
                EngineTimeBudgets.engineDefaults().withProbeAttemptRetryCap(probeRetryCap),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);
        return SimExecutor.run(scenario, store, "in-memory deep-nested shared prefix");
    }

    private static LatencyModel structureProbesNeverAnswer() {
        Map<CallClass, Long> byClass = new EnumMap<>(CallClass.class);
        byClass.put(CallClass.WORKER_PAGE, TimeUnit.MILLISECONDS.toNanos(30));
        byClass.put(CallClass.PIVOT_PROBE, TimeUnit.MILLISECONDS.toNanos(8));
        byClass.put(CallClass.SEED_PROBE, TimeUnit.MILLISECONDS.toNanos(8));
        byClass.put(CallClass.STRUCTURE_PROBE, TimeUnit.SECONDS.toNanos(30));
        return ConstantLatencyModel.perClass(byClass);
    }
}
