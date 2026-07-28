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
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.model.CallClass;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A descent that never gets an answer gives up and lists anyway.
 *
 * <p>The seed phase is the one part of a run with nothing to fall back on: it happens before any
 * worker exists, and every decision after it is taken over the range set it produced. So the
 * behaviour that matters when its probes stop answering is not that it retries — {@code
 * ProbeRetryBudgetTest} pins that ceiling for the thief's probes and the same budget governs these —
 * but what it does on the far side of the ceiling. The engine's answer is to seed nothing and start
 * the fleet on one range over the whole keyspace, which is slow and complete rather than fast and
 * short; the failure this pins against is the other two, an unbounded retry that never seeds at all
 * and a run that ends holding a partial cut set.
 *
 * <p>{@code seed.abandoned} is a trace record rather than a counter because it happens at most once
 * in a run and carries the reason it happened, which is what a reader of a stalled run's trace needs.
 * The run below is therefore traced.
 */
class SeedProbeAbandonmentTest {

    /** Enough retries that spending the whole ceiling is visible in the timeout count. */
    private static final int PROBE_RETRY_CAP = 3;

    @Test
    void aSeedDescentWhoseProbesNeverAnswerAbandonsAtTheCeilingAndListsTheWholeKeyspace() {
        List<byte[]> keys = KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 400, SubtreeMass.HEAVY_TAILED);
        ListingFixtureStore store = new ListingFixtureStore(keys);

        PolicyRunResult result = SimExecutor.run(scenario(), store, "in-memory deep-nested shared prefix");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted())
                .as("abandoning the descent may cost parallelism; it may not cost a key")
                .isEqualTo(keys.size());

        // The very first probe of the descent is the one that dies: one initial attempt plus the
        // declared ceiling, and then the phase ends.
        assertThat(result.counter(SimExecutor.PROBE_TIMEOUTS_COUNTER))
                .as("the ceiling is spent in full before the descent gives up")
                .isEqualTo(PROBE_RETRY_CAP + 1L);
        assertThat(abandonReason(result)).isEqualTo("probe_retry_cap");
        // No cuts survive an abandoned descent, so the fleet starts on the undivided keyspace.
        assertThat(result.counter("seed.ranges")).isEqualTo(1L);
        assertThat(result.counter(SimExecutor.SEED_PROBES_COUNTER))
                .as("a descent that never planned never reports a probe budget spent")
                .isZero();
    }

    /** The reason recorded with {@code seed.abandoned}, or {@code null} if the run never abandoned. */
    private static String abandonReason(PolicyRunResult result) {
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (entry.kind().equals("seed.abandoned")) {
                return entry.detail();
            }
        }
        return null;
    }

    private static PolicyScenario scenario() {
        return new PolicyScenario(20260727L, 4, 100, new byte[0],
                PolicyScenario.SimSeedMode.SHALLOW, EngineToggles.DEFAULT, seedProbesNeverAnswer(),
                PolicyRunFixtures.zeroedCost("what a seed probe costs is not what this measures"),
                EngineTimeBudgets.engineDefaults().withProbeAttemptRetryCap(PROBE_RETRY_CAP),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, true, PolicyScenario.DEFAULT_MAX_EVENTS);
    }

    /** Pages and pivot probes answer promptly; a seed probe never answers within its attempt budget. */
    private static LatencyModel seedProbesNeverAnswer() {
        Map<CallClass, Long> byClass = new EnumMap<>(CallClass.class);
        byClass.put(CallClass.WORKER_PAGE, TimeUnit.MILLISECONDS.toNanos(30));
        byClass.put(CallClass.PIVOT_PROBE, TimeUnit.MILLISECONDS.toNanos(8));
        byClass.put(CallClass.STRUCTURE_PROBE, TimeUnit.MILLISECONDS.toNanos(8));
        byClass.put(CallClass.SEED_PROBE, TimeUnit.SECONDS.toNanos(30));
        return ConstantLatencyModel.perClass(byClass);
    }
}
