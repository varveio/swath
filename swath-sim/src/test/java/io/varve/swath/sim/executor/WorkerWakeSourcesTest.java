/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * What can wake a parked worker, read back out of a trace rather than out of a document.
 *
 * <p>Four signals cut a park short, they are the engine's four, and their relative frequency is the
 * whole reason a run dispatches as many events as it does — the per-page one alone fires once for every
 * page any worker commits. That list is written down in {@code docs/executor-ordering.md}; this test is
 * what stops it going stale, by taking the set from the run itself. A fifth wake source added without
 * a thought fails here, and so does one quietly removed.
 */
class WorkerWakeSourcesTest {

    @Test
    void aRunWakesParkedWorkersFromExactlyTheFourDocumentedSignals() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(20_000));
        LatencyModel latency = PolicyRunFixtures.perClass(TimeUnit.MILLISECONDS.toNanos(30),
                TimeUnit.MILLISECONDS.toNanos(1));
        PolicyScenario scenario = new PolicyScenario(20260727L, 8, 100, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, latency,
                PolicyRunFixtures.zeroedCost("a wake is about ordering, not about cost"),
                EngineTimeBudgets.engineDefaults(), PolicyScenario.FaultDisposition.RIDE_OUT, 0, true,
                PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        Map<String, Long> wakes = new TreeMap<>();
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (entry.kind().startsWith(SimExecutor.WAKE_EVENT_PREFIX)) {
                wakes.merge(entry.kind().substring(SimExecutor.WAKE_EVENT_PREFIX.length()), 1L, Long::sum);
            }
        }

        assertThat(wakes).containsOnlyKeys(SimExecutor.WAKE_CHILD_PUBLISHED,
                SimExecutor.WAKE_PAGE_COMMITTED, SimExecutor.WAKE_RANGE_COMPLETED,
                SimExecutor.WAKE_STEAL_ATTEMPT_FINISHED);
        assertThat(wakes.get(SimExecutor.WAKE_PAGE_COMMITTED))
                .as("the per-commit signal dominates: it fires for every page any worker commits")
                .isGreaterThan(wakes.get(SimExecutor.WAKE_CHILD_PUBLISHED)
                        + wakes.get(SimExecutor.WAKE_RANGE_COMPLETED));
    }
}
