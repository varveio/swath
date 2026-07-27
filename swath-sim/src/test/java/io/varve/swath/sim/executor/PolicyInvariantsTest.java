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
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The kernel's analytic invariants, restated with the real policies wired in.
 *
 * <p>Wiring policies removes most closed forms, on purpose: the whole reason to run them is that what
 * they do is not predictable from the fixture. Two things survive, and both are worth pinning.
 *
 * <p><b>Where the policy declines to act, the arithmetic must still be exact.</b> One worker, nothing
 * to steal from, the owner-side split ablated off: the run degenerates to "list this range to the end",
 * whose cost is `floor(n / pageSize) + 1` calls and whose duration is that many latencies. An equality,
 * not a tolerance — with constant latency and the cost term deliberately zeroed there is nothing left
 * for a discrepancy to be, except a defect in the executor's own clock or ordering.
 *
 * <p><b>Where it does act, scaling must be monotonic and must not be linear.</b> More workers may not
 * make a run slower, and they will not make it proportionally faster: a range is claimed whole, pacing
 * windows and client costs are fixed durations, and the concurrency controller ramps on its own clock
 * rather than at the fleet's convenience. A test asserting proportional speedup would be asserting a
 * bug.
 */
class PolicyInvariantsTest {

    private static final long LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final LatencyModel CONSTANT = PolicyRunFixtures.perClass(LATENCY_NANOS, LATENCY_NANOS);
    private static final int PAGE_SIZE = 100;

    @Test
    void aLoneWorkerWithSplittingAblatedOffCostsExactlyTheClosedForm() {
        int keys = 4_000;
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(keys));
        PolicyScenario scenario = new PolicyScenario(20260727L, 1, PAGE_SIZE, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT.withOwnerSplit(false), CONSTANT,
                PolicyRunFixtures.zeroedCost("the closed form is arithmetic, not a prediction"),
                EngineTimeBudgets.engineDefaults(), 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        long expectedCalls = keys / PAGE_SIZE + 1;   // the last, short page is how a lister learns it is done
        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(keys);
        assertThat(result.storeCalls()).isEqualTo(expectedCalls);
        assertThat(result.virtualNanos()).isEqualTo(expectedCalls * LATENCY_NANOS);
    }

    @Test
    void scalingIsMonotonicAndNotProportional() {
        List<byte[]> keys = KeyspaceFixtures.denseFlatLeaf(60_000);
        List<Long> durations = new ArrayList<>();
        for (int workers : new int[] {1, 2, 4, 8}) {
            PolicyScenario scenario = PolicyRunFixtures.unseededScenario(workers, PAGE_SIZE, CONSTANT,
                    PolicyRunFixtures.zeroedCost("scaling is about when calls happen, not what they cost"));

            PolicyRunResult result = SimExecutor.run(scenario, new ListingFixtureStore(keys), "fixture");

            assertThat(result.completed()).as(result::describe).isTrue();
            assertThat(result.keysEmitted()).as("no worker count may lose a key").isEqualTo(keys.size());
            durations.add(result.virtualNanos());
        }

        assertThat(durations).as("more workers never make a run slower")
                .isSortedAccordingTo(Comparator.reverseOrder());
        assertThat(durations.getLast())
                .as("a range is claimed whole and the controller ramps on its own clock, so eight "
                        + "workers cannot be eight times one")
                .isGreaterThan(durations.getFirst() / 8);
        assertThat(durations.getLast())
                .as("but they must be worth having at all").isLessThan(durations.getFirst());
    }

    @Test
    void everyKeyIsEmittedExactlyOnceHoweverTheKeyspaceIsCut() {
        // Splitting rewrites the range set continuously, so the tiling claim is worth stating against a
        // shape where a great many splits happen: no gap (a lost key) and no overlap (a duplicated one)
        // are the two failures a split protocol can have, and the emitted count catches both at once
        // only because the fixture's keys are distinct.
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(100_000));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.unseededScenario(8, PAGE_SIZE, CONSTANT,
                        PolicyRunFixtures.measuredCost()), store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        assertThat(result.nodesCreated()).as("and the run really did cut it, many times").isGreaterThan(10);
    }
}
