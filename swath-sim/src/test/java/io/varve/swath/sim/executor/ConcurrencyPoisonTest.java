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
import io.varve.swath.sim.model.OccupancyScaledLatencyModel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * <b>Concurrency poison</b>: a store that answers a wider fleet more slowly, and the one control rung
 * that exists to notice.
 *
 * <p>Every other regime worth simulating is client-shaped, because measurement of the live system never
 * reached a store ceiling at the rates a real run drives. This is the deliberate exception. The
 * adaptive controller's latency-freeze rung reacts to one thing and one thing only — the latency of
 * <em>successful</em> attempts inflating against a rolling-minimum baseline — and nothing in a healthy
 * or a throttling store produces that. A store whose service time rises with occupancy does, which
 * makes this the rung's only real test.
 *
 * <p>It is also a test that cannot be staged against a real store on purpose, and that is the argument
 * for the simulator existing: here it costs one constructor argument, and the fleet's reaction is
 * observable event by event rather than inferred afterwards from a rate that moved.
 *
 * <p>The freeze is a <b>growth gate, never a decrease</b>. The run below must still finish, and finish
 * having emitted every key: a rung that stalled the fleet instead of holding it would be a defect, and
 * the assertions say so rather than only checking that the counter fired.
 */
class ConcurrencyPoisonTest {

    /** 30 ms uncontended, plus 40 ms for every other call already in flight, capped at five seconds. */
    private static final LatencyModel POISONED = new OccupancyScaledLatencyModel(
            PolicyRunFixtures.perClass(TimeUnit.MILLISECONDS.toNanos(30), TimeUnit.MILLISECONDS.toNanos(8)),
            TimeUnit.MILLISECONDS.toNanos(40), TimeUnit.SECONDS.toNanos(5));

    @Test
    void aStoreThatSlowsUnderConcurrencyFreezesGrowthWithoutStallingTheRun() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(400_000));
        PolicyScenario scenario = new PolicyScenario(20260727L, 32, 1_000, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, POISONED,
                PolicyRunFixtures.measuredCost(), EngineTimeBudgets.engineDefaults(), 0, false,
                PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).as("a frozen fleet is still a working fleet")
                .isEqualTo(store.size());
        assertThat(result.counter("FREEZE.latency_inflation"))
                .as("the rung that reacts to a degrading store, engaged").isPositive();
        assertThat(result.finalConcurrencyTarget())
                .as("held below the ceiling the fleet was allowed to reach")
                .isLessThan(scenario.workerCount());
        assertThat(result.counter("AIMD.votes") + result.counter("AIMD.timeout_shed"))
                .as("a freeze is a growth gate, not a decrease: nothing here may lower the target")
                .isZero();
    }

    /**
     * The control. The same fixture and fleet against a store that does <em>not</em> degrade must not
     * freeze at all — otherwise the assertion above would be satisfied by any run at all, and the
     * fixture would be measuring the simulator's own behaviour rather than the store's.
     */
    @Test
    void anUnpoisonedStoreOfTheSameShapeNeverFreezes() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(400_000));
        PolicyScenario scenario = new PolicyScenario(20260727L, 32, 1_000, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost(), EngineTimeBudgets.engineDefaults(), 0, false,
                PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.counter("FREEZE.latency_inflation")).isZero();
    }
}
