/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.ClientCostModel;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The sweep shape: one open store, many runs, and the two mistakes that reusing a handle invites.
 *
 * <p>Both mistakes produce numbers rather than errors, which is what makes them worth a test. A cost
 * model carried across legs makes a leg's result depend on its predecessor's; a cumulative meter read
 * per leg attributes the whole sweep's store work to whichever leg happened to read last.
 */
class SimSweepTest {

    @Test
    void oneHandleServesEveryLegAndIsNeverClosedByTheSweep() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(20_000));
        List<PolicyScenario> legs = List.of(
                scenario(2), scenario(4), scenario(8));

        List<SimSweep.Leg> results = SimSweep.run(legs, PolicyRunFixtures::measuredCost, store, null,
                "in-memory dense flat leaf");

        assertThat(results).hasSize(3);
        assertThat(results).allSatisfy(leg -> {
            assertThat(leg.run().completed()).as(leg.run()::describe).isTrue();
            assertThat(leg.run().keysEmitted()).isEqualTo(store.size());
        });
        assertThat(store.closes())
                .as("opening a large fixture costs more than a run does; the sweep never closes it")
                .isZero();
    }

    @Test
    void eachLegGetsAFreshCostModelSoNoLegInheritsAnothersQueue() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(10_000));
        List<PolicyScenario> legs = List.of(scenario(4), scenario(4));

        List<SimSweep.Leg> results = SimSweep.run(legs, PolicyRunFixtures::measuredCost, store, null,
                "fixture");

        assertThat(results.get(1).run().virtualNanos())
                .as("two identical legs must produce identical runs, or a leg is inheriting state")
                .isEqualTo(results.get(0).run().virtualNanos());
        assertThat(results.get(1).run().counters()).isEqualTo(results.get(0).run().counters());
    }

    @Test
    void theSweepTakesASupplierOfCostModelsAndCallsItOncePerLeg() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(5_000));
        List<ClientCostModel> handedOut = new ArrayList<>();

        SimSweep.run(List.of(scenario(2), scenario(4), scenario(8)), () -> {
            ClientCostModel fresh = PolicyRunFixtures.measuredCost();
            handedOut.add(fresh);
            return fresh;
        }, store, null, "fixture");

        assertThat(handedOut).as("one model per leg, and never the same one twice: a stateful model "
                + "left mid-service by a leg that hit a ceiling would carry that queue into the next")
                .hasSize(3).doesNotHaveDuplicates();
        assertThatThrownBy(() -> SimSweep.run(List.of(scenario(2)), null, store, null, "fixture"))
                .as("a sweep takes a supplier of models, never a model")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("supplier");
    }

    @Test
    void storeMetricsAreReportedPerLegAsADeltaNotAsARunningTotal() {
        MeterRegistry registry = new SimpleMeterRegistry();
        Counter reads = Counter.builder("swath.sim.test.reads").register(registry);
        ListingFixtureStore store = new CountingFixtureStore(reads);

        List<SimSweep.Leg> results = SimSweep.run(List.of(scenario(2), scenario(2)),
                PolicyRunFixtures::measuredCost, store, registry, "fixture");

        double firstLeg = results.get(0).storeMetricsDelta().get("swath.sim.test.reads");
        double secondLeg = results.get(1).storeMetricsDelta().get("swath.sim.test.reads");
        assertThat(firstLeg).isPositive();
        assertThat(secondLeg)
                .as("the second leg reads the same fixture the same way; a cumulative meter would have "
                        + "reported roughly twice as much")
                .isEqualTo(firstLeg);
        assertThat(reads.count()).isEqualTo(firstLeg + secondLeg);
    }

    private static PolicyScenario scenario(int workers) {
        return PolicyRunFixtures.unseededScenario(workers, 100, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost());
    }

    /** A fixture store that ticks a registry counter per range read, standing in for a real backend. */
    private static final class CountingFixtureStore extends ListingFixtureStore {

        private final Counter reads;

        private CountingFixtureStore(Counter reads) {
            super(KeyspaceFixtures.denseFlatLeaf(10_000));
            this.reads = reads;
        }

        @Override
        public List<io.varve.swath.replay.protocol.ListedObject> rows(
                io.varve.swath.replay.protocol.ByteKey from, boolean fromInclusive,
                io.varve.swath.replay.protocol.ByteKey toExclusive, int limit,
                io.varve.swath.replay.store.Projection projection) {
            reads.increment();
            return super.rows(from, fromInclusive, toExclusive, limit, projection);
        }
    }
}
