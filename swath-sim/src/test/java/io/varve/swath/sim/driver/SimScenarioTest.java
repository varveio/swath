/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.sim.model.ClientCostModel;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import io.varve.swath.sim.model.LatencyModel;
import io.varve.swath.sim.model.MissingSimDependencyException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A scenario refuses to be half-specified, and says which half is missing. */
class SimScenarioTest {

    private static final LatencyModel LATENCY = ConstantLatencyModel.uniform(1_000L);
    private static final ClientCostModel CLIENT_COST =
            new IidClientCost(ClientCostTerm.zeroedForExactMode("scenario validation"));
    private static final List<KeyRange> RANGES = List.of(KeyRange.wholeKeyspace());

    @Test
    void eachMissingModelIsNamedAsAMissingDependency() {
        assertThatThrownBy(() -> scenario(1, 10, RANGES, null, CLIENT_COST, EngineTimeBudgets.engineDefaults()))
                .isInstanceOf(MissingSimDependencyException.class).hasMessageContaining("latency model");
        assertThatThrownBy(() -> scenario(1, 10, RANGES, LATENCY, null, EngineTimeBudgets.engineDefaults()))
                .isInstanceOf(MissingSimDependencyException.class).hasMessageContaining("client cost model");
        assertThatThrownBy(() -> scenario(1, 10, RANGES, LATENCY, CLIENT_COST, null))
                .isInstanceOf(MissingSimDependencyException.class)
                .hasMessageContaining("engine time budgets");
    }

    @Test
    void degenerateWorkloadsAreRejected() {
        EngineTimeBudgets budgets = EngineTimeBudgets.engineDefaults();

        assertThatThrownBy(() -> scenario(0, 10, RANGES, LATENCY, CLIENT_COST, budgets))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("workerCount");
        assertThatThrownBy(() -> scenario(1, 0, RANGES, LATENCY, CLIENT_COST, budgets))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pageSize");
        assertThatThrownBy(() -> scenario(1, 10, List.of(), LATENCY, CLIENT_COST, budgets))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least one range");
    }

    @Test
    void anInvertedRangeListsNothingAndIsRejected() {
        ByteKey low = ByteKey.copyOf("aaa".getBytes(StandardCharsets.UTF_8));
        ByteKey high = ByteKey.copyOf("zzz".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> new KeyRange(high, low)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KeyRange(low, low)).isInstanceOf(IllegalArgumentException.class);
        assertThat(KeyRange.wholeKeyspace().fromInclusive()).isNull();
    }

    @Test
    void theRunApiRefusesToInventAStoreHandle() {
        SimScenario scenario = scenario(1, 10, RANGES, LATENCY, CLIENT_COST,
                EngineTimeBudgets.engineDefaults());

        assertThatThrownBy(() -> SequentialListingDriver.run(scenario, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already-open store handle");
    }

    @Test
    void theSweepAxesAreHeldFixedApartFromTheOneBeingVaried() {
        SimScenario base = scenario(2, 10, RANGES, LATENCY, CLIENT_COST,
                EngineTimeBudgets.engineDefaults());

        assertThat(base.withWorkerCount(9)).isEqualTo(new SimScenario(base.seed(), 9, base.pageSize(),
                base.ranges(), base.latency(), base.clientCost(), base.budgets(), base.recordEventLog(),
                base.maxEvents()));
        assertThat(base.withSeed(77L).seed()).isEqualTo(77L);
        assertThat(base.withSeed(77L).withSeed(base.seed())).isEqualTo(base);
    }

    private static SimScenario scenario(int workers, int pageSize, List<KeyRange> ranges,
                                        LatencyModel latency, ClientCostModel clientCost,
                                        EngineTimeBudgets budgets) {
        return new SimScenario(1L, workers, pageSize, ranges, latency, clientCost, budgets, false,
                SimScenario.DEFAULT_MAX_EVENTS);
    }
}
