/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * More workers must not make a run slower. That is the whole scaling claim, and it is deliberately
 * the whole of it.
 *
 * <p><b>Why monotonicity and not proportionality.</b> It is tempting to assert that doubling the
 * worker count halves the wall time, and it would be wrong. Real listing work does not divide evenly:
 * a range is claimed whole, so once every range has a worker, further workers have nothing to claim
 * and the run is bounded by its largest range no matter how much concurrency is available. The same
 * shape recurs for reasons that have nothing to do with this driver — pacing intervals, adaptive
 * concurrency windows, and client-side costs are all fixed durations that do not shrink when workers
 * are added. A proportionality assertion would therefore fail for correct behaviour, and, worse,
 * would have to be relaxed with a tolerance until it stopped detecting anything. Monotonicity is
 * weaker and true.
 *
 * <p>The workload here is deliberately uneven ({@code 100/50/25/5} keys) so that the sublinearity is
 * not an artefact of a tolerance but a fact about the schedule: the largest range alone costs eleven
 * calls, so no worker count can bring the run below eleven latencies, and this test asserts that
 * bound rather than papering over it.
 */
class ConcurrencyScalingTest {

    private static final int PAGE_SIZE = 10;
    private static final long LATENCY_NANOS = 1_000L;
    /** Range sizes in keys; uneven on purpose. */
    private static final List<Integer> RANGE_SIZES = List.of(100, 50, 25, 5);

    /** {@code c_r = floor(n_r / 10) + 1} = 11, 6, 3, 1. */
    private static final long LARGEST_RANGE_CALLS = 11;
    /** {@code P = 11 + 6 + 3 + 1}. */
    private static final long TOTAL_CALLS = 21;

    @Test
    void wallTimeNeverIncreasesWithTheWorkerCount() {
        long previous = Long.MAX_VALUE;
        for (int workers : List.of(1, 2, 3, 4, 8, 16)) {
            SimRunResult result = run(workers);
            assertThat(result.wallNanos()).as("wall at T=%d must not exceed wall at the previous T", workers)
                    .isLessThanOrEqualTo(previous);
            assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER))
                    .as("the same work at T=%d", workers).isEqualTo(TOTAL_CALLS);
            previous = result.wallNanos();
        }
    }

    @Test
    void concurrencyActuallyHelpsButNotProportionally() {
        long atOne = run(1).wallNanos();
        long atTwo = run(2).wallNanos();
        long atFour = run(4).wallNanos();

        assertThat(atOne).as("wall = P x L on one worker").isEqualTo(TOTAL_CALLS * LATENCY_NANOS);
        assertThat(atTwo).as("a second worker must actually help, or monotonicity is vacuous here")
                .isLessThan(atOne);
        assertThat(atFour).as("the largest range is indivisible, so it floors the run")
                .isEqualTo(LARGEST_RANGE_CALLS * LATENCY_NANOS);
        assertThat(atFour).as("four workers do NOT give a fourfold speedup, and that is correct")
                .isGreaterThan(atOne / 4);
    }

    private static SimRunResult run(int workers) {
        int totalKeys = RANGE_SIZES.stream().mapToInt(Integer::intValue).sum();
        SimScenario scenario = new SimScenario(
                7L,
                workers,
                PAGE_SIZE,
                ranges(),
                ConstantLatencyModel.uniform(LATENCY_NANOS),
                new IidClientCost(ClientCostTerm.zeroedForExactMode("concurrency scaling monotonicity")),
                EngineTimeBudgets.engineDefaults(),
                false,
                SimScenario.DEFAULT_MAX_EVENTS);
        return SequentialListingDriver.run(scenario, ListingFixtureStore.ofGeneratedKeys(totalKeys));
    }

    private static List<KeyRange> ranges() {
        List<KeyRange> ranges = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < RANGE_SIZES.size(); i++) {
            int end = start + RANGE_SIZES.get(i);
            ByteKey from = start == 0 ? null : ByteKey.copyOf(ListingFixtureStore.key(start));
            ByteKey to = i == RANGE_SIZES.size() - 1 ? null : ByteKey.copyOf(ListingFixtureStore.key(end));
            ranges.add(new KeyRange(from, to));
            start = end;
        }
        return ranges;
    }
}
