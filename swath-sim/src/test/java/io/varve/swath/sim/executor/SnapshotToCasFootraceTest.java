/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.LatencyModel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The race the simulator exists to be able to lose.
 *
 * <p>A thief reads its victim's cursor and bound, places a pivot ahead of that cursor, and spends
 * probes proving the pivot is worth splitting at. All of that is speculative: the victim is draining
 * the whole time, and by the instant the thief proposes its split the cursor may already have passed
 * the pivot. The proposal then has to fail — the range above the pivot is no longer the victim's tail,
 * it is keys the victim has already emitted — and a simulator that quietly let it succeed would be
 * counting splits a real run never gets, on exactly the workloads where stealing is hardest.
 *
 * <p>Nothing here is staged. The race is produced by making the probes slow relative to the pages, the
 * way a real one is: the widened window between a snapshot and its re-validation is the interval two
 * probe round trips take, and everything the victim does in that interval happens in event bodies of
 * its own.
 */
class SnapshotToCasFootraceTest {

    /**
     * Probes just inside their own attempt budget, pages an order of magnitude faster. This is the
     * shape a real deployment produces when a probe crosses a cold path — and it is the shape the
     * futility pacing exists for.
     */
    private static final LatencyModel SLOW_PROBES_FAST_PAGES = PolicyRunFixtures.perClass(
            TimeUnit.MILLISECONDS.toNanos(1), TimeUnit.MILLISECONDS.toNanos(2_900));

    @Test
    void aVictimThatDrainsPastThePivotMakesTheReValidationFail() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(60_000));
        PolicyScenario scenario = PolicyRunFixtures.unseededScenario(2, 10, SLOW_PROBES_FAST_PAGES,
                PolicyRunFixtures.zeroedCost("the race is about ordering, not about cost"));

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted())
                .as("a lost race must cost a split, never a key").isEqualTo(store.size());
        assertThat(result.counter("RETRY.cursor_passed_pivot"))
                .as("the victim drained past the pivot while the probes were in flight, and the "
                        + "re-validation refused the split")
                .isPositive();
    }

    /**
     * The same race seen from the other side: a victim whose bound moved is refused too, and both
     * refusals are recorded against that victim as futile — which is what eventually paces attempts
     * against a drainer nobody can catch, instead of spending a probe per cycle forever.
     */
    @Test
    void repeatedlyLosingTheRaceAgainstOneVictimEventuallyPacesAttemptsAgainstIt() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(120_000));
        PolicyScenario scenario = PolicyRunFixtures.unseededScenario(4, 10, SLOW_PROBES_FAST_PAGES,
                PolicyRunFixtures.zeroedCost("the race is about ordering, not about cost"));

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        long futileOutcomes = result.counter("RETRY.cursor_passed_pivot")
                + result.counter("RETRY.bound_moved")
                + result.counter("RETRY.split_aborted");
        assertThat(futileOutcomes).as(result::describe).isPositive();
        assertThat(result.counter("STEAL.futility_paced"))
                .as("a victim that keeps winning the race is eventually skipped during selection")
                .isPositive();
    }
}
