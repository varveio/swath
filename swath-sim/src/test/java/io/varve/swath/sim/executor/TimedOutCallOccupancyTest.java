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
 * A call the client has given up on is still work the store is doing.
 *
 * <p>That distinction is invisible until two things are true at once — the store's service time depends
 * on how many calls are in flight, and calls are timing out — which is exactly the regime the
 * occupancy-scaled store exists to model and the regime nothing else here drives. Retire a timed-out
 * call's occupancy at the client's timeout and the model reports a store that empties out precisely
 * when it is most congested: the calls a struggling store is struggling with stop counting against it,
 * and the inflation the controller's freeze rung reads is understated by exactly those. Occupancy is
 * therefore retired when the <em>store</em> finishes, whichever way the call ended, in both of the
 * executor's issue paths.
 *
 * <p>The cost of doing it that way is one extra event per timed-out call in the path that would
 * otherwise have needed none — the kernel cannot cancel a timer, so the retirement has to be one — and
 * it is counted rather than hidden, which is what the equality below pins.
 */
class TimedOutCallOccupancyTest {

    /** 40 ms uncontended, plus 60 ms per call already in flight: a fleet of 32 outruns any budget. */
    private static final LatencyModel POISONED = new OccupancyScaledLatencyModel(
            PolicyRunFixtures.perClass(TimeUnit.MILLISECONDS.toNanos(40), TimeUnit.MILLISECONDS.toNanos(8)),
            TimeUnit.MILLISECONDS.toNanos(60), TimeUnit.SECONDS.toNanos(20));

    @Test
    void aTimedOutCallKeepsItsOccupancyUntilTheStoreWouldHaveAnswered() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(200_000));
        PolicyScenario scenario = new PolicyScenario(20260727L, 32, 1_000, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, POISONED,
                PolicyRunFixtures.zeroedCost("occupancy is about when calls retire, not what they cost"),
                EngineTimeBudgets.engineDefaults().withAttemptTimeouts(
                        TimeUnit.MILLISECONDS.toNanos(300), TimeUnit.MILLISECONDS.toNanos(300)),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).as("a timing-out fleet is still a finishing fleet")
                .isEqualTo(store.size());
        long timedOut = result.counter(SimExecutor.PAGE_TIMEOUTS_COUNTER)
                + result.counter(SimExecutor.PROBE_TIMEOUTS_COUNTER);
        assertThat(timedOut).as("the scenario must actually reach the timeout regime").isPositive();
        assertThat(result.counter(SimExecutor.OCCUPANCY_DRAIN_EVENTS_COUNTER))
                .as("one occupancy retirement per timed-out call, charged as the event it is")
                .isEqualTo(timedOut);
    }
}
