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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * What a modelled timeout costs, counted rather than assumed.
 *
 * <p>The kernel has no way to cancel a scheduled event, so a timeout that pre-empts an in-flight call
 * has to retire the call's own response some other way. Where the response instant is known when the
 * call is issued — the ordinary case — the executor simply schedules one or the other, and a timeout
 * costs nothing extra. Where it is not, because the call is queued behind other calls and its
 * completion depends on them, both are armed and whichever fires second finds its subject already
 * retired and returns.
 *
 * <p>Those second firings are dispatched events. They occupy the queue, and they count against the
 * run's event budget exactly like every other event, so a run that is expected to time out heavily has
 * to be given a budget that includes them. This test pins the arithmetic on the extreme case — a store
 * so slow that <em>every</em> call times out — where the number of retired events is exactly the number
 * of calls made.
 */
class ContendedStoreTimeoutTest {

    /** A store that serves one call at a time, each taking twice the attempt budget below. */
    private static final int SERIAL_STORE = 1;

    @Test
    void whenEveryCallTimesOutTheRetiredEventCountIsExactlyTheCallCount() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(5_000));
        EngineTimeBudgets budgets = EngineTimeBudgets.engineDefaults()
                .withAttemptTimeouts(TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(1));
        PolicyScenario scenario = new PolicyScenario(20260727L, 2, 100, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT,
                PolicyRunFixtures.perClass(TimeUnit.SECONDS.toNanos(2), TimeUnit.SECONDS.toNanos(2)),
                PolicyRunFixtures.zeroedCost("a run that emits nothing has no client cost to model"),
                budgets, SERIAL_STORE, false, PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.stuck())
                .as("a page that exhausts its transient retries is a failed run, not a finished one")
                .isTrue();
        assertThat(result.counter(SimExecutor.PAGE_TIMEOUTS_COUNTER)
                + result.counter(SimExecutor.PROBE_TIMEOUTS_COUNTER))
                .as("every call timed out, so every call is a timeout")
                .isEqualTo(result.storeCalls());
        assertThat(result.counter("events.stale.store_response"))
                .as("one retired response per timed-out call, and not one more")
                .isEqualTo(result.storeCalls());
        assertThat(result.counter("events.stale.timeout"))
                .as("no timeout was itself retired: none of these calls answered in time")
                .isZero();
        assertThat(result.keysEmitted()).isZero();
    }

    /**
     * The same machinery on a store that is merely contended rather than hopeless: with the attempt
     * budget above the service time, every call answers, and no event is retired at all — the armed
     * timeouts are the ones retired instead, one per call, which is what "both are armed" costs when
     * the calls succeed.
     */
    @Test
    void aContendedStoreThatStillAnswersInTimeRetiresItsTimeoutsInstead() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(5_000));
        PolicyScenario scenario = new PolicyScenario(20260727L, 4, 100, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT,
                PolicyRunFixtures.perClass(TimeUnit.MILLISECONDS.toNanos(30),
                        TimeUnit.MILLISECONDS.toNanos(8)),
                PolicyRunFixtures.zeroedCost("the point here is the event accounting"),
                EngineTimeBudgets.engineDefaults(), SERIAL_STORE, false,
                PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        assertThat(result.counter("events.stale.store_response")).isZero();
        assertThat(result.counter("events.stale.timeout"))
                .as("one armed timeout retired per call that answered in time")
                .isEqualTo(result.storeCalls());
    }
}
