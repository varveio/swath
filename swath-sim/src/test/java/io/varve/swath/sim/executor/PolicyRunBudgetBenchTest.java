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
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * How much a policy run costs <b>this machine</b>, as opposed to how long the modelled system would
 * have taken — the two numbers a simulator must never confuse.
 *
 * <p>The question this answers is whether a run at the scale of a real large bucket is affordable
 * enough to sweep over. A run's own cost is dominated by how many events the kernel dispatches, and
 * that count is a property of the policies and the fixture, not something anyone can assume: a
 * timeout-heavy scenario arms events that a healthy one never does, and every one of them is
 * dispatched whether or not it turns out to matter. So it is measured, per modelled store call, and
 * reported with the wall time that produced it.
 *
 * <p>Opt-in ({@code @Tag("perf")}): it lists a few hundred thousand keys, which is far too slow for the
 * ordinary suite and far too small to be interesting on its own. What it produces is a rate, and a rate
 * extrapolates.
 */
@Tag("perf")
class PolicyRunBudgetBenchTest {

    private static final int PAGE_SIZE = 1_000;

    /**
     * Two sizes of the same shape, because an extrapolation from one point is an assumption. If the cost
     * per modelled call is the same at both, the figure travels; if it is not, the run below says so and
     * the extrapolation has to be weakened rather than repeated.
     */
    @Test
    void measuresTheEventVolumeAndWallCostOfAPolicyRunPerModelledCall() {
        double smaller = measure(500_000);
        double larger = measure(2_000_000);

        // Not a performance gate: a ceiling loose enough that only a change of shape trips it, so the
        // measurements above stay the deliverable and this stays a guard against an accidental
        // event-per-call explosion.
        assertThat(larger)
                .as("a modelled call that costs tens of events would put a large sweep out of reach")
                .isLessThan(50.0);
        assertThat(smaller).isLessThan(50.0);
    }

    /** Runs the fixture at {@code keyCount}, prints the record, and returns events per modelled call. */
    private static double measure(int keyCount) {
        List<byte[]> keys = KeyspaceFixtures.denseFlatLeaf(keyCount);
        ListingFixtureStore store = new ListingFixtureStore(keys);
        PolicyScenario scenario = new PolicyScenario(20260727L, 32, PAGE_SIZE, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost(), EngineTimeBudgets.engineDefaults(),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false,
                PolicyScenario.DEFAULT_MAX_EVENTS);

        long startedAt = System.nanoTime();
        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");
        long wallNanos = System.nanoTime() - startedAt;

        double eventsPerCall = (double) result.run().eventsProcessed() / result.storeCalls();
        double wallMicrosPerCall = wallNanos / 1_000.0 / result.storeCalls();
        double parkShare = 100.0 * result.counter("events.stale.park") / result.run().eventsProcessed();
        System.out.printf(Locale.ROOT,
                "policy_run_budget keys=%d calls=%d events=%d retired_park_timers=%d (%.0f%%) "
                        + "events_per_call=%.1f wall_seconds=%.2f wall_micros_per_call=%.1f "
                        + "store_reads=%d virtual_seconds=%.1f%n",
                store.size(), result.storeCalls(), result.run().eventsProcessed(),
                result.counter("events.stale.park"), parkShare, eventsPerCall, wallNanos / 1e9,
                wallMicrosPerCall, result.storeReads(), result.virtualNanos() / 1e9);

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        return eventsPerCall;
    }
}
