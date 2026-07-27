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
 * The idle-steal backoff rung, and whether it is reachable at all.
 *
 * <p>The ladder paces a fleet of idle thieves after consecutive non-productive attempts, from a 5 ms
 * base to a 50 ms cap under the engine's own defaults. Three things reset it, and the third is the
 * surprising one: a range claimed, a steal that produced a child, and <b>every non-empty page commit by
 * any worker anywhere</b>. That is the engine's own choice, not a simplification here — the engine
 * broadcasts the same reset from its page-commit path so a fleet handed fresh progress does not sit out
 * a full backoff window — and its consequence is that the ladder's <em>level</em> is pinned near its
 * bottom rung for as long as anything is committing steadily.
 *
 * <p><b>What that does not mean is that the rung is never used</b>, and the measurement is worth
 * writing down because the intuition points the other way. Pacing refusals happen in both regimes,
 * because a thief whose attempt just came back non-productive re-enters the idle path at the same
 * instant and is turned away by the backoff it has only just re-armed — no commit gets a chance to
 * intervene. The same 20,000-key fixture and the same eight workers, at page latencies either side of
 * the 5 ms base park, produce 640 refusals over 1.73 s of modelled time with 30 ms pages, and 578 over
 * 31.6 ms with 200 µs pages. The per-commit reset governs how high the ladder climbs, not whether it is
 * consulted; only the second of those runs is a fleet whose parks are constantly cut short.
 *
 * <p>This test pins the reachability, which nothing else in the module did: no other test refers to
 * {@code IDLE_SLOT.paced}, so a reset that fired often enough to disable the rung entirely would have
 * gone unnoticed.
 */
class IdleStealPacingReachabilityTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    /** Pages far slower than the 5 ms base park: a parked thief wakes before the next commit does. */
    private static final LatencyModel SLOW_PAGES = PolicyRunFixtures.perClass(
            TimeUnit.MILLISECONDS.toNanos(30), TimeUnit.MILLISECONDS.toNanos(1));

    @Test
    void theIdleStealPacingRungIsReachedAndSuppressesMoreAttemptsThanItAdmits() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(20_000));
        PolicyScenario scenario = PolicyRunFixtures.unseededScenario(WORKERS, PAGE_SIZE, SLOW_PAGES,
                PolicyRunFixtures.zeroedCost("pacing is about when attempts happen, not what they cost"));

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER))
                .as("seven idle workers against one drainer: the thief path is reached").isPositive();
        // 640 refusals against 176 attempts admitted.
        assertThat(result.counter("IDLE_SLOT.paced"))
                .as("the pacing rung is reachable — a control the module otherwise never observes")
                .isGreaterThan(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));
    }
}
