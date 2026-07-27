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
 * <p>Both regimes are asserted, not just the slow one, because the surprising half is the fast one: a
 * reader told that every commit resets the ladder will predict a steadily-committing fleet is never
 * paced, and it is paced 578 times. Pinning only the regime that agrees with the intuition would leave
 * that correction as prose.
 */
class IdleStealPacingReachabilityTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    /** Pages far slower than the 5 ms base park: a parked thief wakes before the next commit does. */
    private static final LatencyModel SLOW_PAGES = PolicyRunFixtures.perClass(
            TimeUnit.MILLISECONDS.toNanos(30), TimeUnit.MILLISECONDS.toNanos(1));

    /** Pages far faster than it: a commit, and its reset, lands before any park could have expired. */
    private static final LatencyModel FAST_PAGES = PolicyRunFixtures.perClass(
            TimeUnit.MICROSECONDS.toNanos(200), TimeUnit.MICROSECONDS.toNanos(200));

    @Test
    void theIdleStealPacingRungIsReachedAndSuppressesMoreAttemptsThanItAdmits() {
        PolicyRunResult result = run(SLOW_PAGES);

        assertThat(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER))
                .as("seven idle workers against one drainer: the thief path is reached").isPositive();
        // 640 refusals against 176 attempts admitted, over 1.73 s of modelled time.
        assertThat(result.counter("IDLE_SLOT.paced"))
                .as("the pacing rung is reachable — a control the module otherwise never observes")
                .isGreaterThan(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));
    }

    /**
     * The same fleet with commits arriving faster than the shortest park it could take — where the
     * per-commit reset should, on the obvious reading, hold the rung at zero. It does not: a thief whose
     * attempt has just come back non-productive re-enters the idle path in the <em>same instant</em> and
     * is turned away by the backoff it re-armed itself, with no commit in between to clear it. What the
     * reset governs is how high the ladder climbs, not whether it is consulted.
     */
    @Test
    void aFleetWhoseCommitsOutpaceItsBaseParkIsPacedAllTheSame() {
        PolicyRunResult result = run(FAST_PAGES);

        assertThat(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isPositive();
        // 578 refusals against 118 attempts admitted, over 31.6 ms — the same rung, a fiftieth of the
        // modelled time, and every park cut short by a commit rather than expiring.
        assertThat(result.counter("IDLE_SLOT.paced"))
                .as("a commit resets the ladder's level; it does not stop the rung being consulted")
                .isGreaterThan(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));
    }

    private static PolicyRunResult run(LatencyModel latency) {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(20_000));
        PolicyScenario scenario = PolicyRunFixtures.unseededScenario(WORKERS, PAGE_SIZE, latency,
                PolicyRunFixtures.zeroedCost("pacing is about when attempts happen, not what they cost"));
        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");
        assertThat(result.completed()).as(result::describe).isTrue();
        return result;
    }
}
