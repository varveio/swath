/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.ClientCostTerm;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The whole thing, end to end: swath's real policies driving a real fixture to completion in virtual
 * time, on three keyspace shapes that behave differently on purpose.
 *
 * <p>What these assert is <b>completeness and shape</b>, not agreement with any other instrument. A
 * simulated run's duration is a consequence of the latency model and the client-cost term it was given,
 * and neither has been certified against a live run — so a number here is quoted with its inputs or not
 * at all. What can be checked without any such certification is that every key in the fixture is
 * emitted exactly once, that the run reaches quiescence rather than a ceiling, and that the phases have
 * the shape the design predicts: a seed that cuts the keyspace, a fan-out where the fleet spreads over
 * the cuts, and a tail where idle workers steal from whoever is left.
 */
class PolicyRunEndToEndTest {

    @Test
    void observationArchiveCompletesUnderRealPolicies() {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.observationArchive(4, 6, 8, 40));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(8, 100, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, "in-memory observation archive");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted())
                .as("every key exactly once: no gap, no overlap").isEqualTo(store.size());
        assertThat(result.counter(SimExecutor.SEED_PROBES_COUNTER))
                .as("a structured keyspace is worth probing").isPositive();
        assertThat(result.counter("seed.ranges"))
                .as("the descent found cuts, so the fleet starts parallel").isGreaterThan(1L);
        assertThat(result.virtualNanos()).isPositive();
        assertThat(result.scenario().clientCost().term().provenance())
                .isEqualTo(ClientCostTerm.Provenance.FINAL);
    }

    @Test
    void hashFannedCorpusCompletesUnderRealPolicies() {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.hashFannedCorpus(4, 4, 300));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(8, 100, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, "in-memory hash-fanned corpus");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
    }

    /**
     * The shape the whole design is for: one range, many idle workers, and no structure to seed on. The
     * fleet can only parallelise by splitting the range it has — the owner carving its own far tail as
     * it drains, and idle thieves carving what is left — so a run that completes here without either
     * mechanism firing would mean the policies were never actually reached.
     */
    @Test
    void aDenseFlatLeafIsParallelisedBySplittingRatherThanBySeeding() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(20_000));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(8, 100, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        assertThat(result.nodesCreated())
                .as("the keyspace was cut at run time, not at seed time")
                .isGreaterThan(result.counter("seed.ranges"));
        assertThat(result.ownerSplitChildren() + result.thiefChildren())
                .as("both split mechanisms are reachable from a single seeded range").isPositive();
        assertThat(result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER))
                .as("idle workers become thieves rather than parking out the run").isPositive();
    }

    @Test
    void oneObjectPerDirectoryCompletesWithoutEnumeratingTheTree() {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.oneObjectPerDirectory(2_000));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(4, 100, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, "in-memory one-object-per-directory tree");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(store.size());
        // The whole hazard of this shape is treating a directory as a unit of work: at one object per
        // directory that costs a call per object, where a flat scan costs one per page. The seed is
        // supposed to recognise the explosion and leave it whole, so the run's total call count must
        // stay far below the object count.
        assertThat(result.storeCalls())
                .as("a 1:1 tree must be flat-scanned, never enumerated")
                .isLessThan(store.size() / 4L);
    }

    @Test
    void aRunReproducesItselfExactlyIncludingItsTrace() {
        List<byte[]> keys = KeyspaceFixtures.observationArchive(2, 4, 6, 20);
        PolicyScenario scenario = PolicyRunFixtures.scenario(6, 50, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost()).withEventLog(true);

        PolicyRunResult first = SimExecutor.run(scenario, new ListingFixtureStore(keys), "fixture");
        PolicyRunResult second = SimExecutor.run(
                scenario.withClientCost(PolicyRunFixtures.measuredCost()),
                new ListingFixtureStore(keys), "fixture");

        assertThat(second.virtualNanos()).isEqualTo(first.virtualNanos());
        assertThat(second.log().canonicalBytes())
                .as("one scenario at one seed reproduces itself, event for event")
                .isEqualTo(first.log().canonicalBytes());
    }
}
