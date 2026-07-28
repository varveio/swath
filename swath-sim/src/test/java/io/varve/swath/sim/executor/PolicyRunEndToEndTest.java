/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.ClientCostTerm;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        // A run's duration is its time to QUIESCENCE. The kernel's own last event is later — it cannot
        // cancel a park timer, so parks armed before the last range completed still fire afterwards and
        // still move the clock — and that residue is an artifact of the kernel rather than anything the
        // policies did. Quoting it as the duration would charge every run a constant of up to the
        // steal-attempt-slot park, which is more than the difference between two variants on a fixture
        // this size.
        assertThat(result.virtualNanos()).isEqualTo(result.timeline().endNanos());
        assertThat(result.kernelNanos())
                .as("the kernel's last event is the quiescence instant plus the retired-park drain")
                .isGreaterThan(result.virtualNanos());
        assertThat(result.scenario().clientCost().term().provenance())
                .isEqualTo(ClientCostTerm.Provenance.FINAL);
    }

    /**
     * The run record's field set, pinned by name.
     *
     * <p>A record that quietly loses a field is worse than one that never had it: everything still
     * prints, everything still parses, and the reader is left inferring an input that was stated
     * yesterday. The seed is the sharpest case — without it a record cannot be reproduced at all, and
     * nothing about the rest of the line would look wrong. So the whole set is asserted exactly:
     * adding a field is a deliberate act, and so is removing one.
     */
    @Test
    void theRunRecordStatesEveryInputAReproductionNeeds() {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.hashFannedCorpus(4, 4, 100));

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(4, 100, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, "in-memory hash-fanned corpus");

        // Parenthesised text is removed before the scan, innermost first: a store's own name and a cost
        // term's provenance label are free-form, and a label that happened to contain `x=` would
        // otherwise be counted as a field this record states.
        String runRecord = result.describe();
        for (String previous = ""; !runRecord.equals(previous); ) {
            previous = runRecord;
            runRecord = runRecord.replaceAll("\\([^()]*\\)", "");
        }
        Set<String> fields = new TreeSet<>();
        // Anchored at a line start or a space, so only a field NAME can match: `idle_park=5ms..50ms`
        // contributes one field, not one per token in its value.
        Matcher matcher = Pattern.compile("(?m)(?:^|(?<= ))([a-z_]+)=").matcher(runRecord);
        while (matcher.find()) {
            fields.add(matcher.group(1));
        }

        assertThat(fields).containsExactlyInAnyOrder(
                // what the run was asked to do
                "seed", "workers", "page_size", "seed_mode", "store", "client_cost",
                "store_server_capacity", "max_events", "fault_disposition", "sensing",
                // its declared budgets
                "worker_attempt_timeout", "probe_attempt_timeout", "clean_window", "idle_park",
                "attempt_slot_park",
                // what it did
                "virtual_duration", "stop_reason", "completed", "stuck", "events", "stale_events",
                "final_concurrency_target", "keys_emitted", "pages", "store_calls", "store_reads",
                "ranges", "owner_split_children", "thief_children", "splits_lost_revalidation",
                "splits_rejected", "seed_probes", "seed_ranges",
                // when it did it
                "seed_end", "last_split", "quiesced", "kernel_end", "tail_fraction",
                "keys_per_virtual_second", "tail_keys_per_virtual_second", "keys_in_tail",
                "mean_ranges", "mean_tail_ranges", "serial_fraction", "max_concurrent_ranges",
                // and what its position sensor read while it did
                "cursor_advance_invisible", "victims_scanned", "est_ignores_keys", "est_zero");

        // The other half of that contract, enforced rather than declared. The record's own javadoc says
        // a number quoted without saying which sensor produced it is not a result -- so the field that
        // makes every other field ambiguous is in the constructor's guard, alongside the three whose
        // absence would merely have thrown later.
        assertThatIllegalArgumentException().isThrownBy(() -> new PolicyRunResult(result.run(),
                result.scenario(), result.storeLabel(), result.counters(), result.nodesCreated(),
                result.splitsRejected(), result.storeReads(), result.finalConcurrencyTarget(),
                result.stuck(), result.timeline(), null));
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
