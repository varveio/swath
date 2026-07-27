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
import io.varve.swath.sim.model.CallClass;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.FittedLatencyModel;
import io.varve.swath.sim.model.IidClientCost;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The determinism claim, asserted the strict way: two runs of one scenario at one seed produce
 * <b>byte-identical</b> event logs.
 *
 * <p>Comparing serialized traces rather than summary numbers is the point. A wall time and a call
 * count can agree across two runs whose interleavings differ, and the interleaving is what decides
 * which of two racing actors wins — so a summary comparison would pass on exactly the runs a
 * simulator most needs to catch. The trace is compared as bytes so that nothing about the comparison
 * can be lenient.
 *
 * <p>The scenario deliberately uses a <em>random</em> latency model. Under constant latency a
 * simulator with no seeding at all would pass this test; the draws are what make reproducibility a
 * property of the seed rather than an accident of there being nothing to reproduce. That the seed is
 * genuinely load-bearing is asserted directly below — without that, this test could be passing
 * vacuously.
 *
 * <p><b>Scope of the claim.</b> This is determinism <em>within the simulator</em>: one kernel thread,
 * one draw tape per actor and purpose. It is not a claim that a seeded live run reproduces a
 * simulated one — a real multi-worker run's assignment of work to threads remains at the mercy of
 * the OS scheduler, and no amount of seeding changes that.
 *
 * @see #differentSeedsProduceDifferentTraces()
 */
class EventLogDeterminismTest {

    private static final int PAGE_SIZE = 8;
    private static final int TOTAL_KEYS = 300;
    private static final int RANGE_COUNT = 6;

    @Test
    void twoRunsAtOneSeedProduceByteIdenticalTraces() {
        SimScenario scenario = scenario(42L, 4);

        SimRunResult first = SequentialListingDriver.run(scenario, ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS));
        SimRunResult second = SequentialListingDriver.run(scenario, ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS));

        assertThat(first.log().canonicalBytes())
                .as("a recorded trace must be non-trivial, or this comparison checks nothing")
                .hasSizeGreaterThan(1000);
        assertThat(second.log().canonicalBytes()).isEqualTo(first.log().canonicalBytes());
        assertThat(second.wallNanos()).isEqualTo(first.wallNanos());
        assertThat(second.eventsProcessed()).isEqualTo(first.eventsProcessed());
        assertThat(second.counters()).isEqualTo(first.counters());
    }

    @Test
    void differentSeedsProduceDifferentTraces() {
        SimRunResult first = SequentialListingDriver.run(scenario(42L, 4),
                ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS));
        SimRunResult other = SequentialListingDriver.run(scenario(43L, 4),
                ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS));

        assertThat(other.log().canonicalBytes()).isNotEqualTo(first.log().canonicalBytes());
        assertThat(other.wallNanos()).isNotEqualTo(first.wallNanos());
    }

    /**
     * A change to the worker count must not re-tape an actor that is still there. At seven workers
     * worker 1 claims one range and at four it claims more, so its two draw sequences differ in
     * length — but the shorter is a prefix of the longer, because both come off the same tape, whose
     * seed is derived from that worker's own id and never from the run's worker count. This is the
     * property that lets a scaling sweep attribute a difference to concurrency rather than to a
     * different set of random numbers.
     */
    @Test
    void perActorDrawTapesSurviveAChangeInTheWorkerCount() {
        List<Long> atFour = latenciesOfWorker(1, scenario(42L, 4));
        List<Long> atSeven = latenciesOfWorker(1, scenario(42L, 7));

        assertThat(atSeven).as("worker 1 must actually draw, or this compares two empty lists")
                .isNotEmpty();
        assertThat(atFour).as("the two runs must differ in how much worker 1 does, or the prefix "
                        + "property is asserted against an identical list")
                .hasSizeGreaterThan(atSeven.size());
        assertThat(atFour.subList(0, atSeven.size())).isEqualTo(atSeven);
    }

    /** The service times worker {@code actorId} drew, in order, read out of the recorded trace. */
    private static List<Long> latenciesOfWorker(int actorId, SimScenario scenario) {
        SimRunResult result = SequentialListingDriver.run(scenario, ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS));
        List<Long> latencies = new ArrayList<>();
        Long requestedAt = null;
        for (var entry : result.log().entries()) {
            if (entry.actorId() != actorId) {
                continue;
            }
            if (entry.kind().equals("list.request")) {
                requestedAt = entry.atNanos();
            } else if (entry.kind().equals("list.response") && requestedAt != null) {
                latencies.add(entry.atNanos() - requestedAt);
                requestedAt = null;
            }
        }
        return latencies;
    }

    private static SimScenario scenario(long seed, int workers) {
        Map<CallClass, FittedLatencyModel.Params> params = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            params.put(callClass, new FittedLatencyModel.Params(1_000_000L, 4_000_000L));
        }
        return new SimScenario(
                seed,
                workers,
                PAGE_SIZE,
                ranges(),
                FittedLatencyModel.of(params),
                new IidClientCost(new ClientCostTerm(ClientCostTerm.Provenance.PROVISIONAL,
                        250_000L, 500L, "illustrative provisional term, this test only")),
                EngineTimeBudgets.engineDefaults(),
                true,
                SimScenario.DEFAULT_MAX_EVENTS);
    }

    private static List<KeyRange> ranges() {
        int perRange = TOTAL_KEYS / RANGE_COUNT;
        List<KeyRange> ranges = new ArrayList<>();
        for (int r = 0; r < RANGE_COUNT; r++) {
            ByteKey from = r == 0 ? null : ByteKey.copyOf(ListingFixtureStore.key(r * perRange));
            ByteKey to = r == RANGE_COUNT - 1 ? null : ByteKey.copyOf(ListingFixtureStore.key((r + 1) * perRange));
            ranges.add(new KeyRange(from, to));
        }
        return ranges;
    }
}
