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
import io.varve.swath.sim.kernel.SimStopReason;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The kernel's arithmetic, checked in the one mode where it is <b>exact</b>: constant latency, and
 * client costs explicitly zeroed. Everything below is an equality, not a tolerance — a simulator
 * whose wall time is off by a nanosecond from the closed form has a defect in its clock or its
 * ordering, and the whole point of having an exact mode is that there is nowhere for such a defect
 * to hide behind plausible-looking noise.
 *
 * <h2>The closed forms, derived</h2>
 * Write {@code p} for the page size, {@code n_r} for the number of keys in range {@code r},
 * {@code R} for the number of ranges, {@code L} for the constant per-call latency, and
 * {@code T} for the worker count.
 *
 * <p><b>Calls per range.</b> A worker lists a range by asking for {@code p} keys at a time and
 * stopping when a page comes back short — a short page is the only evidence the protocol gives that
 * a range is exhausted. It therefore issues {@code floor(n_r / p)} full pages and then exactly one
 * short page:
 * <pre>{@code c_r = floor(n_r / p) + 1}</pre>
 * The {@code +1} is not an off-by-one. A range holding exactly {@code p} keys costs two calls, and
 * an empty range still costs one, because in both cases the lister has to see a short page to know
 * it is finished.
 *
 * <p><b>Total calls.</b> Every range is listed exactly once by exactly one worker, and no worker
 * duplicates or skips a page, so
 * <pre>{@code P = sum_r c_r}</pre>
 * and — the load-bearing part — {@code P} does not mention {@code T}. Adding workers changes when
 * calls happen, never how many.
 *
 * <p><b>Wall time.</b> With client costs zero, a worker's only expense is {@code L} per call, so a
 * worker that ends up serving a set {@code S} of ranges finishes at {@code L x sum_{r in S} c_r}, and
 * the run ends when the last worker does:
 * <pre>{@code wall(T) = L x max over workers of (sum of c_r assigned to that worker)}</pre>
 * Three corners of that are fixed independently of how the greedy assignment falls out:
 * <ul>
 *   <li>{@code L = 0} forces every term to zero: {@code wall = 0}, with all {@code P} calls still
 *       made. This is the test that the clock advances for modelled reasons only.</li>
 *   <li>{@code T = 1} puts every range on one worker: {@code wall = P x L}, the sum of the run's
 *       latencies.</li>
 *   <li>{@code T >= R} gives every range its own worker from the start: {@code wall = L x max_r c_r}.
 *       Adding workers beyond {@code R} cannot help, so this is the floor for this workload.</li>
 * </ul>
 */
class ExactModeInvariantsTest {

    private static final int PAGE_SIZE = 10;
    private static final int RANGE_COUNT = 5;
    private static final int KEYS_PER_RANGE = 50;
    private static final int TOTAL_KEYS = RANGE_COUNT * KEYS_PER_RANGE;

    /** {@code c_r = floor(50 / 10) + 1 = 6} for every range here. */
    private static final long CALLS_PER_RANGE = KEYS_PER_RANGE / PAGE_SIZE + 1;
    /** {@code P = 5 x 6 = 30}. */
    private static final long TOTAL_CALLS = RANGE_COUNT * CALLS_PER_RANGE;

    private static final long LATENCY_NANOS = 1_000L;

    @Test
    void zeroLatencyCostsNoVirtualTimeAndStillMakesEveryCall() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS);

        SimRunResult result = SequentialListingDriver.run(scenario(4, 0L), store);

        assertThat(result.stopReason()).isEqualTo(SimStopReason.QUIESCED);
        assertThat(result.wallNanos()).as("wall = 0 when L = 0 and client costs are zeroed").isZero();
        assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER)).isEqualTo(TOTAL_CALLS);
        assertThat(result.counter(SequentialListingDriver.KEYS_LISTED_COUNTER)).isEqualTo(TOTAL_KEYS);
    }

    @Test
    void oneWorkerCostsTheSumOfEveryCallsLatency() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS);

        SimRunResult result = SequentialListingDriver.run(scenario(1, LATENCY_NANOS), store);

        assertThat(result.wallNanos()).as("wall = P x L on a single worker")
                .isEqualTo(TOTAL_CALLS * LATENCY_NANOS);
        assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER)).isEqualTo(TOTAL_CALLS);
    }

    @Test
    void aWorkerPerRangeCostsTheSlowestRangeAndNoMore() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS);

        SimRunResult atCapacity = SequentialListingDriver.run(scenario(RANGE_COUNT, LATENCY_NANOS), store);
        SimRunResult oversubscribed =
                SequentialListingDriver.run(scenario(RANGE_COUNT * 3, LATENCY_NANOS), store);

        assertThat(atCapacity.wallNanos()).as("wall = L x max_r c_r once every range has its own worker")
                .isEqualTo(CALLS_PER_RANGE * LATENCY_NANOS);
        assertThat(oversubscribed.wallNanos()).as("workers beyond the range count have nothing to claim")
                .isEqualTo(atCapacity.wallNanos());
    }

    /**
     * The call count is a structural property of the workload, not of the schedule — which is what
     * makes it safe for a scaling sweep to attribute a wall-time change entirely to concurrency.
     */
    @Test
    void theCallCountDoesNotDependOnTheWorkerCount() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS);

        for (int workers : List.of(1, 2, 3, 5, 8, 17)) {
            SimRunResult result = SequentialListingDriver.run(scenario(workers, LATENCY_NANOS), store);
            assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER))
                    .as("P at T=%d", workers).isEqualTo(TOTAL_CALLS);
            assertThat(result.counter(SequentialListingDriver.KEYS_LISTED_COUNTER))
                    .as("keys at T=%d", workers).isEqualTo(TOTAL_KEYS);
        }
    }

    /**
     * <b>A zero-cost client-cost charge still costs one event, and that is load-bearing.</b> The
     * charge is paid through the schedule even when the term is zero, so the continuation runs in a
     * fresh event rather than inside the caller's. That is not a formality: an event body is atomic in
     * virtual time, so short-circuiting a zero charge would fuse the page's handling with whatever
     * follows it and remove interleavings another actor can currently land in — silently changing
     * which of two racing actors wins, with every wall-time and call-count assertion still green.
     * Pinning the event count is what makes such a "harmless optimisation" fail a test.
     *
     * <p>Per page there are exactly two events: the response arriving, and the client-cost charge
     * completing. Every worker additionally costs its own bootstrap event, whether or not it ever
     * claims a range. So {@code events = T + 2P}.
     */
    @Test
    void aZeroCostChargeStillCostsOneEventSoTheInterleavingsDoNotChange() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(TOTAL_KEYS);
        int workers = 4;

        SimRunResult result = SequentialListingDriver.run(scenario(workers, 0L), store);

        assertThat(result.eventsProcessed()).as("events = T + 2P = %d + 2 x %d", workers, TOTAL_CALLS)
                .isEqualTo(workers + 2 * TOTAL_CALLS);
        assertThat(result.counter("client_cost.charges"))
                .as("one charge per page, zero-valued but still scheduled").isEqualTo(TOTAL_CALLS);
        assertThat(result.counter("client_cost.nanos")).isZero();
    }

    /**
     * The {@code +1} in {@code c_r} is a claim about the protocol, so it is asserted directly at the
     * two boundaries where getting it wrong is invisible in the bulk case: a range whose size is an
     * exact multiple of the page size, and a range with nothing in it.
     */
    @Test
    void aFullFinalPageAndAnEmptyRangeEachStillCostOneShortCall() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(PAGE_SIZE);
        List<KeyRange> exactlyOnePageThenNothing = List.of(
                new KeyRange(null, ByteKey.copyOf(ListingFixtureStore.key(PAGE_SIZE))),
                new KeyRange(ByteKey.copyOf(ListingFixtureStore.key(PAGE_SIZE)), null));

        SimRunResult result = SequentialListingDriver.run(
                scenario(1, LATENCY_NANOS, exactlyOnePageThenNothing), store);

        assertThat(result.counter(SequentialListingDriver.STORE_CALLS_COUNTER))
                .as("floor(10/10)+1 = 2 for the full range, floor(0/10)+1 = 1 for the empty one")
                .isEqualTo(3);
        assertThat(result.wallNanos()).isEqualTo(3 * LATENCY_NANOS);
    }

    private static SimScenario scenario(int workers, long latencyNanos) {
        return scenario(workers, latencyNanos, evenRanges());
    }

    private static SimScenario scenario(int workers, long latencyNanos, List<KeyRange> ranges) {
        return new SimScenario(
                1L,
                workers,
                PAGE_SIZE,
                ranges,
                ConstantLatencyModel.uniform(latencyNanos),
                new IidClientCost(ClientCostTerm.zeroedForExactMode("kernel closed-form invariants")),
                EngineTimeBudgets.engineDefaults(),
                false,
                SimScenario.DEFAULT_MAX_EVENTS);
    }

    /** {@link #RANGE_COUNT} contiguous ranges of {@link #KEYS_PER_RANGE} keys, the last open-ended. */
    private static List<KeyRange> evenRanges() {
        List<KeyRange> ranges = new ArrayList<>();
        for (int r = 0; r < RANGE_COUNT; r++) {
            ByteKey from = r == 0 ? null : ByteKey.copyOf(ListingFixtureStore.key(r * KEYS_PER_RANGE));
            ByteKey to = r == RANGE_COUNT - 1 ? null : ByteKey.copyOf(ListingFixtureStore.key((r + 1) * KEYS_PER_RANGE));
            ranges.add(new KeyRange(from, to));
        }
        return ranges;
    }
}
