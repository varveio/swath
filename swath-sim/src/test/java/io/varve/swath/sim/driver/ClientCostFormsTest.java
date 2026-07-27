/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.kernel.SimStopReason;
import io.varve.swath.sim.model.ClientCostModel;
import io.varve.swath.sim.model.ClientCostTerm;
import io.varve.swath.sim.model.ConstantLatencyModel;
import io.varve.swath.sim.model.ContendedClientCost;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.IidClientCost;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The two client-cost model forms, run against the identical workload and shown to give different
 * answers — which is the evidence that both are genuinely expressible, and that choosing between
 * them is a modelling decision rather than a formality.
 *
 * <h2>The closed forms</h2>
 * Four ranges of twenty keys at a page size of ten cost {@code c_r = floor(20/10) + 1 = 3} calls
 * each, so {@code P = 12} pages arrive in total. Latency is zero, so the <em>only</em> cost in the
 * run is the client-side charge {@code S} per page, and the two forms place it differently:
 *
 * <ul>
 *   <li><b>Independent</b> — each of the four workers pays its own three charges on its own timeline,
 *       all four in parallel: {@code wall = max_r c_r x S = 3S}.</li>
 *   <li><b>Contended, one server</b> — all twelve charges queue for a single resource. Every worker
 *       submits at once at {@code t = 0}, so the queue is never empty until the work runs out and the
 *       server is busy from the first instant to the last: {@code wall = P x S = 12S}.</li>
 * </ul>
 *
 * A factor of four between two models of the same measurement is the point. No adjustment to
 * {@code S} converts one into the other, because the difference is in how arrivals interact, not in
 * how much a page costs — which is exactly why a model fitted to one run's aggregate throughput can
 * reproduce that run and still rank a bursty policy against a steady one backwards.
 */
class ClientCostFormsTest {

    private static final int PAGE_SIZE = 10;
    private static final int RANGE_COUNT = 4;
    private static final int KEYS_PER_RANGE = 20;
    private static final int WORKERS = RANGE_COUNT;

    /** {@code c_r = floor(20/10) + 1}. */
    private static final long CALLS_PER_RANGE = 3;
    /** {@code P = 4 x 3}. */
    private static final long TOTAL_PAGES = RANGE_COUNT * CALLS_PER_RANGE;

    /** {@code S}: charged per page, nothing per key, so the arithmetic above is exact. */
    private static final long PER_PAGE_NANOS = 1_000L;

    private static final ClientCostTerm TERM = new ClientCostTerm(ClientCostTerm.Provenance.PROVISIONAL,
            PER_PAGE_NANOS, 0L, "illustrative provisional term, this test only");

    @Test
    void theIndependentFormPaysEachWorkersPagesInParallel() {
        SimRunResult result = run(new IidClientCost(TERM), SimScenario.DEFAULT_MAX_EVENTS);

        assertThat(result.wallNanos()).as("wall = max_r c_r x S")
                .isEqualTo(CALLS_PER_RANGE * PER_PAGE_NANOS);
        assertThat(result.counter("client_cost.charges")).isEqualTo(TOTAL_PAGES);
        assertThat(result.counter("client_cost.nanos")).isEqualTo(TOTAL_PAGES * PER_PAGE_NANOS);
    }

    @Test
    void theContendedFormSerialisesEveryPageThroughOneServer() {
        ContendedClientCost contended = new ContendedClientCost(TERM, 1);

        SimRunResult result = run(contended, SimScenario.DEFAULT_MAX_EVENTS);

        assertThat(result.wallNanos()).as("wall = P x S: the single server is never idle")
                .isEqualTo(TOTAL_PAGES * PER_PAGE_NANOS);
        assertThat(result.counter(ContendedClientCost.RESOURCE_NAME + ".submitted")).isEqualTo(TOTAL_PAGES);
        assertThat(result.counter(ContendedClientCost.RESOURCE_NAME + ".queued"))
                .as("all but the first submission had to wait, or the resource was not contended")
                .isEqualTo(TOTAL_PAGES - 1);
        assertThat(contended.queueDepth()).as("a quiesced run leaves nothing queued").isZero();
    }

    @Test
    void theTwoFormsDisagreeByMoreThanATolerance() {
        long independent = run(new IidClientCost(TERM), SimScenario.DEFAULT_MAX_EVENTS).wallNanos();
        long contended = run(new ContendedClientCost(TERM, 1), SimScenario.DEFAULT_MAX_EVENTS).wallNanos();

        assertThat(contended).isEqualTo(independent * RANGE_COUNT);
    }

    /**
     * Raising the resource's capacity to the worker count removes the contention entirely, so the
     * contended form collapses onto the independent one. Worth pinning: it shows the divergence above
     * is queueing rather than a constant the contended form adds.
     */
    @Test
    void theContendedFormMatchesTheIndependentOneWhenNothingHasToWait() {
        long independent = run(new IidClientCost(TERM), SimScenario.DEFAULT_MAX_EVENTS).wallNanos();
        SimRunResult uncontended = run(new ContendedClientCost(TERM, WORKERS), SimScenario.DEFAULT_MAX_EVENTS);

        assertThat(uncontended.wallNanos()).isEqualTo(independent);
        assertThat(uncontended.counter(ContendedClientCost.RESOURCE_NAME + ".queued")).isZero();
    }

    /**
     * A shared resource is per-run state, and a run cut short by its event cap leaves that resource
     * mid-service. Reusing the model would then let the abandoned run's queue decide the next run's
     * timings, so the second run is refused rather than quietly contaminated.
     */
    @Test
    void aReusedContendedResourceIsRefusedAfterARunThatDidNotFinish() {
        ContendedClientCost contended = new ContendedClientCost(TERM, 1);

        SimRunResult cutShort = run(contended, 10);

        assertThat(cutShort.stopReason()).isEqualTo(SimStopReason.EVENT_CAP);
        assertThat(contended.queueDepth()).isPositive();
        assertThatThrownBy(() -> run(contended, SimScenario.DEFAULT_MAX_EVENTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ContendedClientCost.RESOURCE_NAME);
    }

    private static SimRunResult run(ClientCostModel clientCost, long maxEvents) {
        SimScenario scenario = new SimScenario(
                3L,
                WORKERS,
                PAGE_SIZE,
                ranges(),
                ConstantLatencyModel.uniform(0L),
                clientCost,
                EngineTimeBudgets.engineDefaults(),
                false,
                maxEvents);
        return SequentialListingDriver.run(scenario,
                KeyListStore.ofGeneratedKeys(RANGE_COUNT * KEYS_PER_RANGE));
    }

    private static List<KeyRange> ranges() {
        List<KeyRange> ranges = new ArrayList<>();
        for (int r = 0; r < RANGE_COUNT; r++) {
            ByteKey from = r == 0 ? null : ByteKey.copyOf(KeyListStore.key(r * KEYS_PER_RANGE));
            ByteKey to = r == RANGE_COUNT - 1 ? null
                    : ByteKey.copyOf(KeyListStore.key((r + 1) * KEYS_PER_RANGE));
            ranges.add(new KeyRange(from, to));
        }
        return ranges;
    }
}
