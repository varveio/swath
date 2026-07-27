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
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The kernel's analytic invariants, restated with the real policies wired in.
 *
 * <p>Wiring policies removes most closed forms, on purpose: the whole reason to run them is that what
 * they do is not predictable from the fixture. Two things survive, and both are worth pinning.
 *
 * <p><b>Where the policy declines to act, the arithmetic must still be exact.</b> One worker, nothing
 * to steal from, the owner-side split ablated off: the run degenerates to "list this range to the end",
 * whose cost is `ceil(n / pageSize)` calls and whose duration is that many latencies. An equality,
 * not a tolerance — with constant latency and the cost term deliberately zeroed there is nothing left
 * for a discrepancy to be, except a defect in the executor's own clock or ordering.
 *
 * <p>{@code ceil}, not {@code floor(n / pageSize) + 1}: S3 looks one key past the page it returns, so
 * the page that consumes a range's last key comes back full and <b>not</b> truncated, and the scanner
 * is finished without an extra empty call. The fixture below is deliberately an exact multiple of the
 * page size, which is the only size at which the two forms disagree; {@code SimListingViewProtocolTest}
 * pins the underlying rule against {@code ListObjectsV2Pager}. (The naive short-page rule still
 * describes {@code SequentialListingDriver}, which is a load generator reading bounded ranges rather
 * than a model of the listing protocol — see {@code ExactModeInvariantsTest}.)
 *
 * <p><b>Where it does act, scaling must be monotonic and must not be linear.</b> More workers may not
 * make a run slower, and they will not make it proportionally faster: a range is claimed whole, pacing
 * windows and client costs are fixed durations, and the concurrency controller ramps on its own clock
 * rather than at the fleet's convenience. A test asserting proportional speedup would be asserting a
 * bug.
 */
class PolicyInvariantsTest {

    private static final long LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final LatencyModel CONSTANT = PolicyRunFixtures.perClass(LATENCY_NANOS, LATENCY_NANOS);
    private static final int PAGE_SIZE = 100;

    @Test
    void aLoneWorkerWithSplittingAblatedOffCostsExactlyTheClosedForm() {
        int keys = 4_000;
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(keys));
        PolicyScenario scenario = new PolicyScenario(20260727L, 1, PAGE_SIZE, new byte[0],
                PolicyScenario.SimSeedMode.NONE, EngineToggles.DEFAULT.withOwnerSplit(false), CONSTANT,
                PolicyRunFixtures.zeroedCost("the closed form is arithmetic, not a prediction"),
                EngineTimeBudgets.engineDefaults(),
                PolicyScenario.FaultDisposition.RIDE_OUT, 0, false, PolicyScenario.DEFAULT_MAX_EVENTS);

        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory dense flat leaf");

        // 4,000 keys at 100 a page: forty full pages, the fortieth of them not truncated.
        long expectedCalls = keys / PAGE_SIZE;
        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(keys);
        assertThat(result.storeCalls()).isEqualTo(expectedCalls);
        assertThat(result.virtualNanos()).isEqualTo(expectedCalls * LATENCY_NANOS);
    }

    @Test
    void scalingIsMonotonicAndNotProportional() {
        List<byte[]> keys = KeyspaceFixtures.denseFlatLeaf(60_000);
        List<Long> durations = new ArrayList<>();
        for (int workers : new int[] {1, 2, 4, 8}) {
            PolicyScenario scenario = PolicyRunFixtures.unseededScenario(workers, PAGE_SIZE, CONSTANT,
                    PolicyRunFixtures.zeroedCost("scaling is about when calls happen, not what they cost"));

            PolicyRunResult result = SimExecutor.run(scenario, new ListingFixtureStore(keys), "fixture");

            assertThat(result.completed()).as(result::describe).isTrue();
            assertThat(result.keysEmitted()).as("no worker count may lose a key").isEqualTo(keys.size());
            durations.add(result.virtualNanos());
        }

        assertThat(durations).as("more workers never make a run slower")
                .isSortedAccordingTo(Comparator.reverseOrder());
        assertThat(durations.getLast())
                .as("a range is claimed whole and the controller ramps on its own clock, so eight "
                        + "workers cannot be eight times one")
                .isGreaterThan(durations.getFirst() / 8);
        assertThat(durations.getLast())
                .as("but they must be worth having at all").isLessThan(durations.getFirst());
    }

    /**
     * No gap and no overlap, checked as intervals rather than as a total.
     *
     * <p>Splitting rewrites the range set continuously, and the two failures a split protocol can have —
     * a lost key and a duplicated one — cancel exactly in a count. So this reads every committed page's
     * emitted interval out of the trace, sorts them, and asserts they tile the fixture: each interval
     * starts strictly after the previous one ended, the first starts at the fixture's first key, the last
     * ends at its last, and their sizes sum to the whole.
     */
    @Test
    void theCommittedPagesTileTheKeyspaceWithNoGapAndNoOverlap() {
        List<byte[]> keys = KeyspaceFixtures.denseFlatLeaf(100_000);
        ListingFixtureStore store = new ListingFixtureStore(keys);

        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.unseededScenario(8, PAGE_SIZE, CONSTANT,
                        PolicyRunFixtures.measuredCost()).withEventLog(true), store,
                "in-memory dense flat leaf");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(keys.size());
        assertThat(result.nodesCreated()).as("and the run really did cut it, many times").isGreaterThan(10);

        List<CommittedPage> pages = committedPages(result);
        assertThat(pages).isNotEmpty();
        pages.sort(Comparator.comparing(CommittedPage::from, Arrays::compareUnsigned));
        long emitted = 0;
        byte[] previousTo = null;
        for (CommittedPage page : pages) {
            if (previousTo != null) {
                assertThat(Arrays.compareUnsigned(page.from(), previousTo))
                        .as("a page that starts at or before the previous page's last key is an overlap")
                        .isPositive();
            }
            previousTo = page.to();
            emitted += page.keys();
        }
        assertThat(emitted).isEqualTo(keys.size());
        assertThat(pages.getFirst().from()).isEqualTo(keys.getFirst());
        assertThat(previousTo).isEqualTo(keys.getLast());
    }

    /** One committed page's emitted interval, read back out of the trace. */
    private record CommittedPage(byte[] from, byte[] to, long keys) {
    }

    private static List<CommittedPage> committedPages(PolicyRunResult result) {
        List<CommittedPage> pages = new ArrayList<>();
        HexFormat hex = HexFormat.of();
        for (SimEventLog.Entry entry : result.log().entries()) {
            if (!entry.kind().equals("page.commit")) {
                continue;
            }
            Map<String, String> fields = new HashMap<>();
            for (String field : entry.detail().split("\\|")) {
                int split = field.indexOf('=');
                if (split > 0) {
                    fields.put(field.substring(0, split), field.substring(split + 1));
                }
            }
            long keys = Long.parseLong(fields.get("keys"));
            if (keys == 0) {
                continue;   // an empty page commits a cursor, not an interval
            }
            pages.add(new CommittedPage(hex.parseHex(fields.get("from")), hex.parseHex(fields.get("to")),
                    keys));
        }
        return pages;
    }
}
