/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * What today's policies do on a keyspace their position sensor cannot see, and — beside it, at the same
 * size and the same settings — what they do on one it can.
 *
 * <p><b>These are characterizations of current behaviour, not statements of what the policies ought to
 * do.</b> They pin the readings a deep-nested shared-prefix keyspace produces: a position fraction that
 * does not move while keys come out, a remaining-work estimate that discards how many came out, and an
 * owner-side split governor that refuses most of its carves on the strength of that estimate. A change
 * to how remaining work is measured is expected to make this test fail — deliberately, and that failure
 * is what it is here for. The control's numbers are pinned for the same reason in reverse: the contrast
 * is a fact about the shapes, and it belongs in the repository rather than in someone's notes.
 *
 * <p>Both runs are one scenario in everything but the keys: 8 workers, the shallow seed, the measured
 * composite client cost, the engine's own budgets, a store answering a page in 30 ms — the configuration
 * the end-to-end fixture runs use. Numbers below are from the runs themselves; the thresholds carry
 * enough margin that only a change in behaviour moves them.
 *
 * <p><b>What is deliberately not asserted here:</b> a serial tail. At this size the fleet does not
 * develop one on either shape — the seed's own cut set is large enough relative to the fixture that the
 * runtime rarely has to divide anything, so the cost of being unable to is not yet visible. The tail,
 * the occupancy collapse and the steal machinery spinning appear an order of magnitude further up, and
 * are pinned there by {@code PositionSensorAtScaleTest}.
 */
class PositionSensorCharacterizationTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    /**
     * The taxonomy-shaped keyspace: 64 species subtrees over a heavy-tailed file count, 75,680 keys.
     * Sibling species diverge at byte 10 and their contents vary only from byte 39 on, which is the
     * whole shape; the size is the smallest that gives the busiest subtrees tens of pages each.
     */
    private static List<byte[]> deepNested() {
        return KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 2_000,
                KeyspaceFixtures.SubtreeMass.HEAVY_TAILED);
    }

    /** The control: a same-size keyspace whose bytes vary inside the window position is measured over. */
    private static List<byte[]> hashFanned() {
        return KeyspaceFixtures.hashFannedCorpus(8, 8, 1_200);
    }

    @Test
    void aDeepNestedSharedPrefixLeavesTheRemainingWorkEstimateBlind() {
        PolicyRunResult result = run(deepNested(), "in-memory deep-nested shared prefix");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(deepNested().size());

        // 690 of 750 bounded page commits (92%) moved the cursor without moving the fraction at all.
        assertThat(invisibleAdvanceShare(result))
                .as("keys come out; the position the policies measure does not move")
                .isGreaterThan(0.85);
        // 92 of 227 scanned victims (41%) had a consumed span of zero, so their emitted keys — 6,000 of
        // them in some cases — were discarded and the estimate fell back to raw remaining width.
        assertThat(estIgnoresKeysShare(result))
                .as("the estimate throws away the one exact quantity the run has")
                .isGreaterThan(0.30);
        // 524 refusals over 750 bounded commits: the owner-side governor declining to carve because the
        // estimate says the tail it would shed is below the mass floor.
        assertThat(estFloorRefusalsPerCommit(result))
                .as("the carve the shape most needs is the one the estimate refuses")
                .isGreaterThan(0.5);
    }

    @Test
    void theSameSizedHashFannedCorpusKeepsItsEstimateAndDividesFurther() {
        PolicyRunResult control = run(hashFanned(), "in-memory hash-fanned corpus");
        PolicyRunResult deep = run(deepNested(), "in-memory deep-nested shared prefix");

        assertThat(control.completed()).as(control::describe).isTrue();
        assertThat(control.keysEmitted()).isEqualTo(hashFanned().size());

        // 34 of 231 scanned victims, against the deep-nested run's 92 of 227.
        assertThat(estIgnoresKeysShare(control))
                .as("a shape whose bytes vary in the window keeps its density signal").isLessThan(0.20);
        assertThat(estIgnoresKeysShare(deep)).isGreaterThan(2.0 * estIgnoresKeysShare(control));
        // 137 refusals over 745 bounded commits, against 524 over 750.
        assertThat(estFloorRefusalsPerCommit(control)).isLessThan(0.3);
        // 103 ranges against 57: the same key count, divided nearly twice as far.
        assertThat(control.nodesCreated())
                .as("the deep-nested keyspace is the one the runtime cannot cut")
                .isGreaterThan(deep.nodesCreated());
    }

    private static double invisibleAdvanceShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_INVISIBLE_ADVANCE_COUNTER)
                / result.counter(SimExecutor.SENSOR_BOUNDED_COMMITS_COUNTER);
    }

    private static double estIgnoresKeysShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER)
                / result.counter(SimExecutor.SENSOR_VICTIMS_SCANNED_COUNTER);
    }

    private static double estFloorRefusalsPerCommit(PolicyRunResult result) {
        return (double) result.counter("OWNER_SPLIT.remaining_est_floor")
                / result.counter(SimExecutor.SENSOR_BOUNDED_COMMITS_COUNTER);
    }

    private static PolicyRunResult run(List<byte[]> keys, String label) {
        ListingFixtureStore store = new ListingFixtureStore(keys);
        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(WORKERS, PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, label);
        System.out.printf(Locale.ROOT, "== %s (%d keys)%n%s", label, keys.size(), result.describe());
        return result;
    }
}
