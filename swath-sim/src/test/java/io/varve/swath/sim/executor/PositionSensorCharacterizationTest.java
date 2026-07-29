/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.KeyspaceFixtures.SubtreeMass;
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
 * <p>Three runs, one scenario in everything but the keys: 8 workers, the shallow seed, the measured
 * composite client cost, the engine's own budgets, a store answering a page in 30 ms — the configuration
 * the end-to-end fixture runs use. The third is the same deep-nested geometry under a <b>uniform</b>
 * mass, which is what separates the two claims being made: the geometry is what blinds the sensor, and
 * the mass distribution is what decides whether being blind costs the run anything.
 *
 * <p><b>What is deliberately not asserted here.</b> Not a serial tail: at this size neither shape
 * develops one, because the seed's own cut set is large relative to the fixture and a runtime that
 * cannot divide further is rarely asked to. And not an inability to divide — the deep-nested run
 * publishes fewer children here, but an order of magnitude further up it publishes <em>three times</em>
 * as many as the control and still finishes less parallel. What is wrong with its division is that it
 * is late, steal-spun and refused by its own estimate, not that it is absent; the tail, the occupancy
 * and the steal machinery spinning are pinned at the size where they exist, in
 * {@code PositionSensorAtScaleTest}.
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
        return KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 2_000, SubtreeMass.HEAVY_TAILED);
    }

    /** The same geometry with every subtree the same size — the control on mass rather than on shape. */
    private static List<byte[]> deepNestedUniform() {
        return KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 150, SubtreeMass.UNIFORM);
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

        // 705 of 757 bounded page commits (93%) moved the cursor without moving the fraction at all.
        assertThat(invisibleAdvanceShare(result))
                .as("keys come out; the position the policies measure does not move")
                .isGreaterThan(0.85);
        // 62 of 198 scored bounded victims (31%) had a consumed span of zero, so their emitted keys —
        // and a candidate is only steal-eligible once it has emitted some — were discarded, leaving the
        // estimate a raw remaining width.
        assertThat(estIgnoresKeysShare(result))
                .as("the estimate throws away the one exact quantity the run has")
                .isGreaterThan(0.25);
        // 490 refusals over 757 bounded commits: the owner-side governor declining to carve because the
        // estimate says the tail it would shed is below the mass floor.
        assertThat(estFloorRefusalsPerCommit(result))
                .as("the carve the shape most needs is the one the estimate refuses")
                .isGreaterThan(0.5);
    }

    @Test
    void theSameSizedHashFannedCorpusKeepsItsEstimateAndItsOwnerSideSplit() {
        PolicyRunResult control = run(hashFanned(), "in-memory hash-fanned corpus");
        PolicyRunResult deep = run(deepNested(), "in-memory deep-nested shared prefix");

        assertThat(control.completed()).as(control::describe).isTrue();
        assertThat(control.keysEmitted()).isEqualTo(hashFanned().size());

        // 34 of 209 scored bounded victims, against the deep-nested run's 62 of 198.
        assertThat(estIgnoresKeysShare(control))
                .as("a shape whose bytes vary in the window keeps its density signal").isLessThan(0.20);
        assertThat(estIgnoresKeysShare(deep)).isGreaterThan(1.5 * estIgnoresKeysShare(control));
        // 137 refusals over 745 bounded commits (0.18 each), against 490 over 757 (0.65).
        assertThat(estFloorRefusalsPerCommit(control)).isLessThan(0.3);
        assertThat(estFloorRefusalsPerCommit(deep))
                .isGreaterThan(3.0 * estFloorRefusalsPerCommit(control));
        // The mechanism those refusals belong to is the owner's own: 7 children published against 22.
        // This is a statement about the owner-side split under a blind estimate, NOT about the fleet's
        // total ability to divide — at ten times the size the deep-nested run out-publishes this
        // control three to one and is still the less parallel of the two.
        assertThat(deep.ownerSplitChildren())
                .as("the owner-side carve is the mechanism the estimate gates")
                .isLessThan(control.ownerSplitChildren());
    }

    /**
     * The separation the other two assume: the same geometry, the same everything, and a uniform file
     * count instead of a heavy-tailed one. The sensor is just as blind — a cursor still crosses whole
     * subtrees without moving the fraction — and the run is nonetheless the healthiest of the three,
     * because equal-sized subtrees make the seed's own division balanced and the fleet is never left
     * having to divide anything at run time. Blindness is a property of the byte geometry; paying for
     * it is a property of the mass distribution.
     */
    @Test
    void theSameGeometryWithUniformMassIsJustAsBlindAndCostsNothing() {
        PolicyRunResult result = run(deepNestedUniform(), "in-memory deep-nested shared prefix, uniform");

        assertThat(result.completed()).as(result::describe).isTrue();
        assertThat(result.keysEmitted()).isEqualTo(deepNestedUniform().size());

        // 698 of 743 bounded commits (94%) invisible — the heavy-tailed run's 93%, on the same shape.
        // Only the comparison with that run is being made here: this share does not by itself separate
        // the deep-nested shape from anything, since the flat control reaches it too once its ranges are
        // wide enough. What it says is that changing the mass leaves this geometry exactly as blind.
        assertThat(invisibleAdvanceShare(result))
                .as("the same geometry is just as blind under a uniform mass").isGreaterThan(0.85);
        // And nothing to pay for it, in the terms that matter: 7.7 of 8 ranges in flight on average and
        // 0.2% of the run serial. These are the run-time cost claims and they are unchanged.
        assertThat(result.timeline().meanOccupancy())
                .as("balanced subtrees keep the fleet full without a single run-time division")
                .isGreaterThan(7.0);
        assertThat(result.timeline().serialFraction()).isLessThan(0.05);
        // The est-ignores-keys share, however, is NO LONGER ~0 (it read 0 of 59 before the seed's
        // open-tile sentinel existed). The sentinel closes the scan scope by appending
        // prefixCeil(last top prefix) as a cut, which leaves a final (u, null] tile that is EMPTY by
        // construction. An empty range never advances its cursor, so spanIn(lo, cursor, lo, hi) <= 0
        // and WindowEstimator.ignoresEmittedKeys reports true for it on every scan that scores it,
        // until it is claimed and drained. One such range dominates this ratio.
        //
        // Deliberately pinned as a BAND rather than relaxed to a ceiling: the share must stay in the
        // regime explained by exactly one degenerate empty range. If it climbs materially above this,
        // something OTHER than the sentinel tile is reading blind and this test should fail again.
        // Whether that empty tile should exist at all is tracked separately — see the open-tile
        // follow-up issue; the alternatives (a bounded final tile, or seeding it pre-completed) trade
        // it for a keyspace-coverage or concurrency-spine risk respectively.
        assertThat(estIgnoresKeysShare(result))
                .as("the only blind-estimate victim is the sentinel's empty final tile")
                .isBetween(0.05, 0.45);
    }

    private static double invisibleAdvanceShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_INVISIBLE_ADVANCE_COUNTER)
                / result.counter(SimExecutor.SENSOR_BOUNDED_COMMITS_COUNTER);
    }

    private static double estIgnoresKeysShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER)
                / result.counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER);
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
