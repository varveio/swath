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
import io.varve.swath.sim.model.LatencyModel;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The deep-nested shape with its mass where a real archive's is: a few subtrees holding almost
 * everything, and inside one of them a single directory holding hundreds of thousands of keys.
 *
 * <p><b>Why the mass had to move.</b> The same geometry with its mass spread over 20,000-key leaves
 * ({@code PositionSensorAtScaleTest}) reproduces the blind sensor exactly and costs the run 3.5% of its
 * duration in serial time — against the 60–90% a real deep-nested bucket loses. Two measured properties
 * of those buckets were missing, and both are about depth rather than geometry: a third of the objects
 * sat in one subtree and 90% in five, and the directory chain leading to them had a fan-out of one or
 * two the whole way down, ending in one directory holding some 1.8 million objects. A keyspace whose
 * heaviest leaf holds 20,000 never asks the runtime the question the tail is made of, because the seed's
 * own cut set is enough.
 *
 * <p>Both fixtures here hold 1.09 million keys and give every species subtree the <b>identical</b> mass;
 * the single thing that differs is whether that mass is spread across an accession's four data
 * directories or concentrated in one, which is what makes this a controlled comparison rather than a
 * second experiment. Eight species, not sixty-four, because the rank law is Zipf and matching the
 * measured concentration — a third of the keyspace in the largest subtree, 90% in the largest five —
 * needs either a steeper law or fewer subtrees, and the subtree count is a parameter this generator
 * already has. The result is 37% of the keyspace in one subtree and 87% in five.
 *
 * <p>Opt-in ({@code @Tag("perf")}) for memory: a million-key fixture is a large share of a default test
 * worker's heap. The fixtures are generated one at a time, and the results compared, so only one is ever
 * live. Like its siblings these pin <b>current</b> behaviour.
 */
@Tag("perf")
class MassConcentrationAtScaleTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    /** Eight species over a Zipf rank law, each holding its whole file count in one leaf directory. */
    private static Supplier<List<byte[]>> concentrated() {
        return () -> KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 400_000,
                SubtreeMass.LEAF_CONCENTRATED);
    }

    /** The same eight species, the same mass apiece, spread over all four of their leaf directories. */
    private static Supplier<List<byte[]>> spread() {
        return () -> KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 100_000, SubtreeMass.HEAVY_TAILED);
    }

    @Test
    void massAtLiveDepthCostsTheFleetAThirdOfTheRunInSerialTime() {
        PolicyRunResult deep = run(concentrated(), PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                "in-memory deep-nested, mass in one leaf directory");
        PolicyRunResult spread = run(spread(), PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                "in-memory deep-nested, mass spread over the accession");

        assertThat(deep.completed()).as(deep::describe).isTrue();
        assertThat(spread.completed()).as(spread::describe).isTrue();

        // 33.2% of the post-seed run has at most one range being drained, and 34.1% of it comes after
        // the last split anything managed to make — against 3.5% and 8.4% for the same geometry at a
        // twentieth of the mass per leaf. Within a factor of two of the 60% a real deep-nested bucket
        // loses, and the first fixture in this repository that is in that régime at all.
        assertThat(deep.timeline().serialFraction())
                .as("the fleet spends a third of the run as one worker").isGreaterThan(0.25);
        assertThat(deep.timeline().tailFraction()).isGreaterThan(0.25);
        assertThat(deep.timeline().meanTailOccupancy()).isLessThan(1.2);
        // The spread control reaches 31.0% too: at this concentration the tail is bought by the mass
        // being in a few subtrees, and moving it DOWN adds to it rather than causing it. What the depth
        // changes is how the run fails, below.
        assertThat(spread.timeline().serialFraction()).isGreaterThan(0.25);

        // Structural rescue runs out. Every child the thief published on the concentrated keyspace was
        // placed by extrapolation or interpolation; not one came from a structure probe, though 219 of
        // them were issued. The spread keyspace still wins three that way.
        assertThat(structureSourcedChildren(deep))
                .as("a fan-out that carries no mass is not a pivot").isZero();
        assertThat(structureSourcedChildren(spread)).isPositive();

        // And the estimate that decides where to cut is degenerate for 71.5% of the victims it is
        // computed over: their consumed span reads zero, so their emitted keys are discarded outright.
        assertThat(estIgnoresKeysShare(deep)).isGreaterThan(0.6);
    }

    /**
     * The footrace, at the size where it decides the run: a thief snapshots a victim's cursor, spends
     * its probes placing a pivot ahead of it, and by the time it proposes the split the victim has
     * drained past it. Four proposals in five die that way here — the shape of the 85–93% measured on a
     * real deep-nested bucket, and the reason a serial tail is not merely "the fleet declining to
     * split".
     *
     * <p>The durable guard, meanwhile, rejects nothing, and that is correct rather than a defect: it
     * only sees proposals the re-validation above has already passed, so rejecting one needs a change
     * between the two checks — a second in-flight proposer, which the fleet's one-attempt-at-a-time rule
     * forbids. A simulator that showed losses there instead would be modelling a race the engine does
     * not run.
     */
    @Test
    void mostSplitProposalsLoseTheRaceToTheVictimsOwnCursor() {
        PolicyRunResult deep = run(concentrated(), PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                "in-memory deep-nested, mass in one leaf directory");

        assertThat(deep.completed()).as(deep::describe).isTrue();
        // 195 of 242 proposals lost (80.6%), against 128 of 246 (52.0%) at a twentieth of the mass.
        assertThat(revalidationLossShare(deep))
                .as("the thief is not refusing to split; it is trying and losing").isGreaterThan(0.7);
        assertThat(deep.splitsRejected())
                .as("the durable guard is the late loser, and nothing can reach it here").isZero();
    }

    /**
     * The same race under the page regime it was measured in — a 1,000-key page answered in 110 ms and a
     * 35 ms probe, against this test's own 100-key page in 30 ms and 8 ms probe.
     *
     * <p>It survives the move, which is the point: both the distance a pivot is placed ahead of the
     * cursor and the keys a victim drains while the probes are in flight scale with the page, so the
     * loss share is a property of the keyspace and the pivot cascade rather than of the timings chosen
     * for a run. The window itself is generous either way — a cascade of six to seven probes costs 1.7
     * page cycles of draining at this test's own regime and 1.9 at the measured one, against the 0.3 a
     * 35 ms probe buys against a 110 ms page on the deployment those numbers came from. A bench that
     * loses fewer races than a real deployment is therefore telling you about its keyspace, not about
     * its clock.
     */
    @Test
    void theRaceIsLostAtTheMeasuredPageRegimeToo() {
        PolicyRunResult deep = run(concentrated(), PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                PolicyRunFixtures.MEASURED_TAIL_LATENCY,
                "in-memory deep-nested, mass in one leaf directory");

        assertThat(deep.completed()).as(deep::describe).isTrue();
        // 39 of 59 proposals lost (66.1%) at a tenth of the page count.
        assertThat(revalidationLossShare(deep)).isGreaterThan(0.5);
    }

    /** Thief children whose pivot came from a structure probe rather than from arithmetic over keys. */
    private static long structureSourcedChildren(PolicyRunResult result) {
        return result.counter("PIVOT.structure_probe") + result.counter("PIVOT.structure_capped")
                + result.counter("PIVOT.adaptive_structure")
                + result.counter("PIVOT.adaptive_structure_capped");
    }

    /** Proposals that died at the re-validation, as a share of every proposal that reached it. */
    private static double revalidationLossShare(PolicyRunResult result) {
        long lost = result.splitsLostAtRevalidation();
        return (double) lost / (lost + result.thiefChildren());
    }

    private static double estIgnoresKeysShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER)
                / result.counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER);
    }

    private static PolicyRunResult run(Supplier<List<byte[]>> fixture, int pageSize, LatencyModel latency,
                                       String label) {
        ListingFixtureStore store = new ListingFixtureStore(fixture.get());
        int size = store.size();
        long startedAt = System.nanoTime();
        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(WORKERS, pageSize, latency, PolicyRunFixtures.measuredCost()),
                store, label);
        System.out.printf(Locale.ROOT, "== %s (%d keys, page %d, %.2f s wall)%n%s", label, size, pageSize,
                (System.nanoTime() - startedAt) / 1e9, result.describe());
        assertThat(result.keysEmitted()).as("a run must emit every key in its fixture").isEqualTo(size);
        return result;
    }
}
