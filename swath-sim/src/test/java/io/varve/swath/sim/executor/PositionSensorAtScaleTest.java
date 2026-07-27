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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The same two shapes an order of magnitude further up, where being unable to measure progress starts
 * to cost the run its parallelism.
 *
 * <p><b>Why a second size exists at all.</b> At the size its sibling test runs, the seed's cut set is
 * large relative to the fixture: the fleet is handed more ranges than it has workers and finishes them
 * at roughly the same time, so a runtime that cannot divide anything further is never asked to. Ten
 * times the pages per range changes that — ranges outlive the seed's balance, the heavy subtrees become
 * the run, and whether the fleet can carve them decides how it finishes. The shape parameters are the
 * same; only the size moves, which is what makes the comparison a scale result rather than a different
 * experiment.
 *
 * <p>Opt-in ({@code @Tag("perf")}), for <b>memory</b> rather than time: each run computes in about a
 * third of a second, but the two fixtures are three quarters of a million keys apiece and are held on
 * the heap at once, which is a large share of a default test worker's budget and not something the fast
 * tier should carry on every commit. Like the characterization these pin <b>current</b> behaviour, and
 * a change to how remaining work is measured should move them.
 */
@Tag("perf")
class PositionSensorAtScaleTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    @Test
    void aDeepNestedKeyspaceFinishesOnADwindlingFleetWhileTheControlStaysFlat() {
        PolicyRunResult deep = run(KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 20_000,
                KeyspaceFixtures.SubtreeMass.HEAVY_TAILED), "in-memory deep-nested shared prefix");
        PolicyRunResult control = run(KeyspaceFixtures.hashFannedCorpus(16, 16, 3_000),
                "in-memory hash-fanned corpus");

        assertThat(deep.completed()).as(deep::describe).isTrue();
        assertThat(control.completed()).as(control::describe).isTrue();

        // Deep-nested: 8.4% of the run happens after the last split anything managed to make, on a mean
        // of 1.6 ranges. Control: 0.8%, and it is still splitting when it finishes.
        assertThat(deep.timeline().tailFraction())
                .as("the fleet runs out of ways to divide before it runs out of work").isGreaterThan(0.05);
        assertThat(control.timeline().tailFraction()).isLessThan(0.02);
        // Mean ranges in flight after the seed: 7.0 against 7.8 of a possible 8.
        assertThat(deep.timeline().meanOccupancy()).isLessThan(7.3);
        assertThat(control.timeline().meanOccupancy()).isGreaterThan(7.5);
        // Time spent with at most one range being drained: 3.5% against 0.4%.
        assertThat(deep.timeline().serialFraction()).isGreaterThan(0.02);
        assertThat(control.timeline().serialFraction()).isLessThan(0.01);

        // The division itself is not missing — it is the opposite. The deep-nested run publishes 205
        // children to the control's 64, and is still the less parallel of the two: it divides late,
        // through the thief rather than the owner, and only after spinning. 691 steal attempts for 118
        // thief children, 393 of them turned away before a probe was even issued; the control publishes
        // its 21 from 84 attempts and is never once left without a victim.
        assertThat(deep.ownerSplitChildren() + deep.thiefChildren())
                .as("what fails here is when and how the keyspace gets cut, not whether")
                .isGreaterThan(control.ownerSplitChildren() + control.thiefChildren());
        assertThat(deep.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isGreaterThan(500L);
        assertThat(deep.counter("steal.outcome.NO_VICTIM") + deep.counter("steal.outcome.RETRY"))
                .as("most of what the thief does on this shape produces nothing")
                .isGreaterThan(4L * deep.thiefChildren());
        assertThat(control.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isLessThan(200L);
        assertThat(control.counter("steal.outcome.NO_VICTIM")).isZero();

        // 570 of 1,509 scored bounded victims (38%) score zero remaining span outright, against 13 of
        // 399 (3%) — the reading a victim-selection cure has to move.
        assertThat(estZeroShare(deep)).isGreaterThan(0.25);
        assertThat(estZeroShare(control)).isLessThan(0.10);
    }

    private static double estZeroShare(PolicyRunResult result) {
        return (double) result.counter(SimExecutor.SENSOR_EST_ZERO_COUNTER)
                / result.counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER);
    }

    private static PolicyRunResult run(List<byte[]> keys, String label) {
        ListingFixtureStore store = new ListingFixtureStore(keys);
        long startedAt = System.nanoTime();
        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(WORKERS, PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost()),
                store, label);
        System.out.printf(Locale.ROOT, "== %s (%d keys, %.2f s wall)%n%s", label, keys.size(),
                (System.nanoTime() - startedAt) / 1e9, result.describe());
        return result;
    }
}
