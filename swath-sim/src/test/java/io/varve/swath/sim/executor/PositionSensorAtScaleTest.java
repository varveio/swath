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
 * <p><b>The numbers quoted below moved once already</b>, when the simulator stopped charging a
 * trailing empty listing call on every range whose size happened to divide by the page size — see
 * {@code SimListingViewProtocolTest}. Removing calls is the trigger, not the mechanism: it changes
 * <em>when</em> such a range completes, and the fleet's whole subsequent trajectory — which worker
 * idles next, which victim it finds, where the owner-split governor is in its rate limit — is taken
 * against that ordering. The shapes and the ordering of the comparison survived; the magnitudes did
 * not, which is what a pin on current behaviour is for.
 *
 * <p><b>How much of that is the seed.</b> Re-measured at four seeds (20260727, 1, 424242, 987654321),
 * the deep run's serial fraction reads 0.0964 / 0.0128 / 0.0467 / 0.0413 and its tail fraction
 * 0.1711 / 0.0010 / 0.0505 / 0.0813 — both an order of magnitude wide. What holds at every seed is
 * the <b>comparison</b>: the deep run publishes more children than the control (115–207 against
 * 46–63) and is the less parallel of the two anyway (mean occupancy 6.47–7.19 against 7.48–7.84),
 * and its degenerate-estimate share is 0.400–0.456 against the control's 0.000–0.013. The absolute
 * thresholds on the deep run's own tail and serial fractions below are <b>single-seed</b> and are
 * known not to survive re-seeding (seed 1 clears neither); they are left as-is because re-deriving
 * this characterization's thresholds is a larger question than the protocol fix that disturbed them.
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

        // Deep-nested: 17.1% of the run happens after the last split anything managed to make, on a mean
        // of 1.5 ranges. Control: 0.2%, and it is still splitting when it finishes.
        assertThat(deep.timeline().tailFraction())
                .as("the fleet runs out of ways to divide before it runs out of work").isGreaterThan(0.05);
        assertThat(control.timeline().tailFraction()).isLessThan(0.02);
        // Mean ranges in flight after the seed: 6.5 against 7.8 of a possible 8.
        assertThat(deep.timeline().meanOccupancy()).isLessThan(7.3);
        assertThat(control.timeline().meanOccupancy()).isGreaterThan(7.5);
        // Time spent with at most one range being drained: 9.6% against 0.0%.
        assertThat(deep.timeline().serialFraction()).isGreaterThan(0.02);
        assertThat(control.timeline().serialFraction()).isLessThan(0.01);

        // The division itself is not missing — it is the opposite. The deep-nested run publishes 115
        // children to the control's 61, and is still the less parallel of the two: it divides late,
        // through the thief rather than the owner, and only after spinning. 1,017 steal attempts for 102
        // thief children, 734 of them turned away before a probe was even issued.
        assertThat(deep.ownerSplitChildren() + deep.thiefChildren())
                .as("what fails here is when and how the keyspace gets cut, not whether")
                .isGreaterThan(control.ownerSplitChildren() + control.thiefChildren());
        assertThat(deep.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isGreaterThan(500L);
        assertThat(deep.counter("steal.outcome.NO_VICTIM") + deep.counter("steal.outcome.RETRY"))
                .as("most of what the thief does on this shape produces nothing")
                .isGreaterThan(4L * deep.thiefChildren());
        assertThat(control.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isLessThan(200L);
        // Being turned away for want of a victim is a SHARE of what the thief tried, not a count: the
        // control turns 1 to 6 of its 61-141 attempts away across the four seeds (0.012 to 0.075), the
        // deep run 0.556 to 0.722 of its own. A count would pin the noise; the share is the property.
        assertThat(noVictimShare(control))
                .as("a fleet with balanced ranges is essentially never short of a victim")
                .isLessThan(0.15);
        assertThat(noVictimShare(deep))
                .as("and one without them spends most of its steal attempts finding none")
                .isGreaterThan(0.5);

        // 627 of 1,515 scored bounded victims (41%) score zero remaining span outright, against 3 of
        // 398 (0.8%) — the reading a victim-selection cure has to move, and the one comparison here
        // that barely moves under re-seeding at all (0.400-0.456 against 0.000-0.013).
        assertThat(estZeroShare(deep)).isGreaterThan(0.25);
        assertThat(estZeroShare(control)).isLessThan(0.10);
    }

    /** Steal attempts that found no victim at all, as a share of every attempt made. */
    private static double noVictimShare(PolicyRunResult result) {
        return (double) result.counter("steal.outcome.NO_VICTIM")
                / result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER);
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
