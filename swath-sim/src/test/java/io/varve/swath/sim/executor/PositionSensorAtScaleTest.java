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
 * <h2>This fixture's tail magnitude is seed-sensitive; only its separations are pinned</h2>
 * Re-measured at four seeds (20260727, 1, 424242, 987654321), the deep run reads:
 *
 * <pre>{@code
 *   serial fraction   0.0964  0.0128  0.0467  0.0413
 *   tail fraction     0.1711  0.0010  0.0505  0.0813
 *   mean occupancy      6.47    6.87    7.19    6.93
 * }</pre>
 *
 * <p>An order of magnitude wide, and at seed 1 the tail fraction does not merely shrink — it drops
 * <em>below the control's</em> 0.0063, inverting the comparison the quantity was being read for. So
 * <b>no absolute threshold on this fixture's tail, serial or occupancy magnitude is asserted below</b>;
 * those numbers appear in the comments as base-seed characterization and nowhere else. What is pinned
 * is only what held at all four seeds: the two runs' serial fractions in order (deep above control by
 * 2.6× at worst), their occupancies in order (control above deep by 0.61 at worst), the children
 * ordering (115–207 against 46–63), the no-victim shares, and the degenerate-estimate shares
 * (0.400–0.456 against 0.000–0.013 — the one reading here that barely moves at all).
 *
 * <p><b>Where the seed-robust pathology lives.</b> Not here. This fixture's mass sits in 20,000-key
 * leaves, and at that size whether the fleet gets stuck is decided by an interleaving. The tail that
 * is a property of the keyspace rather than of a schedule belongs to the {@code LEAF_CONCENTRATED}
 * fixture in {@code MassConcentrationAtScaleTest} — serial 0.331 with a coefficient of variation of
 * 0.3% over the same four seeds — which is the bench a variant is evaluated on. This pair's job is
 * the sensor comparison, and the sensor comparison is what it now asserts.
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

        // At the base seed the deep run spends 9.6% of itself serial to the control's 0.0%, and 17.1%
        // of it after the last split anything managed against the control's 0.2%, on a mean of 1.5
        // ranges. Those magnitudes are characterization only (class note): what is asserted is that the
        // deep run is the more serial of the two, which held at all four seeds by 2.6x at worst.
        assertThat(deep.timeline().serialFraction())
                .as("the fleet runs out of ways to divide before it runs out of work")
                .isGreaterThan(control.timeline().serialFraction());
        // Mean ranges in flight after the seed: 6.5 against 7.8 of a possible 8 at the base seed, and
        // the control ahead by 0.61 to 1.31 across the four. An absolute floor on either would have
        // been a pin on the interleaving — the control alone ranges 7.48 to 7.84.
        assertThat(control.timeline().meanOccupancy())
                .as("the control keeps the fleet fuller, whatever the interleaving")
                .isGreaterThan(deep.timeline().meanOccupancy());

        // The division itself is not missing — it is the opposite. The deep-nested run publishes 115
        // children to the control's 61, and is still the less parallel of the two: it divides late,
        // through the thief rather than the owner, and only after spinning. 1,017 steal attempts for 102
        // thief children, 734 of them turned away before a probe was even issued.
        assertThat(deep.ownerSplitChildren() + deep.thiefChildren())
                .as("what fails here is when and how the keyspace gets cut, not whether")
                .isGreaterThan(control.ownerSplitChildren() + control.thiefChildren());
        // How much spinning it takes to get them, as a ratio rather than a floor: the deep run makes
        // 6.0 to 12.4 times the control's attempts across the four seeds, where a "> 500" floor sat
        // 3.6% above one of them.
        assertThat(deep.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER))
                .as("the deep run has to try many times over for each cut it lands")
                .isGreaterThan(3L * control.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));
        // Anchoring the denominator of that ratio: the control's own attempt count stays in the
        // dozens (61-141 across the four seeds), so the ratio above cannot be met by a control that
        // has itself started spinning.
        assertThat(control.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)).isLessThan(200L);
        // Being turned away for want of a victim is a SHARE of what the thief tried, not a count: the
        // control turns 1 to 6 of its 61-141 attempts away across the four seeds (0.012 to 0.075), the
        // deep run 0.556 to 0.722 of its own. A count would pin the noise; the share is the property,
        // and it subsumes the older "no-victim plus retry exceeds four children" reading, which could
        // not be shown to hold at every seed.
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
