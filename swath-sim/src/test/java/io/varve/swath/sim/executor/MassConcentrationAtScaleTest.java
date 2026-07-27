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
 * ({@code PositionSensorAtScaleTest}) reproduces the blind sensor exactly and costs the run 9.6% of its
 * duration in serial time — against the 60–90% a real deep-nested bucket loses. Two measured properties
 * of those buckets were missing, and both are about depth rather than geometry: a third of the objects
 * sat in one subtree and 90% in five, and the directory chain leading to them had a fan-out of one or
 * two the whole way down, ending in one directory holding some 1.8 million objects. A keyspace whose
 * heaviest leaf holds 20,000 never asks the runtime the question the tail is made of, because the seed's
 * own cut set is enough.
 *
 * <p>Both fixtures here hold 1.09 million keys and give every species subtree the same mass to within
 * the rank law's integer division — at most two keys per species, plus the three token files the
 * concentrated law leaves in an accession's other directories (1,087,165 keys against 1,087,136). The
 * single thing that differs is whether that mass is spread across an accession's four data directories
 * or concentrated in one, which is what makes this a controlled comparison rather than a second
 * experiment. Eight species, not sixty-four, because the rank law is Zipf and matching the measured
 * concentration needs either a steeper law or fewer subtrees, and the subtree count is a parameter this
 * generator already has. The match is good at the head and loose in the middle: <b>36.8% of the
 * keyspace in the largest subtree against a measured 32.6%, and 84.0% in the largest five against a
 * measured 90.7%</b> — so this fixture concentrates slightly harder than the real one at rank 1 and
 * distinctly less hard across the head. Both shares are pinned in {@code KeyspaceFixturesTest}.
 *
 * <p><b>The page size is a stand-in for bucket size, and the tail below does not survive without it.</b>
 * What a fleet has to serialise is not keys but round trips, so the scaling variable is <em>pages per
 * range</em>: this fixture's heavy directory is 400,000 keys, which is 4,000 pages at the 100-key page
 * used here and 400 at a real 1,000-key page, against roughly 1,800 for the 1.8-million-object
 * directory measured on a real bucket. At 100 keys a page this run is therefore page-faithful and
 * mass-short — its biggest range is 4,000 of the run's 10,969 pages, where the real one was ~1,800 of
 * ~8,800 — and the same fixture at the measured 1,000-key page is mass-faithful and page-short, with a
 * biggest range of 400 pages out of 1,182 and a serial fraction of 0.009. Both are asserted below,
 * because quoting either alone would be quoting a choice of page size as a property of the keyspace.
 * The honest statement of what the tail measurement needs is a bucket about ten times this one's size;
 * the small page buys that at a tenth of the memory, and buys nothing else.
 *
 * <h2>What replicates across seeds, and what does not</h2>
 * Every figure below was re-measured at four seeds (20260727, 1, 424242, 987654321) after the
 * simulator stopped charging a trailing empty listing call on ranges whose size divides by the page
 * size (see {@code SimListingViewProtocolTest}). The two fixtures behave completely differently under
 * re-seeding, and the difference is the result:
 *
 * <ul>
 *   <li><b>The concentrated fixture is a constant.</b> Serial fraction 0.3314 / 0.3296 / 0.3308 /
 *       0.3306, post-split tail 0.334–0.340, mean tail occupancy 1.02–1.04. It has one enormous range
 *       and no amount of re-seeding gives the fleet a way to divide it, so the tail is a property of
 *       the keyspace rather than of an interleaving.</li>
 *   <li><b>The spread control is not a quantity at all.</b> Serial fraction 0.1476 / 0.0069 / 0.0189 /
 *       0.2551 — a 37-fold spread over four seeds, with owner-split children ranging 25–231 over the
 *       same runs. Its trajectory is chaotic in the interleaving, so <b>no threshold on the spread
 *       control's serial fraction may be pinned, and none is below.</b></li>
 * </ul>
 *
 * <p><b>Trigger versus mechanism.</b> The empty-call correction is what made the spread control jump,
 * but it is not what the jump is made of: that run's page count went <em>up</em> (11,117 to 11,287),
 * so it cannot be explained by calls being removed. Removing them changes <em>when</em> a range whose
 * size divides by the page size completes, and every downstream decision — which worker goes idle
 * next, which victim it finds, whether the owner-split governor is inside its rate limit — is taken
 * against that ordering. One call earlier is a perturbation, and this fixture amplifies a
 * perturbation; re-seeding produces swings of the same size, which is how we know the correction is
 * the trigger and the governor's trajectory is the mechanism.
 *
 * <p>So the surviving comparison is the one that holds at every seed: the concentrated fixture ends
 * in a long single-worker tail (tail fraction ~0.34) and the spread one does not (0.0007–0.0024),
 * and the concentrated fixture's serial fraction exceeds the spread one's in all four runs. Whether
 * spreading the mass <em>halves</em> the serial time — the reading a single seed offered — is
 * <b>unsettled</b>, and would need a sample rather than a rerun.
 *
 * <h2>{@code LEAF_CONCENTRATED} at a 100-key page is the bench a variant is judged on</h2>
 * It is the only fixture in this repository whose pathology is a property of the keyspace rather than
 * of a schedule: serial fraction 0.331 with a coefficient of variation of 0.3% across four seeds, and
 * a post-split tail separated from its mass-matched control by roughly 150-fold. That is what makes a
 * change to victim selection, pivot placement or the owner-side split legible — a cure has to move a
 * number that re-seeding does not.
 *
 * <p>So: <b>evaluate a variant here, over at least four seeds</b>, with the healthy shapes
 * ({@code PositionSensorAtScaleTest}'s hash-fanned control, {@code PositionSensorCharacterizationTest}'s
 * uniform run) carried alongside as regression guards — a cure that fixes the tail by making balanced
 * keyspaces worse is not a cure. Four is a floor rather than a target: it is what it took to show the
 * spread fixture below was not a control at all.
 *
 * <p>And the spread fixture is <b>explicitly not a settled control</b>. It is the mass-matched
 * comparison — same species, same mass apiece, differing only in whether that mass pools in one leaf —
 * and it is genuinely useful as that. It is not a baseline anything can be measured against, because
 * its serial fraction swings 37-fold under re-seeding. Quote it with that stated, or not at all.
 *
 * <p>Opt-in ({@code @Tag("perf")}) for memory: a million-key fixture is a large share of a default test
 * worker's heap. The fixtures are generated one at a time, and the results compared, so only one is ever
 * live. Like its siblings these pin <b>current</b> behaviour.
 */
@Tag("perf")
class MassConcentrationAtScaleTest {

    private static final int WORKERS = 8;
    private static final int PAGE_SIZE = 100;

    /**
     * The concentrated fixture's serial fraction at {@link #PAGE_SIZE}, as measured by
     * {@link #massAtLiveDepthCostsTheFleetAThirdOfTheRunInSerialTime}: 0.3296–0.3314 over the four
     * seeds of the class note. The measured-page-regime test states its own result relative to this
     * rather than against an absolute floor — see there for why.
     */
    private static final double SERIAL_FRACTION_AT_HUNDRED_KEY_PAGE = 0.33;

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

        // 33.1% of the post-seed run has at most one range being drained, and 34.0% of it comes after
        // the last split anything managed to make. Both hold to three decimal places at all four seeds
        // of the class note. Within a factor of two of the 60% a real deep-nested bucket loses, and the
        // first fixture in this repository that is in that régime at all.
        assertThat(deep.timeline().serialFraction())
                .as("the fleet spends a third of the run as one worker").isGreaterThan(0.25);
        assertThat(deep.timeline().tailFraction()).isGreaterThan(0.25);
        assertThat(deep.timeline().meanTailOccupancy()).isLessThan(1.2);
        // The spread control's post-split tail all but vanishes — 0.0007 to 0.0024 across the same four
        // seeds, against the concentrated fixture's 0.334 to 0.340. THAT is the separation depth buys,
        // and it is the only one stated here: the spread run's serial fraction swings from 0.007 to
        // 0.255 under re-seeding and is deliberately left unpinned (class note).
        assertThat(spread.timeline().tailFraction())
                .as("the fleet can still carve the spread keyspace, whatever the interleaving")
                .isLessThan(0.05);
        assertThat(spread.timeline().serialFraction())
                .as("and it never ends up as serial as the concentrated one")
                .isLessThan(deep.timeline().serialFraction());

        // Structural rescue runs out. Every child the thief published on the concentrated keyspace was
        // placed by extrapolation or interpolation; not one came from a structure probe, at any of the
        // four seeds. The spread keyspace still wins two or three that way in every run.
        assertThat(structureSourcedChildren(deep))
                .as("a fan-out that carries no mass is not a pivot").isZero();
        assertThat(structureSourcedChildren(spread)).isPositive();

        // And the estimate that decides where to cut is degenerate for 69.6% of the victims it is
        // computed over (69.2-70.6% across the four seeds): their consumed span reads zero, so their
        // emitted keys are discarded outright.
        assertThat(estIgnoresKeysShare(deep)).isGreaterThan(0.6);
    }

    /**
     * The footrace, at the size where it decides the run: a thief snapshots a victim's cursor, spends
     * its probes placing a pivot ahead of it, and by the time it proposes the split the victim has
     * drained past it. Four proposals in five die that way here — the shape of the 85–93% measured on a
     * real deep-nested bucket, and the reason a serial tail is not merely "the fleet declining to
     * split".
     *
     * <p><b>Both sides of that comparison have to be read on one denominator.</b> The share below is
     * proposals lost over proposals that reached the re-validation at all — 196 of 252. The live
     * measurements are usually quoted per steal <em>attempt</em> (85% and 93%), which counts attempts
     * that never got that far; on this denominator the same live runs read 96% and 90%. Either way the
     * bench loses fewer races than the deployment does, which is the conservative direction for a bench
     * whose purpose is to make a cure prove itself.
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
        // 196 of 252 proposals lost (77.8%; 78-85% across the four seeds of the class note), against
        // 140 of 242 (57.9%) at a twentieth of the mass; the same denominator reads 90-96% on the
        // deployment's own runs.
        assertThat(revalidationLossShare(deep))
                .as("the thief is not refusing to split; it is trying and losing").isGreaterThan(0.7);
        assertThat(deep.splitsRejected())
                .as("the durable guard is the late loser, and nothing can reach it here").isZero();
    }

    /**
     * The same race under the page regime it was measured in — a 1,000-key page answered in 110 ms and a
     * 35 ms probe, against this test's own 100-key page in 30 ms and 8 ms probe.
     *
     * <p><b>The loss share survives the move; the serial tail does not, and both halves are asserted
     * here.</b> Where a pivot lands relative to the cursor and how far the cursor travels while the
     * probes are in flight both scale with the page, so the race is decided by the keyspace and the
     * pivot cascade rather than by the timings a scenario declares. The window is generous either way —
     * a cascade of six to seven probes costs 1.7 page cycles of draining at the other regime and 1.9
     * here, against the 0.3 a 35 ms probe buys against a 110 ms page on the deployment those numbers
     * came from — so a bench that loses fewer races than a real deployment is telling you about its
     * keyspace, not about its clock.
     *
     * <p>What does <em>not</em> survive is the tail: at a full page this fixture's biggest range is 400
     * pages of the run's 1,182, the fleet absorbs it, and the serial fraction reads 0.009 against the
     * other regime's 0.331. That is the pages-per-range arithmetic in the class note, stated as a
     * measurement so the tail magnitude cannot be quoted without the page size it was taken at.
     */
    @Test
    void theRaceIsLostAtTheMeasuredPageRegimeToo() {
        PolicyRunResult deep = run(concentrated(), PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                PolicyRunFixtures.MEASURED_TAIL_LATENCY,
                "in-memory deep-nested, mass in one leaf directory");

        assertThat(deep.completed()).as(deep::describe).isTrue();
        // 43 of 60 proposals lost (71.7%; 66-73% across the four seeds of the class note) at a tenth of
        // the page count.
        assertThat(revalidationLossShare(deep)).isGreaterThan(0.5);
        // And the serial fraction reads 0.0003-0.0096 over those seeds against a rock-steady 0.330 at a
        // 100-key page: two orders of magnitude apart, so the claim is stated as a ratio to the other
        // regime. A flat "< 0.01" would have been a threshold the 0.0096 seed sits on the edge of, and
        // the point being made is "small at a full page", not "below one particular hundredth".
        assertThat(deep.timeline().serialFraction())
                .as("a tail is pages per range, and at a full page this fixture has a tenth of them")
                .isLessThan(0.1 * SERIAL_FRACTION_AT_HUNDRED_KEY_PAGE);
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
