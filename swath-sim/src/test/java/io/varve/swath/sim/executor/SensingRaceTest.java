/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.kernel.SimEventLog;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The position-sensor race. <b>Its protocol — bench, seeds, criteria, guards, and the rules about
 * cherry-picking and regime disclosure — is written down in {@link SensingRaceProtocol}, and was
 * committed before the first variant was implemented.</b> Read that file first; this one runs what it
 * declares and states, below, what came out.
 *
 * <h2>Result: on the bench, all three candidates cure it, and it is not close</h2>
 * At the bench's own 100-key page, across all four seeds:
 *
 * <pre>
 * variant       serial          tail            estZero        estIgnores   revLoss      dur (s)  occ
 * current       0.3296-0.3314   0.3344-0.3399   0.036-0.039    0.692-0.706  0.778-0.852  75.2-75.4  5.5
 * E1 rate       0.0021-0.0029   0.0013-0.0020   0.000          0.000        0.423-0.541  54.2-54.7  7.7
 * E2 anchored   0.0003-0.0090   0.0009-0.0016   0.015-0.050    0.000        0.375-0.604  52.4-53.9  7.8
 * E1+E2         0.0003-0.0024   0.0008-0.0014   0.000          0.000        0.321-0.662  52.5-53.1  7.9
 * </pre>
 *
 * <b>Two of those columns are not findings.</b> {@code estZro} and {@code estIgn} are <b>zero by
 * construction</b> for E1 and E1+E2: an estimator whose estimate <em>is</em> the emitted count cannot
 * discard it, and the only score that can read zero is a cursor already at its bound, which is a
 * finished range and never a scanned candidate. The invisible-advance counter is zero for them for the
 * same reason. So only E2's readings in those columns are measurements — and E2's {@code estZro} is not
 * an improvement (0.015–0.050 against the control's 0.036–0.039, worse at one seed). They are still
 * asserted below, because what they pin is that the counters follow the installed sensor at all.
 *
 * <p>The serial tail the bench exists to produce falls by roughly two orders of magnitude at every seed,
 * the run finishes about 28% sooner, mean occupancy goes from 5.5 of 8 workers to nearly 8, and it
 * costs <em>fewer</em> store calls, not more. Steal attempts fall from ~2,100 to 100–355 and the
 * NO_VICTIM share from 0.88 to under 0.07: the fleet stops spinning because there is work to find.
 *
 * <h2>Which mechanism moved</h2>
 * The one the estimate gates directly: <b>the owner-side remaining-work floor stops refusing</b>. The
 * {@code flr/pg} column below is owner carves refused by that floor per page committed, and it reads
 * 0.740–0.747 under the shipped sensor against 0.053–0.057 (E1), 0.063–0.070 (E2) and 0.010–0.018
 * (E1+E2) — an order of magnitude, at every seed.
 *
 * <p>What replaces those refusals, read off the traces at seed 20260727:
 * {@code OWNER_SPLIT.demand_gated} rises from 1,934 to 8,400–9,011 — the gate that declines to carve
 * because the ready queue <em>already</em> holds enough live ranges to keep every worker busy, which
 * is a healthy refusal and the signature of a fleet that is no longer starving. Ranges claimed rise
 * from 99 to 164–224. Downstream at the same seed, {@code RETRY.cursor_passed_pivot} falls from 192
 * to 30–110 and the idle-pacing ladder's own spin ({@code IDLE_SLOT.paced}) from 13,996 to 82–555;
 * both were symptoms of workers with nothing to do, not causes.
 *
 * <h2>Where they cost something, which is where the protocol said to look</h2>
 * <b>At the measured 1,000-key page every candidate is worse, at every seed</b>: serial fraction
 * 0.093–0.241 against a control range of 0.0003–0.0096, and for E1 a 47–60% longer run at 21% more
 * store calls. The mechanism is the second column of the table and is specific: an estimate with no
 * sense of position places the owner's far-ahead carve on a range that is nearly drained, the child
 * comes back confetti-sized (E1's {@code cnfti} column reads 0.933–1.000 at this regime — every
 * owner-split child but one, over four seeds — against the control's 0.213–0.241), and the confetti
 * feedback gate — which exists to notice exactly
 * that — then suppresses owner splitting for the rest of the run. Division falls back onto the
 * thief's probe-driven path, which is what the extra calls and the extra time are.
 *
 * <p><b>E1 alone also damages a healthy keyspace — at one of the four seeds.</b> All three candidates
 * are read here on <b>one</b> yardstick, the hash-fanned control's own serial fraction <em>at the same
 * seed</em>: that control moves more than twentyfold across the four seeds (0.0004, 0.0049, 0.0093,
 * 0.0037), so a fixed line lets the same reading be called a regression for one candidate and a hold
 * for another. On that yardstick E1 reaches <b>0.1198 at seed 20260727 — two orders of magnitude past
 * the control at that seed, and about fifteen times what E1+E2 does at it (0.0080)</b> — with mean
 * occupancy falling below 7 of 8 workers, the NO_VICTIM share at 0.80
 * against the control's 0.012, and {@code NO_VICTIM.all_futility_paced} at 381 against the control's 1.
 * At 987654321 it is elevated at 0.0301, 8× the control — but E1+E2 reads 0.0293 there, 8× the control
 * too, so <b>that seed does not separate the candidates and is not counted against E1</b>. At the
 * remaining two seeds E1 sits at or below the control. The verdict is therefore <b>one decisive seed of
 * four, reported as the split verdict it is</b> rather than averaged away, and the mechanism is exactly
 * the cost E1's own note predicted for an estimator that gives up position: with no position term a
 * nearly-finished range still outranks everything, thieves attack it, fail, and the per-victim futility
 * cooldown pages every candidate out. E2 and E1+E2 hold both guards at all four seeds — on serial
 * fraction, on occupancy, and on throughput.
 *
 * <h2>What this says, and what it does not</h2>
 * The sensor was the thing gating division on this keyspace: fixing the reading, and nothing else,
 * removes the serial tail entirely at the regime that produces it. It does not follow that any of
 * these three is shippable — all three lose at the page size a real deployment uses, which is the
 * regime the deployment's own tail was measured in, and the reason for that loss is a calibration
 * problem rather than a visibility one. E2 is the only candidate whose reading is byte-identical to
 * the shipped one wherever the two windows are anchored at the same byte — a narrower set than
 * "wherever the shipped one works", so its holding both guards at 4 of 4 seeds is a measurement rather
 * than a construction — and it does hold them; E1 is the one whose degenerate readings are all zero,
 * which is a property of its arithmetic rather than a result, and it is the one that breaks a healthy
 * shape. The combination keeps both properties and still loses the measured regime.
 *
 * <p>Opt-in ({@code @Tag("perf")}) for memory and time, like every at-scale fixture here: the bench
 * is a million-key keyspace and the race runs it once per seed per variant.
 */
@Tag("perf")
class SensingRaceTest {

    /** The candidates, in the order the protocol names them. */
    private static final SensingVariant[] CANDIDATES = {
        SensingVariant.RATE, SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED};

    /** Every variant, control first — the shape a race table is read in. */
    private static final SensingVariant[] ALL = {
        SensingVariant.CURRENT, SensingVariant.RATE, SensingVariant.CURSOR_ANCHORED,
        SensingVariant.RATE_CURSOR_ANCHORED};

    @Test
    void theBenchAndItsGuardsHoldStillBeforeAnyVariantExists() {
        List<SensingRaceProtocol.Leg> bench = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, SensingVariant.CURRENT);
        List<SensingRaceProtocol.Leg> hashFanned = SensingRaceProtocol.raceOn("hash-fanned",
                SensingRaceProtocol.hashFannedGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, SensingVariant.CURRENT);
        List<SensingRaceProtocol.Leg> uniform = SensingRaceProtocol.raceOn("uniform",
                SensingRaceProtocol.uniformGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, SensingVariant.CURRENT);
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>(bench);
        legs.addAll(hashFanned);
        legs.addAll(uniform);
        SensingRaceProtocol.printTable("== control: the algorithm as it ships", legs);

        // The bench is a constant, which is the whole reason it is the bench: a cure has to move a
        // number that re-seeding does not.
        for (SensingRaceProtocol.Leg leg : bench) {
            assertThat(leg.serialFraction()).as("bench serial fraction at seed %d", leg.seed())
                    .isBetween(0.25, 0.40);
            assertThat(leg.tailFraction()).as("bench tail fraction at seed %d", leg.seed())
                    .isGreaterThan(0.25);
            assertThat(leg.estIgnoresKeysShare()).as("bench estimate discards keys at seed %d", leg.seed())
                    .isGreaterThan(0.6);
        }
        // And the two guards are healthy at every seed, so a variant that damages them is visible.
        for (SensingRaceProtocol.Leg leg : hashFanned) {
            assertThat(leg.tailFraction()).as("hash-fanned tail at seed %d", leg.seed()).isLessThan(0.05);
            assertThat(leg.serialFraction()).as("hash-fanned serial at seed %d", leg.seed())
                    .isLessThan(0.05);
        }
        for (SensingRaceProtocol.Leg leg : uniform) {
            assertThat(leg.serialFraction()).as("uniform serial at seed %d", leg.seed()).isLessThan(0.05);
        }
    }

    @Test
    void everyCandidateCuresTheBenchAtItsOwnPageRegime() {
        List<SensingRaceProtocol.Leg> legs = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, ALL);
        SensingRaceProtocol.printTable("== race: LEAF_CONCENTRATED at a 100-key page", legs);

        for (SensingVariant variant : CANDIDATES) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(legs, variant, seed);
                SensingRaceProtocol.Leg control =
                        SensingRaceProtocol.at(legs, SensingVariant.CURRENT, seed);
                String at = variant + " at seed " + seed;

                // PRIMARY 2: the estimate stops discarding a range's emitted keys. Exactly zero, at
                // every seed, for every candidate -- 0.692-0.706 for the shipped one. A measurement for
                // E2 only: E1 and E1+E2 estimate FROM the emitted count, so a zero here is an
                // arithmetic identity for them and what the assertion pins is that the counter follows
                // the installed sensor, not that the sensor improved.
                assertThat(leg.estIgnoresKeysShare())
                        .as("%s: estimate discards keys (zero by construction for the position-free "
                                + "variants)", at)
                        .isZero();
                // PRIMARY 3: fewer split proposals lose the race to the victim's own cursor. The
                // control's four seeds span 0.778-0.852 and no candidate seed reaches 0.70.
                assertThat(leg.revalidationLossShare()).as("%s: revalidation loss share", at)
                        .isLessThan(0.70);
                // PRIMARY 4, at the regime it is quoted for: the tail the bench exists to produce is
                // gone. Two orders of magnitude, at every seed.
                assertThat(leg.serialFraction()).as("%s: serial fraction at a 100-key page", at)
                        .isLessThan(0.05);
                assertThat(leg.tailFraction()).as("%s: post-split tail at a 100-key page", at)
                        .isLessThan(0.01);
                // And the fleet that was starving is not: nearly every worker busy, on a fraction of
                // the steal attempts, in less time, for no more store calls.
                assertThat(leg.result().timeline().meanOccupancy()).as("%s: mean occupancy", at)
                        .isGreaterThan(7.0);
                assertThat(leg.noVictimShare()).as("%s: steal attempts that found no victim", at)
                        .isLessThan(0.50);
                assertThat(leg.stealAttempts()).as("%s: steal attempts", at).isLessThan(500L);
                assertThat(leg.result().virtualNanos()).as("%s: virtual duration", at)
                        .isLessThan((long) (0.85 * control.result().virtualNanos()));
                assertThat(leg.result().storeCalls()).as("%s: store calls", at)
                        .isLessThanOrEqualTo(control.result().storeCalls());
                // The mechanism: the owner-side floor stops refusing carves it cannot afford to refuse.
                assertThat(leg.estFloorRefusalsPerPage())
                        .as("%s: owner carves refused by the remaining-work floor, per page", at)
                        .isLessThan(0.2 * control.estFloorRefusalsPerPage());
            }
        }
        // Only the two variants that carry no position term drive the zero-estimate reading itself to
        // zero -- and for them that is construction rather than a cure: their estimate is the emitted
        // count floored at a page, so the only score that can read zero is a cursor already at its
        // bound, which is a finished range and never a scanned candidate. E2 keeps a position, so it
        // keeps a few genuinely-exhausted candidates -- 0.015-0.050 against the control's 0.036-0.039,
        // WORSE at one seed. That is why this is pinned per variant and not for all, and why E2 is
        // nowhere reported as improving this reading.
        for (SensingVariant variant : new SensingVariant[] {
            SensingVariant.RATE, SensingVariant.RATE_CURSOR_ANCHORED}) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                assertThat(SensingRaceProtocol.at(legs, variant, seed).estZeroShare())
                        .as("%s at seed %d: zero estimates (zero by construction -- no position term)",
                                variant, seed)
                        .isZero();
            }
        }
    }

    @Test
    void onlyTheAnchoredVariantsKeepTheHealthyShapesHealthy() {
        List<SensingRaceProtocol.Leg> hashFanned = SensingRaceProtocol.raceOn("hash-fanned",
                SensingRaceProtocol.hashFannedGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, ALL);
        List<SensingRaceProtocol.Leg> uniform = SensingRaceProtocol.raceOn("uniform",
                SensingRaceProtocol.uniformGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, ALL);
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>(hashFanned);
        legs.addAll(uniform);
        SensingRaceProtocol.printTable("== guards: the two healthy shapes", legs);

        // The uniform guard is held by every candidate at every seed: same geometry, mass spread, and
        // nothing any of them does to the estimate costs it anything -- not the serial fraction, not
        // occupancy, and not throughput. Its tail is reported rather than asserted, per the dated
        // qualification in the protocol: the uniform CONTROL's own tail is 0.034-0.069, so the
        // hash-fanned guard's <0.05 line would fail the shipped algorithm on a healthy fixture.
        for (SensingVariant variant : CANDIDATES) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                assertGuardHeld(SensingRaceProtocol.at(uniform, variant, seed),
                        SensingRaceProtocol.at(uniform, SensingVariant.CURRENT, seed),
                        variant + " on the uniform guard at seed " + seed);
            }
        }
        // The hash-fanned guard separates the candidates. The two that keep a position term hold it at
        // every seed, on the guard's own health threshold and on the tail as well.
        for (SensingVariant variant : new SensingVariant[] {
            SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED}) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(hashFanned, variant, seed);
                assertGuardHeld(leg, SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, seed),
                        variant + " on the hash-fanned guard at seed " + seed);
                assertThat(leg.tailFraction()).as("%s hash-fanned tail at seed %d", variant, seed)
                        .isLessThan(0.05);
            }
        }
        // E1 does not, at one of the four seeds -- read on the same-seed yardstick as everything above.
        // Only a four-seed reading may be pinned, so what is asserted is the set-level one: E1's worst
        // seed is an order of magnitude past the control's worst, with the fleet spending most of its
        // steal attempts finding every candidate paged out by the futility cooldown. The per-seed
        // detail, including the seed where E1 and E1+E2 are 3% apart and neither is separated from the
        // other, is in this class's own prose.
        double controlWorst = 0.0;
        double rateWorst = 0.0;
        double rateWorstNoVictim = 0.0;
        for (long seed : SensingRaceProtocol.SEEDS) {
            controlWorst = Math.max(controlWorst,
                    SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, seed).serialFraction());
            SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(hashFanned, SensingVariant.RATE, seed);
            rateWorst = Math.max(rateWorst, leg.serialFraction());
            rateWorstNoVictim = Math.max(rateWorstNoVictim, leg.noVictimShare());
        }
        assertThat(rateWorst).as("the rate estimate's worst hash-fanned seed against the control's worst")
                .isGreaterThan(10.0 * controlWorst);
        assertThat(rateWorstNoVictim).as("and the fleet finding no victim while it happens")
                .isGreaterThan(0.5);
    }

    /**
     * The regression-guard criterion the protocol declares, in full: a near-zero serial fraction,
     * <b>healthy occupancy</b>, and <b>no material loss of throughput</b>. The last two were declared
     * and then left unasserted in this test's first cut; both are read against the control <em>at the
     * same seed</em>, because a guard's own numbers move across re-seeding and a fixed line would be a
     * different yardstick per seed. Five percent is the "material" line: the widest same-seed spread
     * any candidate that holds a guard actually shows is 3%.
     */
    private static void assertGuardHeld(SensingRaceProtocol.Leg leg, SensingRaceProtocol.Leg control,
                                        String at) {
        assertThat(leg.serialFraction()).as("%s: serial fraction", at).isLessThan(0.05);
        assertThat(leg.result().timeline().meanOccupancy()).as("%s: mean occupancy", at)
                .isGreaterThan(7.0);
        assertThat(leg.result().virtualNanos()).as("%s: virtual duration against the control's", at)
                .isLessThan((long) (1.05 * control.result().virtualNanos()));
        assertThat(leg.result().storeCalls()).as("%s: store calls against the control's", at)
                .isLessThan((long) (1.05 * control.result().storeCalls()));
    }

    @Test
    void everyCandidateIsWorseAtTheMeasuredPageRegime() {
        List<SensingRaceProtocol.Leg> legs = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                PolicyRunFixtures.MEASURED_TAIL_LATENCY, ALL);
        SensingRaceProtocol.printTable("== race: LEAF_CONCENTRATED at the measured 1000-key page", legs);

        for (long seed : SensingRaceProtocol.SEEDS) {
            // The control's whole four-seed range at this regime is 0.0003-0.0096, and every candidate
            // clears the top of it at every seed. The bench win does not transfer, and the protocol's
            // regime-disclosure rule is what makes that visible rather than a footnote.
            assertThat(SensingRaceProtocol.at(legs, SensingVariant.CURRENT, seed).serialFraction())
                    .as("control serial at the measured regime, seed %d", seed).isLessThan(0.05);
            for (SensingVariant variant : CANDIDATES) {
                assertThat(SensingRaceProtocol.at(legs, variant, seed).serialFraction())
                        .as("%s serial at the measured regime, seed %d", variant, seed)
                        .isGreaterThan(0.05);
            }
        }
        // E1's cost here is the sharpest and is paid in the two currencies that matter: a run half as
        // long again, for a fifth more store calls, at every seed.
        for (long seed : SensingRaceProtocol.SEEDS) {
            SensingRaceProtocol.Leg control = SensingRaceProtocol.at(legs, SensingVariant.CURRENT, seed);
            SensingRaceProtocol.Leg rate = SensingRaceProtocol.at(legs, SensingVariant.RATE, seed);
            assertThat(rate.result().virtualNanos()).as("rate duration at seed %d", seed)
                    .isGreaterThan((long) (1.3 * control.result().virtualNanos()));
            assertThat(rate.result().storeCalls()).as("rate store calls at seed %d", seed)
                    .isGreaterThan((long) (1.15 * control.result().storeCalls()));
            // And the mechanism, read off the run's own classification: every owner-split child it
            // published came back confetti-sized, which is what shuts the owner side down.
            assertThat(rate.confettiChildShare()).as("rate confetti children at seed %d", seed)
                    .isGreaterThan(control.confettiChildShare());
        }
    }

    /**
     * <b>{@code CarveAdmissionRaceProtocol}'s C4 and its synthetic must-not-regress leg</b>, on the
     * fixtures this class already owns. The later candidate is the combination with the geometry band's
     * lower half removed, and what it has to show here is that a change made to cure a real bucket's
     * straggler has not spent the bench's cure or either guard to do it: it holds every criterion the
     * combination holds at the bench regime, holds both healthy shapes on serial fraction, occupancy and
     * throughput, and is <b>no worse than the combination</b> at the measured regime — which is the
     * fixture the whole carve-placement question was originally raised on, and where every candidate
     * loses to the shipped sensor.
     *
     * <p>That last reading is asserted as a comparison against the combination and not against the
     * control, because losing to the control there is this family's known, recorded property; the claim
     * under test is that this candidate does not make it worse.
     *
     * <p><b>One criterion is a split, and is recorded as one.</b> The hash-fanned guard's extra tail
     * line — the {@code < 0.05} the two anchored candidates hold at four seeds of four — this candidate
     * <b>misses at one seed of four</b>, at <b>0.0510 against 0.0500</b> at seed 987654321, where the
     * combination reads 0.0324 and the control 0.0092. Everything else that guard is held to holds at
     * all four: serial fraction, occupancy, duration and store calls, each against the control at its
     * own seed. Per the protocol only a four-seed reading may be pinned, so what is asserted here is the
     * split as measured — three seeds clear the line and the table prints all four — rather than a line
     * moved until the candidate clears it.
     *
     * <p><b>The same seed's other two readings are marginal, and are stated rather than left to the
     * table.</b> At 987654321 on the hash-fanned guard this candidate reads a <b>serial fraction of
     * 0.0481</b> — 96% of the {@code < 0.05} line {@link #assertGuardHeld} holds it to, i.e. a pass with
     * no room in it — and a <b>NO_VICTIM share of 0.6729</b>, which is above the 0.5 worst-seed reading
     * this campaign counted against E1 on this very fixture. Neither is asserted here: the guard set is
     * the protocol's, and widening or tightening it to fit a candidate is the move the pre-registration
     * exists to prevent. What they mean is that this candidate's hold on the hash-fanned guard at that
     * seed is a hold by a margin, on three readings that all point the same way — a promotion decision
     * has to be taken with that in front of it rather than reconstructed from a table afterwards.
     */
    @Test
    void theCarveAdmissionCandidateKeepsTheBenchCureAndBothGuards() {
        SensingVariant[] arms = {SensingVariant.CURRENT, SensingVariant.RATE_CURSOR_ANCHORED,
            SensingVariant.RATE_ANCHORED_LIFT_ONLY};
        SensingVariant candidate = SensingVariant.RATE_ANCHORED_LIFT_ONLY;
        List<SensingRaceProtocol.Leg> bench = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> hashFanned = SensingRaceProtocol.raceOn("hash-fanned",
                SensingRaceProtocol.hashFannedGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> uniform = SensingRaceProtocol.raceOn("uniform",
                SensingRaceProtocol.uniformGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> measured = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                PolicyRunFixtures.MEASURED_TAIL_LATENCY, arms);
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>(bench);
        legs.addAll(hashFanned);
        legs.addAll(uniform);
        SensingRaceProtocol.printTable("== carve admission: bench and guards at a 100-key page", legs);
        SensingRaceProtocol.printTable("== carve admission: the bench at the measured 1000-key page",
                measured);

        int hashFannedTailsClear = 0;
        for (long seed : SensingRaceProtocol.SEEDS) {
            SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(bench, candidate, seed);
            SensingRaceProtocol.Leg control = SensingRaceProtocol.at(bench, SensingVariant.CURRENT, seed);
            String at = candidate + " on the bench at seed " + seed;
            assertThat(leg.estIgnoresKeysShare())
                    .as("%s: estimate discards keys (zero by construction)", at).isZero();
            assertThat(leg.estZeroShare()).as("%s: zero estimates (zero by construction)", at).isZero();
            assertThat(leg.revalidationLossShare()).as("%s: revalidation loss share", at).isLessThan(0.70);
            assertThat(leg.serialFraction()).as("%s: serial fraction", at).isLessThan(0.05);
            assertThat(leg.tailFraction()).as("%s: post-split tail", at).isLessThan(0.01);
            assertThat(leg.result().timeline().meanOccupancy()).as("%s: mean occupancy", at)
                    .isGreaterThan(7.0);
            assertThat(leg.noVictimShare()).as("%s: steal attempts that found no victim", at)
                    .isLessThan(0.50);
            assertThat(leg.stealAttempts()).as("%s: steal attempts", at).isLessThan(500L);
            assertThat(leg.result().virtualNanos()).as("%s: virtual duration", at)
                    .isLessThan((long) (0.85 * control.result().virtualNanos()));
            assertThat(leg.result().storeCalls()).as("%s: store calls", at)
                    .isLessThanOrEqualTo(control.result().storeCalls());
            assertThat(leg.estFloorRefusalsPerPage())
                    .as("%s: owner carves refused by the remaining-work floor, per page", at)
                    .isLessThan(0.2 * control.estFloorRefusalsPerPage());

            assertGuardHeld(SensingRaceProtocol.at(uniform, candidate, seed),
                    SensingRaceProtocol.at(uniform, SensingVariant.CURRENT, seed),
                    candidate + " on the uniform guard at seed " + seed);
            SensingRaceProtocol.Leg fanned = SensingRaceProtocol.at(hashFanned, candidate, seed);
            assertGuardHeld(fanned, SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, seed),
                    candidate + " on the hash-fanned guard at seed " + seed);
            if (fanned.tailFraction() < 0.05) {
                hashFannedTailsClear++;
            }

            assertThat(SensingRaceProtocol.at(measured, candidate, seed).result().virtualNanos())
                    .as("%s: measured-regime duration against the combination's at seed %d", candidate, seed)
                    .isLessThan((long) (1.05 * SensingRaceProtocol
                            .at(measured, SensingVariant.RATE_CURSOR_ANCHORED, seed)
                            .result().virtualNanos()));
        }
        assertThat(hashFannedTailsClear)
                .as("%s: seeds clearing the hash-fanned guard's tail line — at least the three measured, "
                        + "the split this candidate is recorded with rather than a line moved to fit "
                        + "it. A floor and not an equality: a candidate that later clears the line at "
                        + "all four has removed the split, which this must not report as a failure",
                        candidate)
                .isGreaterThanOrEqualTo(3);
    }

    /**
     * <b>{@code GeometryFloorSweepProtocol}'s F4, for the best interior floor the sweep found</b> — the
     * quarter, the only floor that holds all four of the roster's cures while returning the larger of
     * its two regressions to its incumbent's own reading. It is held to exactly what the lift-only end
     * is held to above, on the same three fixtures at the same regimes and against the same baselines,
     * because a floor that bought its behaviour on the roster by spending the bench's cure or either
     * healthy shape would be no better than the end it replaces.
     *
     * <p>This is a reading on a candidate that <b>did not pass its round</b>: the sweep's deciding
     * question needed both regressions returned and this floor returns one. It is taken anyway because
     * the floor is what a later frame-conditioned candidate would sit on top of, and a guard reading
     * gathered now is one that round does not have to re-derive.
     *
     * <p><b>It inherits the lift-only end's split, and deepens it.</b> The hash-fanned guard's extra
     * tail line — the {@code < 0.05} the two anchored candidates hold at four seeds of four — this
     * floor misses at the same one seed of four, at <b>0.0651</b> against the lift-only end's 0.0510,
     * the combination's 0.0324 and the control's 0.0092, all at seed 987654321. Everything else that
     * guard is held to holds at all four, including that seed's serial fraction at 0.0370 against the
     * {@code < 0.05} line. So the tail reading is <b>not</b> something the floor buys back from the end
     * it descends from: on this guard the ladder's two upper rungs behave alike and the interior floor
     * is the worse of them. Asserted as the split as measured — three seeds clear — rather than as a
     * line moved to fit it.
     *
     * <p>Its own seed's third reading is stated for the same reason it was on the end above: at
     * 987654321 the NO_VICTIM share is <b>0.5843</b>, above both the 0.5 worst-seed reading this
     * campaign counted against E1 on this fixture and the end's own 0.4720. Not asserted — the guard
     * set is the protocol's — but a promotion decision has to be taken with it in front of it.
     */
    @Test
    void theBestInteriorFloorKeepsTheBenchCureAndBothGuards() {
        SensingVariant[] arms = {SensingVariant.CURRENT, SensingVariant.RATE_CURSOR_ANCHORED,
            SensingVariant.RATE_ANCHORED_FLOOR_QUARTER};
        SensingVariant candidate = SensingVariant.RATE_ANCHORED_FLOOR_QUARTER;
        List<SensingRaceProtocol.Leg> bench = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> hashFanned = SensingRaceProtocol.raceOn("hash-fanned",
                SensingRaceProtocol.hashFannedGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> uniform = SensingRaceProtocol.raceOn("uniform",
                SensingRaceProtocol.uniformGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, arms);
        List<SensingRaceProtocol.Leg> measured = SensingRaceProtocol.raceOn("leaf-conc",
                SensingRaceProtocol.bench(), PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                PolicyRunFixtures.MEASURED_TAIL_LATENCY, arms);
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>(bench);
        legs.addAll(hashFanned);
        legs.addAll(uniform);
        SensingRaceProtocol.printTable("== interior floor: bench and guards at a 100-key page", legs);
        SensingRaceProtocol.printTable("== interior floor: the bench at the measured 1000-key page",
                measured);

        int hashFannedTailsClear = 0;
        for (long seed : SensingRaceProtocol.SEEDS) {
            SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(bench, candidate, seed);
            SensingRaceProtocol.Leg control = SensingRaceProtocol.at(bench, SensingVariant.CURRENT, seed);
            String at = candidate + " on the bench at seed " + seed;
            assertThat(leg.estIgnoresKeysShare())
                    .as("%s: estimate discards keys (zero by construction)", at).isZero();
            assertThat(leg.estZeroShare()).as("%s: zero estimates (zero by construction)", at).isZero();
            assertThat(leg.revalidationLossShare()).as("%s: revalidation loss share", at).isLessThan(0.70);
            assertThat(leg.serialFraction()).as("%s: serial fraction", at).isLessThan(0.05);
            assertThat(leg.tailFraction()).as("%s: post-split tail", at).isLessThan(0.01);
            assertThat(leg.result().timeline().meanOccupancy()).as("%s: mean occupancy", at)
                    .isGreaterThan(7.0);
            assertThat(leg.noVictimShare()).as("%s: steal attempts that found no victim", at)
                    .isLessThan(0.50);
            assertThat(leg.stealAttempts()).as("%s: steal attempts", at).isLessThan(500L);
            assertThat(leg.result().virtualNanos()).as("%s: virtual duration", at)
                    .isLessThan((long) (0.85 * control.result().virtualNanos()));
            assertThat(leg.result().storeCalls()).as("%s: store calls", at)
                    .isLessThanOrEqualTo(control.result().storeCalls());
            assertThat(leg.estFloorRefusalsPerPage())
                    .as("%s: owner carves refused by the remaining-work floor, per page", at)
                    .isLessThan(0.2 * control.estFloorRefusalsPerPage());

            assertGuardHeld(SensingRaceProtocol.at(uniform, candidate, seed),
                    SensingRaceProtocol.at(uniform, SensingVariant.CURRENT, seed),
                    candidate + " on the uniform guard at seed " + seed);
            SensingRaceProtocol.Leg fanned = SensingRaceProtocol.at(hashFanned, candidate, seed);
            assertGuardHeld(fanned, SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, seed),
                    candidate + " on the hash-fanned guard at seed " + seed);
            if (fanned.tailFraction() < 0.05) {
                hashFannedTailsClear++;
            }

            assertThat(SensingRaceProtocol.at(measured, candidate, seed).result().virtualNanos())
                    .as("%s: measured-regime duration against the combination's at seed %d", candidate, seed)
                    .isLessThan((long) (1.05 * SensingRaceProtocol
                            .at(measured, SensingVariant.RATE_CURSOR_ANCHORED, seed)
                            .result().virtualNanos()));
        }
        assertThat(hashFannedTailsClear)
                .as("%s: seeds clearing the hash-fanned guard's tail line — at least the three measured, "
                        + "the split this floor inherits from the end it descends from", candidate)
                .isGreaterThanOrEqualTo(3);
    }

    /**
     * <b>What the hash-fanned guard's 0.05 tail line is actually reading at seed 987654321, pinned so a
     * future change that moves it is visible.</b> The split disclosed above at this one seed — the
     * E4-family readings of 0.051-0.065 against the {@code < 0.05} line — is not a quality signal at this
     * seed: the range still active when the run goes quiescent is the fixture's own open-frontier seed
     * range ({@code hi == null}, the unbounded tail {@code seedRanges} always tiles the keyspace with),
     * and it is never split, by either side, over the whole run. {@link RateAnchoredEstimator#estRemaining}
     * reads an open frontier as infinite remaining mass, which clears every floor built on that estimate
     * without exception, so the owner side refuses every carve of it with
     * {@code OwnerSplitSkipReason#OPEN_FRONTIER} — hundreds of times over a run this size — and no thief
     * ever catches it either. The tail line is therefore scoring the fleet's last-carve instant against a
     * range nothing here can divide, not against a healthy shape that failed to divide, which is why no
     * tail-fraction magnitude is asserted below. <b>If this test ever fails, the guard's 0.05 line on this
     * fixture has become meaningful again, and the disclosed E4-family readings above must be re-read
     * against whatever changed it.</b> One arm, {@code RATE_ANCHORED_FLOOR_QUARTER} — the promoted
     * floor — is enough: the invariant is about the fixture and the seed, not about which rate-family arm
     * is steering.
     */
    @Test
    void theHashFannedGuardsFinalRangeAtSeed987654321IsAnUnsplitOpenFrontier() {
        ListingFixtureStore store = new ListingFixtureStore(SensingRaceProtocol.hashFannedGuard().get());
        SensingVariant variant = SensingVariant.RATE_ANCHORED_FLOOR_QUARTER;
        long seed = 987654321L;
        PolicyScenario scenario = PolicyRunFixtures.scenario(SensingRaceProtocol.WORKERS,
                        SensingRaceProtocol.BENCH_PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY,
                        PolicyRunFixtures.measuredCost())
                .withSeed(seed)
                .withEventLog(true);
        PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory hash-fanned", variant);
        SensingRaceProtocol.requireCompleted(result, variant + "/hash-fanned/" + seed);

        List<SimEventLog.Entry> trace = result.log().entries();
        String finalRange = null;
        for (SimEventLog.Entry entry : trace) {
            if (entry.kind().equals("range.complete")) {
                finalRange = field(entry.detail(), "node");
            }
        }
        assertThat(finalRange).as("a completed run leaves at least one range to finish last").isNotNull();

        long openFrontierRefusals = 0;
        boolean finalRangeIsTheOpenFrontier = false;
        for (SimEventLog.Entry entry : trace) {
            switch (entry.kind()) {
                case "owner_split" -> assertThat(field(entry.detail(), "node"))
                        .as("the range active at run end was never split by its owner")
                        .isNotEqualTo(finalRange);
                case "steal.split" -> assertThat(field(entry.detail(), "victim"))
                        .as("the range active at run end was never split by a thief either")
                        .isNotEqualTo(finalRange);
                case "owner_split.skip" -> {
                    if ("open_frontier".equals(field(entry.detail(), "reason"))) {
                        openFrontierRefusals++;
                        finalRangeIsTheOpenFrontier |= finalRange.equals(field(entry.detail(), "node"));
                    }
                }
                default -> { }
            }
        }
        assertThat(openFrontierRefusals)
                .as("open-frontier owner-split refusals recorded on this leg").isGreaterThan(0L);
        assertThat(finalRangeIsTheOpenFrontier)
                .as("the range active at run end (%s) is the fixture's own open frontier", finalRange)
                .isTrue();
    }

    /** One {@code key=value} field out of a trace entry's {@code |}-joined detail string. */
    private static String field(String detail, String key) {
        for (String part : detail.split("\\|")) {
            int split = part.indexOf('=');
            if (split > 0 && part.substring(0, split).equals(key)) {
                return part.substring(split + 1);
            }
        }
        throw new AssertionError("no field '" + key + "' in '" + detail + "'");
    }
}
