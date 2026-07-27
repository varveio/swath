/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

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
 * The serial tail the bench exists to produce falls by roughly two orders of magnitude at every seed,
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
 * <p><b>E1 alone also damages a healthy keyspace.</b> On the hash-fanned guard it reaches serial
 * fraction 0.120 at seed 20260727 and 0.030 at 987654321, against a control range of 0.0004–0.0093,
 * with the NO_VICTIM share at 0.80 and 0.48 against 0.012 and 0.075. The trace says why: with no
 * position term, a nearly-finished range still outranks everything, thieves attack it, fail, and the
 * per-victim futility cooldown pages every candidate out — {@code NO_VICTIM.all_futility_paced} 381
 * against the control's 1. That is a <b>2-of-4-seed regression and is reported as the split verdict
 * it is</b>, not averaged away; it is also exactly the cost E1's own note predicted for an estimator
 * that gives up position. E2 and E1+E2 hold both guards at all four seeds.
 *
 * <h2>What this says, and what it does not</h2>
 * The sensor was the thing gating division on this keyspace: fixing the reading, and nothing else,
 * removes the serial tail entirely at the regime that produces it. It does not follow that any of
 * these three is shippable — all three lose at the page size a real deployment uses, which is the
 * regime the deployment's own tail was measured in, and the reason for that loss is a calibration
 * problem rather than a visibility one. E2 is the only candidate whose reading is byte-identical to
 * the shipped one wherever the shipped one works, and it is the one that keeps both guards; E1 is the
 * one that eliminates every degenerate reading, and it is the one that breaks a healthy shape. The
 * combination keeps both properties and still loses the measured regime.
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
                // every seed, for every candidate -- 0.692-0.706 for the shipped one.
                assertThat(leg.estIgnoresKeysShare()).as("%s: estimate discards keys", at).isZero();
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
        // zero. E2 keeps a position, so it keeps a few genuinely-exhausted candidates -- 0.015-0.050,
        // against the control's 0.036-0.039, which is why this is pinned per variant and not for all.
        for (SensingVariant variant : new SensingVariant[] {
            SensingVariant.RATE, SensingVariant.RATE_CURSOR_ANCHORED}) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                assertThat(SensingRaceProtocol.at(legs, variant, seed).estZeroShare())
                        .as("%s at seed %d: zero estimates", variant, seed).isZero();
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
        // nothing any of them does to the estimate costs it anything.
        for (SensingVariant variant : CANDIDATES) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                assertThat(SensingRaceProtocol.at(uniform, variant, seed).serialFraction())
                        .as("%s on the uniform guard at seed %d", variant, seed).isLessThan(0.05);
            }
        }
        // The hash-fanned guard separates the candidates. The two that keep a position term hold it at
        // every seed, on the guard's own health threshold.
        for (SensingVariant variant : new SensingVariant[] {
            SensingVariant.CURSOR_ANCHORED, SensingVariant.RATE_CURSOR_ANCHORED}) {
            for (long seed : SensingRaceProtocol.SEEDS) {
                SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(hashFanned, variant, seed);
                assertThat(leg.serialFraction()).as("%s on the hash-fanned guard at seed %d", variant, seed)
                        .isLessThan(0.05);
                assertThat(leg.tailFraction()).as("%s hash-fanned tail at seed %d", variant, seed)
                        .isLessThan(0.05);
            }
        }
        // E1 does not. Stated as what it is -- a regression at two of the four seeds, so the claim
        // pinned here is over the four-seed set rather than at any one of them: its worst seed is an
        // order of magnitude past the control's worst, with the fleet spending most of its steal
        // attempts finding every candidate paged out by the futility cooldown.
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
}
