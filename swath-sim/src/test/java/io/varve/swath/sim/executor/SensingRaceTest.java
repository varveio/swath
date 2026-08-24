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
 * <p><b>Top-scope closure changed which healthy shape rejects the candidates.</b> Once the seed planner
 * bounds the rightmost mass-bearing range, all three original candidates fail the uniform guard at
 * seed 424242: serial fraction crosses the pre-registered 0.05 line and mean occupancy falls below 7
 * of 8. The plain rate arm also exceeds the hash-fanned store-call budget. These lines are deliberately
 * not relaxed after seeing the result. A one-seed failure is a rejected candidate under the
 * protocol's four-seed rule.
 *
 * <h2>What this says, and what it does not</h2>
 * The sensor was the thing gating division on this keyspace: fixing the reading, and nothing else,
 * removes the serial tail entirely at the regime that produces it. It does not follow that any of
 * these three is shippable — all three lose at the page size a real deployment uses, which is the
 * regime the deployment's own tail was measured in, and the reason for that loss is a calibration
 * problem rather than a visibility one. E1 is the one whose degenerate readings are all zero, which
 * is a property of its arithmetic rather than a result. Top-scope closure does not rescue any arm:
 * every candidate still loses the measured regime and now also fails the uniform healthy-shape guard.
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
                assertBenchCureHeld(SensingRaceProtocol.at(legs, variant, seed),
                        SensingRaceProtocol.at(legs, SensingVariant.CURRENT, seed),
                        variant + " at seed " + seed);
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
    void topScopeClosureRejectsRateOnHashFannedAndEveryCandidateOnUniform() {
        List<SensingRaceProtocol.Leg> hashFanned = SensingRaceProtocol.raceOn("hash-fanned",
                SensingRaceProtocol.hashFannedGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, ALL);
        List<SensingRaceProtocol.Leg> uniform = SensingRaceProtocol.raceOn("uniform",
                SensingRaceProtocol.uniformGuard(), SensingRaceProtocol.BENCH_PAGE_SIZE,
                PolicyRunFixtures.REMOTE_LATENCY, ALL);
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>(hashFanned);
        legs.addAll(uniform);
        SensingRaceProtocol.printTable("== guards: the two healthy shapes", legs);

        // Closing the top scope invalidated every candidate on the uniform guard at seed 424242. Keep
        // the pre-registered line fixed: a candidate that crosses it is rejected, not accommodated by
        // moving the line after the result is known. At this seed all three candidates also cross the
        // occupancy boundary, so two independent readings pin the regression.
        for (SensingVariant variant : CANDIDATES) {
            SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(uniform, variant, 424242L);
            assertThat(leg.serialFraction())
                    .as("%s on the uniform guard at seed 424242: disclosed rejection", variant)
                    .isGreaterThanOrEqualTo(0.05);
            assertThat(leg.result().timeline().meanOccupancy())
                    .as("%s on the uniform guard at seed 424242: occupancy regression", variant)
                    .isLessThan(7.0);
        }
        // The two anchored candidates hold the hash-fanned guard on every seed, including its
        // additional tail criterion.
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
        // The plain rate arm is also rejected on hash-fanned: at this seed it exceeds the unchanged
        // five-percent store-call budget even though its serial and occupancy readings look healthy.
        SensingRaceProtocol.Leg rate =
                SensingRaceProtocol.at(hashFanned, SensingVariant.RATE, 987654321L);
        SensingRaceProtocol.Leg control =
                SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, 987654321L);
        assertThat(rate.result().storeCalls())
                .as("rate on hash-fanned at seed 987654321: store-call regression")
                .isGreaterThanOrEqualTo((long) (1.05 * control.result().storeCalls()));
    }

    /**
     * <b>The bench cure, in full, as the protocol declares it</b> — every criterion a candidate is held
     * to on the bench at its own regime, read against the control <em>at the same seed</em> wherever the
     * criterion is a comparison. One helper because three rounds hold three different candidates to
     * exactly this list, and a list restated per round is a list that drifts per round: a threshold
     * softened in one copy would read as a candidate that passed rather than as a criterion that moved.
     *
     * <p>{@code estZeroShare} is <b>not</b> here, and deliberately: it is zero by construction only for
     * the variants that carry no position term, so the rounds that assert it assert it at their own call
     * sites — see {@link #everyCandidateCuresTheBenchAtItsOwnPageRegime()}, which pins it for two of its
     * three candidates and nowhere reports E2 as improving it.
     */
    private static void assertBenchCureHeld(SensingRaceProtocol.Leg leg, SensingRaceProtocol.Leg control,
                                            String at) {
        // The denominator both degenerate-estimate criteria are read against. A leg that scored no
        // bounded victim would satisfy them having measured nothing, and a share over nothing is
        // reported as zero so that the printed table has no NaN in it -- so the denominator is asserted
        // where the criteria are, rather than trusted.
        assertThat(leg.boundedVictimsScanned())
                .as("%s: bounded victims scored, the denominator the estimate readings are shares of", at)
                .isPositive();
        // PRIMARY 2: the estimate stops discarding a range's emitted keys. Exactly zero, at every seed,
        // for every candidate -- 0.692-0.706 for the shipped one. A measurement for E2 only: E1 and
        // E1+E2 estimate FROM the emitted count, so a zero here is an arithmetic identity for them and
        // what the assertion pins is that the counter follows the installed sensor, not that the sensor
        // improved.
        assertThat(leg.estIgnoresKeysShare())
                .as("%s: estimate discards keys (zero by construction for the position-free variants)", at)
                .isZero();
        // PRIMARY 3: fewer split proposals lose the race to the victim's own cursor. The control's four
        // seeds span 0.778-0.852 and no candidate seed reaches 0.70.
        assertThat(leg.revalidationLossShare()).as("%s: revalidation loss share", at).isLessThan(0.70);
        // PRIMARY 4, at the regime it is quoted for: the tail the bench exists to produce is gone. Two
        // orders of magnitude, at every seed.
        assertThat(leg.serialFraction()).as("%s: serial fraction at a 100-key page", at)
                .isLessThan(0.05);
        assertThat(leg.tailFraction()).as("%s: post-split tail at a 100-key page", at).isLessThan(0.01);
        // And the fleet that was starving is not: nearly every worker busy, on a fraction of the steal
        // attempts, in less time, for no more store calls.
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

    /** The lift-only candidate keeps its bench cure but is rejected by the uniform guard. */
    @Test
    void theCarveAdmissionCandidateKeepsTheBenchCureButFailsAHealthyGuard() {
        runRejectedCandidateAssessment(SensingVariant.RATE_ANCHORED_LIFT_ONLY, "carve admission",
                "%s: seeds clearing the hash-fanned guard's tail line — at least the three measured, "
                        + "the split this candidate is recorded with rather than a line moved to fit "
                        + "it. A floor and not an equality: a candidate that later clears the line at "
                        + "all four has removed the split, which this must not report as a failure");
    }

    /** The quarter-floor candidate keeps its bench cure but is rejected by the uniform guard. */
    @Test
    void theBestInteriorFloorKeepsTheBenchCureButFailsAHealthyGuard() {
        runRejectedCandidateAssessment(SensingVariant.RATE_ANCHORED_FLOOR_QUARTER, "interior floor",
                "%s: seeds clearing the hash-fanned guard's tail line — at least the three measured, "
                        + "the split this floor inherits from the end it descends from");
    }

    /**
     * Assess one later candidate against the control and incumbent combination. A candidate must keep
     * the bench cure and measured-regime comparison, but is now expected to be rejected by at least
     * one seeded leg of either healthy-shape guard.
     *
     * @param candidate the arm under test, raced on the bench, both guards and the measured regime
     * @param round     the caption both printed tables are titled with
     * @param tailSplit the description of the hash-fanned tail split this round records, taking the
     *                  candidate as its one format argument — a floor and not an equality in both
     *                  rounds, so a candidate that later clears all four seeds is not a failure here
     */
    private static void runRejectedCandidateAssessment(SensingVariant candidate, String round,
                                                       String tailSplit) {
        SensingVariant[] arms = {SensingVariant.CURRENT, SensingVariant.RATE_CURSOR_ANCHORED, candidate};
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
        SensingRaceProtocol.printTable("== " + round + ": bench and guards at a 100-key page", legs);
        SensingRaceProtocol.printTable("== " + round + ": the bench at the measured 1000-key page",
                measured);

        int hashFannedTailsClear = 0;
        int uniformGuardFailures = 0;
        int hashFannedGuardFailures = 0;
        for (long seed : SensingRaceProtocol.SEEDS) {
            SensingRaceProtocol.Leg leg = SensingRaceProtocol.at(bench, candidate, seed);
            String at = candidate + " on the bench at seed " + seed;
            // The criterion the rate family carries beyond the shared list: with no position term the
            // only score that can read zero is a cursor already at its bound, which is a finished range
            // and never a scanned candidate.
            assertThat(leg.estZeroShare()).as("%s: zero estimates (zero by construction)", at).isZero();
            assertBenchCureHeld(leg, SensingRaceProtocol.at(bench, SensingVariant.CURRENT, seed), at);

            SensingRaceProtocol.Leg uniformCandidate = SensingRaceProtocol.at(uniform, candidate, seed);
            SensingRaceProtocol.Leg uniformControl =
                    SensingRaceProtocol.at(uniform, SensingVariant.CURRENT, seed);
            if (!guardHeld(uniformCandidate, uniformControl)) {
                uniformGuardFailures++;
            }
            SensingRaceProtocol.Leg fanned = SensingRaceProtocol.at(hashFanned, candidate, seed);
            if (!guardHeld(fanned,
                    SensingRaceProtocol.at(hashFanned, SensingVariant.CURRENT, seed))) {
                hashFannedGuardFailures++;
            }
            if (fanned.tailFraction() < 0.05) {
                hashFannedTailsClear++;
            }

            assertThat(SensingRaceProtocol.at(measured, candidate, seed).result().virtualNanos())
                    .as("%s: measured-regime duration against the combination's at seed %d", candidate, seed)
                    .isLessThan((long) (1.05 * SensingRaceProtocol
                            .at(measured, SensingVariant.RATE_CURSOR_ANCHORED, seed)
                            .result().virtualNanos()));
        }
        assertThat(uniformGuardFailures + hashFannedGuardFailures)
                .as("%s: seeded legs rejected by the pre-registered healthy-shape guards", candidate)
                .isPositive();
        assertThat(hashFannedTailsClear).as(tailSplit, candidate).isGreaterThanOrEqualTo(3);
    }

    private static boolean guardHeld(SensingRaceProtocol.Leg leg, SensingRaceProtocol.Leg control) {
        return leg.serialFraction() < 0.05
                && leg.result().timeline().meanOccupancy() > 7.0
                && leg.result().virtualNanos() < (long) (1.05 * control.result().virtualNanos())
                && leg.result().storeCalls() < (long) (1.05 * control.result().storeCalls());
    }

    /**
     * Pin the mechanism that invalidated the old open-frontier characterization. Top-scope closure
     * gives the final mass-bearing seed range a finite upper bound; on this seed its owner uses that
     * eligibility and splits it. The final range must therefore be seen on an owner-split event and
     * must never be refused as an open frontier.
     */
    @Test
    void theHashFannedGuardsFinalRangeAtSeed987654321IsNowBoundedAndOwnerSplit() {
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

        boolean finalRangeWasOwnerSplit = false;
        boolean finalRangeIsTheOpenFrontier = false;
        for (SimEventLog.Entry entry : trace) {
            switch (entry.kind()) {
                case "owner_split" ->
                    finalRangeWasOwnerSplit |= finalRange.equals(field(entry.detail(), "node"));
                case "owner_split.skip" -> {
                    if ("open_frontier".equals(field(entry.detail(), "reason"))) {
                        finalRangeIsTheOpenFrontier |= finalRange.equals(field(entry.detail(), "node"));
                    }
                }
                default -> { }
            }
        }
        assertThat(finalRangeWasOwnerSplit)
                .as("top-scope closure made the final mass-bearing range eligible for owner splitting")
                .isTrue();
        assertThat(finalRangeIsTheOpenFrontier)
                .as("the final mass-bearing range is no longer the seed's open frontier")
                .isFalse();
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
