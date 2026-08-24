/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.model.LatencyModel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>The tail is a property of the probe:page cost ratio, and this is the leg that says so.</b> One
 * fixture, one fleet, one page size, one arm — and the only thing that varies between a run whose
 * heaviest directory drains almost entirely in one 96-second serial range and a run whose fleet
 * carves it up is how much a one-key probe costs relative to a page.
 *
 * <p>{@link PolicyRunFixtures#MEASURED_TAIL_LATENCY} prices a probe at 35 ms against a 110 ms page
 * (0.32); the live store was measured at 121 ms against 223 ms (0.54,
 * {@link PolicyRunFixtures#LIVE_S3_LATENCY}). A thief snapshots its victim's cursor, spends a cascade
 * of probes placing a pivot, and loses if the victim drained past it meanwhile — so that ratio, times
 * the probes an attempt issues, <em>is</em> the race window measured in the owner's pages. At 0.32 the
 * window is under one page and a simulated thief keeps winning; at the live 0.54 it is about two, and
 * the thief loses essentially every race, which is what the engine was measured doing on a real
 * bucket's wide-flat tail (serial-tail campaign, E-10/E-11: 460 of 461 attempts on the tail range died
 * {@code cursor_passed_pivot}, and the range drained 6.6M keys alone).
 *
 * <p><b>Why this fixture, at this page size.</b> {@link SensingRaceProtocol#bench()} is the module's
 * one keyspace whose pathology is a property of the keys rather than of a schedule — eight species
 * under a Zipf rank law, the heaviest holding all
 * {@link SensingRaceProtocol#BENCH_HEAVIEST_LEAF_FILES} of its files in one flat leaf directory, which
 * is the nara-shaped tail in miniature. It runs here at the <b>full 1,000-key page</b>
 * ({@link PolicyRunFixtures#MEASURED_TAIL_PAGE_SIZE}), the page both latency profiles were measured
 * at, and at the fleet of eight every other at-scale leg on it uses. {@code MassConcentrationAtScaleTest}
 * records that at this page and the bench profile the fleet <em>absorbs</em> the heavy leaf (serial
 * fraction 0.0003–0.0096 there, 0.088–0.211 under the promoted arm) — that reading is correct, and it
 * is a reading about a 0.32 probe:page ratio. Nothing else moves below.
 *
 * <h2>What is pinned, and what the numbers were on both sides</h2>
 * The arm is {@code RATE_ANCHORED_FLOOR_QUARTER}, the promoted sensor, at all four
 * {@link SensingRaceProtocol#SEEDS}. Each threshold is stated with the reading it holds at and the
 * reading the same fixture, seed and arm produce at the bench ratio — the falsification run, which
 * fails every one of them. That run is not just recorded here: {@link
 * #atTheBenchProbeToPageRatioTheSameFixtureHandsTheLeafToTheFleet} executes it and asserts each
 * threshold inverts, so the table below is pinned rather than remembered.
 *
 * <table border="1">
 *   <caption>live 121/223 (asserted) against bench 35/110, four seeds after top-scope closure</caption>
 *   <tr><th>reading</th><th>at 0.54</th><th>at 0.32</th></tr>
 *   <tr><td>tail victim's own share of the heavy leaf</td><td>0.9725–1.000</td><td>0.4475–0.5825</td></tr>
 *   <tr><td>keys drained by the children the thief carved off it</td><td>0 ×3; 11,003 ×1</td><td>1,001–1,003</td></tr>
 *   <tr><td>proposal loss share</td><td>0.9808–0.9939</td><td>0.7160–0.7679</td></tr>
 *   <tr><td>splits published (owner + thief)</td><td>18–21</td><td>43–59</td></tr>
 *   <tr><td>serial fraction</td><td>0.4850–0.4881</td><td>0.0696–0.1790</td></tr>
 * </table>
 *
 * <p>The open-tile sentinel added by the top-scope closure makes one seed a disclosed split verdict:
 * at seed 987654321 the thief drains 11,003 keys from the tail victim. That is 2.75% of the heavy
 * leaf, not a cure: the victim still drains more than 97% and the run still spends 48.5% serial. The
 * other three seeds drain no keys through a thief child. Pinning both sides prevents this test from
 * quietly reverting to the obsolete claim that the mass-bearing tail is always an open frontier.
 *
 * <p><b>Not pinned: the window in pages itself.</b> E-11's third threshold — a scan-to-attempt window
 * with a median of one to three owner pages on the tail victim — is not asserted here because it is not
 * computable from what a run records: the event log carries {@code steal.split} for a race the thief
 * <em>wins</em> and nothing at all for the scan that opened an attempt or the retry that ended one, so
 * there is no per-attempt bracket to take a median over. Deriving it needs new executor plumbing, and
 * an instrument's leg is not the place to add it. The four thresholds above pin the same mechanism
 * through its outcomes.
 *
 * <p>Opt-in ({@code @Tag("perf")}) for memory, like its siblings on this fixture: a million-key
 * keyspace is a large share of a default test worker's heap. One fixture serves all four seeds.
 */
@Tag("perf")
class ProbeToPageRatioTailTest {

    /** The promoted sensor — the arm whose sim verdict on a real bucket this leg exists to protect. */
    private static final SensingVariant ARM = SensingVariant.RATE_ANCHORED_FLOOR_QUARTER;

    /**
     * The share of the heavy leaf the tail victim must drain by itself. It reads 0.9725–1.000: one
     * seed lets a mass-bearing thief child drain 11,003 keys, while the other three leave the victim
     * the whole leaf. The bench ratio reads 0.4475–0.5825, where the fleet takes the rest off it.
     */
    private static final double VICTIM_SHARE_FLOOR = 0.95;

    /** Proposals lost at re-validation over proposals that reached it: 0.98–0.99 here, 0.71–0.77 at 0.32. */
    private static final double PROPOSAL_LOSS_FLOOR = 0.95;

    /**
     * The band the published splits must land in: 18–21 at the live ratio, against 43–59 at
     * the bench ratio. The ceiling is what fails the optimistic regime; the floor is there because a
     * profile that priced probes so high that nothing was ever attempted would satisfy every other
     * threshold below while modelling a fleet that had stopped trying.
     */
    private static final long SPLITS_FLOOR = 18;

    private static final long SPLITS_CEILING = 21;

    /** Serial fraction: 0.4850–0.4881 here, 0.0696–0.1790 at the bench ratio. */
    private static final double SERIAL_FRACTION_FLOOR = 0.40;

    @Test
    void atTheLiveProbeToPageRatioTheHeavyLeafStillDrainsAlmostEntirelyOnOneRange() {
        List<byte[]> keys = SensingRaceProtocol.bench().get();
        List<String> rows = new ArrayList<>();
        int emptyThiefChildren = 0;
        int massBearingThiefChildren = 0;
        for (long seed : SensingRaceProtocol.SEEDS) {
            PolicyRunResult result = run(keys, seed);
            String leg = SensingRaceProtocol.label(ARM) + "/seed " + seed;
            SensingRaceProtocol.requireCompleted(result, leg);
            assertThat(result.keysEmitted()).as("%s: every key emitted", leg).isEqualTo(keys.size());

            long victim = tailVictim(result);
            Map<Long, Long> keysByNode = keysCommittedByNode(result);
            double victimShare = (double) keysByNode.getOrDefault(victim, 0L)
                    / SensingRaceProtocol.BENCH_HEAVIEST_LEAF_FILES;
            long stolenKeys = keysDrainedByThiefChildrenOf(result, victim, keysByNode);
            long splits = result.ownerSplitChildren() + result.thiefChildren();
            double lossShare = (double) result.splitsLostAtRevalidation()
                    / (result.splitsLostAtRevalidation() + result.thiefChildren());

            rows.add(String.format(Locale.ROOT,
                    "probe_page_ratio arm=%s seed=%d victim=%d victim_share=%.4f stolen_keys=%d "
                            + "owner_children=%d thief_children=%d splits=%d loss_share=%.4f "
                            + "serial=%.4f tail=%.4f tail_occupancy=%.3f duration_s=%.1f "
                            + "cursor_passed_pivot=%d all_futility_paced=%d steal_attempts=%d",
                    SensingRaceProtocol.label(ARM), seed, victim, victimShare, stolenKeys,
                    result.ownerSplitChildren(), result.thiefChildren(), splits, lossShare,
                    result.timeline().serialFraction(), result.timeline().tailFraction(),
                    result.timeline().meanTailOccupancy(), result.timeline().endNanos() / 1e9,
                    result.counter("RETRY.cursor_passed_pivot"),
                    result.counter("NO_VICTIM.all_futility_paced"),
                    result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER)));

            if (stolenKeys == 0) {
                emptyThiefChildren++;
            } else {
                massBearingThiefChildren++;
            }

            assertThat(victimShare)
                    .as("%s: the range holding the fleet drained almost all of the heavy leaf", leg)
                    .isGreaterThan(VICTIM_SHARE_FLOOR);
            assertThat(lossShare)
                    .as("%s: the thief is trying and losing, not declining to try", leg)
                    .isGreaterThan(PROPOSAL_LOSS_FLOOR);
            assertThat(splits).as("%s: splits published", leg)
                    .isBetween(SPLITS_FLOOR, SPLITS_CEILING);
            assertThat(result.timeline().serialFraction())
                    .as("%s: the fleet spends almost half the run as one worker", leg)
                    .isGreaterThan(SERIAL_FRACTION_FLOOR);
        }
        assertThat(emptyThiefChildren)
                .as("three seeds still publish only empty children from the tail victim")
                .isEqualTo(3);
        assertThat(massBearingThiefChildren)
                .as("top-scope closure lets one seed take real mass from the bounded tail")
                .isEqualTo(1);
        rows.forEach(System.out::println);
    }

    /**
     * <b>The falsification run, asserted rather than described.</b> Same fixture, same four seeds, same
     * arm, same fleet, same 1,000-key page — the only thing that moves is the probe:page ratio, from the
     * live 121/223 to the bench 35/110. Every threshold the leg above holds at must fail here, and this
     * pins that inversion mechanically so the table in this class's javadoc cannot quietly become
     * folklore: a change that made the live thresholds pass for some reason other than the ratio would
     * keep passing there and start failing here.
     *
     * <p>Asserted as the negation of each live threshold rather than against the recorded bench bands
     * (0.4475–0.5825 share, 43–59 splits, and so on). The claim being protected is "these readings are a
     * property of the ratio", which is a claim about which side of each threshold a run lands on; the
     * bands themselves are one instrument's readings and would make this brittle against any
     * scheduling-visible change without saying anything more.
     */
    @Test
    void atTheBenchProbeToPageRatioTheSameFixtureHandsTheLeafToTheFleet() {
        List<byte[]> keys = SensingRaceProtocol.bench().get();
        List<String> rows = new ArrayList<>();
        for (long seed : SensingRaceProtocol.SEEDS) {
            PolicyRunResult result = run(keys, seed, PolicyRunFixtures.MEASURED_TAIL_LATENCY);
            String leg = SensingRaceProtocol.label(ARM) + "/seed " + seed + " @bench";
            SensingRaceProtocol.requireCompleted(result, leg);
            assertThat(result.keysEmitted()).as("%s: every key emitted", leg).isEqualTo(keys.size());

            long victim = tailVictim(result);
            Map<Long, Long> keysByNode = keysCommittedByNode(result);
            double victimShare = (double) keysByNode.getOrDefault(victim, 0L)
                    / SensingRaceProtocol.BENCH_HEAVIEST_LEAF_FILES;
            long stolenKeys = keysDrainedByThiefChildrenOf(result, victim, keysByNode);
            long splits = result.ownerSplitChildren() + result.thiefChildren();
            double lossShare = (double) result.splitsLostAtRevalidation()
                    / (result.splitsLostAtRevalidation() + result.thiefChildren());

            rows.add(String.format(Locale.ROOT,
                    "probe_page_ratio_bench arm=%s seed=%d victim=%d victim_share=%.4f stolen_keys=%d "
                            + "splits=%d loss_share=%.4f serial=%.4f",
                    SensingRaceProtocol.label(ARM), seed, victim, victimShare, stolenKeys, splits,
                    lossShare, result.timeline().serialFraction()));

            assertThat(victimShare)
                    .as("%s: the fleet took the heavy leaf off the victim", leg)
                    .isLessThan(VICTIM_SHARE_FLOOR);
            assertThat(stolenKeys)
                    .as("%s: what the thief carved off it actually drained", leg)
                    .isPositive();
            assertThat(lossShare)
                    .as("%s: proposals survive re-validation here", leg)
                    .isLessThan(PROPOSAL_LOSS_FLOOR);
            assertThat(splits).as("%s: the fleet published more than the live ceiling", leg)
                    .isGreaterThan(SPLITS_CEILING);
            assertThat(result.timeline().serialFraction())
                    .as("%s: the tail is not spent as one worker", leg)
                    .isLessThan(SERIAL_FRACTION_FLOOR);
        }
        rows.forEach(System.out::println);
    }

    private static PolicyRunResult run(List<byte[]> keys, long seed) {
        return run(keys, seed, PolicyRunFixtures.LIVE_S3_LATENCY);
    }

    private static PolicyRunResult run(List<byte[]> keys, long seed, LatencyModel latency) {
        PolicyScenario scenario = PolicyRunFixtures
                .scenario(SensingRaceProtocol.WORKERS, PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                        latency, PolicyRunFixtures.measuredCost())
                .withSeed(seed)
                .withEventLog(true);
        return SimExecutor.run(scenario, new ListingFixtureStore(keys),
                "in-memory deep-nested, mass in one leaf directory", ARM);
    }

    /**
     * The range that held the fleet longest while at most one range was being drained — the tail's
     * address. Taken from the trace rather than from a counter because "the fleet went serial" and "it
     * went serial on <em>this</em> range" are different claims, and only the second can be asserted
     * against. Spans where nothing at all was draining ({@link SimTrace#FLEET_IDLE}) are not a range
     * and cannot win this.
     */
    private static long tailVictim(PolicyRunResult result) {
        return SimTrace.serialNanosByNode(result).entrySet().stream()
                .filter(held -> held.getKey() != SimTrace.FLEET_IDLE)
                .max(Map.Entry.comparingByValue())
                .orElseThrow(() -> new AssertionError("no range ever held the fleet alone"))
                .getKey();
    }

    /** Keys committed per range, from the trace's own page records. */
    private static Map<Long, Long> keysCommittedByNode(PolicyRunResult result) {
        Map<Long, Long> keys = new LinkedHashMap<>();
        for (SimEventLog.Entry entry : result.log().entries()) {
            if ("page.commit".equals(entry.kind())) {
                keys.merge(SimTrace.nodeOf(entry), Long.parseLong(SimTrace.field(entry, "keys=")),
                        Long::sum);
            }
        }
        return keys;
    }

    /**
     * Keys drained by the children a thief carved off {@code node} — the mass a steal actually took
     * off the range, as opposed to the fact that it published one. A pivot placed beyond the end
     * of the range's remaining mass produces a child that owns an empty span, which costs the fleet a
     * split and buys it nothing; a count of children cannot tell the two apart and this can.
     *
     * <p>Direct children only: it answers what the carve took, not what its descendants went on to
     * divide.
     */
    private static long keysDrainedByThiefChildrenOf(PolicyRunResult result, long node,
                                                     Map<Long, Long> keysByNode) {
        long keys = 0;
        for (SimEventLog.Entry entry : result.log().entries()) {
            if ("steal.split".equals(entry.kind())
                    && Long.parseLong(SimTrace.field(entry, "victim=")) == node) {
                keys += keysByNode.getOrDefault(Long.parseLong(SimTrace.field(entry, "child=")), 0L);
            }
        }
        return keys;
    }
}
