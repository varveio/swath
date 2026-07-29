/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.CarveBrakeMode;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link CarveBrakeSweepProtocol}'s {@code #78} bench, run.</b> The dense/uniform 20k-key flat leaf —
 * {@code KeyspaceFixtures#denseFlatLeaf}, the sim's own generator for the same shape {@code
 * ConfettiFeedbackWiringTest#denseFlat} builds in the real engine — raced under every {@link
 * CarveBrakeSweepProtocol#ARMS} entry at {@link CarveBrakeSweepProtocol#BENCH_SEEDS} (ten, not four:
 * see that protocol's own javadoc for why). One fixture serves every arm and every seed, since the keys
 * are immutable and nothing a leg does to the store is observable in another leg's result.
 *
 * <p><b>What is read, and what is not.</b> {@code ConfettiFeedbackWiringTest}'s own bench asserts, under
 * the 0.2.0 default pair with no brake, that {@code OWNER_SPLIT.confetti_suppressed} stays zero and that
 * substantial children dominate confetti ones — and discloses that assertion is non-deterministic there
 * (4/10 passes). This round does not repeat that assertion as a magnitude on the candidate arms: whether
 * a {@code K} closes {@code #78} is exactly this round's open question, so the classification read here
 * is a <b>reported pass rate</b>, not an enforced threshold. What every leg <em>is</em> required to do,
 * whichever arm it ran under, is complete and emit the fixture's own full key count — a leg that stalls
 * or drops keys is not a candidate that "won", it is a candidate that is broken.
 */
class CarveBrakeDenseUniformRaceTest {

    /** One arm's readings across every seed: the classification pass rate and the brake's own engagement. */
    private record ArmSummary(String arm, int passes, int seeds, long brakedTotal, long probedTotal,
                              long confettiSuppressedTotal) {

        String row() {
            return String.format(Locale.ROOT, "%-10s %2d/%-2d  suppressed=%-4d braked=%-6d probe=%-6d",
                    arm, passes, seeds, confettiSuppressedTotal, brakedTotal, probedTotal);
        }
    }

    @Test
    void everyCarveBrakeModeOnTheDenseUniformShapeAtTenSeeds() {
        ListingFixtureStore store = new ListingFixtureStore(
                KeyspaceFixtures.denseFlatLeaf(CarveBrakeSweepProtocol.BENCH_KEYS));
        List<ArmSummary> summaries = new ArrayList<>();
        for (CarveBrakeMode mode : CarveBrakeSweepProtocol.ARMS) {
            Map<Long, PolicyRunResult> bySeed = new LinkedHashMap<>();
            for (long seed : CarveBrakeSweepProtocol.BENCH_SEEDS) {
                bySeed.put(seed, runLeg(mode, store, seed));
            }
            summaries.add(summarize(mode, bySeed));
        }
        printSummary(summaries);

        for (ArmSummary summary : summaries) {
            assertThat(summary.passes()).as("%s: legs measured", summary.arm())
                    .isGreaterThanOrEqualTo(0);
        }
    }

    private static PolicyRunResult runLeg(CarveBrakeMode mode, ListingFixtureStore store, long seed) {
        EngineToggles toggles = EngineToggles.DEFAULT.withCarveBrake(mode);
        PolicyScenario base = PolicyRunFixtures
                .scenario(CarveBrakeSweepProtocol.BENCH_WORKERS, CarveBrakeSweepProtocol.BENCH_PAGE_SIZE,
                        PolicyRunFixtures.REMOTE_LATENCY, PolicyRunFixtures.measuredCost())
                .withSeed(seed);
        PolicyScenario scenario = new PolicyScenario(base.seed(), base.workerCount(), base.pageSize(),
                base.scanPrefix(), base.seedMode(), toggles, base.latency(), base.clientCost(),
                base.budgets(), base.faultDisposition(), base.storeServerCapacity(), base.recordEventLog(),
                base.maxEvents());
        PolicyRunResult result = SimExecutor.run(scenario, store, "dense-uniform (" + mode.code() + ")",
                SensingVariant.CURRENT);
        String leg = mode.code() + "/dense-uniform/seed " + seed;
        SensingRaceProtocol.requireCompleted(result, leg);
        assertThat(result.keysEmitted()).as("leg %s emitted every key", leg).isEqualTo(store.size());
        return result;
    }

    /**
     * The classification rule {@code ConfettiFeedbackWiringTest#denseUniformShapeNeverEngagesTheGate
     * AndSubstantialDominates} pins: a "pass" is the suppression gate never tripping AND substantial
     * children outnumbering confetti ones. Read the same way here, per seed, so the pass rate is
     * comparable to that test's own 4/10.
     */
    private static ArmSummary summarize(CarveBrakeMode mode, Map<Long, PolicyRunResult> bySeed) {
        int passes = 0;
        long brakedTotal = 0;
        long probedTotal = 0;
        long suppressedTotal = 0;
        for (PolicyRunResult result : bySeed.values()) {
            long suppressed = result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + ".confetti_suppressed");
            long confetti = result.counter(SimExecutor.OWNER_SPLIT_CHILD_CONFETTI_COUNTER);
            long substantial = result.counter(SimExecutor.OWNER_SPLIT_CHILD_SUBSTANTIAL_COUNTER);
            suppressedTotal += suppressed;
            brakedTotal += result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + ".carve_braked");
            probedTotal += result.counter(SimExecutor.OWNER_SPLIT_CATEGORY + ".carve_brake_probe");
            if (suppressed == 0 && substantial > confetti) {
                passes++;
            }
        }
        return new ArmSummary(mode.code(), passes, bySeed.size(), brakedTotal, probedTotal, suppressedTotal);
    }

    private static void printSummary(List<ArmSummary> summaries) {
        StringBuilder out = new StringBuilder("carve-brake #78 dense/uniform bench — ")
                .append(CarveBrakeSweepProtocol.BENCH_SEEDS.length).append(" seeds per arm")
                .append(System.lineSeparator());
        for (ArmSummary summary : summaries) {
            out.append(summary.row()).append(System.lineSeparator());
        }
        System.out.print(out);
    }
}
