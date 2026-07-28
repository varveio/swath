/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.AlphabetDigest;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.StealPolicy;
import io.varve.swath.engine.policy.ThiefPolicy;
import io.varve.swath.engine.policy.VictimView;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * <b>The two mirrored policies are held to the engine's own.</b> A sensing variant is installed by
 * reproducing victim selection and the owner-split gate chain in this module with one substitution —
 * where the estimate comes from — and a copy that drifts from its original would turn a race result
 * into a comparison of two unrelated algorithms without failing anything.
 *
 * <p>So both mirrors are driven with the <b>incumbent</b> estimator installed, over the same inputs
 * as the engine's own policies, and required to decide identically. That is what guards the two
 * constants the engine keeps package-private and this module therefore has to duplicate — but only as
 * far as the battery's own observations reach, which is stated exactly on
 * {@link #confettiObservations()} rather than implied: the confetti rates bracket the suppression
 * threshold at 0.5 and 0.5625, and the probe sequences catch a probe period that halves or doubles.
 */
class SensingVariantParityTest {

    private static final int WORKERS = 8;
    private static final int PAGE = 100;

    @Test
    void mirroredVictimSelectionDecidesExactlyAsTheEnginesDoes() {
        StealPolicy engine = new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0, null);
        StealPolicy mirror = new EstimatorStealPolicy(new WindowEstimator(),
                new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0, null));

        int pools = 0;
        for (List<VictimView> pool : victimPools()) {
            pools++;
            Selection expected = engine.selectVictim(pool);
            Selection actual = mirror.selectVictim(pool);
            assertThat(actual).as("pool %s", pool).isEqualTo(expected);
        }
        assertThat(pools).as("the battery has to actually exercise something").isGreaterThan(200);
    }

    @Test
    void theMirroredOwnerSplitGateChainDecidesExactlyAsTheEnginesDoes() {
        OwnerSplitPolicy engine = new OwnerSplitGovernor(EngineToggles.DEFAULT, WORKERS, PAGE, null);
        OwnerSplitPolicy mirror = new EstimatorOwnerSplitPolicy(new WindowEstimator(),
                EngineToggles.DEFAULT, WORKERS, PAGE);

        int carves = 0;
        int skips = 0;
        Set<String> reasons = new HashSet<>();
        for (OwnerSplitView view : ownerSplitViews()) {
            OwnerSplitDecision expected = engine.decide(view);
            OwnerSplitDecision actual = mirror.decide(view);
            assertSameDecision(expected, actual, view);
            if (expected instanceof Carve) {
                carves++;
            } else {
                skips++;
            }
            expected.engagements().forEach(e -> reasons.add(e.reason()));
        }
        // A parity check that only ever saw refusals would prove nothing about the pivot half of the
        // chain, and one that never reached a branch says nothing about the constants that govern it --
        // so the branches the battery has to reach are named rather than counted.
        assertThat(carves).as("carves reached").isPositive();
        assertThat(skips).as("skips reached").isPositive();
        assertThat(reasons).as("branches reached")
                .contains(OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code(), "confetti_probe",
                        OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code(),
                        "alphabet_chosen", "alphabet_fallback");
    }

    /**
     * <b>Which route a run's decisions were taken through, out of the run's own counters.</b> The
     * mirrors above are only the algorithm they claim to be if they were the objects the run steered
     * with, and a run that quietly installed the shipped pair under a variant's name would produce a
     * whole race table of the incumbent measured twice. The route is instrumented like every other algo
     * path here (AGENTS.md), so the check is a counter read rather than a reflection over fields.
     */
    @Test
    void aRunsCountersNameTheRouteItsDecisionsWereTakenThrough() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(2_000));
        PolicyScenario scenario = PolicyRunFixtures.scenario(4, PAGE, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost());

        PolicyRunResult shipped = SimExecutor.run(scenario, store, "in-memory dense flat leaf");
        PolicyRunResult variant = SimExecutor.run(
                scenario.withClientCost(PolicyRunFixtures.measuredCost()), store,
                "in-memory dense flat leaf", SensingVariant.RATE_CURSOR_ANCHORED);

        assertThat(route(shipped)).as("the shipped sensor steers on the engine's own pair")
                .containsExactly(SimExecutor.OWNER_SPLIT_ROUTE_SHIPPED, SimExecutor.THIEF_ROUTE_SHIPPED);
        assertThat(route(variant)).as("a variant steers on both mirrors, not one of them")
                .containsExactly(SimExecutor.OWNER_SPLIT_ROUTE_ESTIMATOR,
                        SimExecutor.THIEF_ROUTE_ESTIMATOR);
    }

    /** The routes {@code result} counted, in name order — one counter per route, once per run. */
    private static List<String> route(PolicyRunResult result) {
        String prefix = SimExecutor.SENSING_ROUTE_CATEGORY + ".";
        List<String> routes = new ArrayList<>();
        result.counters().forEach((name, value) -> {
            if (name.startsWith(prefix)) {
                assertThat(value).as("%s is a per-run route, not a tally", name).isEqualTo(1L);
                routes.add(name.substring(prefix.length()));
            }
        });
        return routes;
    }

    private static void assertSameDecision(OwnerSplitDecision expected, OwnerSplitDecision actual,
                                           OwnerSplitView view) {
        assertThat(actual.getClass()).as("decision kind for %s", describe(view)).isEqualTo(expected.getClass());
        assertThat(actual.engagements()).as("engagements for %s", describe(view))
                .isEqualTo(expected.engagements());
        assertThat(actual.mutations()).as("mutations for %s", describe(view))
                .isEqualTo(expected.mutations());
        if (expected instanceof Carve carve) {
            // Carve's generated equals compares the pivot array by reference, so the bytes are compared
            // here instead -- the whole point of this test is that they are the same bytes.
            assertThat(((Carve) actual).pivot()).as("pivot for %s", describe(view))
                    .isEqualTo(carve.pivot());
        }
    }

    private static String describe(OwnerSplitView view) {
        return "lo=" + text(view.lo()) + " cursor=" + text(view.cursorTo()) + " hi=" + text(view.hi())
                + " keys=" + view.keysEmitted() + " committed=" + view.committed()
                + " outstanding=" + view.outstanding() + " confetti=" + view.confetti();
    }

    private static String text(byte[] key) {
        return key == null ? "<none>" : new String(key, StandardCharsets.UTF_8);
    }

    // ---- the batteries ------------------------------------------------------------------

    /** Keys shaped like the fixtures: a deep shared prefix, plus shallow ones for the healthy case. */
    private static final byte[][] KEYS = {
        null,
        key(""),
        key("species/"),
        key("species/Balearica_regulorum/bBalReg1/"),
        key("species/Balearica_regulorum/bBalReg1/assembly_vgp/intermediates/00417"),
        key("species/Balearica_regulorum/bBalReg1/assembly_vgp/intermediates/09999"),
        key("species/Bathysaurus_mollis/fBatMol1/"),
        key("species/Zebra/"),
    };

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Pools that reach every branch of selection: empty, all-unsplittable, all-paced, all-zero-span,
     * mixed skips, an open frontier, and ordinary contests decided on the estimate.
     */
    private static List<List<VictimView>> victimPools() {
        List<VictimView> candidates = new ArrayList<>();
        long nodeId = 0;
        for (int loIdx = 0; loIdx < KEYS.length; loIdx++) {
            for (int hiIdx = loIdx; hiIdx < KEYS.length; hiIdx++) {
                for (int curIdx = 0; curIdx < KEYS.length; curIdx++) {
                    for (long keys : new long[] {0L, 1L, 400_000L}) {
                        candidates.add(new VictimView(nodeId++, KEYS[loIdx], KEYS[curIdx],
                                hiIdx == KEYS.length - 1 ? null : KEYS[hiIdx], keys,
                                (nodeId % 7) == 0, (nodeId % 5) == 0));
                    }
                }
            }
        }
        List<List<VictimView>> pools = new ArrayList<>();
        pools.add(List.of());
        // Sliding windows of the battery, so a pool is a contest between neighbours rather than one
        // candidate seen in isolation, and every skip-only pool appears somewhere in the sweep.
        for (int i = 0; i + 4 <= candidates.size(); i += 3) {
            pools.add(List.copyOf(candidates.subList(i, i + 4)));
        }
        return pools;
    }

    /** Views that reach every gate: the floors, the rate limit, the demand gate, the confetti gate. */
    private static List<OwnerSplitView> ownerSplitViews() {
        List<OwnerSplitView> views = new ArrayList<>();
        for (byte[] lo : Arrays.asList(KEYS[1], KEYS[2], KEYS[3])) {
            for (byte[] hi : Arrays.asList(null, KEYS[6], KEYS[7])) {
                for (byte[] cursor : Arrays.asList(KEYS[3], KEYS[4], KEYS[5])) {
                    for (long keys : new long[] {0L, 5_000L, 400_000L}) {
                        for (long committed : new long[] {1L, 40L}) {
                            for (long outstanding : new long[] {1L, 8L}) {
                                for (double density : new double[] {0.5, 0.9}) {
                                    for (long[] confetti : confettiObservations()) {
                                        for (PivotShape shape : pivotShapes()) {
                                            views.add(new OwnerSplitView(hi, lo, cursor, keys, committed,
                                                    0L, outstanding, density,
                                                    shape.observedDensityRatio(), shape.alphabet(),
                                                    new ConfettiObservation(confetti[0], confetti[1],
                                                            confetti[2])));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return views;
    }

    /**
     * Confetti readings chosen to bracket the gate's own two constants, so drift in either is caught
     * rather than merely likely to be. {@code {taggedTotal, taggedConfetti, probeSeq}}:
     * <ul>
     *   <li><b>the suppression threshold</b> — 2/16 = 0.125 and 15/16 = 0.9375 straddle it loosely;
     *       <b>8/16 = 0.5 (which must NOT suppress, the comparison being strict) and 9/16 = 0.5625
     *       (which must)</b> straddle it tightly. Between them, any engine threshold outside
     *       {@code [0.5, 0.5625)} makes one of these two decide differently here than there.</li>
     *   <li><b>the probe period</b> — {@code probeSeq} 15 probes at a period of sixteen and suppresses
     *       at thirty-two, so a doubling is caught; {@code probeSeq} 7 suppresses at sixteen and
     *       probes at eight, so a halving is caught. The old battery had neither: 3 and 15 alone
     *       decide identically under 8 and 16.</li>
     * </ul>
     */
    private static List<long[]> confettiObservations() {
        return List.of(
                new long[] {0L, 0L, 0L},
                new long[] {16L, 2L, 3L},
                new long[] {16L, 8L, 3L},
                new long[] {16L, 9L, 3L},
                new long[] {16L, 9L, 7L},
                new long[] {16L, 15L, 3L},
                new long[] {16L, 15L, 15L});
    }

    /**
     * One (observed density ratio, observed alphabet) pair the gate battery is run under: the neutral
     * default, a <b>thinning</b> density whose ratio below 1 shrinks the reachable child tail the
     * observed-mass floor and both reflection decisions read, and an <b>observed alphabet</b>, which
     * is what sends pivot synthesis down its alphabet-aware branch instead of its fallback. Both sides
     * call the same {@code StealMath} for these, so this is coverage of the mirrored chain rather than
     * a suspected divergence.
     */
    private record PivotShape(double observedDensityRatio, AlphabetDigest.Snapshot alphabet) {
    }

    private static List<PivotShape> pivotShapes() {
        return List.of(new PivotShape(1.0, null),
                new PivotShape(0.25, null),
                new PivotShape(1.0, observedAlphabet()));
    }

    /**
     * An observed alphabet built the way a run builds one — a {@link WorkerState} over the battery's
     * own outermost bounds, fed the page endpoints it would have committed — rather than a synthetic
     * digest, so the branch this exercises is the one a run reaches. The third species is what makes
     * the digest bite: it populates the gap between the battery's cursors and their bounds at the
     * positions a pivot is synthesised at, so the alphabet-aware choice differs from the plain one
     * instead of falling back through it.
     */
    private static AlphabetDigest.Snapshot observedAlphabet() {
        WorkerState state = new WorkerState(0L, KEYS[2], null, KEYS[7]);
        state.recordPage(KEYS[3], KEYS[5], 4_000L);
        state.recordPage(KEYS[5], key("species/Mammoth_primigenius/mMamPri1/"), 4_000L);
        return state.alphabetDigest().snapshot();
    }
}
