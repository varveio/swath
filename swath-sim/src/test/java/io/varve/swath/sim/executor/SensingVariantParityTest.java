/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.StealPolicy;
import io.varve.swath.engine.policy.ThiefPolicy;
import io.varve.swath.engine.policy.VictimView;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * <b>The two mirrored policies are held to the engine's own.</b> A sensing variant is installed by
 * reproducing victim selection and the owner-split gate chain in this module with one substitution —
 * where the estimate comes from — and a copy that drifts from its original would turn a race result
 * into a comparison of two unrelated algorithms without failing anything.
 *
 * <p>So both mirrors are driven with the <b>incumbent</b> estimator installed, over the same inputs
 * as the engine's own policies, and required to decide identically. That covers the two constants the
 * engine keeps package-private and this module therefore has to duplicate: a change to either in the
 * engine breaks the confetti-gate cases below rather than quietly changing what a variant is measured
 * against.
 */
class SensingVariantParityTest {

    private static final int WORKERS = 8;
    private static final int PAGE = 100;

    @Test
    void mirroredVictimSelectionDecidesExactlyAsTheEnginesDoes() {
        StealPolicy engine = new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0);
        StealPolicy mirror = new EstimatorStealPolicy(new WindowEstimator(),
                new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0));

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
        OwnerSplitPolicy engine = new OwnerSplitGovernor(EngineToggles.DEFAULT, WORKERS, PAGE);
        OwnerSplitPolicy mirror = new EstimatorOwnerSplitPolicy(new WindowEstimator(),
                EngineToggles.DEFAULT, WORKERS, PAGE);

        int carves = 0;
        int skips = 0;
        int confettiBranches = 0;
        for (OwnerSplitView view : ownerSplitViews()) {
            OwnerSplitDecision expected = engine.decide(view);
            OwnerSplitDecision actual = mirror.decide(view);
            assertSameDecision(expected, actual, view);
            if (expected instanceof Carve) {
                carves++;
            } else {
                skips++;
            }
            if (expected.engagements().stream()
                    .anyMatch(e -> e.reason().startsWith("confetti"))) {
                confettiBranches++;
            }
        }
        // A parity check that only ever saw refusals would prove nothing about the pivot half of the
        // chain, and one that never reached the confetti gate would not cover the duplicated constants.
        assertThat(carves).as("carves reached").isPositive();
        assertThat(skips).as("skips reached").isPositive();
        assertThat(confettiBranches).as("the confetti gate's own branches reached").isPositive();
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
                                        views.add(new OwnerSplitView(hi, lo, cursor, keys, committed, 0L,
                                                outstanding, density, 1.0, null,
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
        return views;
    }

    /**
     * Confetti readings that sit either side of the gate's own thresholds: below the warmup sample,
     * above it but under the suppression rate, over the rate on a sequence that suppresses, and over
     * the rate on the one sequence out of sixteen that probes instead.
     */
    private static List<long[]> confettiObservations() {
        return List.of(
                new long[] {0L, 0L, 0L},
                new long[] {16L, 2L, 3L},
                new long[] {16L, 15L, 3L},
                new long[] {16L, 15L, 15L});
    }
}
