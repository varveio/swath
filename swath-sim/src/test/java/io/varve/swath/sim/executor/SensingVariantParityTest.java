/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.AlphabetDigest;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.TailFloorMode;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGateInputs;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.Skip;
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
 * <b>Every arm drives the engine's own gate chain, and the control leg drives it unchanged.</b> A
 * sensing variant is an estimator installed into the engine's {@code OwnerSplitGovernor} and
 * {@code ThiefPolicy} through the seam they already have — nothing in this module reproduces what
 * either decides — so what needs guarding is no longer "does the copy match the original" but
 * "does the chain under each arm decide completely and stably, and does the control arm leave the
 * shipped path exactly as it was".
 *
 * <p>The batteries below are the ones the mirror comparison used, driven now through the engine
 * chain under all eight arms. In particular the confetti observations still bracket the gate's two
 * package-private constants (stated exactly on {@link #confettiObservations()}): those constants are
 * no longer duplicated anywhere, but the battery that reaches the branches they govern is what makes
 * the chain-under-arm decisions cover them at all.
 */
class SensingVariantParityTest {

    private static final int WORKERS = 8;
    private static final int PAGE = 100;

    /**
     * <b>Every arm decides, on every view, and decides the same way twice.</b> A variant that faulted
     * or that carried state between decisions would produce a race table nobody could reproduce, and
     * the estimator contract is a pure function of its arguments — so the battery is run twice per arm
     * and the two passes are required to agree, decision for decision, including the gate inputs each
     * decision reports.
     *
     * <p>The named branches are asserted rather than counted: an arm whose estimate is so large that
     * the chain never reaches the confetti gate, or so small that it never carves, would cover far
     * less of the chain than its race table implies.
     */
    @Test
    void everyArmDecidesCompletelyAndDeterministicallyAcrossTheBattery() {
        List<OwnerSplitView> views = ownerSplitViews();
        List<List<VictimView>> pools = victimPools();
        for (SensingVariant arm : SensingVariant.values()) {
            OwnerSplitPolicy governor = governor(arm);
            OwnerSplitPolicy again = governor(arm);
            StealPolicy thief = thief(arm);
            StealPolicy thiefAgain = thief(arm);

            int carves = 0;
            int skips = 0;
            Set<String> reasons = new HashSet<>();
            for (OwnerSplitView view : views) {
                OwnerSplitDecision decision = governor.decide(view);
                assertSameDecision(decision, again.decide(view), view);
                if (decision instanceof Carve) {
                    carves++;
                } else {
                    skips++;
                }
                decision.engagements().forEach(e -> reasons.add(e.reason()));
            }
            assertThat(carves + skips).as("%s decided every view", arm).isEqualTo(views.size());
            assertThat(carves).as("carves reached under %s", arm).isPositive();
            assertThat(skips).as("skips reached under %s", arm).isPositive();
            assertThat(reasons).as("branches reached under %s", arm)
                    .contains(OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code(), "confetti_probe",
                            OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code(),
                            "alphabet_chosen", "alphabet_fallback");

            for (List<VictimView> pool : pools) {
                Selection selection = thief.selectVictim(pool);
                assertThat(selection).as("%s, pool %s", arm, pool).isNotNull()
                        .isEqualTo(thiefAgain.selectVictim(pool));
            }
        }
        assertThat(pools).as("the battery has to actually exercise something").hasSizeGreaterThan(200);
    }

    /**
     * <b>The control leg is the shipped path, byte for byte.</b> {@code CURRENT} installs no estimator
     * at all — the executor passes {@code null} and the engine keeps its own {@code WINDOW} reading —
     * and this is the pin that the convergence of the two construction paths changed nothing there:
     * the chain steered by this module's incumbent delegate decides identically to the chain nobody
     * steered, on every view and every pool of the battery, down to the gate inputs.
     */
    @Test
    void theCurrentArmDecidesExactlyAsAnUnsteeredEngineChainDoes() {
        // Same toggles on both sides — the comparison is sensor-vs-no-sensor, so the floor mode has
        // to be held identical or the test would be measuring the 0.2.0 default flip instead.
        OwnerSplitPolicy unsteered = new OwnerSplitGovernor(
                EngineToggles.DEFAULT.withTailFloor(TailFloorMode.CURRENT), WORKERS, PAGE, null);
        OwnerSplitPolicy current = governor(SensingVariant.CURRENT);
        for (OwnerSplitView view : ownerSplitViews()) {
            assertSameDecision(unsteered.decide(view), current.decide(view), view);
        }

        StealPolicy unsteeredThief = new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0, null);
        StealPolicy currentThief = thief(SensingVariant.CURRENT);
        for (List<VictimView> pool : victimPools()) {
            assertThat(currentThief.selectVictim(pool)).as("pool %s", pool)
                    .isEqualTo(unsteeredThief.selectVictim(pool));
        }
    }

    /**
     * <b>The observed-mass floor's structural zero is visible under every arm.</b> When
     * {@code min(1, densityRatio) <= f} the reachable child tail is zero however large the estimate
     * is, so the gate blocks on a range the sensor reads as enormous. An arm that hid that — by
     * reporting an estimate the chain never consumed, or by a chain that stopped short of the gate —
     * would let a race table claim a cure the gate below it refuses; the assertion is therefore made
     * of the gate's OWN reported inputs at its own terminal decision, per arm.
     */
    @Test
    void everyArmExposesTheStructuralZeroAtTheObservedMassFloor() {
        for (SensingVariant arm : SensingVariant.values()) {
            OwnerSplitPolicy governor = governor(arm);
            List<OwnerSplitGateInputs> structuralZeroes = new ArrayList<>();
            for (OwnerSplitView view : ownerSplitViews()) {
                if (!(governor.decide(view) instanceof Skip skip)
                        || skip.reason() != OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED) {
                    continue;
                }
                OwnerSplitGateInputs inputs = skip.gateInputs();
                if (inputs.est() > (double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * PAGE
                        && Math.min(1.0, inputs.densityRatio()) <= inputs.farAheadFraction()) {
                    structuralZeroes.add(inputs);
                }
            }
            assertThat(structuralZeroes)
                    .as("%s: a large estimate blocked by a zero reachable tail", arm)
                    .isNotEmpty();
        }
    }

    /**
     * <b>Which sensor a run's decisions were taken through, out of the run's own counters.</b> Both
     * routes are the engine's own policy objects now, so what the counters state is which sensor was
     * installed through their seam: a run that quietly left the seam unsteered under a variant's name
     * would produce a whole race table of the incumbent measured twice. The route is instrumented like
     * every other algo path here (AGENTS.md), so the check is a counter read rather than a reflection
     * over fields.
     */
    @Test
    void aRunsCountersNameTheSensorItsDecisionsWereTakenThrough() {
        ListingFixtureStore store = new ListingFixtureStore(KeyspaceFixtures.denseFlatLeaf(2_000));
        PolicyScenario scenario = PolicyRunFixtures.scenario(4, PAGE, PolicyRunFixtures.REMOTE_LATENCY,
                PolicyRunFixtures.measuredCost());

        PolicyRunResult shipped = SimExecutor.run(scenario, store, "in-memory dense flat leaf");
        PolicyRunResult variant = SimExecutor.run(
                scenario.withClientCost(PolicyRunFixtures.measuredCost()), store,
                "in-memory dense flat leaf", SensingVariant.RATE_CURSOR_ANCHORED);

        assertThat(route(shipped)).as("the shipped sensor leaves both seams unsteered")
                .containsExactly(SimExecutor.OWNER_SPLIT_ROUTE_SHIPPED, SimExecutor.THIEF_ROUTE_SHIPPED);
        assertThat(route(variant)).as("a variant steers both seams, not one of them")
                .containsExactly(SimExecutor.OWNER_SPLIT_ROUTE_ESTIMATOR,
                        SimExecutor.THIEF_ROUTE_ESTIMATOR);
    }

    /**
     * The engine's governor with {@code arm}'s sensor installed through its seam, reading the
     * child-tail floor at {@code tail_floor=current}.
     *
     * <p>Pinned to the pre-0.2.0 floor deliberately. Both tests above are about what a SENSING arm
     * exposes as it drives the gate chain, and they use the legacy floor's structural zero
     * ({@code min(1, densityRatio) <= f} ⇒ zero reachable tail however large the estimate) as the
     * observable. The 0.2.0 default, {@code reach_floored}, exists precisely to eliminate that
     * structural zero — so leaving these on the default would delete the very branch they assert
     * and turn both tests into tautologies. The floor's own arms are covered by
     * {@code OwnerSplitTailFloorModeTest} and {@code OwnerSplitGovernorTest}; this file holds the
     * sensor comparison steady by holding the floor fixed.
     */
    private static OwnerSplitPolicy governor(SensingVariant arm) {
        return new OwnerSplitGovernor(
                EngineToggles.DEFAULT.withTailFloor(TailFloorMode.CURRENT), WORKERS, PAGE, arm.estimator(PAGE));
    }

    /** The engine's thief with {@code arm}'s sensor installed through its seam. */
    private static StealPolicy thief(SensingVariant arm) {
        return new ThiefPolicy(EngineToggles.DEFAULT, new byte[0], bound -> 0, arm.estimator(PAGE));
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
        assertThat(actual.gateInputs()).as("gate inputs for %s", describe(view))
                .isEqualTo(expected.gateInputs());
        if (expected instanceof Skip skip) {
            assertThat(((Skip) actual).reason()).as("skip reason for %s", describe(view))
                    .isEqualTo(skip.reason());
        }
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
     *       (which must)</b> straddle it tightly.</li>
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
     * is what sends pivot synthesis down its alphabet-aware branch instead of its fallback.
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
