/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.AlphabetDigest;
import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.model.KeyBytes;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Direct, table-driven unit tests for {@link OwnerSplitGovernor}'s gate chain (algorithms.md
 * §3.3): one test per gate, hitting both sides of its boundary — mirrors {@code
 * ThiefPolicySelectionTest}/{@code ThiefPolicyCascadeTest}'s shape. Drives {@link
 * OwnerSplitGovernor#decide} directly against hand-built {@link OwnerSplitView}s — no engine, no
 * lock, no I/O.
 *
 * <p>The observed-mass child-tail floor and the reflection clamp/lift are pure {@link StealMath}
 * predicates already boundary-tested in isolation ({@code OwnerSplitChildMassFloorTest}, {@code
 * OwnerSplitReflectClampTest}, {@code OwnerSplitReflectLiftTest}, {@code
 * ObservedMassFloorContractTest}); this class instead pins that the GOVERNOR wires each gate in
 * the right order with the right threshold and reports the right {@link Skip}/{@link Carve}.
 */
class OwnerSplitGovernorTest {

    private static final int MAX_KEYS = 100;
    private static final int WORKER_COUNT = 4;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A cold (no observations) digest — the same starting state a fresh {@link WorkerState} has. */
    private static AlphabetDigest coldDigest(byte[] lo, byte[] hi) {
        return new WorkerState(0, lo, lo, hi, null).alphabetDigest();
    }

    private static OwnerSplitGovernor governor(EngineToggles toggles, int workerCount, ConfettiFeedbackGate gate) {
        return new OwnerSplitGovernor(toggles, workerCount, MAX_KEYS, gate);
    }

    private static OwnerSplitGovernor governor() {
        return governor(EngineToggles.DEFAULT, WORKER_COUNT, new ConfettiFeedbackGate());
    }

    /**
     * A single-byte-key view: {@code lo="a"}, {@code hi="z"}, {@code cursorTo="n"} — chosen so
     * {@code est = keysEmitted * (12/13)} exactly (in double precision), letting {@link
     * #REMAINING_EST_FLOOR_BOUNDARY_KEYS} straddle the {@code 4*maxKeys=400} floor with an exact
     * integer {@code keysEmitted}. Every OTHER gate test below uses a keysEmitted far from that
     * boundary (clearly above it) so the est floor never interferes.
     */
    private static OwnerSplitView view(long keysEmitted, long committed, long lastSelfSplitPage, long outstanding,
            double densityFraction, double observedDensityRatio) {
        byte[] lo = b("a");
        byte[] hi = b("z");
        byte[] cursorTo = b("n");
        return new OwnerSplitView(hi, lo, cursorTo, keysEmitted, committed, lastSelfSplitPage, outstanding,
                densityFraction, observedDensityRatio, coldDigest(lo, hi));
    }

    /** A view whose est is far above every floor (~92,307 pages) and every other gate cleared. */
    private static OwnerSplitView clearedView(long committed, long lastSelfSplitPage, long outstanding) {
        return view(100_000L, committed, lastSelfSplitPage, outstanding, 0.5, 1.0);
    }

    // -------------------------------------------------------------------------
    // Open frontier: structural, not a suppression -- never counted (see OwnerSplitSkipReason).
    // -------------------------------------------------------------------------

    @Test
    void openFrontierSkipsSilentlyRegardlessOfEveryOtherField() {
        OwnerSplitView frontierView = new OwnerSplitView(null, b("a"), b("n"), 100_000L, 0, 0, 0, 0.5, 1.0,
                coldDigest(b("a"), null));

        OwnerSplitDecision decision = governor().decide(frontierView);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.OPEN_FRONTIER);
        assertThat(decision.engagements()).as("open frontier is silent").isEmpty();
    }

    @Test
    void aBoundedRangeProceedsPastTheOpenFrontierCheck() {
        // Bounded + rate-limited (committed - lastSelfSplitPage < 32): proves the frontier check let
        // it through to the NEXT gate, without needing a full carve.
        OwnerSplitDecision decision = governor().decide(clearedView(0, 0, 0));

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isNotEqualTo(OwnerSplitSkipReason.OPEN_FRONTIER);
    }

    // -------------------------------------------------------------------------
    // Remaining-est floor (issue #16): est <= SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys.
    // -------------------------------------------------------------------------

    @Test
    void remainingEstFloorBlocksAtExactlyTheThreshold() {
        // keysEmitted=433: est = 433*(12/13) = 399.69... <= 400 (4*100) -- blocked.
        OwnerSplitView v = view(433L, 0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0, 0.5, 1.0);
        assertThat(StealMath.estRemaining(v.cursorTo(), v.lo(), v.hi(), v.keysEmitted()))
                .as("fixture precondition: est just at/under the floor")
                .isLessThanOrEqualTo((double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * MAX_KEYS);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.REMAINING_EST_FLOOR);
        assertThat(decision.engagements())
                .as("issue #16: the floor now records its own engagement")
                .containsExactly(new Engagement("OWNER_SPLIT", "remaining_est_floor"));
    }

    @Test
    void remainingEstFloorClearsJustAboveTheThreshold() {
        // keysEmitted=434: est = 434*(12/13) = 400.61... > 400 -- clears the floor. Rate-limited
        // immediately after (committed - lastSelfSplitPage = 0 < 32) proves it passed through.
        OwnerSplitView v = view(434L, 0, 0, 0, 0.5, 1.0);
        assertThat(StealMath.estRemaining(v.cursorTo(), v.lo(), v.hi(), v.keysEmitted()))
                .as("fixture precondition: est just clears the floor")
                .isGreaterThan((double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * MAX_KEYS);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.RATE_LIMITED);
    }

    // -------------------------------------------------------------------------
    // Page rate-limit: committed - lastSelfSplitPage < SELF_SPLIT_MIN_PAGES_BETWEEN (32).
    // -------------------------------------------------------------------------

    @Test
    void rateLimitBlocksOneShortOfTheWindow() {
        OwnerSplitView v = clearedView(31L, 0L, 0);   // diff = 31 < 32

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.RATE_LIMITED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "rate_limited"));
    }

    @Test
    void rateLimitClearsExactlyAtTheWindow() {
        // diff = 32 == SELF_SPLIT_MIN_PAGES_BETWEEN clears the rate limit (not "<"). Demand-gated
        // immediately after (outstanding >= workerCount) proves it passed through.
        OwnerSplitView v = clearedView(32L, 0L, WORKER_COUNT);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.DEMAND_GATED);
    }

    // -------------------------------------------------------------------------
    // Demand gate: workerCount > 1 && outstanding >= workerCount.
    // -------------------------------------------------------------------------

    @Test
    void demandGateBlocksWhenOutstandingReachesWorkerCount() {
        OwnerSplitView v = clearedView(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, WORKER_COUNT);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.DEMAND_GATED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "demand_gated"));
    }

    @Test
    void demandGateClearsOneBelowWorkerCount() {
        // outstanding = workerCount - 1: below the gate. Floor-reflected-blocked immediately after
        // (densityRatio below f) proves it passed through.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, WORKER_COUNT - 1,
                0.5, 0.0);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
    }

    @Test
    void demandGateNeverEngagesWithASingleWorker() {
        // workerCount == 1: "buys zero parallelism" is moot (no thief exists), so the gate is
        // skipped even at a saturated outstanding count.
        OwnerSplitGovernor solo = governor(EngineToggles.DEFAULT, 1, new ConfettiFeedbackGate());
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 999L, 0.5, 0.0);

        OwnerSplitDecision decision = solo.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason())
                .as("workerCount == 1 bypasses the demand gate entirely")
                .isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
    }

    // -------------------------------------------------------------------------
    // Observed-mass child-tail floor (StealMath.childTailBelowObservedMassFloor).
    // -------------------------------------------------------------------------

    @Test
    void observedMassFloorBlocksAThinningTail() {
        // densityRatio (0.0) < f (0.5): reach clamps to 0, realized child mass collapses to 0.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 0.0);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "floor_reflected_blocked"));
    }

    @Test
    void observedMassFloorClearsAUniformTail() {
        // densityRatio (1.0, uniform) >= f: the floor reduces to the plain (1-f)*est span, which is
        // huge here. Confetti-suppressed immediately after (a pre-warmed gate) proves it passed through.
        ConfettiFeedbackGate warm = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            warm.recordCompletion(true);
        }
        OwnerSplitGovernor g = governor(EngineToggles.DEFAULT, WORKER_COUNT, warm);
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0);

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
    }

    // -------------------------------------------------------------------------
    // Confetti feedback gate (ConfettiFeedbackGate#decide, consulted as a collaborator).
    // -------------------------------------------------------------------------

    @Test
    void confettiFeedbackSuppressesOnceTheObservedRateTripsTheGate() {
        ConfettiFeedbackGate warm = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            warm.recordCompletion(true);   // 100% confetti -> over threshold
        }
        OwnerSplitGovernor g = governor(EngineToggles.DEFAULT, WORKER_COUNT, warm);
        OwnerSplitView v = clearedView(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0);

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "confetti_suppressed"));
    }

    @Test
    void confettiFeedbackCarvesDuringWarmup() {
        // A fresh gate (below MIN_SAMPLE) always CARVEs -- proceeds to pivot synthesis. Adjacent
        // cursor/hi (no safe key strictly between) proves it passed through to the terminal
        // unsplittable-pivot check, not stopping at confetti.
        OwnerSplitGovernor g = governor();
        OwnerSplitView v = unsplittablePivotView();

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
        assertThat(decision.engagements()).as("warmup CARVE adds no engagement").isEmpty();
    }

    @Test
    void confettiFeedbackOffNeverConsultsTheGateEvenWhenItWouldSuppress() {
        ConfettiFeedbackGate warm = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            warm.recordCompletion(true);
        }
        OwnerSplitGovernor g = governor(EngineToggles.DEFAULT.withConfettiFeedback(false), WORKER_COUNT, warm);
        OwnerSplitView v = unsplittablePivotView();

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason())
                .as("confetti_feedback=off bypasses the gate entirely, even though it would suppress")
                .isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
    }

    // -------------------------------------------------------------------------
    // Pivot synthesis validity: null, or not strictly inside (cursorTo, hi].
    // -------------------------------------------------------------------------

    /**
     * {@code hi = cursorTo + 0x01}: a byte-adjacent extension below {@code MIN_SAFE} (0x20), so no
     * valid-UTF-8 key exists strictly between (mirrors {@code
     * ThiefPolicyCascadeTest#unsplittable_terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds}'s
     * recipe). The nonzero (if tiny) extension byte keeps {@code est}'s remaining-span fraction
     * away from the double-precision-underflow-to-zero the {@code +0x00} extension would hit,
     * so a huge {@code keysEmitted} clears every earlier gate.
     */
    private static OwnerSplitView unsplittablePivotView() {
        byte[] lo = new byte[0];
        byte[] cursorTo = b("a");
        byte[] hi = new byte[] {'a', 0x01};
        double est = StealMath.estRemaining(cursorTo, lo, hi, 10_000_000L);
        assertThat(est).as("fixture precondition: est clears the remaining-est floor")
                .isGreaterThan((double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * MAX_KEYS);
        return new OwnerSplitView(hi, lo, cursorTo, 10_000_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L,
                0L, 0.5, 1.0, coldDigest(lo, hi));
    }

    @Test
    void terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds() {
        OwnerSplitDecision decision = governor().decide(unsplittablePivotView());

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
    }

    @Test
    void aRoomyPivotCommitsToACarveWithTheAlphabetEngagement() {
        OwnerSplitView v = clearedView(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        Carve carve = (Carve) decision;
        assertThat(carve.pivot()).isNotNull();
        assertThat(KeyBytes.compareUnsigned(v.cursorTo(), carve.pivot())).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(carve.pivot(), v.hi())).isLessThanOrEqualTo(0);
        assertThat(carve.engagements()).anyMatch(e -> e.category().equals("ALPHABET"));
    }
}
