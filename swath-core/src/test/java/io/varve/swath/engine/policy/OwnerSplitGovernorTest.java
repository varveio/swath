/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.AlphabetDigest;
import io.varve.swath.engine.CarveBrakeMode;
import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.TailFloorMode;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.model.KeyBytes;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Direct, table-driven unit tests for {@link OwnerSplitGovernor}'s gate chain (algorithms.md
 * §3.3): one test per gate, hitting both sides of its boundary — mirrors {@code
 * ThiefPolicySelectionTest}/{@code ThiefPolicyCascadeTest}'s shape. Drives {@link
 * OwnerSplitGovernor#decide} directly against hand-built {@link OwnerSplitView}s — no engine, no
 * lock, no I/O, and (since issue #22's fix) no live {@code ConfettiFeedbackGate} either: the
 * confetti check's warmup/threshold/probe-cycle boundary is exercised as pure arithmetic over a
 * {@link ConfettiObservation}, exactly like every other view field.
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

    /** Warmup (below {@code MIN_SAMPLE}): the confetti check never engages. */
    private static final ConfettiObservation NO_CONFETTI_SIGNAL = new ConfettiObservation(0, 0, 0, Double.NaN, 0);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A cold (no observations) digest — the same starting state a fresh {@link WorkerState} has. */
    private static AlphabetDigest.Snapshot coldDigest(byte[] lo, byte[] hi) {
        return new WorkerState(0, lo, lo, hi).alphabetDigest().snapshot();
    }

    private static OwnerSplitGovernor governor(EngineToggles toggles, int workerCount) {
        return new OwnerSplitGovernor(toggles, workerCount, MAX_KEYS, null);
    }

    private static OwnerSplitGovernor governor() {
        return governor(EngineToggles.DEFAULT, WORKER_COUNT);
    }

    /**
     * A governor pinned to the pre-0.2.0 child-tail floor ({@code tail_floor=current}).
     *
     * <p>Two distinct groups of tests need it, and neither is "a test that broke when the default
     * flipped". First, the tests of the legacy floor arithmetic itself: {@code current} still ships
     * as the documented rollback arm, so its refusal semantics must stay pinned somewhere.
     *
     * <p>Second — and less obvious — the gate-ordering tests use the legacy floor's refusal as a
     * <em>downstream sentinel</em>: "this decision reached {@code FLOOR_REFLECTED_BLOCKED}, which
     * proves it passed through the gate actually under test". Under the 0.2.0 default that next
     * gate admits, so the sentinel dissolves into a {@code Carve} and the test stops asserting
     * anything about the gate it was written for. Pinning them here keeps the sentinel intact.
     * Re-pointing them at the new default would have made them pass while testing less.
     */
    private static OwnerSplitGovernor legacyFloorGovernor(int workerCount) {
        return governor(EngineToggles.DEFAULT.withTailFloor(TailFloorMode.CURRENT), workerCount);
    }

    private static OwnerSplitGovernor legacyFloorGovernor() {
        return legacyFloorGovernor(WORKER_COUNT);
    }

    /**
     * A single-byte-key view: {@code lo="a"}, {@code hi="z"}, {@code cursorTo="n"} — chosen so
     * {@code est = keysEmitted * (12/13)} exactly (in double precision), letting {@link
     * #remainingEstFloorBlocksAtExactlyTheThreshold} straddle the {@code 4*maxKeys=400} floor with
     * an exact integer {@code keysEmitted}. Every OTHER gate test below uses a keysEmitted far from
     * that boundary (clearly above it) so the est floor never interferes.
     */
    private static OwnerSplitView view(long keysEmitted, long committed, long lastSelfSplitPage, long outstanding,
            double densityFraction, double observedDensityRatio, ConfettiObservation confetti) {
        byte[] lo = b("a");
        byte[] hi = b("z");
        byte[] cursorTo = b("n");
        return new OwnerSplitView(hi, lo, cursorTo, keysEmitted, committed, lastSelfSplitPage, outstanding,
                densityFraction, observedDensityRatio, coldDigest(lo, hi), confetti);
    }

    /** As the 7-arg {@link #view}, with no confetti signal (warmup — the common case below). */
    private static OwnerSplitView view(long keysEmitted, long committed, long lastSelfSplitPage, long outstanding,
            double densityFraction, double observedDensityRatio) {
        return view(keysEmitted, committed, lastSelfSplitPage, outstanding, densityFraction, observedDensityRatio,
                NO_CONFETTI_SIGNAL);
    }

    /** A view whose est is far above every floor (~92,307 pages) and every other gate cleared. */
    private static OwnerSplitView clearedView(long committed, long lastSelfSplitPage, long outstanding) {
        return view(100_000L, committed, lastSelfSplitPage, outstanding, 0.5, 1.0);
    }

    // -------------------------------------------------------------------------
    // Tuned constants: pinned by LITERAL, not merely referenced symbolically. Every gate test below
    // reads these constants off OwnerSplitGovernor itself (e.g. `SELF_SPLIT_MIN_PAGES_BETWEEN`), so a
    // change to the constant is invisible to them -- they'd stay green testing the NEW value against
    // itself. These four are tuned, load-bearing thresholds; an accidental edit should fail a build,
    // not silently retune the engine.
    // -------------------------------------------------------------------------

    @Test
    void tunedConstantsArePinnedToTheirLiteralValues() {
        assertThat(OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES).isEqualTo(4);
        assertThat(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN).isEqualTo(32);
        assertThat(OwnerSplitGovernor.SUPPRESS_THRESHOLD).isEqualTo(0.5);
        assertThat(OwnerSplitGovernor.PROBE_K).isEqualTo(16);
    }

    // -------------------------------------------------------------------------
    // Open frontier: structural, not a suppression -- never counted (see OwnerSplitSkipReason).
    // -------------------------------------------------------------------------

    @Test
    void openFrontierSkipsSilentlyRegardlessOfEveryOtherField() {
        OwnerSplitView frontierView = new OwnerSplitView(null, b("a"), b("n"), 100_000L, 0, 0, 0, 0.5, 1.0,
                coldDigest(b("a"), null), NO_CONFETTI_SIGNAL);

        OwnerSplitDecision decision = governor().decide(frontierView);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.OPEN_FRONTIER);
        assertThat(decision.engagements()).as("open frontier is silent").isEmpty();
        assertThat(decision.mutations()).isEmpty();
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
        // (densityRatio below f) proves it passed through -- see legacyFloorGovernor() on why the
        // sentinel needs the pre-0.2.0 floor to stay a sentinel.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, WORKER_COUNT - 1,
                0.5, 0.0);

        OwnerSplitDecision decision = legacyFloorGovernor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
    }

    @Test
    void demandGateNeverEngagesWithASingleWorker() {
        // workerCount == 1: "buys zero parallelism" is moot (no thief exists), so the gate is
        // skipped even at a saturated outstanding count.
        OwnerSplitGovernor solo = legacyFloorGovernor(1);
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

    /**
     * The other side of {@link #observedMassFloorBlocksAThinningTail()}: on the identical view, the
     * 0.2.0 default carves where the legacy floor refused. Without this the suite would pin only
     * the rollback arm's behaviour and say nothing about what ships.
     */
    @Test
    void theDefaultFloorCarvesTheThinningTailTheLegacyFloorBlocks() {
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 0.0);

        assertThat(legacyFloorGovernor().decide(v))
                .as("precondition: the rollback arm still refuses this shape")
                .isInstanceOf(Skip.class);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).as("the shipped 0.2.0 floor admits it").isInstanceOf(Carve.class);
        assertThat(decision.engagements())
                .as("and the run records that the mode flipped this verdict")
                .contains(new Engagement("TAIL_FLOOR", "gate_admit_current_blocks"));
    }

    @Test
    void observedMassFloorBlocksAThinningTail() {
        // densityRatio (0.0) < f (0.5): reach clamps to 0, realized child mass collapses to 0.
        // Pinned to the pre-0.2.0 floor: this IS the legacy arithmetic's refusal, and it still
        // ships as the documented rollback arm, so it stays pinned. The 0.2.0 default's behaviour
        // on the same shape is covered by tailFloorArmsCarveTheWideFlatTailTheLegacyFloorRefuses.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 0.0);

        OwnerSplitDecision decision = legacyFloorGovernor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "floor_reflected_blocked"));
    }

    @Test
    void observedMassFloorClearsAUniformTail() {
        // densityRatio (1.0, uniform) >= f: the floor reduces to the plain (1-f)*est span, which is
        // huge here. Confetti-suppressed immediately after (total>=MIN_SAMPLE, rate over threshold)
        // proves it passed through.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(8, 8, 0, Double.NaN, 0));

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
    }

    // -------------------------------------------------------------------------
    // tail_floor arms: the governor reads the floor at the run's mode, at BOTH sites it consults it,
    // and records what the mode changed (TAIL_FLOOR.* -- metrics-internals.md §5).
    // -------------------------------------------------------------------------

    /**
     * The measured `nara` input profile, mirrored at this test's scale: an honest estimate (~92,307
     * keys here, 322k-1.65M live) over a wide-flat trailing density (0.0003) at f=0.5. The
     * pre-0.2.0 floor multiplies the estimate away and refuses; both cure arms carve. This is the
     * governor-level half of {@code OwnerSplitTailFloorModeTest}'s regression pin -- it fails
     * against the pre-cure engine because the toggle's admitting path did not exist.
     *
     * <p>Renamed at the 0.2.0 default flip: {@code reach_floored} is now the shipped floor, so
     * "the shipped floor refuses" would name the opposite of what it once did. The legacy arm is
     * still asserted here because it is the documented rollback.
     */
    @Test
    void tailFloorArmsCarveTheWideFlatTailTheLegacyFloorRefuses() {
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 0.0003);

        OwnerSplitDecision current = legacyFloorGovernor().decide(v);
        assertThat(current).isInstanceOf(Skip.class);
        assertThat(((Skip) current).reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);
        assertThat(current.engagements())
                .as("the legacy mode never computes a second verdict, so it never records one")
                .containsExactly(new Engagement("OWNER_SPLIT", "floor_reflected_blocked"));

        for (TailFloorMode arm : new TailFloorMode[] {TailFloorMode.EST_DIRECT, TailFloorMode.REACH_FLOORED}) {
            OwnerSplitDecision decision =
                    governor(EngineToggles.DEFAULT.withTailFloor(arm), WORKER_COUNT).decide(v);

            assertThat(decision).as("%s carves the tail", arm).isInstanceOf(Carve.class);
            assertThat(decision.engagements())
                    .as("%s: the gate's verdict flipped, and the run says so", arm)
                    .contains(new Engagement("TAIL_FLOOR", "gate_admit_current_blocks"));
        }
    }

    /**
     * The mode also reaches the reflection clamp, not just the gate — and the clamp's own divergence
     * is counted under its own reason prefix (its denominator is carves that reached pivot synthesis,
     * not qualifying page commits). Geometry: {@code lo=d/00}, {@code cursorTo=d/02}, {@code hi=d/09}
     * puts the density-reflected pivot {@code d/04} strictly inside the f-interpolated one, so the
     * clamp is live and only the floor decides it.
     */
    @Test
    void tailFloorArmsReachTheReflectionClampAndRecordItsOwnDivergence() {
        OwnerSplitView v = new OwnerSplitView(b("d/09"), b("d/00"), b("d/02"), 100_000L,
                OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 0.0003,
                coldDigest(b("d/00"), b("d/09")), NO_CONFETTI_SIGNAL);

        OwnerSplitDecision current = legacyFloorGovernor().decide(v);
        assertThat(((Skip) current).reason())
                .as("under the pre-0.2.0 floor the chain never gets past the gate on this shape")
                .isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED);

        OwnerSplitDecision decision = governor(
                EngineToggles.DEFAULT.withTailFloor(TailFloorMode.EST_DIRECT), WORKER_COUNT).decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        assertThat(decision.engagements()).contains(
                new Engagement("TAIL_FLOOR", "gate_admit_current_blocks"),
                new Engagement("TAIL_FLOOR", "clamp_admit_current_blocks"),
                new Engagement("OWNER_SPLIT", "pivot_reflect_clamped"));
        assertThat(((Carve) decision).pivot())
                .as("the carve lands on the reflected pivot the shipped floor would have refused")
                .isEqualTo(StealMath.extrapolate(b("d/00"), b("d/02"), b("d/09")));
    }

    @Test
    void tailFloorArmsStayOutOfTheWayOnAShapeTheShippedFloorAlreadyAdmits() {
        // Uniform density: the gate admits under every mode, so no verdict changed and nothing is
        // recorded -- the counters must mark divergence, not merely "an arm was selected".
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0);

        OwnerSplitDecision decision = governor(
                EngineToggles.DEFAULT.withTailFloor(TailFloorMode.REACH_FLOORED), WORKER_COUNT).decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        assertThat(decision.engagements())
                .filteredOn(e -> "TAIL_FLOOR".equals(e.category()))
                .as("agreement is silent")
                .isEmpty();
    }

    // -------------------------------------------------------------------------
    // Confetti feedback: pure arithmetic over ConfettiObservation (issue #22 -- no live gate).
    // -------------------------------------------------------------------------

    @Test
    void confettiFeedbackCarvesDuringWarmup() {
        // total < MIN_SAMPLE (8): always CARVEs -- proceeds to pivot synthesis. Adjacent cursor/hi
        // (no safe key strictly between) proves it passed through to the terminal
        // unsplittable-pivot check, not stopping at confetti.
        OwnerSplitGovernor g = governor();
        OwnerSplitView v = unsplittablePivotView(NO_CONFETTI_SIGNAL);

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
        assertThat(decision.engagements())
                .as("warmup CARVE itself adds no engagement; the alphabet consult's own no-room fallback "
                        + "(the adjacent cursor/hi leaves no room for any scalar) and the terminal "
                        + "unsplittable-pivot gate are the only two")
                .containsExactly(new Engagement("ALPHABET", "fallback_no_room"),
                        new Engagement("OWNER_SPLIT", "unsplittable_pivot"));
        assertThat(decision.mutations()).as("warmup never touches the probe sequence").isEmpty();
    }

    @Test
    void confettiFeedbackSuppressesOnceTheObservedRateTripsTheGate() {
        // total=8=MIN_SAMPLE, confetti=8 -> rate=1.0>0.5 (over threshold); probeSeq=0 -> (0+1)%16=1
        // != 0 -> SUPPRESSED (the first-ever over-threshold consult, never a probe-boundary accident).
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(8, 8, 0, Double.NaN, 0));

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
        assertThat(decision.engagements()).containsExactly(new Engagement("OWNER_SPLIT", "confetti_suppressed"));
        assertThat(decision.mutations())
                .as("the over-threshold branch always consumes a probe slot, on EITHER outcome")
                .containsExactly(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT);
    }

    @Test
    void confettiFeedbackOffNeverConsultsTheGateEvenWhenItWouldSuppress() {
        // Same over-threshold observation as the suppress test above, but the toggle is off.
        OwnerSplitGovernor g = governor(EngineToggles.DEFAULT.withConfettiFeedback(false), WORKER_COUNT);
        OwnerSplitView v = unsplittablePivotView(new ConfettiObservation(8, 8, 0, Double.NaN, 0));

        OwnerSplitDecision decision = g.decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason())
                .as("confetti_feedback=off bypasses the gate entirely, even though it would suppress")
                .isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
        assertThat(decision.mutations()).isEmpty();
    }

    @Test
    void confettiProbeSlotLetsTheCarveThroughAtTheProbeKBoundary() {
        // total=16, confetti=12 -> rate=0.75>0.5 (over threshold); probeSeq=15 -> (15+1)%16==0 -> PROBE.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(16, 12, 15, Double.NaN, 0));

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        Carve carve = (Carve) decision;
        assertThat(carve.engagements()).contains(new Engagement("OWNER_SPLIT", "confetti_probe"));
        // CLAIM, not CONSUME (issue #31): the executor must serialize this carve against every other
        // owner that decided PROBE from the same probeSeq snapshot.
        assertThat(carve.mutations()).containsExactly(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT);
    }

    @Test
    void confettiSuppressedOneShortOfTheProbeKBoundary() {
        // Same over-threshold rate, but probeSeq=14 -> (14+1)%16=15 != 0 -> SUPPRESSED.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(16, 12, 14, Double.NaN, 0));

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
        assertThat(skip.mutations()).containsExactly(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT);
    }

    /**
     * <b>Issue #31's regression test.</b> Reproduces the multiplication DETERMINISTICALLY — no threads,
     * no scheduler luck — by driving the real interleaving the race produces: N owners each build a
     * view from the SAME {@code probeSeq} snapshot (they all read the run-scoped gate before any of
     * them advances it, which per-worker locks do nothing to prevent) and each asks the real governor
     * and the real gate what to do.
     *
     * <p>Every one of them decides {@code PROBE} — that half is inherent to {@code decide()} being a
     * pure function of its view, and is asserted here rather than treated as surprising. What must hold
     * is that exactly ONE of their carves is admitted. Before the fix the decision carried an
     * unconditional {@code CONSUME_CONFETTI_PROBE_SLOT} and all N carved, multiplying precisely the
     * confetti-sized carves the gate exists to suppress; now it carries {@code
     * CLAIM_CONFETTI_PROBE_SLOT} and {@link ConfettiFeedbackGate#claimProbeSlot(long)} admits one.
     *
     * <p>Also pins the accounting the pre-#22 fused {@code incrementAndGet()} gave: the sequence
     * advances once per consult, winner or loser, so N racers starting at {@code s} leave it at
     * {@code s + N} — a losing claim is suppressed, not dropped.
     */
    @Test
    void concurrentProbeConsultsSharingOneSlotAdmitExactlyOneCarve() {
        int racers = 4;
        long sharedProbeSeq = OwnerSplitGovernor.PROBE_K - 1;   // (probeSeq + 1) % PROBE_K == 0 -> PROBE
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < sharedProbeSeq; i++) {
            gate.consumeProbeSlot();
        }
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE * 2; i++) {
            gate.recordCompletion(i % 4 != 0, 1L);   // rate = 0.75 > SUPPRESS_THRESHOLD
        }
        ConfettiFeedbackGate.Snapshot snapshot = gate.snapshot();
        assertThat(snapshot.probeSeq()).as("fixture: every racer snapshots the same slot boundary")
                .isEqualTo(sharedProbeSeq);

        int carvesAdmitted = 0;
        for (int i = 0; i < racers; i++) {
            // Each racer decides from the snapshot it took BEFORE any of them advanced the sequence.
            OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                    new ConfettiObservation(snapshot.taggedTotal(), snapshot.taggedConfetti(),
                            snapshot.probeSeq(), Double.NaN, 0));
            OwnerSplitDecision decision = governor().decide(v);

            assertThat(decision).as("racer %s: every consult sharing the slot decides PROBE", i)
                    .isInstanceOf(Carve.class);
            assertThat(decision.mutations())
                    .as("racer %s: the carve must be conditional on winning the slot", i)
                    .containsExactly(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT);
            // Exactly what the executor does with a Carve carrying CLAIM (OwnerSelfSplit).
            if (gate.claimProbeSlot(snapshot.probeSeq())) {
                carvesAdmitted++;
            }
        }

        assertThat(carvesAdmitted)
                .as("exactly one carve per probe slot -- the pre-#22 guarantee, restored")
                .isEqualTo(1);
        assertThat(gate.snapshot().probeSeq())
                .as("every consult consumed a slot, winner or loser (what the fused incrementAndGet did)")
                .isEqualTo(sharedProbeSeq + racers);
    }

    /**
     * A probe consult that then loses its carve to the pivot checks downgrades {@code CLAIM} to the
     * unconditional {@code CONSUME} (issue #31): a claim only means something when there is a carve to
     * admit or exclude, and the slot is spent either way. Without the downgrade the executor would
     * leave that consult's slot unconsumed, so the next consult would re-probe off a stale sequence.
     */
    @Test
    void aProbeConsultThatLosesItsCarveDowngradesTheClaimToAnUnconditionalConsume() {
        // Over threshold (8/8 = 1.0) at the slot boundary, on the view whose pivot is unsplittable.
        OwnerSplitView v = unsplittablePivotView(
                new ConfettiObservation(8, 8, OwnerSplitGovernor.PROBE_K - 1, Double.NaN, 0));

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
        assertThat(decision.engagements()).contains(new Engagement("OWNER_SPLIT", "confetti_probe"));
        assertThat(decision.mutations())
                .containsExactly(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT);
    }

    // -------------------------------------------------------------------------
    // Carve brake (campaign memo §5): the recent window-average mass trend, distinct from
    // confetti's binary rate. taggedConfetti=0 throughout so the confetti gate's own rate (0/8=0)
    // never suppresses first and masks the brake -- these views are built to reach the brake gate.
    // -------------------------------------------------------------------------

    /** A view that clears every gate above the brake, with a chosen carve-brake observation. */
    private static OwnerSplitView brakeView(double windowAverageMass, long carveBrakeProbeSeq) {
        return view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(ConfettiFeedbackGate.MIN_SAMPLE, 0, 0, windowAverageMass,
                        carveBrakeProbeSeq));
    }

    private static final CarveBrakeMode[] BRAKE_MODES =
            {CarveBrakeMode.MASS_K2, CarveBrakeMode.MASS_K4, CarveBrakeMode.MASS_K8};

    @Test
    void carveBrakeSuppressesJustBelowTheKThresholdForEveryMode() {
        for (CarveBrakeMode mode : BRAKE_MODES) {
            double threshold = (double) mode.k() * MAX_KEYS;
            OwnerSplitView v = brakeView(threshold - 1.0, 0L);   // probeSeq=0 -> (0+1)%16=1 != 0

            OwnerSplitDecision decision = governor(EngineToggles.DEFAULT.withCarveBrake(mode), WORKER_COUNT)
                    .decide(v);

            assertThat(decision).as("%s just below its threshold", mode).isInstanceOf(Skip.class);
            assertThat(((Skip) decision).reason()).as(mode.toString())
                    .isEqualTo(OwnerSplitSkipReason.CARVE_BRAKED);
            assertThat(decision.engagements()).as(mode.toString())
                    .containsExactly(new Engagement("OWNER_SPLIT", "carve_braked"));
            assertThat(decision.mutations()).as(mode.toString())
                    .containsExactly(OwnerSplitMutation.CONSUME_CARVE_BRAKE_PROBE_SLOT);
            assertThat(decision.gateInputs().carveBrakeMassAvg()).as(mode.toString())
                    .isEqualTo(threshold - 1.0);
        }
    }

    @Test
    void carveBrakeAdmitsAtAndAboveTheKThresholdForEveryMode() {
        for (CarveBrakeMode mode : BRAKE_MODES) {
            double threshold = (double) mode.k() * MAX_KEYS;
            OwnerSplitView v = brakeView(threshold, 0L);   // "< threshold" is strict: == admits

            OwnerSplitDecision decision = governor(EngineToggles.DEFAULT.withCarveBrake(mode), WORKER_COUNT)
                    .decide(v);

            assertThat(decision).as("%s at its threshold (boundary admits)", mode).isInstanceOf(Carve.class);
            assertThat(decision.gateInputs().carveBrakeMassAvg()).as(mode.toString()).isEqualTo(threshold);
        }
    }

    @Test
    void carveBrakeNeverEngagesBelowWarmup() {
        // taggedTotal=7 < MIN_SAMPLE(8): never engages, however low the mass reading would otherwise be.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0, 0.5, 1.0,
                new ConfettiObservation(ConfettiFeedbackGate.MIN_SAMPLE - 1, 0, 0, 1.0, 0));

        OwnerSplitDecision decision =
                governor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K8), WORKER_COUNT).decide(v);

        assertThat(decision).as("below warmup, mass_k8 never engages even though 1.0 << 8*maxKeys")
                .isInstanceOf(Carve.class);
    }

    @Test
    void carveBrakeProbeEscapeLetsTheCarveThroughAtTheProbeKBoundary() {
        // massAvg=0.0 is below every K's threshold; probeSeq=CARVE_BRAKE_PROBE_K-1 -> (15+1)%16==0 -> PROBE.
        OwnerSplitView v = brakeView(0.0, OwnerSplitGovernor.CARVE_BRAKE_PROBE_K - 1);

        OwnerSplitDecision decision =
                governor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K2), WORKER_COUNT).decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        Carve carve = (Carve) decision;
        assertThat(carve.engagements()).contains(new Engagement("OWNER_SPLIT", "carve_brake_probe"));
        // CLAIM, not CONSUME (issue #31, mirrored): the executor must serialize this carve against
        // every other owner that decided carve_brake_probe from the same carveBrakeProbeSeq snapshot.
        assertThat(carve.mutations()).containsExactly(OwnerSplitMutation.CLAIM_CARVE_BRAKE_PROBE_SLOT);
        assertThat(carve.gateInputs().reason()).isEqualTo("carve_brake_probe");
    }

    @Test
    void carveBrakeSuppressedOneShortOfTheProbeKBoundary() {
        // Same over-threshold mass, but probeSeq=CARVE_BRAKE_PROBE_K-2 -> (14+1)%16=15 != 0.
        OwnerSplitView v = brakeView(0.0, OwnerSplitGovernor.CARVE_BRAKE_PROBE_K - 2);

        OwnerSplitDecision decision =
                governor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K2), WORKER_COUNT).decide(v);

        assertThat(decision).isInstanceOf(Skip.class);
        Skip skip = (Skip) decision;
        assertThat(skip.reason()).isEqualTo(OwnerSplitSkipReason.CARVE_BRAKED);
        assertThat(skip.mutations()).containsExactly(OwnerSplitMutation.CONSUME_CARVE_BRAKE_PROBE_SLOT);
    }

    /**
     * OFF is inert regardless of the observation: a view engineered so every K mode AND the probe
     * boundary would fire if the brake read either field is fed to a governor with {@code
     * carve_brake=off} and to the plain (already-off-by-default) governor -- proving OFF never even
     * consults {@code windowAverageMass}/{@code carveBrakeProbeSeq}, not merely that no mode happens
     * to trip on this input.
     */
    @Test
    void carveBrakeOffIsInertRegardlessOfTheMassObservation() {
        OwnerSplitView v = brakeView(1.0, OwnerSplitGovernor.CARVE_BRAKE_PROBE_K - 1);

        OwnerSplitDecision explicitOff =
                governor(EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.OFF), WORKER_COUNT).decide(v);
        OwnerSplitDecision plainDefault = governor().decide(v);

        assertThat(explicitOff).isInstanceOf(Carve.class);
        assertThat(plainDefault).isInstanceOf(Carve.class);
        assertThat(((Carve) explicitOff).pivot()).isEqualTo(((Carve) plainDefault).pivot());
        assertThat(explicitOff.engagements()).isEqualTo(plainDefault.engagements());
        assertThat(explicitOff.mutations()).isEqualTo(plainDefault.mutations());
        assertThat(explicitOff.gateInputs()).isEqualTo(plainDefault.gateInputs());
        assertThat(explicitOff.gateInputs().carveBrakeMassAvg())
                .as("carve_brake=off omits the reading entirely rather than reporting a not-applicable NaN")
                .isNull();
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
    private static OwnerSplitView unsplittablePivotView(ConfettiObservation confetti) {
        byte[] lo = new byte[0];
        byte[] cursorTo = b("a");
        byte[] hi = new byte[] {'a', 0x01};
        double est = StealMath.estRemaining(cursorTo, lo, hi, 10_000_000L);
        assertThat(est).as("fixture precondition: est clears the remaining-est floor")
                .isGreaterThan((double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * MAX_KEYS);
        return new OwnerSplitView(hi, lo, cursorTo, 10_000_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L,
                0L, 0.5, 1.0, coldDigest(lo, hi), confetti);
    }

    @Test
    void terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds() {
        OwnerSplitDecision decision = governor().decide(unsplittablePivotView(NO_CONFETTI_SIGNAL));

        assertThat(decision).isInstanceOf(Skip.class);
        assertThat(((Skip) decision).reason()).isEqualTo(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT);
        // The adjacent cursor/hi leaves no room for any scalar, so the alphabet consult's own
        // no-room fallback fires alongside the terminal unsplittable-pivot mark.
        assertThat(decision.engagements()).containsExactly(new Engagement("ALPHABET", "fallback_no_room"),
                new Engagement("OWNER_SPLIT", "unsplittable_pivot"));
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
        assertThat(carve.mutations()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Gate inputs: what the executor emits as the owner_split_decision trace event (§7). The gate
    // chain is pure, so the readings a refusal was taken on ride back on the decision itself.
    // -------------------------------------------------------------------------

    @Test
    void aBlockedGateReportsTheReadingsItBlockedOn() {
        // The observed-mass floor: densityRatio (0.0) < f (0.5) -- the gate that blocks REGARDLESS of
        // how large est is, so its report has to carry both of those inputs, not just the reason.
        OwnerSplitView v = view(100_000L, OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 3, 0.5, 0.0);

        // Pinned to the pre-0.2.0 floor: this asserts that a BLOCKED gate reports its readings, so
        // it needs a gate that blocks. Under the 0.2.0 default this shape carves.
        OwnerSplitGateInputs inputs = legacyFloorGovernor().decide(v).gateInputs();

        assertThat(inputs.reason()).isEqualTo(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code());
        assertThat(inputs.est()).isEqualTo(StealMath.estRemaining(v.cursorTo(), v.lo(), v.hi(), v.keysEmitted()));
        assertThat(inputs.farAheadFraction()).as("computed: this gate reads it").isEqualTo(0.5);
        assertThat(inputs.densityRatio()).as("computed: this gate reads it").isEqualTo(0.0);
        assertThat(inputs.pagesSinceLastSelfSplit()).isEqualTo(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN);
        assertThat(inputs.outstanding()).isEqualTo(3L);
        assertThat(inputs.workerCount()).isEqualTo(WORKER_COUNT);
        assertThat(inputs.keysEmitted()).isEqualTo(100_000L);
    }

    @Test
    void aCarveReportsSelfPublishedWithEveryReadingItCleared() {
        OwnerSplitView v = clearedView(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN, 0L, 0);

        OwnerSplitDecision decision = governor().decide(v);

        assertThat(decision).isInstanceOf(Carve.class);
        OwnerSplitGateInputs inputs = decision.gateInputs();
        assertThat(inputs.reason())
                .as("no reflection clamp/lift engaged on this uniform view, so the plain published carve")
                .isEqualTo("self_published");
        assertThat(inputs.est()).isEqualTo(StealMath.estRemaining(v.cursorTo(), v.lo(), v.hi(), v.keysEmitted()));
        assertThat(inputs.farAheadFraction()).isEqualTo(0.5);
        assertThat(inputs.densityRatio()).isEqualTo(1.0);
        assertThat(inputs.pagesSinceLastSelfSplit()).isEqualTo(OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN);
        assertThat(inputs.outstanding()).isZero();
        assertThat(inputs.workerCount()).isEqualTo(WORKER_COUNT);
        assertThat(inputs.keysEmitted()).isEqualTo(100_000L);
    }

    @Test
    void aGateAboveTheFloorReportsTheUncomputedInputsAsNotComputed() {
        // Rate-limited: the chain short-circuits ABOVE where f/densityRatio are computed, so the
        // report must say "not computed" rather than a plausible-looking 0.0.
        OwnerSplitView v = clearedView(0, 0, 0);

        OwnerSplitGateInputs inputs = governor().decide(v).gateInputs();

        assertThat(inputs.reason()).isEqualTo(OwnerSplitSkipReason.RATE_LIMITED.code());
        assertThat(inputs.farAheadFraction()).isNaN();
        assertThat(inputs.densityRatio()).isNaN();
        assertThat(inputs.est()).as("read before the first gate -- always computed").isPositive();
        assertThat(inputs.pagesSinceLastSelfSplit()).isZero();
    }

    @Test
    void theOpenFrontierEarlyOutReportsNoGateInputsAtAll() {
        OwnerSplitView frontierView = new OwnerSplitView(null, b("a"), b("n"), 100_000L, 0, 0, 0, 0.5, 1.0,
                coldDigest(b("a"), null), NO_CONFETTI_SIGNAL);

        assertThat(governor().decide(frontierView).gateInputs())
                .as("it decides nothing and reads nothing -- no owner_split_decision event is due")
                .isNull();
    }
}
