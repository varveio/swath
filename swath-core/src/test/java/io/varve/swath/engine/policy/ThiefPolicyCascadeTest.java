/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.AlphabetDigest;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.WorkerState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Direct, table-driven unit tests for {@link ThiefPolicy}'s pivot cascade (algorithms.md §3,
 * §3.1, §3.3): one test per named mechanism, plus the un-started-frontier {@code RETRY} and the
 * terminal-null-pivot {@code UNSPLITTABLE} path. Drives {@link StealAttempt#start()}/
 * {@link StealAttempt#onProbeResult}, scripting each requested probe's answer directly — no
 * engine, no lock, no I/O, no {@code MockPageFetcher}.
 *
 * <p>{@link #adaptive_structure_capped()} is deliberately absent here — it has its own
 * characterization test ({@code ThiefPolicyAdaptiveStructureCappedTest}), the thinnest-pinned
 * mechanism per the decision-trace-goldens coverage matrix.
 */
class ThiefPolicyCascadeTest {

    private static final byte[] NO_SCAN_PREFIX = new byte[0];

    // None of this suite's fixtures ever set consecutiveZeroFanoutStructureProbes/
    // consecutiveTimedOutStructureProbes above 0, so structureProbingEnabled's escape hatch (the
    // one caller of this rng) is never reached -- this stub is never drawn from.
    private final ThiefPolicy policy = new ThiefPolicy(EngineToggles.DEFAULT, NO_SCAN_PREFIX, bound -> 0);
    private final ThiefPolicy structureOffPolicy =
            new ThiefPolicy(EngineToggles.DEFAULT.withStructureProbes(false), NO_SCAN_PREFIX, bound -> 0);
    private final ThiefPolicy structureAndReflectOffPolicy = new ThiefPolicy(
            EngineToggles.DEFAULT.withStructureProbes(false).withReflect(false), NO_SCAN_PREFIX, bound -> 0);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A cold (no observations) digest — the same starting state a fresh {@link WorkerState} has. */
    private static AlphabetDigest coldDigest(byte[] lo, byte[] hi) {
        return new WorkerState(0, lo, lo, hi, null).alphabetDigest();
    }

    /**
     * The far-ahead fraction with no density signal yet ({@code WorkerState#densityFraction()}'s own
     * default): the plain code-point midpoint, so {@code interpolate} degrades to {@code
     * byteMidpoint} byte for byte.
     */
    private static final double NO_DENSITY_SIGNAL = 0.5;

    /** No progress, no density signal, no structure-probe history — the common test default. */
    private static StealAttemptView freshView(byte[] lo, byte[] cursor, byte[] hi) {
        return new StealAttemptView(1, lo, cursor, hi, 0, NO_DENSITY_SIGNAL, coldDigest(lo, hi), false, 0, 0);
    }

    private static StealAttemptView freshView(byte[] lo, byte[] cursor, byte[] hi, double densityFraction) {
        return new StealAttemptView(1, lo, cursor, hi, 0, densityFraction, coldDigest(lo, hi), false, 0, 0);
    }

    /** Drives {@code attempt} to a terminal action, answering every probe request via {@code script}. */
    private static StealAction drive(StealAttempt attempt, List<ProbeOutcome> script) {
        StealAction action = attempt.start();
        int i = 0;
        while (!(action instanceof Commit) && !(action instanceof Retry) && !(action instanceof MarkUnsplittable)) {
            if (i >= script.size()) {
                throw new AssertionError("cascade requested more probes than scripted: " + action);
            }
            action = attempt.onProbeResult(script.get(i++));
        }
        assertThat(i).as("every scripted outcome was consumed").isEqualTo(script.size());
        return action;
    }

    private static final ProbeOutcome HIT = new KeyProbeOutcome(true);
    private static final ProbeOutcome MISS = new KeyProbeOutcome(false);

    // -------------------------------------------------------------------------
    // Open frontier
    // -------------------------------------------------------------------------

    @Test
    void extrapolate_openFrontierDensityReflection() {
        StealAttemptView view = freshView(b("a"), b("m"), null);
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(HIT));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.EXTRAPOLATE);
        assertThat(commit.pivot()).isEqualTo(b("y"));
    }

    // -------------------------------------------------------------------------
    // Bounded placement
    // -------------------------------------------------------------------------

    @Test
    void midpoint_plainCodePointBisectionWithNoDensitySignal() {
        StealAttemptView view = freshView(b("a"), b("a"), b("z"));
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(HIT));

        assertThat(result).isInstanceOf(Commit.class);
        assertThat(((Commit) result).mechanism()).isEqualTo(PivotMechanism.MIDPOINT);
        assertThat(((Commit) result).pivot()).isEqualTo(b("m"));
    }

    @Test
    void farAhead_interpolatedPivotHitsOnTheFirstProbe() {
        StealAttemptView view = freshView(b("a"), b("m"), b("z"), 0.75);
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(HIT));

        assertThat(result).isInstanceOf(Commit.class);
        assertThat(((Commit) result).mechanism()).isEqualTo(PivotMechanism.FAR_AHEAD);
        assertThat(((Commit) result).pivot()).isEqualTo(b("v"));
    }

    @Test
    void stepBack_farAheadOvershootsAndFallsBackToThePlainMidpoint() {
        StealAttemptView view = freshView(b("a"), b("m"), b("z"), 0.75);
        StealAttempt attempt = policy.beginAttempt(view);

        // First probe (far-ahead pivot "v") is EMPTY -> step back to the plain midpoint "s", which hits.
        StealAction result = drive(attempt, List.of(MISS, HIT));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.MIDPOINT);
        assertThat(commit.pivot()).isEqualTo(b("s"));
    }

    @Test
    void stepBackEngagementFiresWhenTheFarAheadProbeIsEmpty() {
        StealAttemptView view = freshView(b("a"), b("m"), b("z"), 0.75);
        StealAttempt attempt = policy.beginAttempt(view);

        attempt.start();   // the far-ahead probe request
        StealAction stepBackRequest = attempt.onProbeResult(MISS);

        assertThat(stepBackRequest).isInstanceOf(RequestKeyProbe.class);
        assertThat(stepBackRequest.engagements()).contains(new Engagement("PIVOT", "step_back"));
        assertThat(((RequestKeyProbe) stepBackRequest).pivot()).isEqualTo(b("s"));
    }

    // -------------------------------------------------------------------------
    // Density reflection / bisection (empty-upper funnel)
    // -------------------------------------------------------------------------

    @Test
    void reflect_densityReflectedPivotHitsAfterAnEmptyPlacedProbe() {
        StealAttemptView view = freshView(b("a"), b("c"), b("z"));
        StealAttempt attempt = structureOffPolicy.beginAttempt(view);

        // Placed midpoint "n" is empty; the reflected pivot "e" hits.
        StealAction result = drive(attempt, List.of(MISS, HIT));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.REFLECT);
        assertThat(commit.pivot()).isEqualTo(b("e"));
        assertThat(commit.engagements()).contains(new Engagement("PIVOT", "reflect_hit"));
    }

    @Test
    void bisect_emptyUpperRetriesNearerTheCursorAfterReflectAndPlacementBothMiss() {
        StealAttemptView view = freshView(b("a"), b("c"), b("z"));
        StealAttempt attempt = structureAndReflectOffPolicy.beginAttempt(view);

        // Placed midpoint "n" is empty; reflect is off, so the FIRST bisection halving ("h") hits.
        StealAction result = drive(attempt, List.of(MISS, HIT));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.BISECT);
        assertThat(commit.pivot()).isEqualTo(b("h"));
    }

    // -------------------------------------------------------------------------
    // Structure discovery
    // -------------------------------------------------------------------------

    @Test
    void structureProbe_emptyUpperFunnelTakesTheMedianOfACompleteSample() {
        StealAttemptView view = freshView(b("a"), b("a"), b("z"));
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(MISS,
                new StructureProbeOutcome(List.of(b("k")), false)));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.STRUCTURE_PROBE);
        assertThat(commit.pivot()).isEqualTo(b("k"));
    }

    @Test
    void structureCapped_emptyUpperFunnelTakesTheFurthestBoundaryOfATruncatedSample() {
        StealAttemptView view = freshView(b("a"), b("a"), b("z"));
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(MISS,
                new StructureProbeOutcome(List.of(b("k"), b("p"), b("w")), true)));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.STRUCTURE_CAPPED);
        assertThat(commit.pivot()).isEqualTo(b("w"));
        assertThat(commit.engagements()).contains(new Engagement("STRUCTURE", "fanout_capped"));
    }

    @Test
    void adaptiveStructure_parentEmptySliverFindsARealBoundaryAtTheCoarsestLevel() {
        // cursor/hi diverge at adjacent code points ('1'/'2'), so the placed pivot is the degenerate
        // cursor-adjacent MIN_SAFE sliver -- and it HITS (the upper half is non-empty), so this is the
        // PARENT-empty case, not the empty-upper one.
        StealAttemptView view = freshView(b("a"), b("c1"), b("c2"));
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = drive(attempt, List.of(HIT,
                new StructureProbeOutcome(List.of(b("c1z")), false)));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.ADAPTIVE_STRUCTURE);
        assertThat(commit.pivot()).isEqualTo(b("c1z"));
    }

    // -------------------------------------------------------------------------
    // Flat-leaf residual
    // -------------------------------------------------------------------------

    @Test
    void flatLeaf_parentEmptySliverWithNoStructureReflectsABalancedInteriorPivot() {
        byte[] lo = b("led/0100/0010");
        byte[] cursor = b("led/0100/4000");
        byte[] hi = b("led/02");
        StealAttemptView view = freshView(lo, cursor, hi);
        StealAttempt attempt = structureOffPolicy.beginAttempt(view);

        // The placed midpoint is the cursor-adjacent sliver and it HITS (parent-empty); structure
        // discovery is off, so the flat-leaf density reflection commits directly (lo is a real deep
        // key inside the leaf -- zero probes needed to place it).
        StealAction result = drive(attempt, List.of(HIT));

        assertThat(result).isInstanceOf(Commit.class);
        Commit commit = (Commit) result;
        assertThat(commit.mechanism()).isEqualTo(PivotMechanism.FLAT_LEAF);
        assertThat(commit.pivot()).isEqualTo(b("led/0100/8"));
    }

    // -------------------------------------------------------------------------
    // Terminal outcomes that need no probe at all
    // -------------------------------------------------------------------------

    @Test
    void unsplittable_terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds() {
        // hi = cursor + one NUL byte: UTF-8-adjacent, so no valid-UTF-8 key exists strictly between.
        StealAttemptView view = freshView(b("a"), b("a"), new byte[] {'a', 0x00});
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = attempt.start();

        assertThat(result).isInstanceOf(MarkUnsplittable.class);
        MarkUnsplittable unsplittable = (MarkUnsplittable) result;
        assertThat(unsplittable.reason()).isEqualTo(UnsplittableReason.NO_PIVOT);
        assertThat(unsplittable.mutations()).containsExactlyInAnyOrder(
                new VictimMutation(1L, VictimMutation.Kind.SET_UNSPLITTABLE),
                new VictimMutation(1L, VictimMutation.Kind.RECORD_NO_PIVOT_TALLY));
    }

    @Test
    void unstartedFrontier_ownerHasNotCommittedItsFirstPageYet() {
        // The open frontier with cursor still at lo (⊥): no consumed span to reflect forward yet --
        // TRANSIENT (RETRY), never cached UNSPLITTABLE (the seed victim owns the whole keyspace).
        StealAttemptView view = freshView(null, null, null);
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = attempt.start();

        assertThat(result).isInstanceOf(Retry.class);
        assertThat(((Retry) result).reason()).isEqualTo(RetryReason.UNSTARTED_FRONTIER);
        assertThat(result.mutations()).containsExactly(
                new VictimMutation(1L, VictimMutation.Kind.MARK_NON_PRODUCTIVE));
    }

    @Test
    void unchangedNonProductiveSnapshot_repeatsNoWorkUntilTheVictimAdvances() {
        StealAttemptView view = new StealAttemptView(1, b("a"), b("m"), b("z"), 0, 0.0,
                coldDigest(b("a"), b("z")), true, 0, 0);
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = attempt.start();

        assertThat(result).isInstanceOf(Retry.class);
        assertThat(((Retry) result).reason()).isEqualTo(RetryReason.UNCHANGED_NONPRODUCTIVE_SNAPSHOT);
        // Already marked on a prior attempt -- do not mark it again.
        assertThat(result.mutations()).isEmpty();
    }

    @Test
    void cursorAtOrPastHi_theSpeculativeSelectionReadRacedTheOwnersDrain() {
        StealAttemptView view = freshView(b("a"), b("z"), b("z"));
        StealAttempt attempt = policy.beginAttempt(view);

        StealAction result = attempt.start();

        assertThat(result).isInstanceOf(Retry.class);
        assertThat(((Retry) result).reason()).isEqualTo(RetryReason.CURSOR_AT_OR_PAST_HI);
        assertThat(result.mutations()).containsExactly(
                new VictimMutation(1L, VictimMutation.Kind.MARK_NON_PRODUCTIVE));
    }
}
