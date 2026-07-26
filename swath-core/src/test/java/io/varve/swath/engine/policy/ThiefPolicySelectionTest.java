/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Direct, table-driven unit tests for {@link ThiefPolicy#selectVictim} (algorithms.md §3.2):
 * argmax {@code estRemaining} over the curated pool, and the discriminated {@link NoVictimReason}
 * when no candidate qualifies. Drives {@link ThiefPolicy} directly against hand-built
 * {@link VictimView}s — no engine, no lock, no I/O.
 */
class ThiefPolicySelectionTest {

    private final ThiefPolicy policy = new ThiefPolicy(EngineToggles.DEFAULT, new byte[0]);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A bounded candidate with no consumed span yet ({@code cursor == lo}) — ranked by width alone. */
    private static VictimView freshBounded(long nodeId, byte[] lo, byte[] hi) {
        return new VictimView(nodeId, lo, lo, hi, 0, false, false);
    }

    /** As {@link #freshBounded}, but currently paced (a futility cooldown skip available). */
    private static VictimView pacedFreshBounded(long nodeId, byte[] lo, byte[] hi) {
        return new VictimView(nodeId, lo, lo, hi, 0, false, true);
    }

    // -------------------------------------------------------------------------
    // Ordering
    // -------------------------------------------------------------------------

    @Test
    void picksTheCandidateWithTheLargerEstRemaining() {
        // A: no progress, no density signal — estRemaining is the plain remaining span.
        VictimView a = freshBounded(1, b("a"), b("z"));
        // B: half consumed but with a huge local density (keysEmitted far outstrips consumed span) —
        // estRemaining = (keysEmitted / consumed) * remaining, which overwhelms A's plain span.
        VictimView b = new VictimView(2, b("a"), b("m"), b("z"), 1_000_000, false, false);

        Selection selection = policy.selectVictim(List.of(a, b));

        assertThat(selection).isInstanceOf(Selected.class);
        assertThat(((Selected) selection).victimNodeId()).isEqualTo(2L);
    }

    @Test
    void theOpenFrontierAlwaysOutranksABoundedCandidate() {
        VictimView bounded = new VictimView(1, b("a"), b("m"), b("z"), 1_000_000, false, false);
        VictimView frontier = new VictimView(2, b("a"), b("m"), null, 1, false, false);

        Selection selection = policy.selectVictim(List.of(bounded, frontier));

        assertThat(((Selected) selection).victimNodeId()).isEqualTo(2L);
    }

    @Test
    void aStrictTieKeepsTheFirstCandidateInPoolOrder() {
        // Identical shape, identical estRemaining -- selection uses `est > best` (strict), so the
        // FIRST one seen wins a tie, never the last.
        VictimView first = freshBounded(1, b("a"), b("z"));
        VictimView second = freshBounded(2, b("a"), b("z"));

        Selection selection = policy.selectVictim(List.of(first, second));

        assertThat(((Selected) selection).victimNodeId()).isEqualTo(1L);
    }

    @Test
    void skipsUnsplittableCandidatesEvenWhenTheyWouldOtherwiseWin() {
        VictimView unsplittable = new VictimView(1, b("a"), b("m"), b("z"), 1_000_000, true, false);
        VictimView splittable = freshBounded(2, b("a"), b("z"));

        Selection selection = policy.selectVictim(List.of(unsplittable, splittable));

        assertThat(((Selected) selection).victimNodeId()).isEqualTo(2L);
    }

    @Test
    void skipsCandidatesWithNoRemainingSpan() {
        // cursor == hi: remaining span is exactly zero, so estRemaining <= 0.
        VictimView drained = new VictimView(1, b("a"), b("z"), b("z"), 500, false, false);
        VictimView splittable = freshBounded(2, b("a"), b("z"));

        Selection selection = policy.selectVictim(List.of(drained, splittable));

        assertThat(((Selected) selection).victimNodeId()).isEqualTo(2L);
    }

    @Test
    void skipsPacedCandidatesAndReturnsTheirPacingSkipAsAMutationPlusAFutilityPacedEngagement() {
        VictimView paced = pacedFreshBounded(1, b("a"), b("z"));
        VictimView splittable = freshBounded(2, b("a"), b("z"));

        Selection selection = policy.selectVictim(List.of(paced, splittable));

        assertThat(((Selected) selection).victimNodeId()).isEqualTo(2L);
        assertThat(selection.mutations())
                .containsExactly(new VictimMutation(1L, VictimMutation.Kind.CONSUME_PACING_SKIP));
        assertThat(selection.engagements()).containsExactly(new Engagement("STEAL", "futility_paced"));
    }

    // -------------------------------------------------------------------------
    // NO_VICTIM discrimination (algorithms.md §3: exactly one discriminator per refusal)
    // -------------------------------------------------------------------------

    static Stream<Arguments> noVictimCases() {
        VictimView unsplittable = new VictimView(1, b("a"), b("m"), b("z"), 1, true, false);
        VictimView paced = pacedFreshBounded(9, b("a"), b("z"));
        VictimView noSpan = new VictimView(2, b("a"), b("z"), b("z"), 1, false, false);
        return Stream.of(
                Arguments.of("empty pool", List.<VictimView>of(), NoVictimReason.POOL_EMPTY),
                Arguments.of("every candidate has no remaining span",
                        List.of(noSpan), NoVictimReason.ALL_NO_REMAINING_SPAN),
                Arguments.of("every candidate is paced",
                        List.of(paced), NoVictimReason.ALL_FUTILITY_PACED),
                Arguments.of("every candidate is unsplittable",
                        List.of(unsplittable), NoVictimReason.ALL_UNSPLITTABLE),
                Arguments.of("a mix of skip reasons with no productive candidate",
                        List.of(unsplittable, paced), NoVictimReason.MIXED_SKIPS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noVictimCases")
    void discriminatesWhyNoVictimQualified(String description, List<VictimView> pool, NoVictimReason expected) {
        Selection selection = policy.selectVictim(pool);

        assertThat(selection).isInstanceOf(NoVictim.class);
        assertThat(((NoVictim) selection).reason()).isEqualTo(expected);
    }
}
