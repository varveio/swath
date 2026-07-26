/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link HybridSeedPlanner}/{@link SeedDescent} against a RECORDED sequence of
 * {@link SeedProbeOutcome}s with <b>zero I/O</b> — no {@code PageFetcher}, no {@code ListPage}, no
 * {@code SeedStep} — the proof that the seam is real (seam-notes.md): a hypothetical second
 * planner (a seed-diet policy, a hints-file planner) would be tested against this exact same
 * {@link SeedAction}/{@link SeedProbeOutcome} contract. Each test below exercises a distinct phase
 * transition of the descent's explicit state machine (TOP -> optional TOP_EXTRA -> DESCENT, which for
 * an ambiguous truncated level detours through the SAMPLE_CHILD sub-loop -> an optional post-loop
 * WEIGHT_SAMPLE sub-loop -> the terminal {@link SeedPlan}), not just the single happy-path descent.
 *
 * <p>Every scenario below is driven by identity (the response to a probe depends only on WHICH
 * prefix/{@code startAfter} was requested, via {@link #drive}), never on an assumption about which
 * order {@link HybridSeedPlanner}'s span-priority frontier happens to poll same-depth siblings in —
 * that internal ordering is exercised (and pinned) elsewhere ({@code SeedFrontierLevelOrderTest} and
 * neighbors); this suite is about the request/response contract, not the frontier's own tie-breaking.
 */
final class HybridSeedPlannerDescentTest {

    private static final byte[] EMPTY = new byte[0];

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Drains a fresh descent against {@code script}, recording every issued probe and engagement. */
    private static SeedPlan drive(SeedDescent descent, Function<RequestSeedProbe, SeedProbeOutcome> script,
            List<RequestSeedProbe> issued, List<Engagement> engagements) {
        SeedAction action = descent.start();
        while (true) {
            engagements.addAll(action.engagements());
            if (action instanceof RequestSeedProbe request) {
                issued.add(request);
                action = descent.onProbeResult(script.apply(request));
            } else if (action instanceof SeedPlan plan) {
                return plan;
            } else {
                throw new IllegalStateException("unhandled SeedAction: " + action);
            }
        }
    }

    private static List<String> reasons(List<Engagement> engagements) {
        List<String> out = new ArrayList<>();
        for (Engagement e : engagements) {
            assertThat(e.category()).isEqualTo("SEED");
            out.add(e.reason());
        }
        return out;
    }

    /**
     * A content-comparable projection of a {@link SeedLevelDecision} — {@code record}'s generated
     * {@code equals} compares the {@code byte[] prefix} field by REFERENCE (arrays never override
     * {@code equals}), so two decisions built from separately-allocated but byte-identical prefixes
     * would never compare equal without this.
     */
    private static String describe(SeedLevelDecision d) {
        return new String(d.prefix(), StandardCharsets.UTF_8) + '|' + d.fanout() + '|' + d.truncated() + '|'
                + d.classification() + '|' + d.cutsKept() + '|' + d.cutsDiscarded();
    }

    private static String describe(byte[] prefix, int fanout, boolean truncated, String classification,
            int cutsKept, int cutsDiscarded) {
        return new String(prefix, StandardCharsets.UTF_8) + '|' + fanout + '|' + truncated + '|'
                + classification + '|' + cutsKept + '|' + cutsDiscarded;
    }

    /**
     * TOP (narrow) -> DESCENT, where the first-polled child is an ambiguous truncated level: the
     * SAMPLE_CHILD sub-loop samples all 3 of its children (all dense) and confirms it heavy, banding
     * it; the descent then continues past it (frontier not abandoned) to the second, narrow child,
     * before the frontier drains and the plan finalizes. Exercises TOP, DESCENT, and the SAMPLE_CHILD
     * sub-loop end to end.
     */
    @Test
    void sampleChildSubLoopConfirmsHeavyCutThenDescentContinuesToTheRemainingSibling() {
        byte[] prefix = b("b/");
        HybridSeedPlanner planner = new HybridSeedPlanner(prefix, 4, EngineToggles.DEFAULT);
        assertThat(planner.probeBudget()).isEqualTo(16);   // targetSeeds = min(1000, 4*4) = 16

        Function<RequestSeedProbe, SeedProbeOutcome> script = req -> {
            String p = new String(req.probePrefix(), StandardCharsets.UTF_8);
            return switch (p) {
                case "b/" -> new SeedProbeOutcome(List.of(b("b/x/"), b("b/y/")), false, 0, null);
                case "b/x/" -> new SeedProbeOutcome(List.of(b("b/x/p/"), b("b/x/q/"), b("b/x/r/")), true, 0, null);
                case "b/x/p/", "b/x/q/", "b/x/r/" -> new SeedProbeOutcome(List.of(), true, 0, null);   // dense
                case "b/y/" -> new SeedProbeOutcome(List.of(), false, 2, null);   // narrow leaf
                default -> throw new AssertionError("unscripted probe: " + p);
            };
        };

        List<RequestSeedProbe> issued = new ArrayList<>();
        List<Engagement> engagements = new ArrayList<>();
        SeedPlan plan = drive(planner.beginDescent(), script, issued, engagements);

        assertThat(issued).as("top, then b/x/, then its 3 samples, then b/y/ — 6 probes total")
                .hasSize(6);
        assertThat(plan.probes()).isEqualTo(6);

        List<byte[]> cuts = plan.cuts();
        assertThat(cuts.stream().map(c -> new String(c, StandardCharsets.UTF_8)))
                .as("b/x/'s own cut plus its 3 banded children, plus b/y/ — sorted")
                .containsExactly("b/x/", "b/x/p/", "b/x/q/", "b/x/r/", "b/y/");

        assertThat(plan.decisions()).hasSize(3);
        assertThat(describe(plan.decisions().get(0))).isEqualTo(describe(prefix, 2, false, "narrow", 2, 0));
        List<String> tail = plan.decisions().subList(1, 3).stream().map(HybridSeedPlannerDescentTest::describe)
                .toList();
        assertThat(tail).containsExactlyInAnyOrder(
                describe(b("b/x/"), 3, true, "heavy_cut_banded", 3, 0),
                describe(b("b/y/"), 0, false, "narrow", 0, 0));

        List<String> fired = reasons(engagements);
        assertThat(fired).contains("frontier_reordered", "heavy_cut_descended", "heavy_cut_banded",
                "frontier_continued_past_explosion", "top_complete");
        assertThat(fired).doesNotContain("explosion_confirmed", "frontier_level_ordered", "delimiter_seeded");
    }

    /**
     * TOP (page-capped, non-empty cuts) -> TOP_EXTRA (the bounded +1-page pagination), which recovers
     * no further cuts here, then falls straight to finalize (the descent loop is skipped entirely,
     * since a page-capped top never descends — algorithms.md §8). Exercises TOP and TOP_EXTRA without
     * ever reaching DESCENT.
     */
    @Test
    void topExtraPaginationRecoversTheTopLevelPastItsFirstPage() {
        byte[] prefix = b("c/");
        HybridSeedPlanner planner = new HybridSeedPlanner(prefix, 4, EngineToggles.DEFAULT);

        Function<RequestSeedProbe, SeedProbeOutcome> script = req -> {
            String p = new String(req.probePrefix(), StandardCharsets.UTF_8);
            String after = req.startAfter() == null ? null : new String(req.startAfter(), StandardCharsets.UTF_8);
            if ("c/".equals(p) && after == null) {
                return new SeedProbeOutcome(List.of(b("c/a/"), b("c/b/")), true, 0, b("c/b/"));
            }
            // A NON-empty second page (unlike an earlier version of this fixture, which returned
            // List.of() here): deleting the second-page cut-ingestion path entirely (never folding
            // outcome.commonPrefixes() from the TOP_EXTRA response into the accumulated cut set) still
            // satisfied the previous, all-empty-second-page fixture, since page 2 contributed nothing
            // either way -- an independent review's finding. "c/c/" here is a real, distinct cut this
            // test now depends on actually reaching plan.cuts().
            if ("c/".equals(p) && "c/b/".equals(after)) {
                return new SeedProbeOutcome(List.of(b("c/c/")), false, 0, null);
            }
            throw new AssertionError("unscripted probe: prefix=" + p + " startAfter=" + after);
        };

        List<RequestSeedProbe> issued = new ArrayList<>();
        List<Engagement> engagements = new ArrayList<>();
        SeedPlan plan = drive(planner.beginDescent(), script, issued, engagements);

        assertThat(issued).hasSize(2);
        assertThat(issued.get(0).startAfter()).isNull();
        assertThat(issued.get(1).startAfter()).isEqualTo(b("c/b/"));
        assertThat(plan.probes()).isEqualTo(2);

        assertThat(plan.cuts().stream().map(c -> new String(c, StandardCharsets.UTF_8)))
                .containsExactly("c/a/", "c/b/", "c/c/");
        assertThat(plan.decisions().stream().map(HybridSeedPlannerDescentTest::describe).toList())
                .containsExactly(
                        describe(prefix, 2, true, "tiny_leaf_explosion", 2, 0),
                        describe(prefix, 1, false, "top_probe_paginated", 1, 0));

        assertThat(reasons(engagements))
                .containsExactly("top_probe_paginated", "tiny_leaf_explosion", "top_truncated");
    }

    /**
     * TOP (narrow, wide fan-out well past {@code targetSeeds}) -> DESCENT (every child a narrow
     * no-op, so cuts stay at the top's own fan-out) -> the post-loop WEIGHT_SAMPLE sub-loop, which
     * samples enough cuts to clear {@code MIN_WEIGHT_SAMPLES} and mass-weights the over-cap reduction.
     * Exercises the WEIGHT_SAMPLE sub-loop and the {@code delimiter_seeded} decision-trace rewrite
     * (the one case among these three tests where no special classification engages at all).
     */
    @Test
    void weightSampleSubLoopMassWeightsAnOverCapWideTop() {
        byte[] prefix = b("w/");
        int childCount = 60;
        int heavyFromIndex = 9;
        List<byte[]> topChildren = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            topChildren.add(b("w/%03d/".formatted(i)));
        }
        HybridSeedPlanner planner = new HybridSeedPlanner(prefix, 5, EngineToggles.DEFAULT);
        assertThat(planner.probeBudget()).isEqualTo(20);   // targetSeeds = min(1000, 4*5) = 20

        // Children from "w/009/" on report a heavier objectCount than earlier ones -- an earlier
        // version of this fixture gave every child the SAME weight (5), so a mutant that subsamples
        // POSITIONALLY (evenly-spaced over the sorted cut set) yet still emits the
        // mass_weighted_subsample mark passed unnoticed (an independent review's finding). This
        // fixture's own cut count (60) so far exceeds targetSeeds (20) that a genuine weight skew
        // produces a cut SET a positional subsample of the same 60 cuts could never produce (see the
        // direct comparison below, reconstructing subsampleEvenly's own even-index formula) --
        // discriminating without needing to predict massWeightedSubsample's exact selection precisely.
        Function<RequestSeedProbe, SeedProbeOutcome> script = req -> {
            String p = new String(req.probePrefix(), StandardCharsets.UTF_8);
            if ("w/".equals(p)) {
                return new SeedProbeOutcome(topChildren, false, 0, null);
            }
            boolean heavy = p.compareTo("w/%03d/".formatted(heavyFromIndex)) >= 0;
            return new SeedProbeOutcome(List.of(), false, heavy ? 50 : 1, null);
        };

        List<RequestSeedProbe> issued = new ArrayList<>();
        List<Engagement> engagements = new ArrayList<>();
        SeedPlan plan = drive(planner.beginDescent(), script, issued, engagements);

        assertThat(plan.probes()).isEqualTo(20);   // exhausts the full probe budget (maxProbes = 20)
        assertThat(issued).hasSize(20);

        List<String> fired = reasons(engagements);
        assertThat(fired).contains("frontier_reordered", "descent_cuts_subsampled",
                "mass_weighted_subsample", "delimiter_seeded", "top_complete");
        assertThat(fired).doesNotContain("tiny_leaf_explosion", "fanout_tiled", "heavy_cut_banded",
                "frontier_level_ordered");

        assertThat(plan.cuts()).as("the over-cap cut set was reduced to at most targetSeeds")
                .hasSizeLessThanOrEqualTo(20).isNotEmpty();

        // Reconstruct exactly what a POSITIONAL (evenly-spaced) subsample of the same 60 sorted cuts
        // down to targetSeeds(20) would have picked -- subsampleEvenly's own index formula
        // (idx = i*(n-1)/(max-1)), mirrored here since that method is private to HybridSeedPlanner. A
        // mutant that discards the weight map and always calls the positional path (while still
        // emitting the mass_weighted_subsample mark) would make plan.cuts() equal to this reconstructed
        // list; the deliberately skewed weights above make the genuine result provably different.
        List<byte[]> sortedAll = new ArrayList<>(topChildren);
        sortedAll.sort(Arrays::compareUnsigned);
        int n = sortedAll.size();
        int max = 20;
        List<String> positional = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            int idx = (int) ((long) i * (n - 1) / (max - 1));
            positional.add(new String(sortedAll.get(idx), StandardCharsets.UTF_8));
        }
        List<String> actual = plan.cuts().stream().map(c -> new String(c, StandardCharsets.UTF_8)).toList();
        assertThat(actual)
                .as("a genuinely mass-weighted subsample over these deliberately skewed weights must "
                        + "differ from a purely positional (evenly-spaced) subsample of the same 60 cuts")
                .isNotEqualTo(positional);
        // The finalizeDecisions rewrite: the top-level entry's raw "narrow" classification is
        // promoted to "delimiter_seeded" only once the WHOLE run's shape is known to be the
        // generic case (synthesized == 0, no special per-level classification anywhere).
        assertThat(plan.decisions().get(0).classification()).isEqualTo("delimiter_seeded");
    }
}
