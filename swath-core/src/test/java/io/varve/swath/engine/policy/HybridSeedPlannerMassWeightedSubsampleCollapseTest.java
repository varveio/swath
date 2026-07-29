/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * <b>Issue #83: {@link HybridSeedPlanner#massWeightedSubsample} can return FAR fewer picks than
 * {@code max} even when the candidate count comfortably exceeds it.</b> Reconstructs the exact shape
 * the porotomo bench hit (issue #15's E-22/E-23 replay): {@code n = 297} distinct cuts, {@code max =
 * 256} (a generous cap — plenty of room), of which a {@code SAMPLE_BUDGET}-sized (32) subset was
 * weight-sampled during the descent's {@code WEIGHT_SAMPLE} phase and the rest default to weight 1.
 * A handful of the sampled cuts came back page-capped ({@code PROBE_PAGE} = 1000 weight) — a real,
 * unremarkable outcome on a bucket with a few dense directories.
 *
 * <p>The mechanism (as measured, not guessed): the threshold walk's {@code next = acc + step} anchors
 * every subsequent threshold to whatever {@code acc} happens to be the instant a pick fires — this is
 * deliberate (the fix for the OPPOSITE, already-guarded failure in {@link
 * HybridSeedPlannerMassWeightedSubsampleTest}: a heavy cut's weight must not spill onto its sorted
 * neighbors as a consecutive landslide). But it has a cost nobody measured: when a pick's own weight
 * jump {@code w[i]} is itself much larger than {@code step}, that ONE pick consumes a multiple of
 * {@code step}'s worth of "budget" that can never be spent again (a set cannot pick the same index
 * twice to recoup it), and the deficit compounds with every such heavy cut encountered later in the
 * walk. With enough heavy cuts relative to {@code max}, the total distinct picks can collapse to a
 * small fraction of {@code max} — exactly what this test pins.
 */
final class HybridSeedPlannerMassWeightedSubsampleCollapseTest {

    private static final int N = 297;
    private static final int MAX = 256;
    private static final int SAMPLE_BUDGET = 32;
    private static final long PROBE_PAGE_WEIGHT = 1000L;   // HybridSeedPlanner.PROBE_PAGE

    /** {@code cut/0000} .. {@code cut/0296} — a plain, evenly-sortable stand-in for the descent's real
     *  (byte-prefix) cuts; only the COUNT and the weight distribution over sorted position matter here. */
    private static TreeSet<byte[]> cuts() {
        TreeSet<byte[]> cuts = new TreeSet<>(Arrays::compareUnsigned);
        for (int i = 0; i < N; i++) {
            cuts.add("cut/%04d".formatted(i).getBytes(StandardCharsets.UTF_8));
        }
        return cuts;
    }

    /**
     * Mirrors {@code HybridSeedPlanner.Descent#issueNextWeightSample}'s own even-index spread over the
     * sorted cut list ({@code idx = i*(n-1)/(budget-1)}): {@code SAMPLE_BUDGET} (32) of the 297 cuts
     * get a real measured weight, one in six of those page-capped (dense, weight 1000 — plausible on a
     * real bucket with a handful of heavy directories spread through an otherwise light tree); the rest
     * default to weight 1 in {@link HybridSeedPlanner#massWeightedSubsample}.
     */
    private static NavigableMap<byte[], Long> sampledWeights(List<byte[]> sorted) {
        NavigableMap<byte[], Long> weights = new TreeMap<>(Arrays::compareUnsigned);
        for (int k = 0; k < SAMPLE_BUDGET; k++) {
            int idx = (int) ((long) k * (N - 1) / (SAMPLE_BUDGET - 1));
            long w = (k % 6 == 0) ? PROBE_PAGE_WEIGHT : (5L + (k * 7) % 40);
            weights.put(sorted.get(idx), w);
        }
        return weights;
    }

    @Test
    void aGenerousCapStillYieldsRoughlyTheFullBudgetDespiteSeveralHeavySamples() {
        TreeSet<byte[]> cuts = cuts();
        List<byte[]> sorted = List.copyOf(cuts);
        NavigableMap<byte[], Long> weights = sampledWeights(sorted);

        List<byte[]> picked = HybridSeedPlanner.massWeightedSubsample(cuts, MAX, weights);

        // The behavioural claim: max=256 against n=297 is a GENEROUS cap (87% of the candidates) --
        // whatever the weighting, the walk must still spend nearly all of that budget, not collapse to
        // a small fraction of it the way the pre-fix walk does (observed: 24-40 picks out of 256).
        assertThat(picked.size())
                .as("a generous max=%d against n=%d must yield close to the full budget even with "
                        + "several heavy-weighted samples scattered through the walk -- issue #83's "
                        + "collapse, not a legitimate proportional reduction", MAX, N)
                .isGreaterThan(MAX - MAX / 10);   // within 10% of the requested budget

        // Every picked cut is still a real, distinct member of the source set (no synthesized values).
        TreeSet<byte[]> distinctPicked = new TreeSet<>(Arrays::compareUnsigned);
        distinctPicked.addAll(picked);
        assertThat(distinctPicked).hasSameSizeAs(picked);
        assertThat(cuts).containsAll(picked);
    }
}
