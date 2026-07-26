/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link HybridSeedPlanner#massWeightedSubsample} does not let a single heavy-weight sampled cut
 * trigger a landslide of consecutive picks that starves whatever the sorted cut set holds further
 * away.</b> Deterministic reconstruction of a mass-weighted reduction's input shape
 * (feeds the cut list + weights directly — no probes, no {@link HybridSeedPlanner} instance needed):
 * many light {@code contrib/}-style cuts (unsampled, default weight 1), ONE heavy sampled cut
 * ({@link HybridSeedPlanner}'s {@code PROBE_PAGE} weight of 1000, the weight a page-capped sampled
 * child gets), followed by many more light {@code projects/headers-testing/}-style cuts (also
 * unsampled, weight 1) — the exact shape a mass-weighted reduction sees once the descent has actually
 * explored a heavy region with just one confirmed-dense sample.
 *
 * <p>The pre-fix threshold walk advanced its next pick-threshold by a fixed {@code step} every pick,
 * so the ~1000 units of "credit" banked by the single heavy cut left the threshold far behind the
 * running total — every ordinary-weight cut immediately following the heavy one then also satisfied
 * the (stale) crossing check and got auto-picked, a long unbroken run that has nothing to do with
 * those cuts' own weight. The fix anchors the next threshold to the weight actually accumulated at
 * each pick, so a heavy cut's credit cannot spill onto unrelated neighbors.
 */
final class HybridSeedPlannerMassWeightedSubsampleTest {

    private static final int CONTRIB_CUTS = 300;
    private static final int HEADERS_TESTING_CUTS = 300;
    private static final long PROBE_PAGE_WEIGHT = 1000L;   // HybridSeedPlanner.PROBE_PAGE

    /**
     * {@code contrib/000} .. {@code contrib/299} (light, unsampled), then ONE heavy sampled cut
     * ({@code contrib/zzz-heavy}, weight 1000 — sorts after every plain {@code contrib/NNN} cut),
     * then {@code projects/headers-testing/000} .. {@code /299} (light, unsampled) — mirroring the
     * FS3 shape where a heavy sampled cut sits at the contrib/projects boundary and a large, evenly
     * structured region follows it.
     */
    private static TreeSet<byte[]> reconstructedCuts() {
        TreeSet<byte[]> cuts = new TreeSet<>(Arrays::compareUnsigned);
        for (int i = 0; i < CONTRIB_CUTS; i++) {
            cuts.add("contrib/%04d".formatted(i).getBytes(StandardCharsets.UTF_8));
        }
        cuts.add("contrib/zzz-heavy".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < HEADERS_TESTING_CUTS; i++) {
            cuts.add("projects/headers-testing/%04d".formatted(i).getBytes(StandardCharsets.UTF_8));
        }
        return cuts;
    }

    private static NavigableMap<byte[], Long> heavyWeightOnly() {
        NavigableMap<byte[], Long> weights = new TreeMap<>(Arrays::compareUnsigned);
        weights.put("contrib/zzz-heavy".getBytes(StandardCharsets.UTF_8), PROBE_PAGE_WEIGHT);
        return weights;
    }

    @Test
    void headersTestingKeepsRoughlyProportionalRepresentationNotAConsecutiveLandslideHole() {
        TreeSet<byte[]> cuts = reconstructedCuts();
        int max = 100;   // well under n = 601, forces the over-cap reduction

        List<byte[]> picked = HybridSeedPlanner.massWeightedSubsample(cuts, max, heavyWeightOnly());

        List<byte[]> sortedCuts = new ArrayList<>(cuts);
        long headersTestingPicks = picked.stream()
                .filter(c -> startsWith(c, "projects/headers-testing/")).count();
        long contribPicks = picked.stream().filter(c -> startsWith(c, "contrib/")).count();

        // The core proportionality property the FS3 regression violated: projects/headers-testing/,
        // sorting AFTER the heavy sampled cut, still gets a comfortably positive, roughly-fair share
        // of the budget — not a hole. (The fix legitimately spends the heavy cut's own ~1000-weight
        // credit on nothing but ITSELF — there is no candidate cut inside its own unmeasured interval
        // to redistribute that credit to — so the total picked count lands somewhat under `max`
        // rather than being padded out by an unrelated neighbor; that is the honest, not the
        // pathological, outcome.)
        assertThat(headersTestingPicks)
                .as("projects/headers-testing/'s ~300 light cuts must not be starved by contrib/'s "
                        + "single heavy-weighted neighbor")
                .isGreaterThan(10);
        // Both regions carry equal REAL weight (300 unsampled cuts each) once the heavy cut's own
        // single-point credit is excluded, so neither should end up wildly over- or under-represented
        // relative to the other.
        assertThat((double) headersTestingPicks / contribPicks)
                .as("headers-testing/ and contrib/ carry equal measured weight — their pick counts "
                        + "should land within the same order of magnitude, not one dwarfing the other")
                .isBetween(0.5, 2.0);

        // The landslide's signature: a long unbroken run of picks immediately following the heavy
        // cut, positionally determined by nothing but proximity to it. Bound the run length instead.
        assertThat(maxConsecutiveRun(picked, sortedCuts))
                .as("no run of consecutive sorted-cut picks should be anywhere near the heavy cut's "
                        + "own weight-driven pick count — a landslide would pick dozens to hundreds in a "
                        + "row purely because they happen to sort next to the heavy cut")
                .isLessThan(20);
    }

    private static boolean startsWith(byte[] cut, String prefix) {
        return new String(cut, StandardCharsets.UTF_8).startsWith(prefix);
    }

    /** The longest run of picked cuts that are also CONSECUTIVE in the full sorted cut list. */
    private static int maxConsecutiveRun(List<byte[]> picked, List<byte[]> sortedCuts) {
        List<Integer> pickedIndices = new ArrayList<>();
        for (byte[] p : picked) {
            int idx = indexOf(sortedCuts, p);
            pickedIndices.add(idx);
        }
        pickedIndices.sort(Integer::compareTo);
        int longest = pickedIndices.isEmpty() ? 0 : 1;
        int current = longest;
        for (int i = 1; i < pickedIndices.size(); i++) {
            if (pickedIndices.get(i) == pickedIndices.get(i - 1) + 1) {
                current++;
                longest = Math.max(longest, current);
            } else {
                current = 1;
            }
        }
        return longest;
    }

    private static int indexOf(List<byte[]> sortedCuts, byte[] cut) {
        for (int i = 0; i < sortedCuts.size(); i++) {
            if (Arrays.equals(sortedCuts.get(i), cut)) {
                return i;
            }
        }
        throw new AssertionError("picked cut not found in the sorted source set");
    }
}
