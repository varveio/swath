/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * <b>Two heavy subtrees, far apart in frontier order, past the cut-point cap.</b> Regression guard
 * for the residual commoncrawl defect the {@code cuts.size() < targetSeeds} loop condition left in
 * place after {@code SeedMassAwareDescentTest#explodingSiblingDoesNotStrandNarrowSiblingWithRealMass}
 * fixed per-cut disposal: even with the descent continuing past the FIRST exploding sub-level, the
 * loop as a whole used to stop the instant the accumulated cut count reached {@code targetSeeds} —
 * first-come-first-served, so whichever heavy subtree the frontier reached first kept every cut,
 * and an equally heavy sibling reached only after enough small siblings had pushed the total over
 * the cap was never even probed (s3://commoncrawl: {@code projects/headers-testing/} got its cuts
 * and drained in parallel while {@code contrib/datacomp/DCLM-pool/jsonl/} sat un-descended for the
 * rest of the run).
 *
 * <p>The fixture: two truncated, genuinely heavy sub-levels ({@code aheavy/}, {@code zheavy/} — sort
 * first and last respectively) separated by {@code THIN_SIBLINGS} single-object regions. The top
 * level itself stays narrow ({@code < targetSeeds} common prefixes, so the descent loop is entered
 * at all), but {@code THIN_SIBLINGS} is sized so that top-level siblings plus ONE heavy subtree's own
 * banded interior cuts already exceed {@code targetSeeds} — exactly the point at which the old
 * {@code cuts.size() < targetSeeds} loop condition would have stopped, stranding whichever heavy
 * subtree the (mass-aware, best-first) frontier reorder had not yet reached. The probe budget
 * ({@code maxProbes == targetSeeds} at these worker counts) is sized with headroom to visit the
 * WHOLE frontier regardless of order, isolating the cut-point cap as the only thing that could still
 * strand a sibling.
 */
final class SeedDescentTwoHeavySiblingsBothSurviveTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 16;   // targetSeeds = maxProbes = 4*16 = 64
    private static final int THIN_SIBLINGS = 48;
    private static final int HEAVY_BRANCHES = 1200;   // > PROBE_PAGE (1000): the sub-level truncates
    private static final int OBJS_PER_BRANCH = 20;    // >= SAMPLE_DENSE_MIN_OBJECTS: samples dense

    private static final byte[] HEAVY_A_LO = "aheavy/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEAVY_A_HI = prefixCeil(HEAVY_A_LO);
    private static final byte[] HEAVY_B_LO = "zheavy/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEAVY_B_HI = prefixCeil(HEAVY_B_LO);

    /**
     * Two heavy, plain-named (non-{@code key=value/}) truncated sub-levels — {@code aheavy/} sorts
     * before every thin sibling, {@code zheavy/} sorts after all of them — with {@code THIN_SIBLINGS}
     * single-object regions in between.
     */
    private static List<byte[]> twoHeavySubtreesFarApart() {
        List<byte[]> keys = new ArrayList<>();
        for (String root : List.of("aheavy", "zheavy")) {
            for (int b = 0; b < HEAVY_BRANCHES; b++) {
                for (int o = 0; o < OBJS_PER_BRANCH; o++) {
                    keys.add(("%s/branch%04d/obj%02d".formatted(root, b, o)).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        for (int m = 0; m < THIN_SIBLINGS; m++) {
            keys.add(("m%04d/o".formatted(m)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    void bothHeavySubtreesGetInteriorCutPointsAndTheOverCapSetIsMassSubsampled() throws Exception {
        int targetSeeds = 4 * WORKERS;
        List<byte[]> keyspace = twoHeavySubtreesFarApart();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        // (a) BOTH heavy subtrees end up with interior seed cut-points — neither is left as one
        // un-split range because the other one exploded/banded first.
        long insideA = interiorCutsIn(specs, HEAVY_A_LO, HEAVY_A_HI);
        long insideB = interiorCutsIn(specs, HEAVY_B_LO, HEAVY_B_HI);
        assertThat(insideA).as("aheavy/ (reached first in sorted/FIFO order) keeps interior cut-points")
                .isPositive();
        assertThat(insideB)
                .as("zheavy/ (the far sibling the old cuts.size()-capped loop would have stranded) "
                        + "ALSO keeps interior cut-points")
                .isPositive();

        // (b) total ranges <= targetSeeds + synthesized (no flat-wide region here, so synthesized == 0).
        assertThat(specs.size())
                .as("seed_ranges stays within the external targetSeeds(+synthesized) contract")
                .isLessThanOrEqualTo(targetSeeds + 1);

        // Still an exact I2/I3 tiling regardless of how the two heavy regions were sub-tiled and
        // subsampled.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // (c) the over-cap descent cut set was actually mass-weighted and subsampled, not left to a
        // positional trim that would have favored whichever region the frontier reached first.
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED.descent_cuts_subsampled", 0L))
                .as("SEED.descent_cuts_subsampled must fire: the descent's cut set exceeded targetSeeds")
                .isPositive();
        assertThat(reasons.getOrDefault("SEED.mass_weighted_subsample", 0L))
                .as("the reduction is mass-weighted, not positional")
                .isPositive();
        assertThat(reasons.getOrDefault("SEED.heavy_cut_banded", 0L))
                .as("both heavy sub-levels sampled dense and were banded")
                .isEqualTo(2L);
    }

    private static long interiorCutsIn(List<NodeSpec> specs, byte[] lo, byte[] hi) {
        return specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(start -> start != null
                        && Arrays.compareUnsigned(start, lo) > 0
                        && Arrays.compareUnsigned(start, hi) < 0)
                .count();
    }

    /** Increment the last byte of a prefix to form its exclusive ceiling (all prefix keys sort below it). */
    private static byte[] prefixCeil(byte[] prefix) {
        byte[] c = Arrays.copyOf(prefix, prefix.length);
        c[c.length - 1]++;
        return c;
    }
}
