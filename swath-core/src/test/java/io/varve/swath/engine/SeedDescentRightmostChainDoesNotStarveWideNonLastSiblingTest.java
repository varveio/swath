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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * <b>The converse of {@link SeedDescentLastTopLevelSiblingNotStarvedByRegeneratingChainTest}:</b> a
 * lexically-LAST top-level sibling with a deep, narrow, single-child chain (the same "regenerating
 * deepest cut" bait that test reproduces) but genuinely LIGHT — no real mass at the bottom — must not
 * drain the ENTIRE probe budget descending its own bottomless-looking spine at the expense of an
 * EARLIER, genuinely WIDE sibling that holds the run's real structure.
 *
 * <p>An unconditional {@link Long#MAX_VALUE} fallback for "no successor yet in the global cut set"
 * (the fix that test's own regression needed) overcorrects here: {@code zzz/}'s single-child chain
 * regenerates a new globally-last cut at every depth — each freshly-discovered deepest cut ALSO has no
 * successor in the global cut set yet, so it too scores {@code MAX_VALUE} — and the frontier chases
 * {@code zzz/}'s spine depth-first for the whole probe budget, never once descending into {@code
 * aaa/}, whose real partitioned structure ({@code aaa/p00/} … {@code aaa/p09/}) then survives seeding
 * as one giant unsplit {@code (aaa/, zzz/]} range. This is the exact late-wide-sibling shape: {@code
 * seed.decisions} chasing {@code projects/}'s ever-deepening rightmost descendants (each yielding 0-2
 * cuts) while its OTHER, equally real subtrees never get probed at all.
 *
 * <p>Scoping the unmeasured-tail fallback to the DISCOVERING scope's own ceiling ({@code
 * SeedStep#scopeCeiling}) fixes this: {@code zzz/d00/}'s tail is bounded by {@code zzz/}'s own
 * ceiling (a narrow, ordinary span, not the whole keyspace), so it stops out-scoring {@code aaa/}'s
 * top-level gap once the descent is a few levels deep, freeing the frontier to probe {@code aaa/}.
 */
final class SeedDescentRightmostChainDoesNotStarveWideNonLastSiblingTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 8;       // targetSeeds = maxProbes = 4*8 = 32
    private static final int CHAIN_DEPTH = 30;  // deeper than the descent's own probe ceiling (16)

    private static final byte[] AAA_LO = "aaa/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ZZZ_LO = "zzz/".getBytes(StandardCharsets.UTF_8);

    /**
     * {@code aaa/p00/obj} .. {@code aaa/p09/obj}: a real, WIDE, 10-way partitioned subtree — the mass
     * the seed must still discover and tile. {@code zzz/d00/d01/…/d29/obj}: a deep, narrow,
     * single-child-per-level chain with exactly ONE object at the bottom — structurally the same
     * regenerating-deepest-cut bait as {@link
     * SeedDescentLastTopLevelSiblingNotStarvedByRegeneratingChainTest}'s {@code contrib/}, but sorting
     * LAST (no top-level successor) instead of first, and carrying no real mass of its own.
     */
    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>();
        for (int p = 0; p < 10; p++) {
            keys.add(("aaa/p%02d/obj".formatted(p)).getBytes(StandardCharsets.UTF_8));
        }
        StringBuilder chain = new StringBuilder("zzz/");
        for (int d = 0; d < CHAIN_DEPTH; d++) {
            chain.append("d%02d/".formatted(d));
        }
        chain.append("obj");
        keys.add(chain.toString().getBytes(StandardCharsets.UTF_8));
        return keys;
    }

    @Test
    void aaaGetsProbedAndInteriorCutsDespiteZzzsBottomlessLastChain() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace()).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        // Still an exact I2/I3 tiling regardless of how the frontier spent its probes.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // The discriminating property: aaa/ got its OWN interior cut-points (p00/ .. p09/ were
        // discovered and tiled) instead of surviving as one giant unsplit range because zzz/'s
        // bottomless last-sibling chain never stopped outscoring it.
        long interiorCuts = specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(start -> start != null
                        && Arrays.compareUnsigned(start, AAA_LO) > 0
                        && Arrays.compareUnsigned(start, ZZZ_LO) < 0)
                .count();
        assertThat(interiorCuts)
                .as("aaa/ (wide, non-last) must still be probed and get interior cut-points — zzz/'s "
                        + "bottomless last-sibling chain must not drain the whole wallet first")
                .isPositive();

        // No single range should still own the entire aaa/ subtree.
        List<NodeSpec> wholeAaaRange = specs.stream()
                .filter(s -> Arrays.equals(s.rangeStart(), AAA_LO) && Arrays.equals(s.rangeEnd(), ZZZ_LO))
                .toList();
        assertThat(wholeAaaRange)
                .as("aaa/ must not collapse to a single (aaa/, zzz/] unsplit range")
                .isEmpty();
    }
}
