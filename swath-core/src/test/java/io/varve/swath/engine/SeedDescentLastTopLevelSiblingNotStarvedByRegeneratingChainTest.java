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
 * <b>The lexically-LAST top-level sibling is not starved forever by a sibling whose own descent
 * keeps regenerating new, higher-scoring cuts for the WHOLE probe budget.</b> The exact commoncrawl
 * mechanism this reverses: {@code s3://commoncrawl}'s top level is just two entries, {@code
 * contrib/} and {@code projects/}; {@code projects/} sorts last and held ~8.1M of the bucket's
 * ~13.7M objects, but {@code contrib/}'s own descent kept discovering deeper narrow cuts for all 256
 * probes of the wallet — each new deepest cut inherited a real, large {@code spanScore} off the gap
 * to {@code projects/} (its only neighbor in the global cut set) — so {@code contrib/} never once
 * ran out of a higher-scoring candidate than {@code projects/}, which {@link SeedStep}'s frontier
 * scored at a fixed 0 (the "final cut, no successor" case). {@code projects/} was probed ZERO times
 * in the real run; its entire subtree survived seeding as one giant unsplit range.
 *
 * <p>The fixture reproduces the shape at a scale a unit test can afford: {@code contrib/} descends
 * into a deep-but-narrow chain (a top branch fanning into 10 shards, each fanning into 10
 * sub-shards, each a single-object leaf — 111 narrow probes' worth of structure, comfortably more
 * than the fixture's {@code maxProbes}), while {@code projects/} sorts last and holds its own real,
 * probeable structure. A frontier that starves the lexically-last sibling would spend the ENTIRE
 * wallet inside {@code contrib/} and never discover a single cut inside {@code projects/}.
 */
final class SeedDescentLastTopLevelSiblingNotStarvedByRegeneratingChainTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 16;   // targetSeeds = maxProbes = 4*16 = 64
    private static final int SHARDS = 10;
    private static final int SUB_SHARDS = 10;   // 1(deep/) + 10(sNN/) + 100(lNN/) = 111 narrow probes

    private static final byte[] PROJECTS_LO = "projects/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROJECTS_HI = prefixCeil(PROJECTS_LO);

    /**
     * {@code contrib/deep/s00/l00/obj} .. {@code s09/l09/obj}: a deep, narrow, single-object-leaf
     * chain — 111 narrow structure probes' worth, deliberately more than the fixture's {@code
     * maxProbes(64)} so a frontier that keeps preferring {@code contrib/}'s ever-deeper cuts over
     * {@code projects/} would exhaust the wallet entirely inside {@code contrib/}. {@code projects/}
     * sorts last (no top-level sibling after it) and fans into two real sub-directories of its own —
     * the shape the seed must still reach and probe despite {@code contrib/}'s bottomless appetite.
     */
    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>();
        for (int s = 0; s < SHARDS; s++) {
            for (int l = 0; l < SUB_SHARDS; l++) {
                keys.add(("contrib/deep/s%02d/l%02d/obj".formatted(s, l)).getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int h = 0; h < 5; h++) {
            keys.add(("projects/headers-testing/h%02d/obj".formatted(h)).getBytes(StandardCharsets.UTF_8));
        }
        for (int h = 0; h < 3; h++) {
            keys.add(("projects/hyperlinkgraph/h%02d/obj".formatted(h)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    void projectsGetsProbedAndInteriorCutsDespiteContribsBottomlessChain() throws Exception {
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

        // The discriminating property: projects/ got its OWN interior cut-points (headers-testing/
        // and hyperlinkgraph/ were discovered and tiled) instead of surviving as one giant unsplit
        // range because contrib/'s deep chain never stopped outscoring it.
        long interiorCuts = specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(start -> start != null
                        && Arrays.compareUnsigned(start, PROJECTS_LO) > 0
                        && Arrays.compareUnsigned(start, PROJECTS_HI) < 0)
                .count();
        assertThat(interiorCuts)
                .as("projects/ (lexically last, no successor) must still be probed and get interior "
                        + "cut-points — the frontier's degenerate 0-score for the last cut must not let "
                        + "contrib/'s ever-deepening chain drain the whole wallet first")
                .isPositive();

        // No single range should still own the entire projects/ subtree.
        List<NodeSpec> wholeProjectsRange = specs.stream()
                .filter(s -> Arrays.equals(s.rangeStart(), PROJECTS_LO) && s.rangeEnd() == null)
                .toList();
        assertThat(wholeProjectsRange)
                .as("projects/ must not collapse to a single [projects/, null] unsplit range")
                .isEmpty();
    }

    /** Increment the last byte of a prefix to form its exclusive ceiling (all prefix keys sort below it). */
    private static byte[] prefixCeil(byte[] prefix) {
        byte[] c = Arrays.copyOf(prefix, prefix.length);
        c[c.length - 1]++;
        return c;
    }
}
