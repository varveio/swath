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
import org.junit.jupiter.api.Timeout;

/**
 * <b>Guard: a heavy top-level subtree must not be stranded by a deepening narrow chain.</b>
 *
 * <p>The mass-aware descent frontier ranked candidates by {@code spanScore} alone — the keyspace gap
 * from a cut to its next sibling. That is a proxy for mass, and on a name-keyed keyspace it is not
 * merely weak but effectively <b>uncorrelated</b>: measured over {@code s3://wis2globalcache/}'s 111
 * top-level cuts against their true object counts, Spearman {@code rho = -0.153}.
 *
 * <p>The mechanism is that {@code spanScore} rewards a cut whose next sibling diverges EARLY in the
 * name, so a heavy cut is <b>penalised for having a similarly-named neighbour</b>:
 * {@code data/de-dwd-gts-to-wis2/} (1,379,098 objects) sits next to
 * {@code data/de-dwd-global-discovery-catalogue/} (ONE object), shares the prefix {@code de-dwd-g}
 * with it, and so scored the floor (258) and ranked 107th of 111. {@code jp-jma-gts-to-wis2/}
 * (889,294) ranked 108th identically. The descent's probe budget reached 69 of 111 centres, so
 * 61.8% of that bucket's objects were never descended into and stayed undivided — one worker
 * draining a serial tail across 41.7% of the run's wall clock.
 *
 * <p>The fix makes depth the primary key: no cut at depth {@code D+1} is polled while an unprobed
 * cut at depth {@code D} remains. Span survives as the within-level tiebreak, so this is a
 * refinement of the previous order rather than a replacement — and the pure-span order is recovered
 * exactly whenever the frontier is uniform-depth.
 *
 * <p>The fixture makes the naming penalty explicit rather than incidental: {@code zz-heavy-a/} holds
 * essentially all of the keyspace's mass but is followed by the near-empty {@code zz-heavy-b/}, so
 * the two share {@code zz-heavy-} and the heavy cut scores near the floor while eight
 * long-distinct-named light siblings score an order of magnitude higher and offer deep chains for
 * the budget to disappear into.
 */
final class SeedFrontierLevelOrderTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /**
     * {@code workerCount = 8} pins {@code targetSeeds = 4·W = 32} and therefore
     * {@code maxProbes = 32}, of which the descent may spend {@code maxProbes - min(SAMPLE_BUDGET,
     * maxProbes/2) = 16}. A budget that small is the point: the defect only bites when the descent
     * cannot reach every candidate, which on a real bucket is the 256-probe ceiling against 111+
     * centres and here is 16 against ten top-level cuts plus their chains.
     */
    private static final int WORKERS = 8;

    private static final String HEAVY = "zz-heavy-a/";
    private static final String LEVEL_ORDERED = "frontier_level_ordered";

    /**
     * Eight light top-level dirs with long, first-byte-distinct names (so each scores a high span)
     * each carrying a deep narrow chain for the probe budget to sink into, then the heavy dir
     * {@code zz-heavy-a/} — {@code children} sub-dirs of {@code perChild} objects, the real mass —
     * immediately followed by the near-empty {@code zz-heavy-b/} that drags its span to the floor.
     */
    private static List<byte[]> heavyCutShadowedByNamesake(int children, int perChild, int chainDepth) {
        List<byte[]> keys = new ArrayList<>();
        for (char c = 'b'; c <= 'i'; c++) {
            StringBuilder path = new StringBuilder(String.valueOf(c).repeat(8)).append('/');
            for (int d = 0; d < chainDepth; d++) {
                path.append(String.valueOf((char) ('m' + d)).repeat(8)).append('/');
                keys.add((path + "leaf.obj").getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int i = 0; i < children; i++) {
            for (int o = 0; o < perChild; o++) {
                keys.add((HEAVY + "ed%05d/img%03d".formatted(i, o)).getBytes(StandardCharsets.UTF_8));
            }
        }
        keys.add("zz-heavy-b/one.obj".getBytes(StandardCharsets.UTF_8));
        return keys;
    }

    /** Seed boundaries strictly inside {@code zz-heavy-a/} — i.e. the heavy dir was carved up. */
    private static long interiorCuts(List<NodeSpec> specs) {
        byte[] lo = HEAVY.getBytes(StandardCharsets.UTF_8);
        byte[] hi = "zz-heavy-b".getBytes(StandardCharsets.UTF_8);
        return specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(s -> s != null
                        && Arrays.compareUnsigned(s, lo) > 0
                        && Arrays.compareUnsigned(s, hi) < 0)
                .count();
    }

    @Test
    @Timeout(120)
    void heavyLowSpanCutIsReachedBeforeDeeperNarrowChains() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<NodeSpec> specs = SeedSteps.of(
                        MockPageFetcher.builder().keys(heavyCutShadowedByNamesake(1200, 8, 5)).build(),
                        NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();

        // The behavioural claim, asserted before the counter (which is tied to the new code path):
        // the heavy dir holds ~all the mass and must be carved up, not left as one serial range,
        // even though its span score is near the floor and eight higher-scoring siblings offer
        // deeper chains to spend the budget on.
        assertThat(interiorCuts(specs))
                .as("zz-heavy-a/ holds ~all the keyspace mass and must be descended into and carved "
                        + "up; under span-only ordering its namesake neighbour drags it below the "
                        + "deepening narrow chains and the probe budget never reaches it")
                .isPositive();

        assertThat(reasons.getOrDefault("SEED." + LEVEL_ORDERED, 0L))
                .as("SEED.%s must fire — the frontier held cuts at more than one depth", LEVEL_ORDERED)
                .isPositive();

        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);   // I2/I3 still exact
    }
}
