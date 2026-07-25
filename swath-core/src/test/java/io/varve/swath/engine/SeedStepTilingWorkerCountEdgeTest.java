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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression guard: {@link SeedStep#tilePartitionFanout} calls the private {@code
 * subsampleEvenly} helper with {@code max == workerCount} (not {@code targetSeeds}, which is
 * floored at {@code 4 * max(1, workerCount) >= 4}). {@code workerCount} itself is only floored at
 * {@code 1} ({@code --max-parallel-listings 1} is a valid, if degenerate, configuration), so
 * {@code max == 1} is a live, reachable input. Do not let {@code subsampleEvenly}'s
 * evenly-spaced-index formula ({@code idx = i * (n-1) / (max-1)}) divide by zero when {@code max
 * == 1} and more than one cut-point is available to subsample from — an unhandled {@code
 * ArithmeticException} there would abort the entire seed step (not just the tiling) on any
 * Hive-shaped {@code key=value/} partition bucket run at {@code W=1}.
 */
final class SeedStepTilingWorkerCountEdgeTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /** A Hive {@code table/date=<n>/obj} partition fan-out — more than 1000 partitions to force
     * the descended {@code date=/} level's first page to truncate (the shape {@code
     * tilePartitionFanout} tiles). */
    private static List<byte[]> hivePartitioned(int partitions) {
        List<byte[]> keys = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            keys.add(("table/date=%05d/part-00000.parquet".formatted(p))
                    .getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    void w1HiveFanoutTilesWithoutDivideByZero() throws Exception {
        List<byte[]> keyspace = hivePartitioned(1100);   // > the 1000-key probe-page cap
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        // subsampleEvenly(observed, workerCount=1) divides by (max-1)==0 when n>1 and max==1 —
        // this call must not throw ArithmeticException.
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 1, metrics).seedSpecs(1L, SeedMode.SHALLOW);

        // A sane, bounded range count: W=1 tiling degenerates to a single representative cut-point
        // (never zero ranges, never an unbounded per-partition fan-out).
        assertThat(specs.size())
                .as("W=1 Hive fan-out tiles to a small, sane range count")
                .isGreaterThan(0)
                .isLessThanOrEqualTo(8);

        // Still an exact I2/I3 tiling despite the degenerate single-cut subsample.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // The fanout_tiled path genuinely engaged (not silently skipped) even at W=1.
        assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("SEED.fanout_tiled", 0L))
                .as("fanout_tiled engagement counter fired at W=1").isPositive();
    }
}
