/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A truncated TOP-LEVEL {@code delimiter=/} probe whose common prefixes are {@code key=value/}
 * partition names (e.g. a bucket root that is ITSELF a 1:1 {@code pid=<hex>/part-00000.parquet}
 * fan-out, no wrapping top prefix, so the very first probe truncates) never reaches {@link
 * SeedStep#isPartitionFanout}/{@link SeedStep#tilePartitionFanout} — that discriminator and its
 * {@code W}-cap fire ONLY inside {@code collectCutPoints}'s DESCENT loop, for a truncated SUB-level
 * reached after a non-truncated top.
 *
 * <p>This is intended behavior, not a bug to fix: {@code addCutsCounted(top, cuts, expandable,
 * topScopeCeiling)} runs UNCONDITIONALLY, before the truncation check — there is no {@code break}
 * at the top level.
 * So the top level's own (up to 1000) common prefixes are captured into {@code cuts} regardless
 * of truncation, and {@code shallow()}'s {@code subsampleEvenly(collected.cuts, targetSeeds)} —
 * {@code targetSeeds == min(1000, 4*W)}, the GENERIC top-level cap, not the {@code W}-cap {@code
 * tilePartitionFanout} uses — tiles them. A root-level truncated fan-out (key=value-named or not)
 * tiles MORE generously (up to {@code 4W} ranges, not {@code W}) via this generic cap. This class
 * pins that this behavior stays bounded (never per-directory, never unbounded) so a future change
 * can't quietly regress it, and documents the classification it actually gets ({@code
 * tiny_leaf_explosion}, with {@code cuts_kept > 0} — unlike the descent-level {@code
 * tiny_leaf_explosion}, which always has {@code cuts_kept == 0} since that region is truly left
 * whole).
 */
final class SeedStepRootFanoutBudgetTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /**
     * A ROOT-level {@code key=value/} 1:1 explosion: NO wrapping top prefix, so the very FIRST
     * {@code delimiter=/} probe (of the bucket root itself) already truncates with {@code pid=<hex>/}
     * common prefixes.
     */
    private static List<byte[]> rootLevelKeyValueExplosion(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("pid=%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    void rootLevelTruncatedKeyValueFanoutIsBoundedAndClassifiedGenerically() throws Exception {
        int n = 6000;      // > the 1000-key probe-page cap: the TOP-level probe itself truncates
        int workers = 64;
        List<byte[]> keyspace = rootLevelKeyValueExplosion(n);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics)
                .seedSpecs(1L, SeedMode.SHALLOW);

        // Bounded by the GENERIC top-level cap (targetSeeds = min(1000, 4*workers) = 256, here),
        // NOT the W-cap tilePartitionFanout uses for a descended sub-level: this bypass lands on a
        // MORE generous, still-bounded cap — never an unbounded per-directory fan-out (n=6000 dirs).
        int targetSeeds = Math.min(1000, 4 * workers);   // mirrors SeedStep's own private field
        assertThat(specs.size())
                .as("root-level truncated key=value fan-out tiles, bounded by the generic 4W cap")
                .isEqualTo(targetSeeds + 1);

        // Classified via the SAME top-level "tiny_leaf_explosion" label collectCutPoints has always
        // used for ANY truncated-with-CommonPrefixes TOP level (key=value-named or not) — the
        // descent-only "fanout_tiled" classification never applies to the top-level probe itself.
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED.tiny_leaf_explosion", 0L))
                .as("root-level truncated fan-out keeps the generic tiny_leaf_explosion classification")
                .isPositive();
        assertThat(reasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("fanout_tiled is a DESCENT-only classification; never fires for the top-level probe")
                .isZero();

        // Still an exact I2/I3 tiling.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /**
     * INT-8-style budget guard: a full engine run over the root-level truncated key=value fan-out
     * stays within the same LIST-call arithmetic {@code
     * SeedStepTest.int8ExplodedTreeStaysFlatScanNotPerDirectory} already pins for the NESTED case
     * (a narrow top wrapping a truncated {@code pid=} sub-level) — total LIST calls scale with
     * {@code ceil(N/1000) + O(seed_count)}, never {@code O(n)} (one LIST per {@code pid=} directory).
     */
    @Test
    @Timeout(120)
    void rootLevelTruncatedKeyValueFanoutStaysInt8Budget(@TempDir Path dir) throws Exception {
        int n = 6000;
        int workers = 16;
        int maxKeys = 1000;
        List<byte[]> keyspace = rootLevelKeyValueExplosion(n);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<byte[]> emitted = new ArrayList<>();
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers).seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }

        EngineHarness.assertExactlyOnce(emitted, keyspace);
        long ceil = (n + 999) / 1000;
        // Same budget shape SeedStepTest's INT-8 guard already asserts for the nested case: a small
        // multiple of ceil(N/1000) plus the seed's own bounded overhead, nowhere near O(n).
        assertThat(fetcher.apiCalls())
                .as("root-level truncated fan-out LIST budget ~ ceil(N/1000)+O(seed_count), NOT ~N")
                .isLessThan((long) n / 10)
                .isLessThan(ceil + 4L * seedCount + 64);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "root-fanout-budget-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

}
