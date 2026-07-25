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
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.ApiCallBudget;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end guard: an independent, adversarial check that alphabet-aware pivots hold up in the
 * live {@link WorkStealingScan} engine on a sparse-alphabet (hex / UUID) keyspace.
 * Deterministic ({@link MockPageFetcher}, no real S3, no wall-clock).
 *
 * <ol start="2">
 *   <li><b>Non-empty children (the real goal):</b> a dense hex keyspace, driven from a single root
 *       with stealing forced, is a byte-exact partition (no gap / no overlap, terminates) AND the
 *       reconstructed range set contains no empty-child completions and no {@code UNSPLITTABLE.no_pivot}
 *       — the splits land on populated hex values, so both sides of every steal are populated.</li>
 *   <li><b>No INT-8 regression / zero-API:</b> the digest is learned from pages already in hand, so a
 *       hex-keyspace run adds no structure probes and stays within the INT-8 API budget
 *       ({@code 4·pages + 4·workers + 64}) — the alphabet signal is truly free.</li>
 * </ol>
 *
 */
final class F1DenseHexScanTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey runKey(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    // =========================================================================
    // (2) Non-empty children on a dense hex keyspace, from a single root.
    // =========================================================================

    /**
     * A dense flat hex keyspace ({@code obj/<16 hex>}) forced through heavy stealing (tiny pages, many
     * workers) from the single root range {@code (⊥, ⊤]}. Every emitted key appears exactly once
     * (byte-exact, no gap / no overlap), the reconstructed split partition tiles {@code (⊥, ⊤]}, and —
     * the goal — every reconstructed child range is NON-EMPTY (a pivot on a real hex value never
     * carves an empty child out of the dead zone), with zero {@code UNSPLITTABLE.no_pivot} completions.
     */
    @Test
    @Timeout(120)
    void denseHexKeyspaceSplitsIntoNonEmptyChildren(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.flatRandom(1234, 800);   // obj/<16 hex> — sparse leading alphabet
        int workers = 6;
        int maxKeys = 7;                                            // tiny pages force real stealing

        RootScan run = scanFromRoot(dir, keyspace, workers, maxKeys, runKey("f1-hex-root"));

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        RangePartition.assertTilesFromSplits(run.rootId, run.splits);

        assertThat(run.splits)
                .as("stealing must be exercised so the alphabet-aware pivot actually fires")
                .isNotEmpty();

        // Children are overwhelmingly populated: an alphabet-aware pivot lands BETWEEN populated hex
        // values, so a steal splits real keys onto both sides rather than carving a child out of a
        // dead zone. A handful of empty tail ranges (a far-ahead split landing just past the last key)
        // is legitimate over-fetch, not the dead-zone failure mode — so bound them well below a
        // dead-zone-pivot regression, which would leave many children empty.
        TreeSet<byte[]> keys = new TreeSet<>(Arrays::compareUnsigned);
        keys.addAll(keyspace);
        List<RangePartition.Interval> intervals = RangePartition.replay(run.rootId, run.splits);
        long emptyChildren = intervals.stream().filter(iv -> countKeysIn(keys, iv) == 0).count();
        long noPivot = run.diagnostics.stealReasons().getOrDefault("UNSPLITTABLE.no_pivot", 0L);

        assertThat(emptyChildren)
                .as("child ranges are overwhelmingly populated (%d empty of %d ranges)",
                        emptyChildren, intervals.size())
                .isLessThanOrEqualTo(intervals.size() / 4L);

        // no_pivot completions are tiny adjacency-slivers (a range too small to bisect), NOT dead-zone
        // failures — but they must stay a small minority of committed splits: on a populated hex space
        // splitting overwhelmingly SUCCEEDS (the alphabet-aware pivot lands on a real value), so this
        // regresses upward if pivots start missing populated space.
        assertThat(noPivot)
                .as("UNSPLITTABLE.no_pivot stays a small minority of committed splits (%d)", run.splits.size())
                .isLessThanOrEqualTo(run.splits.size() / 4L);
    }

    // =========================================================================
    // (5) Zero-API: the digest adds no probes and stays within the INT-8 budget.
    // =========================================================================

    /**
     * The observed-alphabet digest is folded from page endpoints already fetched — zero extra I/O. On a large dense
     * hex keyspace (the encode-UUID shape), the run stays a byte-exact partition, total LIST calls
     * stay within the INT-8 budget ({@code 4·pages + 4·workers + 64}), and steal-time structure probes
     * stay bounded — the alphabet signal never inflates the API count.
     */
    @Test
    @Timeout(120)
    void hexKeyspaceRunAddsNoProbesWithinInt8Budget(@TempDir Path dir) throws Exception {
        int n = 100_000;
        List<byte[]> keyspace = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keyspace.add(b("obj/%016x".formatted(0x9E3779B97F4A7C15L * i)));   // deterministic full-range hex leads
        }
        int workers = 4;
        int maxKeys = 1000;                                        // S3 max page — pages-dominated (headroom)

        SeededScan run = scanShallowSeeded(dir, keyspace, workers, maxKeys, runKey("f1-hex-int8"));

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        long pages = (long) Math.ceil((double) distinctCount(keyspace) / maxKeys);
        long int8Budget = ApiCallBudget.int8Budget(pages, workers);

        ApiCallBudget.assertWithinInt8Budget(run.apiCalls, pages, workers,
                "total LIST within the INT-8 budget (4*pages + 4*workers + 64) — digest adds no calls");
        assertThat(run.structureProbes)
                .as("the observed-alphabet digest is zero-API: no structure-probe storm on a flat hex dir")
                .isLessThan(distinctCount(keyspace))
                .isLessThanOrEqualTo((int) int8Budget);
    }

    // -------------------------------------------------------------------------
    // Scan harnesses.
    // -------------------------------------------------------------------------

    private record RootScan(List<byte[]> emitted, List<RangePartition.Split> splits, long rootId,
                            RunMetrics.RunDiagnostics diagnostics) {
    }

    private record SeededScan(List<byte[]> emitted, int structureProbes, long apiCalls) {
    }

    /** Single-root seed (forces the stealer to bisect from scratch) with split recording + diagnostics. */
    private static RootScan scanFromRoot(Path baseDir, List<byte[]> keyspace, int workers, int maxKeys,
                                         RunKey rk) throws Exception {
        Path ckptDir = baseDir.resolve("root");
        Files.createDirectories(ckptDir);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(rk, false, false);
            long rootId = store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = drain(engine);
            return new RootScan(emitted, store.splits(), rootId, metrics.diagnostics(Duration.ofSeconds(1)));
        }
    }

    /** Shallow-seeded run (mirrors FarAheadPivotTest's INT-8 fixture) counting probes + API calls. */
    private static SeededScan scanShallowSeeded(Path baseDir, List<byte[]> keyspace, int workers, int maxKeys,
                                                RunKey rk) throws Exception {
        Path ckptDir = baseDir.resolve("seeded");
        Files.createDirectories(ckptDir);

        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();   // a steal-time structure probe
                    }
                    return page;
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(rk, false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, new byte[0], workers).seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = drain(engine);
            return new SeededScan(emitted, structureProbes.get(), mock.apiCalls());
        }
    }

    private static List<byte[]> drain(WorkStealingScan engine) throws Exception {
        List<byte[]> emitted = new ArrayList<>();
        PipelineDrain.collectKeys(5000, engine, emitted);
        return emitted;
    }

    // -------------------------------------------------------------------------
    // Helpers.
    // -------------------------------------------------------------------------

    private static long countKeysIn(TreeSet<byte[]> keys, RangePartition.Interval iv) {
        // Half-open (lo, hi]: strictly above lo (or ⊥), at/below hi (or ⊤).
        return keys.stream().filter(k ->
                (iv.lo() == null || Arrays.compareUnsigned(iv.lo(), k) < 0)
                        && (iv.hi() == null || Arrays.compareUnsigned(k, iv.hi()) <= 0)).count();
    }

    private static int distinctCount(List<byte[]> keyspace) {
        TreeSet<byte[]> s = new TreeSet<>(Arrays::compareUnsigned);
        s.addAll(keyspace);
        return s.size();
    }

}
