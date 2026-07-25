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
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Adversarial guard for the mass-weighted dense-leaf seed planner</b>
 * ({@code SeedStep} radix banding): an adversarial guard of the planner's <em>contract</em>. Every assertion
 * is designed to genuinely FAIL if the classify
 * rule, the {@code appendSpread} radix banding, the root-gap fix, or the zero-probe budget
 * regresses.
 *
 * <p>The contract under guard:
 * <ol>
 *   <li><b>Flat dense ROOT is banded and parallel from t≈0</b> — a single truncated top level
 *       with NO {@code delimiter=/} common prefixes and high-entropy leading bytes (hex/uuid-ish)
 *       is pre-cut into leading-byte radix bands (multiple seed ranges), so the fleet reaches
 *       immediate width instead of draining one serial root range.</li>
 *   <li><b>Byte-exact partition on the banded root (I2/I3)</b> — the banded seed tiles
 *       {@code (⊥, ⊤]} with NO gap and NO overlap, and a full engine run emits exactly the input
 *       key set (byte-exact set equality) and terminates.</li>
 *   <li><b>Tiny-leaf explosion is NOT banded (INT-8 shape)</b> — a truncated level WITH
 *       {@code delimiter=/} common prefixes (a {@code pid=<hex>/} directory explosion) is LEFT
 *       WHOLE, never fanned out into ~94 synthesized ASCII bands; INT-8's API budget holds.</li>
 *   <li><b>Zero probe cost</b> — radix banding adds no fetches; the band count is decoupled from
 *       the probe count (one top-level structure probe, regardless of band count).</li>
 *   <li><b>No over-banding of a small flat bucket</b> — a tiny non-truncated flat top yields a
 *       single seed range, never 94 empty bands.</li>
 * </ol>
 *
 * <p>Deterministic throughout: {@code MockPageFetcher}, seeded RNG, no real S3, no wall-clock
 * assertions (page latency is used only to expose concurrency, never timed).
 */
final class DenseRootSeedGuardTest {

    private static final byte[] NO_PREFIX = new byte[0];
    /** The single-byte printable-ASCII radix span the impl caps band count at ([0x21, 0x7E] = 94). */
    private static final int RADIX_SPAN = 0x7E - 0x21 + 1;

    // ---- keyspace shapes -------------------------------------------------------

    /**
     * A flat DENSE root: {@code n} high-entropy 16-hex-digit keys with NO {@code /} at all, so a
     * {@code delimiter=/} probe of the root returns direct objects only (no common prefixes) and
     * truncates — exactly the classify signal for "heavy dense range". Mirrors a flat, un-seeded
     * root tail with no directory structure, the same pattern a UUID-keyed bucket produces (a
     * UUID has no {@code /} either).
     */
    private static List<byte[]> flatDenseHexRoot(int n, long seed) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("%016x", r.nextLong()).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * A tiny-leaf directory explosion under a shared top prefix: {@code data/<8hex>/part-0.parquet},
     * one object per leaf directory. The top level rolls up to a single {@code data/} common prefix;
     * descending it hits a truncated sub-level that STILL returns {@code <hex>/} common prefixes
     * (sub-directory structure) — the classify signal for "tiny-leaf explosion", LEFT WHOLE. The
     * leaf names are high-entropy so a mis-classify-and-band regression would visibly synthesize
     * ASCII bands here.
     */
    private static List<byte[]> tinyLeafExplosionUnderSharedTop(int dirs) {
        List<byte[]> keys = new ArrayList<>(dirs);
        for (int i = 0; i < dirs; i++) {
            int scrambled = Integer.reverse(i) ^ 0x5bd1e995;
            keys.add(String.format("data/%08x/part-00000.parquet", scrambled)
                    .getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    // ---- 1. flat dense ROOT is banded and parallelizes from t≈0 ----------------

    @Test
    @Timeout(120)
    void flatDenseRootIsBandedAndParallelizesFromStart(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = flatDenseHexRoot(6000, 101L);
        int workers = 16;

        // Seed-time: the dense root is pre-cut into many radix bands, bounded by the single-byte
        // radix span. A regression that leaves the flat dense root as one (⊥, null] range yields
        // exactly 1 seed — this fails.
        Seeded seeded = seedOnly(keyspace, workers);
        assertThat(seeded.specs.size())
                .as("flat dense root radix-banded into many seed ranges (not one serial range)")
                .isGreaterThan(8);
        assertThat(seeded.specs.size())
                .as("band count stays bounded by the radix span (~94), never a probe/seed storm")
                .isLessThanOrEqualTo(RADIX_SPAN + 1);

        // Engine-time: with per-page latency on worker listings, immediate width shows up as many
        // concurrent worker fetches from the very start (each seed band is independently claimable).
        // A single serial root range would ramp slowly and peak near 1–3 under the same latency.
        Run run = scan(dir, "band-width", keyspace, workers, 1000, Duration.ofMillis(4));
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        assertThat(run.peakInFlight)
                .as("banded root reaches immediate width (many ranges claimable at t≈0), peak=%d",
                        run.peakInFlight)
                .isGreaterThanOrEqualTo(workers / 2);
    }

    // ---- 2. byte-exact partition on the banded root (CRITICAL — I2/I3) ---------

    @Test
    @Timeout(120)
    void bandedRootTilesExactlyAndEmitsFullKeysetOnce(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = flatDenseHexRoot(6000, 202L);
        int workers = 16;

        // (a) The synthesized band cut-points tile (⊥, ⊤] with NO gap and NO overlap (I2/I3):
        // a band that gaps or overlaps fails RangePartition.assertTiles here.
        Seeded seeded = seedOnly(keyspace, workers);
        List<RangePartition.Interval> intervals = new ArrayList<>(seeded.specs.size());
        for (NodeSpec s : seeded.specs) {
            assertThat(s.cursor()).as("fresh band cursor == its lower bound").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // (b) A full run over the banded seed emits EXACTLY the input key set, byte-for-byte, each
        // key once, and terminates (the @Timeout). A gapping band drops keys; an overlapping band
        // double-emits — either breaks this set-equality across the whole banded partition.
        Run run = scan(dir, "band-exact", keyspace, workers, 1000, Duration.ZERO);
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
    }

    // ---- 3. tiny-leaf explosion is NOT banded (INT-8 shape preserved) ----------

    @Test
    @Timeout(120)
    void tinyLeafExplosionIsNotBandedAndStaysInt8(@TempDir Path dir) throws Exception {
        int dirs = 3000;                 // 3000 leaf directories, one object each
        List<byte[]> keyspace = tinyLeafExplosionUnderSharedTop(dirs);
        int workers = 16;
        int maxKeys = 1000;

        // Classify: a truncated level WITH common prefixes is LEFT WHOLE, never radix-banded.
        // The impl always synthesizes ≥ MIN_BANDS (8) cuts when it bands a region, so a mis-classify
        // would push the seed count to ≥ 9; the correct classify keeps it tiny (data/ cut + tail).
        Seeded seeded = seedOnly(keyspace, workers);
        assertThat(seeded.specs.size())
                .as("directory explosion left whole — NOT fanned into ≥8 synthesized ASCII bands")
                .isLessThanOrEqualTo(6);
        // Belt-and-braces: no seed cut is a synthesized single-scalar band inside the `data/` dir.
        byte[] top = "data/".getBytes(StandardCharsets.UTF_8);
        for (NodeSpec s : seeded.specs) {
            byte[] hi = s.rangeEnd();
            boolean syntheticBand = hi != null && hi.length == top.length + 1
                    && Arrays.equals(hi, 0, top.length, top, 0, top.length);
            assertThat(syntheticBand)
                    .as("no synthesized leading-scalar band cut inside a directory-explosion level")
                    .isFalse();
        }

        // INT-8: a full run stays flat-scan-bounded — API calls ≈ O(pages + workers), structure
        // probes O(pages), never O(directories). A per-directory recursion (or a mis-band storm)
        // blows both budgets.
        Run run = scan(dir, "explosion-int8", keyspace, workers, maxKeys, Duration.ZERO);
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);
        long pages = (long) Math.ceil((double) keyspace.size() / maxKeys);
        ApiCallBudget.assertWithinInt8Budget(run.apiCalls, pages, workers,
                "total LIST ≈ O(pages+workers), NOT ≈ O(directories=%d)".formatted(dirs));
        assertThat(run.structureProbes)
                .as("structure probes stay O(pages), not O(directories=%d)", dirs)
                .isLessThan(dirs)
                .isLessThanOrEqualTo((int) ApiCallBudget.int8Budget(pages, workers));
    }

    // ---- 4. radix banding adds ZERO probes -------------------------------------

    @Test
    void bandingAddsZeroProbes() throws Exception {
        List<byte[]> keyspace = flatDenseHexRoot(8000, 303L);

        // The band count grows with the worker budget, but the probe count must not: a flat dense
        // root is decided from the ONE top-level structure probe. Assert bands ≫ 1 while api == 1.
        Seeded seeded = seedOnly(keyspace, 64);
        assertThat(seeded.specs.size())
                .as("dense root produced many radix bands").isGreaterThan(8);
        assertThat(seeded.apiCalls)
                .as("radix banding is probe-free — exactly one top-level structure probe, "
                        + "band count decoupled from probe count")
                .isEqualTo(1L);
    }

    // ---- 5. small/flat non-truncated bucket is NOT over-banded ------------------

    @Test
    void smallFlatBucketIsNotOverBanded() throws Exception {
        // A tiny flat top (3 direct objects, NOT truncated, no `/`): must stay a single (⊥, null]
        // range, never 94 empty bands. Only a TRUNCATED flat top is a dense-tail risk worth banding;
        // a regression that bands every flat top would explode this into ~95 ranges.
        List<byte[]> keyspace = List.of(
                "alpha".getBytes(StandardCharsets.UTF_8),
                "bravo".getBytes(StandardCharsets.UTF_8),
                "charlie".getBytes(StandardCharsets.UTF_8));

        Seeded seeded = seedOnly(keyspace, 64);
        assertThat(seeded.specs).as("small non-truncated flat top stays a single seed range").hasSize(1);
        assertThat(seeded.specs.get(0).rangeStart()).isNull();
        assertThat(seeded.specs.get(0).rangeEnd()).isNull();
        assertThat(seeded.apiCalls).as("one structure probe, no banding fetches").isEqualTo(1L);
    }

    // ---- harness ---------------------------------------------------------------

    private record Seeded(List<NodeSpec> specs, long apiCalls) {
    }

    /** Just the seed step (no engine): the seed specs plus the probe count they cost. */
    private static Seeded seedOnly(List<byte[]> keyspace, int workers) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers).seedSpecs(1L, SeedMode.SHALLOW);
        return new Seeded(specs, fetcher.apiCalls());
    }

    private record Run(List<byte[]> emitted, int seedCount, long peakInFlight,
                       int structureProbes, long apiCalls) {
    }

    /** Seed via SHALLOW then drive the full {@link WorkStealingScan}, recording width + probe budget. */
    private static Run scan(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys,
                            Duration pageLatency) throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);

        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Latency only on worker listing pages (recursive, maxKeys>1). Seed/thief probes stay
                // instant, so this exposes concurrent worker fetches without confounding the seed.
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    // A demand-driven structure probe: delimiter listing WITH a cursor (start_after).
                    // The seed's structure probes have start_after == null, so they are excluded.
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    }
                    return page;
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, NO_PREFIX, workers).seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return new Run(emitted, seedCount, metrics.peakInFlight(), structureProbes.get(), mock.apiCalls());
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

}
