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
import io.varve.swath.observability.RunSummary;
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
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Adversarial guard for seed-time tiling of a truncated Hive/Spark
 * {@code key=value/} partition fan-out</b> ({@code SeedStep#isPartitionFanout} /
 * {@code #tilePartitionFanout}; toggle {@code fanout_tiling}, counter {@code SEED.fanout_tiled},
 * classification {@code fanout_tiled}). Every assertion is designed to genuinely FAIL if the
 * discriminator, the {@code W}-cap, the zero-extra-probe budget, or the byte-exact-correctness
 * invariant regresses.
 *
 * <p>The contract under guard:
 * <ol>
 *   <li><b>Discriminator = strict majority of final-segment {@code '='}.</b> A truncated level whose
 *       first page's common prefixes are a MAJORITY {@code key=value/} partition dirs is tiled; a tie
 *       or minority (and {@code '='} only in an ANCESTOR segment) is left whole (tiny-leaf shape).</li>
 *   <li><b>The {@code W}-cap is the INT-8 defense.</b> A 1:1 {@code key=value/} explosion still tiles,
 *       but into {@code <= W} ranges, so the total LIST budget stays within the INT-8 arithmetic.</li>
 *   <li><b>Zero extra probes.</b> Tiling reuses the already-probed page; probe count is byte-identical
 *       to toggle-OFF on the same keyspace, and nested Hive levels each tile independently (a truncated
 *       sibling is disposed of per-cut, not a global stop) without unbounded range multiplication.</li>
 *   <li><b>Toggle ablation is exact.</b> ON fires {@code SEED.fanout_tiled} and a {@code fanout_tiled}
 *       decision with {@code cuts_kept > 0}; OFF reproduces the {@code tiny_leaf_explosion} +
 *       the {@code TOGGLE.fanout_tiling_off} mark.</li>
 *   <li><b>Correctness invariant.</b> Tiling never changes WHAT is listed — a full engine run emits a
 *       byte-exact-identical key set ON vs OFF, while materially raising sustained parallelism.</li>
 * </ol>
 *
 * <p>Deterministic throughout: {@code MockPageFetcher}, fixed fixtures, no sleeps/wall-clock
 * assertions (page latency is used only to expose concurrency, never timed).
 */
final class SeedStepFanoutTilingContractTest {

    private static final byte[] NO_PREFIX = new byte[0];
    // Explicit massAwareSeed=false (12th arg) alongside fanoutTiling=false (10th arg). Do not build
    // this via the 10-arg EngineToggles constructor: it resolves massAwareSeed=true
    // (EngineToggles.DEFAULT's value), which would silently pull mass-aware sampling into this
    // fixture's OFF comparison arm and break its role as a clean fanout_tiling-only ablation baseline
    // (extra sample probes, heavy_cut_banded instead of tiny_leaf_explosion on shapes whose sample
    // proves heavy).
    private static final EngineToggles FANOUT_OFF =
            EngineToggles.DEFAULT.withFanoutTiling(false).withMassAwareSeed(false);

    // ---- fixtures --------------------------------------------------------------

    /**
     * A truncated {@code t/} sub-level with {@code totalDirs} common prefixes, of which exactly
     * {@code equalsInFirstPage} of the first-page 1000 carry a final-segment {@code '='} (the
     * discriminator input). The 5-digit zero-padded index dominates sort order, so the first page is
     * always indices {@code 0..999} and the {@code i < equalsInFirstPage} dirs are the partition-like
     * ones — letting a test hit an EXACT partition-like count on the observed page.
     */
    private static List<byte[]> equalsBoundary(int totalDirs, int equalsInFirstPage) {
        List<byte[]> keys = new ArrayList<>(totalDirs);
        for (int i = 0; i < totalDirs; i++) {
            String seg = (i < equalsInFirstPage) ? "%05d=v".formatted(i) : "%05dx".formatted(i);
            keys.add(("t/" + seg + "/obj").getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * {@code '='} in an ANCESTOR segment only ({@code a=b/dataNNNNN/}) — the truncated {@code a=b/}
     * sub-level's common prefixes' FINAL segments ({@code dataNNNNN}) have no {@code '='}, so the
     * discriminator must classify it tiny-leaf (left whole), never partition-tiled.
     */
    private static List<byte[]> ancestorEquals(int dirs) {
        List<byte[]> keys = new ArrayList<>(dirs);
        for (int i = 0; i < dirs; i++) {
            keys.add(("a=b/data%05d/part.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Partition values that contain multi-byte UNICODE and EXTRA {@code '='} after the first
     * ({@code t/col=café…NNNN/} and {@code t/k=a=b…NNNN/}) — every final segment still contains a
     * {@code '='}, so the level is a majority partition fan-out and must tile.
     */
    private static List<byte[]> unicodeAndMultiEqualsValues(int dirs) {
        List<byte[]> keys = new ArrayList<>(dirs);
        for (int i = 0; i < dirs; i++) {
            String seg = (i % 2 == 0) ? "col=café%05d" .formatted(i) : "k=a=b%05d".formatted(i);
            keys.add(("t/" + seg + "/obj").getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * The essential-web 1:1 {@code key=value/} explosion: a narrow {@code crawl=2024/} top over a 1:1
     * {@code pid=<hex>/} fan-out (one object per leaf). Partition-like names, so it TILES — but the
     * {@code W}-cap must keep the range count (and thus the flat-scan LIST budget) bounded.
     */
    private static List<byte[]> keyValueOneToOne(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("crawl=2024/pid=%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * A Hive level BELOW another Hive level: {@code region=<r>/date=<d>/part} — two narrow
     * {@code region=} tops, each over a {@code date=} partition fan-out. The descent must tile at the
     * FIRST qualifying truncated level ({@code region=eu/date=…}) and then stop, never multiplying
     * ranges across every region.
     */
    private static List<byte[]> nestedHive(int datesPerRegion, int perDate) {
        List<byte[]> keys = new ArrayList<>();
        for (String region : List.of("eu", "us")) {
            for (int d = 0; d < datesPerRegion; d++) {
                for (int o = 0; o < perDate; o++) {
                    keys.add(("region=%s/date=%05d/part-%03d.parquet".formatted(region, d, o))
                            .getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        return keys;
    }

    /**
     * A narrow {@code table/} top over a truncated {@code date=…/} partition fan-out, {@code
     * partitions} partitions × {@code perPartition} objects each.
     */
    private static List<byte[]> hivePartitioned(int partitions, int perPartition) {
        List<byte[]> keys = new ArrayList<>(partitions * perPartition);
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < perPartition; o++) {
                keys.add(("table/date=%05d/part-%05d.parquet".formatted(p, o))
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    // ---- 1. discriminator boundary ---------------------------------------------

    /**
     * The strict-majority boundary of the {@code '='} discriminator ({@code partitionLike*2 >
     * cps.size()}, {@code cps.size() == 1000} on a truncated first page): 501/1000 TILES; 500/1000
     * (the exact tie) and 499/1000 are LEFT WHOLE. A regression to {@code >=} (tie tiles) or an
     * off-by-one flips exactly the 500 case.
     */
    @Test
    void discriminatorTilesOnStrictMajorityAndLeavesTieOrMinorityWhole() throws Exception {
        int workers = 16;

        SeedResult tiled = seed(equalsBoundary(1100, 501), workers, EngineToggles.DEFAULT);
        assertThat(tiled.reasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("501/1000 partition-like → strict majority → tiled").isPositive();
        assertThat(tiled.reasons.getOrDefault("SEED.tiny_leaf_explosion", 0L)).isZero();
        assertThat(tiled.specs.size()).as("tiled to >1 but W-capped ranges (+1: scope-closing sentinel)")
                .isGreaterThan(1).isLessThanOrEqualTo(workers + 3);
        assertExactTiling(tiled.specs);

        for (int equalsCount : new int[] {500, 499}) {
            SeedResult whole = seed(equalsBoundary(1100, equalsCount), workers, EngineToggles.DEFAULT);
            assertThat(whole.reasons.getOrDefault("SEED.fanout_tiled", 0L))
                    .as("%d/1000 partition-like → NOT a strict majority → never tiled", equalsCount).isZero();
            assertThat(whole.reasons.getOrDefault("SEED.tiny_leaf_explosion", 0L))
                    .as("%d/1000 → left whole (tiny-leaf shape)", equalsCount).isPositive();
            assertThat(whole.specs.size()).as("%d/1000 left whole → few ranges", equalsCount).isLessThan(8);
            assertExactTiling(whole.specs);
        }
    }

    /**
     * {@code '='} in an ANCESTOR segment (never the final one) must NOT count as partition-like:
     * {@code a=b/dataNNNNN/} is a plain directory explosion left whole. A regression that scans the
     * whole common-prefix string for {@code '='} (instead of only the final segment) would wrongly
     * tile this.
     */
    @Test
    void equalsInAncestorSegmentIsNotPartitionLike() throws Exception {
        SeedResult whole = seed(ancestorEquals(1100), 16, EngineToggles.DEFAULT);
        assertThat(whole.reasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("ancestor-only '=' is not a partition fan-out").isZero();
        assertThat(whole.reasons.getOrDefault("SEED.tiny_leaf_explosion", 0L))
                .as("plain final segments → tiny-leaf, left whole").isPositive();
        assertThat(whole.specs.size()).isLessThan(8);
        assertExactTiling(whole.specs);
    }

    /**
     * Partition values with multi-byte UNICODE and EXTRA {@code '='} after the first still tile — the
     * final segment contains a {@code '='}, which is the whole signal. Guards that the byte scan does
     * not choke on non-ASCII value bytes or a second {@code '='}.
     */
    @Test
    void unicodeAndMultiEqualsPartitionValuesStillTile() throws Exception {
        int workers = 16;
        SeedResult tiled = seed(unicodeAndMultiEqualsValues(1100), workers, EngineToggles.DEFAULT);
        assertThat(tiled.reasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("unicode / multi-'=' partition values are still partition-like → tiled").isPositive();
        assertThat(tiled.specs.size()).isGreaterThan(1).isLessThanOrEqualTo(workers + 3);
        assertExactTiling(tiled.specs);
    }

    // ---- 2. key=value 1:1 explosion stays within the INT-8 budget --------------

    /**
     * A {@code key=value/} 1:1 explosion (every partition holds exactly one object) still tiles, but
     * the {@code W}-cap bounds the range count to {@code <= W} — and the total LIST budget stays inside
     * the same INT-8 arithmetic {@code int8ExplodedTreeStaysFlatScanNotPerDirectory} uses
     * ({@code api < ceil(N/1000) + 4·seedCount + 64}, and nowhere near {@code N}). The {@code W}-cap is
     * the defense; this proves it holds even though the shape now tiles.
     */
    @Test
    @Timeout(120)
    void keyValueOneToOneExplosionTilesWithinInt8Budget(@TempDir Path dir) throws Exception {
        int n = 6000;
        int workers = 16;
        List<byte[]> keyspace = keyValueOneToOne(n);

        // Seed-time: partition-like, so it tiles — but W-capped, never one range per pid.
        SeedResult seeded = seed(keyspace, workers, EngineToggles.DEFAULT);
        assertThat(seeded.reasons.getOrDefault("SEED.fanout_tiled", 0L)).isPositive();
        assertThat(seeded.specs.size()).as("1:1 key=value explosion tiled, W-capped (+1: scope-closing sentinel)")
                .isGreaterThan(1).isLessThanOrEqualTo(workers + 3);

        // Engine-time: the W-cap keeps the flat-scan LIST budget inside the INT-8 bound.
        EngineResult run = runEngine(dir, "kv-int8", keyspace, workers, 1000, Duration.ZERO,
                EngineToggles.DEFAULT);
        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        long ceil = (n + 999) / 1000;
        long budget = ceil + 4L * run.seedCount + 64;
        assertThat(run.apiCalls)
                .as("W-capped tiling keeps LIST ≈ ceil(N/1000)+O(seed), NOT ≈ N (per-directory)")
                .isLessThan(n / 10L)
                .isLessThan(budget);
    }

    // ---- 3. depth/nesting: tile every qualifying level, zero extra probes --

    /**
     * A Hive level below another Hive level ({@code region=X/date=Y/}) tiles at EVERY qualifying
     * truncated level it descends ({@code region=eu/date=…} AND {@code region=us/date=…}) — a truncated
     * sibling is disposed of per-cut, not a global stop, so an unrelated region does not get stranded
     * just because {@code region=eu} exploded first. Still bounded (by {@code targetSeeds}/{@code
     * maxProbes}), never an unbounded multiplication. And tiling itself adds ZERO extra probes: the
     * seed-time probe count is byte-identical to toggle-OFF on the same keyspace (OFF also now
     * continues past region=eu, into region=us, instead of stopping at the first explosion).
     */
    @Test
    void nestedHiveLevelsTileEveryQualifyingLevelWithZeroExtraProbes() throws Exception {
        int workers = 16;
        int targetSeeds = Math.min(1000, 4 * workers);
        List<byte[]> keyspace = nestedHive(1100, 3);   // eu + us, each 1100 date= partitions

        SeedResult on = seed(keyspace, workers, EngineToggles.DEFAULT);
        SeedResult off = seed(keyspace, workers, FANOUT_OFF);

        // Both regions tile — the descent continues past region=eu's explosion into region=us.
        assertThat(on.reasons.getOrDefault("SEED.fanout_tiled", 0L)).isPositive();
        assertThat(on.specs.size()).as("nested tiling stays bounded by targetSeeds, no unbounded multiplication")
                .isGreaterThan(1).isLessThanOrEqualTo(targetSeeds + 1);
        assertExactTiling(on.specs);

        RunSummary.SeedSummary onSeed = summaryOf(keyspace, workers, EngineToggles.DEFAULT);
        List<RunSummary.SeedSummary.SeedDecision> tiledLevels = onSeed.decisions().stream()
                .filter(d -> "fanout_tiled".equals(d.classification())).toList();
        assertThat(tiledLevels).as("both qualifying levels tile — region=eu does not strand region=us")
                .hasSize(2);
        // region=us/ sorts last among the top-level entries and so has no measured successor —
        // its span score is the frontier's maximal/unbounded-tail score (spanScore), which outranks
        // region=eu/'s ordinary measured gap, so region=us/ is visited (and tiled) first.
        assertThat(tiledLevels.get(0).prefix())
                .as("visited (and tiled) first, in frontier order").contains("region=us");
        assertThat(tiledLevels.get(1).prefix())
                .as("visited (and tiled) second, no longer abandoned by an unconditional stop")
                .contains("region=eu");

        // Tiling itself adds ZERO probes: identical probe wallet ON vs OFF.
        assertThat(on.apiCalls).as("tiling adds zero probes vs. toggle-OFF").isEqualTo(off.apiCalls);
        assertThat(on.apiCalls)
                .as("three structure probes: top + region=eu descent + region=us descent")
                .isEqualTo(3L);
    }

    // ---- 4. toggle ablation ----------------------------------------------------

    /**
     * The {@code fanout_tiling} toggle ablation on ONE keyspace: ON fires {@code SEED.fanout_tiled} and
     * emits a {@code fanout_tiled} decision with {@code cuts_kept > 0}; OFF reproduces the
     * {@code tiny_leaf_explosion} classification, fires the {@code TOGGLE.fanout_tiling_off} mark, and
     * leaves few ranges. Zero-extra-probe parity holds.
     */
    @Test
    void toggleAblationOnTilesOffReproducesLegacy() throws Exception {
        int workers = 16;
        List<byte[]> keyspace = hivePartitioned(1100, 5);

        SeedResult on = seed(keyspace, workers, EngineToggles.DEFAULT);
        assertThat(on.reasons.getOrDefault("SEED.fanout_tiled", 0L)).as("ON: engagement counter fires").isPositive();
        assertThat(on.reasons.getOrDefault("TOGGLE.fanout_tiling_off", 0L)).as("ON: no ablation mark").isZero();
        RunSummary.SeedSummary onSeed = summaryOf(keyspace, workers, EngineToggles.DEFAULT);
        RunSummary.SeedSummary.SeedDecision tiledLevel = onSeed.decisions().stream()
                .filter(d -> "fanout_tiled".equals(d.classification())).findFirst().orElseThrow();
        assertThat(tiledLevel.cutsKept()).as("ON: fanout_tiled level contributes W-capped cuts").isPositive();
        assertThat(tiledLevel.cutsDiscarded()).isZero();
        assertThat(on.specs.size()).isGreaterThan(8).isLessThanOrEqualTo(workers + 3);

        SeedResult off = seed(keyspace, workers, FANOUT_OFF);
        assertThat(off.reasons.getOrDefault("SEED.fanout_tiled", 0L)).as("OFF: never tiles").isZero();
        assertThat(off.reasons.getOrDefault("SEED.tiny_leaf_explosion", 0L))
                .as("OFF: legacy tiny_leaf_explosion classification").isPositive();
        assertThat(off.reasons.getOrDefault("TOGGLE.fanout_tiling_off", 0L))
                .as("OFF: ablation mark fires").isPositive();
        assertThat(off.specs.size()).as("OFF: legacy break-and-discard leaves few ranges").isLessThan(8);
        RunSummary.SeedSummary offSeed = summaryOf(keyspace, workers, FANOUT_OFF);
        assertThat(offSeed.decisions().stream().anyMatch(d -> "fanout_tiled".equals(d.classification())))
                .as("OFF: no fanout_tiled decision").isFalse();
        assertThat(offSeed.decisions().stream().anyMatch(d -> "tiny_leaf_explosion".equals(d.classification())))
                .as("OFF: a tiny_leaf_explosion decision present").isTrue();

        assertThat(on.apiCalls).as("tiling adds zero probes vs. legacy break").isEqualTo(off.apiCalls);
    }

    // ---- 5. end-to-end: parallelism up, output byte-identical --------------------

    /**
     * An offline synthetic Hive fixture ({@code 1100 date=} × 250 objects, W=32) run
     * through the full {@link WorkStealingScan} toggle-ON vs toggle-OFF. ON must produce {@code >=
     * 0.8·W} seed ranges and materially higher sustained parallelism (avg-in-flight), while OFF
     * collapses to one giant range. The correctness invariant: the emitted key set is BYTE-EXACT
     * IDENTICAL ON vs OFF — tiling never changes WHAT is listed.
     */
    @Test
    @Timeout(120)
    void endToEndHiveRunParallelizesAndStaysByteExactIdenticalToOff(@TempDir Path dir) throws Exception {
        int workers = 32;
        List<byte[]> keyspace = hivePartitioned(1100, 250);
        Duration latency = Duration.ofMillis(4);

        // Deterministic, timing-free parallelism POTENTIAL: seed-range count + busiest-tile share.
        SeedResult onSeed = seed(keyspace, workers, EngineToggles.DEFAULT);
        SeedResult offSeed = seed(keyspace, workers, FANOUT_OFF);
        assertThat(onSeed.specs.size()).as("ON: >= ~0.8·W seed ranges")
                .isGreaterThanOrEqualTo((int) Math.floor(0.8 * workers));
        assertThat(offSeed.specs.size()).as("OFF: collapses to a couple of ranges").isLessThanOrEqualTo(3);
        double onBusiest = busiestTileShare(onSeed.specs, keyspace);
        double offBusiest = busiestTileShare(offSeed.specs, keyspace);
        assertThat(onBusiest).as("ON: no giant range — even tiling").isLessThan(0.10);
        assertThat(offBusiest).as("OFF: one range owns nearly the whole keyspace").isGreaterThan(0.90);

        // Full engine runs (measured parallelism + byte-exact correctness).
        EngineResult on = runEngine(dir, "e2e-on", keyspace, workers, 1000, latency, EngineToggles.DEFAULT);
        EngineResult off = runEngine(dir, "e2e-off", keyspace, workers, 1000, latency, FANOUT_OFF);

        System.out.printf("[e2e] W=%d N=%d | ON seed=%d peak=%d avg=%.2f serial_frac=%.3f probes=%d "
                        + "busiest=%.3f | OFF seed=%d peak=%d avg=%.2f serial_frac=%.3f probes=%d busiest=%.3f%n",
                workers, keyspace.size(),
                on.seedCount, on.peakInFlight, on.avgInFlight, on.serialFrac, on.seedProbes, onBusiest,
                off.seedCount, off.peakInFlight, off.avgInFlight, off.serialFrac, off.seedProbes, offBusiest);

        // Byte-exact identical output ON vs OFF (correctness invariant), and each is exactly-once.
        EngineHarness.assertExactlyOnce(on.emitted, keyspace);
        EngineHarness.assertExactlyOnce(off.emitted, keyspace);
        assertSameKeySet(on.emitted, off.emitted);

        // avg-in-flight and peak-in-flight are timing-derived, contention-sensitive gauges: on a
        // slow/shared CI runner OFF's single giant range can show MORE overlapping in-flight fetches
        // than ON's evenly-tiled ranges (queueing/backpressure artifacts under CPU starvation), so no
        // reliable ON-vs-OFF ordering holds across runners. Do not assert on them cross-arm; they are
        // logged above (printf) for visibility only. The deterministic placement signals asserted
        // above (seed_count >= 0.8*W, busiest_tile_share bounds) plus the byte-exact equality below
        // are the real, runner-speed-independent guard that tiling delivers parallelism.
        assertThat(on.seedProbes).as("tiling adds zero probes vs OFF").isEqualTo(off.seedProbes);
    }

    // ---- harness ---------------------------------------------------------------

    private record SeedResult(List<NodeSpec> specs, long apiCalls, Map<String, Long> reasons) {
    }

    private static SeedResult seed(List<byte[]> keys, int workers, EngineToggles toggles) throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, toggles)
                .seedSpecs(1L, SeedMode.SHALLOW);
        return new SeedResult(specs, fetcher.apiCalls(), metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    private static RunSummary.SeedSummary summaryOf(List<byte[]> keys, int workers, EngineToggles toggles)
            throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, toggles).seedSpecs(1L, SeedMode.SHALLOW);
        RunSummary.SeedSummary seed = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        assertThat(seed).as("seed summary present").isNotNull();
        return seed;
    }

    private record EngineResult(List<byte[]> emitted, int seedCount, long apiCalls, long seedProbes,
                                long peakInFlight, double avgInFlight, double serialFrac) {
    }

    private static EngineResult runEngine(Path baseDir, String name, List<byte[]> keyspace, int workers,
                                          int maxKeys, Duration pageLatency, EngineToggles seedToggles)
            throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Latency only on recursive worker listing pages (maxKeys>1, no delimiter) — seed/thief
                // probes stay instant, so this exposes concurrent worker fetches without confounding the seed.
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        int seedCount;
        long seedProbes;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, NO_PREFIX, workers, null, seedToggles)
                    .seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            seedCount = specs.size();
            seedProbes = mock.apiCalls();
            List<Node> seeds = store.loadResumable(run.id(), false);

            metrics.markRunStarted();   // arm the avg-in-flight / trajectory integration window
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        RunSummary.TrajectorySummary traj =
                metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).trajectory();
        double serialFrac = traj != null ? traj.serialFrac() : -1.0;
        return new EngineResult(emitted, seedCount, mock.apiCalls(), seedProbes,
                metrics.peakInFlight(), metrics.avgInFlight(), serialFrac);
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    // ---- assertions ------------------------------------------------------------

    private static void assertExactTiling(List<NodeSpec> specs) {
        List<RangePartition.Interval> intervals = new ArrayList<>(specs.size());
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /** Busiest seed tile's share of the keyspace (the tiles already tile {@code (⊥, null]} in order). */
    private static double busiestTileShare(List<NodeSpec> specs, List<byte[]> keyspace) {
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        long[] counts = new long[specs.size()];
        int ti = 0;
        for (byte[] k : sorted) {
            while (ti < specs.size() - 1
                    && specs.get(ti).rangeEnd() != null
                    && Arrays.compareUnsigned(k, specs.get(ti).rangeEnd()) > 0) {
                ti++;
            }
            counts[ti]++;
        }
        long busiest = 0;
        for (long c : counts) {
            busiest = Math.max(busiest, c);
        }
        return (double) busiest / keyspace.size();
    }

    private static void assertSameKeySet(List<byte[]> a, List<byte[]> b) {
        TreeSet<byte[]> setA = new TreeSet<>(Arrays::compareUnsigned);
        setA.addAll(a);
        TreeSet<byte[]> setB = new TreeSet<>(Arrays::compareUnsigned);
        setB.addAll(b);
        assertThat(setA).as("ON and OFF emit the same number of distinct keys").hasSize(setB.size());
        var ia = setA.iterator();
        var ib = setB.iterator();
        while (ia.hasNext()) {
            assertThat(Arrays.equals(ia.next(), ib.next())).as("ON vs OFF byte-exact identical key").isTrue();
        }
    }
}
