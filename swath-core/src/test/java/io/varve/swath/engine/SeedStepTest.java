/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
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
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit + integration guards for the shallow {@code delimiter=/} seed step (algorithms.md §8).
 * Proves the seed tiles {@code (⊥, null]} exactly, that a deep-nested
 * keyspace parallelizes under {@code --seed shallow} (vs. near-serial under {@code none}) while
 * still emitting every key exactly once, and that the seed never over-fragments a directory
 * explosion (INT-8: total LIST ≈ {@code ceil(N/1000)}, not ≈ N).
 */
final class SeedStepTest {

    private static final byte[] NO_PREFIX = new byte[0];

    // ---- keyspace shapes -------------------------------------------------------

    /**
     * A single-top-prefix deep tree: everything lives under one
     * top-level prefix, so a single root range cannot parallelize on the top level — only the
     * shallow seed's descent exposes the natural sub-range parallelism.
     */
    private static List<byte[]> nestedSingleTop(int paths, int steps, int perLeaf) {
        List<byte[]> keys = new ArrayList<>();
        for (int n = 0; n < paths; n++) {
            for (int m = 0; m < steps; m++) {
                for (int f = 0; f < perLeaf; f++) {
                    keys.add(("IHTest_2025-01-15_DistStA/Path%02d_DistStA/Path%02d_Step%02d_DistStA/file%04d.dat"
                            .formatted(n, n, m, f)).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        return keys;
    }

    /**
     * The {@code essential-web-v1.0} explosion shape: a clean narrow {@code crawl=} top with a
     * 1:1 {@code pid=<hex>} fan-out below (one object per leaf directory). The INT-8 trap — a
     * naive per-directory recursion would cost ~1 LIST per pid.
     */
    private static List<byte[]> explodedOneObjectPerLeaf(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("crawl=2024/pid=%08x/part-00000.parquet".formatted(i))
                    .getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * A PLAIN (non-{@code key=value/}) directory explosion under a shared top: {@code
     * shard/<8hex>/part-00000.parquet}, one object per leaf. Structurally identical page-shape to a
     * Hive partition fan-out (truncated sub-level WITH common prefixes, no direct objects) but the
     * common prefixes are plain {@code <hex>/} names — the partition-fanout discriminator leaves it
     * WHOLE (the INT-8 tiny-leaf shape), never tiled.
     */
    private static List<byte[]> plainDirExplosion(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("shard/%08x/part-00000.parquet".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * The Hive partition shape: a narrow top ({@code table/}) over a truncated {@code date=…/}
     * partition fan-out ({@code partitions} partitions × {@code perPartition} objects each). The seed
     * probes {@code table/}, sees {@code >1000} truncated {@code date=…/} common prefixes, and tiles.
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

    // ---- 1. seed tiling property (structural) ----------------------------------

    @Test
    void shallowSeedTilesKeyspaceExactlyWithManyRanges() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(nestedSingleTop(10, 10, 20)).build();
        SeedStep seed = SeedSteps.of(fetcher, NO_PREFIX, 16);

        List<NodeSpec> specs = seed.seedSpecs(1L, SeedMode.SHALLOW);

        // Many ranges (seed_count ≫ 1): the deep keyspace was exposed, not left as one root range.
        assertThat(specs.size()).as("shallow seed produced many ranges").isGreaterThan(8);

        // Exactly tiles (⊥, null]: pairwise-disjoint, no gap (I2/I3), every range but the last
        // has a finite hi, and each fresh tile's cursor == its lower bound.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (int i = 0; i < specs.size(); i++) {
            NodeSpec s = specs.get(i);
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            if (i < specs.size() - 1) {
                assertThat(s.rangeEnd()).as("non-final range has a finite hi").isNotNull();
            }
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /**
     * A small structured head plus a large FLAT tail (no sub-directory
     * structure) must not leave the tail as one giant range. The seed synthesizes interior
     * cut-points so the tail is partitioned by leading character, while still tiling exactly.
     */
    @Test
    void flatWideTailIsSubdividedIntoBalancedRangesAndStillTiles() throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            keyspace.add(("aa/%02d/obj-%04d".formatted(i % 5, i)).getBytes(StandardCharsets.UTF_8));
        }
        Random r = new Random(7L);
        for (int i = 0; i < 8000; i++) {
            keyspace.add(("zz/%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 64).seedSpecs(1L, SeedMode.SHALLOW);

        // Exact tiling preserved (I2/I3) despite the synthesized cut-points.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // The flat tail is no longer one giant range: the busiest tile owns only a small share.
        assertThat(busiestTileShare(specs, keyspace))
                .as("flat tail subdivided — busiest seed range share")
                .isLessThan(0.25);
    }

    /**
     * {@code collectCutPoints} can record <b>at most one</b> flat-wide region per
     * run — the descent loop keeps only the FIRST truncated flat-wide sub-level it records, so a
     * SECOND sibling directory that is <em>structurally identical</em> (equally flat and dense,
     * equally truncated) does not take the banding slot even once probed, and is left as a single
     * un-banded range. This pins that contract explicitly: with two dense flat siblings ({@code yy/}
     * and {@code zz/}), only the first-visited one is radix-banded; the second stays whole. {@code
     * zz/} sorts last among the top-level entries and so has no measured successor — its span score
     * is the frontier's maximal/unbounded-tail score (see {@code spanScore}), which outranks {@code
     * yy/}'s ordinary measured gap, so {@code zz/} is visited FIRST and wins the single banding slot;
     * {@code yy/} is left whole, its one un-split range running from {@code yy/} to the next real cut
     * ({@code zz/}), not to the open frontier. (Not a defect — the seed still tiles exactly and the
     * un-banded sibling is still correctly claimable/stealable by the work-stealing engine at
     * runtime — but a future change that starts recording multiple flat-wide regions must update
     * {@code SeedStep.Collected}/{@code subdivideFlatWideRegion} together, or it will silently keep
     * banding only one of them.)
     */
    @Test
    void onlyTheFirstOfTwoSiblingDenseFlatRegionsIsRadixBanded() throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        keyspace.add("aa/head".getBytes(StandardCharsets.UTF_8));   // small structured, narrow top
        Random r = new Random(82L);
        for (int i = 0; i < 1500; i++) {
            keyspace.add(("yy/%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
            keyspace.add(("zz/%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 64).seedSpecs(1L, SeedMode.SHALLOW);

        // Still an exact I2/I3 tiling regardless of which sibling got banded.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        byte[] yyCut = "yy/".getBytes(StandardCharsets.UTF_8);
        byte[] zzCut = "zz/".getBytes(StandardCharsets.UTF_8);
        List<NodeSpec> afterYy = specs.stream()
                .filter(s -> Arrays.equals(s.rangeStart(), yyCut))
                .toList();
        // yy/ was probed (the descent continues past every truncated sibling) but did NOT win the
        // single flat-wide banding slot — zz/, visited first, already took it — so yy/'s region is
        // exactly ONE un-subdivided range running to the next real cut (zz/), not radix-banded.
        assertThat(afterYy).as("yy/'s region was left as a single un-banded range").hasSize(1);
        assertThat(afterYy.get(0).rangeEnd()).as("yy/'s single range runs to the next real cut").isEqualTo(zzCut);

        // zz/ (visited first — the frontier's unbounded-tail candidate) WAS radix-banded: multiple
        // ranges start inside (zz/, null].
        long bandedAfterZz = specs.stream()
                .filter(s -> s.rangeStart() != null && Arrays.compareUnsigned(s.rangeStart(), zzCut) >= 0)
                .count();
        assertThat(bandedAfterZz)
                .as("zz/'s region (visited first) was radix-banded into multiple ranges").isGreaterThan(1);
    }

    /**
     * A truncated sub-level with {@code key=value/} COMMON PREFIXES (the {@code crawl=/pid=} Hive
     * partition shape) is tiled along its ALREADY-PROBED partition prefixes — but it must NOT
     * trigger the flat-tail ASCII subdivision (which would add ~90 empty single-character ranges) nor
     * a per-directory walk. Every cut is an observed {@code …/} directory prefix, W-capped, and the
     * tiling stays exact: bounded and prefix-shaped, never ASCII/per-directory.
     */
    @Test
    void directoryExplosionTailIsNotAsciiSubdivided() throws Exception {
        int workers = 64;
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(explodedOneObjectPerLeaf(4000)).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers).seedSpecs(1L, SeedMode.SHALLOW);

        // Tiled along the observed key=value partition prefixes, capped at W — never a ~90-way ASCII
        // fan-out and never one range per leaf directory (4000 dirs).
        assertThat(specs.size())
                .as("key=value fan-out tiled along observed prefixes, W-capped (not ASCII, not per-dir); "
                        + "+1 for the scope-closing sentinel's empty final tile")
                .isGreaterThan(1)
                .isLessThanOrEqualTo(workers + 3);
        // Every synthesized cut is an OBSERVED directory prefix ending in '/', never a one-byte ASCII
        // radix scalar appended inside a directory (the flat-tail subdivision this test's name pins).
        //
        // The seed's scope-closing sentinel is exempt, and is not a counter-example: it is
        // prefixCeil(observed prefix), so its last byte is the '/' incremented to '0' by construction.
        // It subdivides nothing — it bounds the whole scan scope so the final range stops being
        // owner-split-ineligible. Requiring '/' of it would be requiring a prefix, which a ceiling is
        // deliberately not.
        for (NodeSpec s : specs) {
            byte[] hi = s.rangeEnd();
            if (hi != null && !isScopeCeiling(hi)) {
                assertThat(hi[hi.length - 1])
                        .as("tiled cut is a '/'-terminated directory prefix, not a synthesized ASCII scalar")
                        .isEqualTo((byte) '/');
            }
        }
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /**
     * A single DENSE flat directory at the ROOT with no
     * {@code delimiter=/} structure (a high-entropy-hex-key root, no {@code /}) is no longer left as
     * one near-serial root range. The classifier sees a truncated
     * top with no common prefixes and pre-cuts it into leading-byte radix bands, while still tiling
     * {@code (⊥, null]} exactly. Contrast {@link #flatTopSeedsSingleRootRange()}: a SMALL flat top
     * (not truncated) stays a single range.
     */
    @Test
    void flatDenseRootIsRadixBandedAndStillTiles() throws Exception {
        Random r = new Random(11L);
        List<byte[]> keyspace = new ArrayList<>(8000);
        for (int i = 0; i < 8000; i++) {
            keyspace.add(("%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 64).seedSpecs(1L, SeedMode.SHALLOW);

        // The dense flat root was radix-banded, not left whole.
        assertThat(specs.size()).as("dense flat root is radix-banded into many ranges").isGreaterThan(8);

        // Exact tiling preserved (I2/I3) despite the synthesized root cut-points.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // No single band owns a disproportionate share of the hex-uniform root keyspace.
        assertThat(busiestTileShare(specs, keyspace))
                .as("dense flat root subdivided — busiest seed range share")
                .isLessThan(0.25);

        // Radix banding uses NO extra probes beyond the one top-level structure probe.
        assertThat(fetcher.apiCalls()).as("root banding is probe-free (one top probe only)").isEqualTo(1);
    }

    /**
     * A full-width radix band must place cut points across the ENTIRE printable range
     * {@code [0x21, 0x7E]} INCLUSIVE — the first ({@code '!'}) and last ({@code '~'}) printable
     * scalars each get a band boundary. The old strict-betweenness interpolation could only ever emit
     * interior scalars ({@code 0x22..0x7D}), leaving the span endpoints unbanded (a placement bug).
     */
    @Test
    void radixBandsReachTheFullPrintableSpanEndpoints() throws Exception {
        List<byte[]> keyspace = hexRoot(8000, 11L);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        // 64 workers → the single dense root is banded at full width (93 bands — every safe printable
        // leading scalar except '%'), so the span endpoints still each get their own cut;
        // NO_PREFIX makes each cut a single-byte key.
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 64).seedSpecs(1L, SeedMode.SHALLOW);

        TreeSet<byte[]> cutEnds = new TreeSet<>(Arrays::compareUnsigned);
        for (NodeSpec s : specs) {
            if (s.rangeEnd() != null) {
                cutEnds.add(s.rangeEnd());
            }
        }
        assertThat(cutEnds).as("the '!' (0x21) band boundary is present (span LO reached)")
                .anyMatch(c -> Arrays.equals(c, new byte[] {0x21}));
        assertThat(cutEnds).as("the '~' (0x7E) band boundary is present (span HI reached)")
                .anyMatch(c -> Arrays.equals(c, new byte[] {0x7E}));
    }

    /**
     * INTERIOR band count: the existing {@code appendSpread} guards all use
     * {@code workerCount=64} → the full-span {@code K=SPAN=94}. Drive {@code perRegion} strictly
     * between {@code MIN_BANDS=8} and {@code SPAN=94}: a single dense-root region has
     * {@code perRegion = min(94, max(8, targetSeeds))} with {@code targetSeeds = 4·W}, so {@code W=10}
     * yields the interior {@code K=40}. Assert the cut set is a valid I2/I3 tiling AND that the exact
     * evenly-spaced radix scalars were synthesized (distinct from the K=SPAN enumeration).
     */
    @Test
    void radixBandsAtInteriorBandCountTileExactlyAndSpanTheWholeRange() throws Exception {
        List<byte[]> keyspace = hexRoot(8000, 404L);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        int workers = 10;   // targetSeeds = 4·10 = 40 ⇒ interior band count K = 40 (8 < 40 < 94)

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers).seedSpecs(1L, SeedMode.SHALLOW);

        int bandCount = specs.size() - 1;   // K synthesized cuts + 1 final open range
        assertThat(bandCount).as("band count strictly interior (MIN_BANDS=8 < K < SPAN=94)")
                .isGreaterThan(8).isLessThan(94);

        // Valid I2/I3 tiling: half-open, exactly abutting, no gap/overlap, strictly ascending byte order.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        byte[] prevEnd = null;
        for (NodeSpec s : specs) {
            assertThat(s.cursor()).as("fresh band cursor == its lower bound").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
            if (s.rangeEnd() != null) {
                if (prevEnd != null) {
                    assertThat(Arrays.compareUnsigned(prevEnd, s.rangeEnd()))
                            .as("cut ends strictly ascending").isLessThan(0);
                }
                prevEnd = s.rangeEnd();
            }
        }
        RangePartition.assertTiles(intervals);

        // The union covers the whole span: first band starts at the parent-range lo (⊥), last ends at ⊤.
        assertThat(specs.get(0).rangeStart()).as("first band starts at the parent-range lo (⊥)").isNull();
        assertThat(specs.get(specs.size() - 1).rangeEnd()).as("last band ends at the parent-range hi (⊤)").isNull();

        // Explicit cut-list assertion: the K interior single-byte cuts are exactly the evenly-spaced
        // SAFE printable-ASCII scalars appendSpread synthesizes over [0x21, 0x7E] \ {'%'}
        // ('%' is excluded — see SeedStep.UNSAFE_SCALAR) — NOT the full enumeration.
        int lo = 0x21;
        int hi = 0x7E;
        int unsafe = '%';
        int span = hi - lo;   // 93: the 94-wide inclusive range, less the excluded '%'
        TreeSet<byte[]> expected = new TreeSet<>(Arrays::compareUnsigned);
        for (int i = 0; i < bandCount; i++) {
            int rank = (int) Math.round((double) i * (span - 1) / (bandCount - 1));
            int scalar = lo + rank;
            if (scalar >= unsafe) {
                scalar += 1;
            }
            expected.add(new byte[] {(byte) scalar});
        }
        TreeSet<byte[]> actual = new TreeSet<>(Arrays::compareUnsigned);
        for (NodeSpec s : specs) {
            if (s.rangeEnd() != null) {
                actual.add(s.rangeEnd());
            }
        }
        assertThat(actual).as("interior band cuts are the evenly-spaced radix scalars").isEqualTo(expected);
    }

    /**
     * A dense-root radix band must never synthesize a cut point ending in a
     * raw {@code '%'} (0x25) byte. That byte becomes a {@code start-after}/{@code prefix} the engine
     * sends on the wire; LocalStack (and possibly other S3-compatible stores) echoes it back
     * un-re-encoded in the {@code StartAfter}/{@code Prefix}/{@code Marker} response fields under
     * {@code encoding-type=url}, and the AWS SDK's own always-on {@code
     * DecodeUrlEncodedResponseInterceptor} strict-decodes those fields with {@code
     * java.net.URLDecoder} — which throws {@code IllegalArgumentException("Incomplete trailing escape
     * (%) pattern")} on the lone {@code '%'}, aborting the whole listing before swath ever sees the
     * response. Full radix width (94 requested bands from 64
     * workers) is exactly the shape that used to include {@code '%'} as one of the evenly-spaced
     * scalars — this is the width most likely to hit it.
     */
    @Test
    void radixBandsNeverSynthesizeAnUnsafePercentScalar() throws Exception {
        List<byte[]> keyspace = hexRoot(8000, 707L);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 64).seedSpecs(1L, SeedMode.SHALLOW);

        for (NodeSpec s : specs) {
            for (byte[] bound : new byte[][] {s.rangeStart(), s.rangeEnd()}) {
                if (bound == null) {
                    continue;
                }
                for (byte b : bound) {
                    assertThat(b).as("no seed range boundary byte is the unsafe '%' scalar")
                            .isNotEqualTo((byte) '%');
                }
            }
        }
    }

    /**
     * The K boundary the interior case brackets from below: a dense flat root band count is FLOORED at
     * {@code MIN_BANDS=8} and never collapses to {@code K=1}, even for a single worker
     * ({@code targetSeeds = 4 < MIN_BANDS} ⇒ {@code perRegion = max(8, 4) = 8}). Still tiles exactly.
     */
    @Test
    void radixBandCountFloorsAtMinBandsForOneWorker() throws Exception {
        List<byte[]> keyspace = hexRoot(8000, 405L);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 1).seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs.size() - 1).as("dense-root band count floors at MIN_BANDS=8 (never K=1)").isEqualTo(8);
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /**
     * §5 instrumentation: the seed step records a per-shape engagement counter (surfaced in
     * {@code list_run_diagnostics} steal_reasons) so post-hoc analysis can tell from the metrics alone
     * which seed path engaged — dense-root radix banding (+ band count), tiny-leaf explosion left
     * whole, or a trivial flat top.
     */
    @Test
    void seedRecordsPerShapeEngagementCounters() throws Exception {
        // (a) dense flat hex root → dense_root_radix_banded + a radix_bands classification count.
        RunMetrics dense = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(hexRoot(8000, 5L)).build(), NO_PREFIX, 64, dense)
                .seedSpecs(1L, SeedMode.SHALLOW);
        Map<String, Long> denseReasons = dense.diagnostics(Duration.ZERO).stealReasons();
        assertThat(denseReasons.getOrDefault("SEED.dense_root_radix_banded", 0L))
                .as("dense root radix-banding engagement counter fired").isPositive();
        assertThat(denseReasons.getOrDefault("SEED.radix_bands", 0L))
                .as("band-count classification signal recorded").isGreaterThan(8L);

        // (b) plain (non-key=value) directory explosion (truncated level WITH common prefixes)
        //     → tiny_leaf_explosion, left whole.
        RunMetrics explosion = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(plainDirExplosion(4000)).build(), NO_PREFIX, 64, explosion)
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(explosion.diagnostics(Duration.ZERO).stealReasons().getOrDefault("SEED.tiny_leaf_explosion", 0L))
                .as("tiny-leaf explosion engagement counter fired").isPositive();

        // (c) small non-truncated flat top → flat_trivial.
        RunMetrics flat = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> smallFlat = List.of(
                "alpha".getBytes(StandardCharsets.UTF_8),
                "bravo".getBytes(StandardCharsets.UTF_8));
        SeedSteps.of(MockPageFetcher.builder().keys(smallFlat).build(), NO_PREFIX, 64, flat)
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(flat.diagnostics(Duration.ZERO).stealReasons().getOrDefault("SEED.flat_trivial", 0L))
                .as("trivial flat-top engagement counter fired").isPositive();
    }

    /**
     * {@code SeedStep}'s already-computed mode/probes/cut_points/synthesized_cuts/ranges (previously
     * log-only) are promoted onto {@link RunMetrics} for the JSON run-summary's {@code seed} block, and a
     * plain delimiter-tiled shape (no dense-root/tiny-leaf/scatter-scout subtype) records the missing
     * {@code SEED.delimiter_seeded} classification.
     */
    @Test
    void seedSummaryIsPromotedToMetricsForEachMode() throws Exception {
        // (a) NONE: mode=none, no probes/cuts, exactly one range.
        RunMetrics noneMetrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(nestedSingleTop(2, 2, 2)).build(), NO_PREFIX, 16, noneMetrics)
                .seedSpecs(1L, SeedMode.NONE);
        RunSummary noneSummary = noneMetrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        assertThat(noneSummary.seed()).isNotNull();
        assertThat(noneSummary.seed().mode()).isEqualTo("none");
        assertThat(noneSummary.seed().probes()).isZero();
        assertThat(noneSummary.seed().cutPoints()).isZero();
        assertThat(noneSummary.seed().synthesizedCuts()).isZero();
        assertThat(noneSummary.seed().ranges()).isEqualTo(1L);

        // (b) A plain multi-top-prefix shape (no dense-root/tiny-leaf/scatter subtype) — the generic
        // "delimiter_seeded" classification, and a non-trivial seed summary.
        RunMetrics plainMetrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> plainKeys = new ArrayList<>();
        for (String top : List.of("a/", "b/", "c/")) {
            for (int i = 0; i < 5; i++) {
                plainKeys.add((top + "obj-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        SeedSteps.of(MockPageFetcher.builder().keys(plainKeys).build(), NO_PREFIX, 16, plainMetrics)
                .seedSpecs(2L, SeedMode.SHALLOW);
        var plainReasons = plainMetrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(plainReasons.getOrDefault("SEED.delimiter_seeded", 0L))
                .as("plain delimiter-tiled shape (no dense-root/tiny-leaf/scatter subtype) is classified")
                .isPositive();
        RunSummary plainSummary = plainMetrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        assertThat(plainSummary.seed()).isNotNull();
        assertThat(plainSummary.seed().mode()).isEqualTo("shallow");
        assertThat(plainSummary.seed().cutPoints()).isEqualTo(4L);   // a/, b/, c/ + the scope-closing sentinel
        // 4 cuts + the (sentinel, null] tile, which is empty by construction.
        assertThat(plainSummary.seed().ranges()).isEqualTo(5L);

        // (c) A resumed/never-seeded run: RunMetrics without any recordSeedSummary call renders
        // seed() == null (the JSON writer's null-safety guard, exercised in JsonRunSummaryWriterTest).
        RunMetrics unseeded = new RunMetrics(new SimpleMeterRegistry());
        assertThat(unseeded.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed()).isNull();
    }

    /**
     * {@code seed.decisions[]} — one entry per probed {@code delimiter=/} level. On the
     * Hive-shape {@code explodedOneObjectPerLeaf} fixture (a narrow {@code crawl=2024/} top over a
     * 1:1 {@code pid=<hex>/} explosion below), the top level's own probe is narrow/not-truncated and
     * the descended {@code crawl=2024/} sub-level is truncated WITH common prefixes (the 1000-way
     * {@code pid=} page) — a directory explosion left whole, exactly the
     * {@code tiny_leaf_explosion} classification.
     */
    @Test
    void seedDecisionTraceRecordsTheTruncatedHiveLevelWithCommonPrefixes() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(explodedOneObjectPerLeaf(4000)).build();

        SeedSteps.of(fetcher, NO_PREFIX, 64, metrics).seedSpecs(1L, SeedMode.SHALLOW);

        RunSummary summary = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.seed()).isNotNull();
        List<RunSummary.SeedSummary.SeedDecision> decisions = summary.seed().decisions();
        assertThat(decisions).as("one entry per probed level: the narrow top + the exploded sub-level")
                .hasSize(2);

        RunSummary.SeedSummary.SeedDecision top = decisions.get(0);
        assertThat(top.truncated()).as("the crawl=2024/ top level is a single, non-truncated cut").isFalse();
        assertThat(top.classification()).isEqualTo("narrow");
        assertThat(top.cutsKept()).isEqualTo(1);
        assertThat(top.cutsDiscarded()).isZero();

        RunSummary.SeedSummary.SeedDecision exploded = decisions.get(1);
        assertThat(exploded.truncated()).as("the pid= sub-level's first page hits the 1000-key cap").isTrue();
        assertThat(exploded.fanout()).as("the truncated page's raw CommonPrefixes count").isGreaterThan(900);
        // The crawl=/pid= sub-level is a key=value partition fan-out — tiled, not left whole.
        assertThat(exploded.classification()).isEqualTo("fanout_tiled");
        assertThat(exploded.cutsKept()).as("a fanout_tiled level contributes its W-capped partition cuts").isPositive();
        assertThat(exploded.cutsDiscarded()).isZero();
        assertThat(exploded.prefix()).contains("crawl=2024");
    }

    /**
     * A Hive {@code key=value/} partition fan-out reached by descending a
     * narrow top ({@code table/} → truncated {@code date=…/}) is TILED at seed time along a W-capped
     * subset of the already-probed partition prefixes when {@code fanout_tiling} is ON (the default),
     * and left whole when it is OFF — proving both the tiling and the toggle. Zero extra probes
     * either way; the tiling stays an exact I2/I3 partition.
     */
    @Test
    void hiveKeyValueFanoutIsTiledWhenOnAndLeftWholeWhenOff() throws Exception {
        List<byte[]> keyspace = hivePartitioned(1100, 5);   // 1100 date= partitions × 5 objects each
        int workers = 32;

        // ON (default): the descended key=value fan-out is tiled, W-capped, and SEED.fanout_tiled fires.
        RunMetrics on = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher onFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> tiled = SeedSteps.of(onFetcher, NO_PREFIX, workers, on, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        assertThat(tiled.size()).as("ON: Hive key=value fan-out tiled to ~W ranges (+1 for the "
                        + "scope-closing sentinel's empty final tile)")
                .isGreaterThan(8).isLessThanOrEqualTo(workers + 3);
        assertThat(on.diagnostics(Duration.ZERO).stealReasons().getOrDefault("SEED.fanout_tiled", 0L))
                .as("ON: SEED.fanout_tiled engagement counter fired").isPositive();
        // Exact I2/I3 tiling preserved.
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : tiled) {
            assertThat(s.cursor()).as("fresh tile cursor == rangeStart").isEqualTo(s.rangeStart());
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);

        // OFF: break-and-discard — the fan-out is left whole (tiny_leaf_explosion), few ranges,
        // and the TOGGLE.fanout_tiling_off ablation mark is observable.
        // Explicit mass_aware_seed=off alongside fanout_tiling=off — EngineToggles.parse's other
        // toggles resolve at their OWN defaults, and mass_aware_seed's default is ON, so an
        // unqualified "fanout_tiling=off" would ALSO silently enable mass-aware sampling. This guard
        // is specifically about fanout_tiling's on/off behavior (probe-neutral tiling vs.
        // break-and-discard) — NOT mass-aware sampling — so pin mass_aware_seed off too to keep the
        // "zero extra probes either way" comparison isolated to the mechanism under test.
        RunMetrics off = new RunMetrics(new SimpleMeterRegistry());
        EngineToggles fanoutOff = EngineToggles.parse(List.of("fanout_tiling=off", "mass_aware_seed=off"), false);
        MockPageFetcher offFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> legacy = SeedSteps.of(offFetcher, NO_PREFIX, workers, off, fanoutOff)
                .seedSpecs(2L, SeedMode.SHALLOW);
        assertThat(legacy.size()).as("OFF: legacy break-and-discard leaves few ranges").isLessThan(8);
        var offReasons = off.diagnostics(Duration.ZERO).stealReasons();
        assertThat(offReasons.getOrDefault("SEED.tiny_leaf_explosion", 0L))
                .as("OFF: tiny_leaf_explosion classification retained").isPositive();
        assertThat(offReasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("OFF: fanout_tiled never fires").isZero();
        assertThat(offReasons.getOrDefault("TOGGLE.fanout_tiling_off", 0L))
                .as("OFF: toggle ablation mark observable").isPositive();
        // Both modes cost the SAME probe count (zero extra probes to tile).
        assertThat(onFetcher.apiCalls()).as("tiling adds zero probes vs. the legacy break")
                .isEqualTo(offFetcher.apiCalls());
    }

    /**
     * {@code fanout_tiling=off} ALONE does not restore the "leave the partition fan-out whole"
     * behavior, because {@code mass_aware_seed} defaults ON — with the {@code isPartitionFanout}
     * branch disabled by this toggle, the SAME truncated cut falls into the ambiguous else-branch
     * and gets mass-aware SAMPLED instead: a heavy {@code date=} partition (150 objects each, well
     * above {@code SeedStep.SAMPLE_DENSE_MIN_OBJECTS} = 8, so every sampled child comes back dense
     * and {@code sampleProvesHeavy}'s majority rule fires) samples heavy and is BANDED
     * ({@code SEED.heavy_cut_banded}), not left whole. The pure "leave whole" ablation is the
     * COMBINED {@code fanout_tiling=off mass_aware_seed=off} (see the previous test). Pins this
     * interaction so a future change to either toggle trips a test instead of silently drifting the
     * ablation's meaning.
     */
    @Test
    void fanoutTilingOffAloneIsBandedNotLeftWholeSinceMassAwareSeedDefaultsOn() throws Exception {
        List<byte[]> keyspace = hivePartitioned(1100, 150);   // 150 objs/partition: samples dense (>= 8)
        int workers = 32;

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        EngineToggles fanoutOffOnly = EngineToggles.parse(List.of("fanout_tiling=off"), false);
        assertThat(fanoutOffOnly.massAwareSeed()).as("mass_aware_seed stays at its own ON default").isTrue();

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, fanoutOffOnly)
                .seedSpecs(1L, SeedMode.SHALLOW);

        var reasons = metrics.diagnostics(Duration.ZERO).stealReasons();

        assertThat(reasons.getOrDefault("SEED.fanout_tiled", 0L))
                .as("fanout_tiling=off: the partition-fanout branch never fires").isZero();
        assertThat(reasons.getOrDefault("SEED.heavy_cut_banded", 0L))
                .as("mass_aware_seed (still ON) samples the cut, proves it heavy, and bands it")
                .isPositive();
        assertThat(reasons.getOrDefault("TOGGLE.fanout_tiling_off", 0L))
                .as("the ablation mark still fires").isPositive();

        // Reference: the pure "leave whole" ablation (mass_aware_seed ALSO off) leaves the fan-out whole.
        RunMetrics wholeMetrics = new RunMetrics(new SimpleMeterRegistry());
        EngineToggles combinedOff = EngineToggles.parse(List.of("fanout_tiling=off", "mass_aware_seed=off"), false);
        MockPageFetcher wholeFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> whole = SeedSteps.of(wholeFetcher, NO_PREFIX, workers, wholeMetrics, combinedOff)
                .seedSpecs(2L, SeedMode.SHALLOW);

        assertThat(specs.size())
                .as("banded (fanout_tiling=off alone) yields MORE ranges than the fully-whole "
                        + "combined ablation (fanout_tiling=off mass_aware_seed=off)")
                .isGreaterThan(whole.size());
    }

    /**
     * {@code finalizeDecisions} rewrite branch (a): a dense flat hex root is classified
     * {@code flat_wide} by its own probe, then PROMOTED to {@code dense_root_radix_banded} once the
     * run-level post-processing has actually radix-banded it (a disposition knowable only after the
     * whole seed collection is in hand). The per-level {@code decisions[]} must show the rewritten
     * classification, not the raw {@code flat_wide}.
     */
    @Test
    void seedDecisionTraceRewritesBandedRootToDenseRootRadixBanded() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(hexRoot(8000, 909L)).build(), NO_PREFIX, 64, metrics)
                .seedSpecs(1L, SeedMode.SHALLOW);

        RunSummary summary = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.seed()).isNotNull();
        List<RunSummary.SeedSummary.SeedDecision> decisions = summary.seed().decisions();
        assertThat(decisions).as("a flat dense root probes exactly one level (the root)").hasSize(1);
        RunSummary.SeedSummary.SeedDecision root = decisions.get(0);
        assertThat(root.truncated()).as("the dense root's top probe is truncated").isTrue();
        assertThat(root.classification())
                .as("flat_wide root rewritten to dense_root_radix_banded after banding")
                .isEqualTo("dense_root_radix_banded");
    }

    /**
     * {@code finalizeDecisions} rewrite branch (b): a plain multi-top-prefix keyspace (no
     * dense-root/tiny-leaf/partition subtype) is the generic delimiter-tiled run — the TOP level's own
     * probe is {@code narrow}, and the run-level post-processing promotes that top entry to
     * {@code delimiter_seeded} (a classification known only once the whole collection is in hand).
     */
    @Test
    void seedDecisionTraceRewritesTopLevelToDelimiterSeeded() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> plainKeys = new ArrayList<>();
        for (String top : List.of("a/", "b/", "c/")) {
            for (int i = 0; i < 5; i++) {
                plainKeys.add((top + "obj-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        SeedSteps.of(MockPageFetcher.builder().keys(plainKeys).build(), NO_PREFIX, 16, metrics)
                .seedSpecs(1L, SeedMode.SHALLOW);

        RunSummary summary = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.seed()).isNotNull();
        List<RunSummary.SeedSummary.SeedDecision> decisions = summary.seed().decisions();
        assertThat(decisions).as("top level plus one entry per descended top prefix").isNotEmpty();
        RunSummary.SeedSummary.SeedDecision top = decisions.get(0);
        assertThat(top.truncated()).as("the plain top probe fits in one page").isFalse();
        assertThat(top.classification())
                .as("the generic delimiter-tiled top level is rewritten to delimiter_seeded")
                .isEqualTo("delimiter_seeded");
    }

    /** A flat dense high-entropy hex root (no {@code /}) — the "heavy dense range" classify shape. */
    private static List<byte[]> hexRoot(int n, long seed) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
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

    // ---- 2. seed-mode wiring ---------------------------------------------------

    @Test
    void noneSeedsSingleRootRange() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(nestedSingleTop(4, 4, 4)).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 16).seedSpecs(7L, SeedMode.NONE);

        assertThat(specs).hasSize(1);
        NodeSpec root = specs.get(0);
        assertThat(root.rangeStart()).isNull();
        assertThat(root.rangeEnd()).isNull();
        assertThat(root.cursor()).isNull();
        // NONE issues no probe RPCs.
        assertThat(fetcher.apiCalls()).isZero();
    }

    @Test
    void flatTopSeedsSingleRootRange() throws Exception {
        // No delimiter in any key ⇒ no common prefixes ⇒ a single (⊥, null] range.
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("a".getBytes(), "b".getBytes(), "c".getBytes())).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, 16).seedSpecs(3L, SeedMode.SHALLOW);

        assertThat(specs).hasSize(1);
        assertThat(specs.get(0).rangeStart()).isNull();
        assertThat(specs.get(0).rangeEnd()).isNull();
    }

    @Test
    void hintsSeedRejectedUntilImplemented() {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(List.of("a/b".getBytes())).build();
        assertThatThrownBy(() -> SeedSteps.of(fetcher, NO_PREFIX, 16).seedSpecs(1L, SeedMode.HINTS))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("hints");
    }

    // ---- 3. INT-8: a directory explosion stays ≈ ceil(N/1000) LIST calls -------

    @Test
    @Timeout(120)
    void int8ExplodedTreeStaysFlatScanNotPerDirectory(@TempDir Path dir) throws Exception {
        int n = 6000;   // 6000 pid= leaf directories, one object each
        List<byte[]> keyspace = explodedOneObjectPerLeaf(n);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        EngineRun run = runEngine(fetcher, NO_PREFIX, SeedMode.SHALLOW, 16, 1000, dir);

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        long ceil = (n + 999) / 1000;              // ≈ 6
        long perDirectory = n;                     // the trap: ~1 LIST per leaf
        // Flat-scanned, not recursed: total LIST is a small multiple of ceil(N/1000) (the seed
        // probes + the bounded seed ranges + the flat scan), nowhere near N.
        assertThat(fetcher.apiCalls())
                .as("LIST calls ≈ ceil(N/1000)+O(seed), NOT ≈ N (per-directory recursion)")
                .isLessThan(perDirectory / 10)
                .isLessThan(ceil + 4L * run.seedCount + 64);
    }

    // ---- 4. deep-nested keyspace parallelizes under shallow, exactly-once ------

    @Test
    @Timeout(120)
    void nestedKeyspaceParallelizesUnderShallowSeed(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = nestedSingleTop(10, 10, 30);   // 3000 objects, one top prefix
        int workers = 16;
        Duration pageLatency = Duration.ofMillis(4);

        EngineRun none = runEngine(
                MockPageFetcher.builder().keys(keyspace).pageDelay(pageLatency).build(),
                NO_PREFIX, SeedMode.NONE, workers, 64, dir.resolve("none"));
        EngineRun shallow = runEngine(
                MockPageFetcher.builder().keys(keyspace).pageDelay(pageLatency).build(),
                NO_PREFIX, SeedMode.SHALLOW, workers, 64, dir.resolve("shallow"));

        // Both must emit every key exactly once (the seed never drops/dupes a key).
        EngineHarness.assertExactlyOnce(none.emitted, keyspace);
        EngineHarness.assertExactlyOnce(shallow.emitted, keyspace);

        // The seed exposed many ranges where none had a single root range — the ROBUST, deterministic
        // discriminator of the shallow seed's parallelism advantage (unaffected by slow-start).
        assertThat(none.seedCount).isEqualTo(1);
        assertThat(shallow.seedCount).as("shallow seed_count ≫ 1").isGreaterThan(8);

        // Shallow reaches broad parallelism from its many ready ranges — at least half the worker count.
        // Slow-start note: T ramps from SLOW_START_INITIAL_T by paced (≤1 step/s) doubling, so within
        // this short in-memory run the T CEILING (~8), not the seed structure, is the binding
        // constraint on peak_in_flight for BOTH modes — none's work-stealing fills workers to that
        // same ceiling. The seed-structure advantage is therefore asserted on the deterministic
        // seed_count above (1 vs ≫8), not on a ceiling-bound transient peak comparison.
        assertThat(shallow.peakInFlight).as("shallow parallelized to at least half the workers")
                .isGreaterThanOrEqualTo(workers / 2);
    }

    // ---- 5. atomicity: partial seed must never create a durable gap (BLOCKER fix) --

    /**
     * Reproduces the pre-fix bug (partial seed loop creates a durable gap) and proves the
     * atomic fix (insertNodes commits all-or-nothing so a crash leaves either a complete
     * partition or zero nodes — no silent missing-object gap).
     */
    @Test
    @Timeout(120)
    void partialSeedLoopCausesGap_atomicInsertNodesDoesNot(@TempDir Path dir) throws Exception {
        // Keyspace: 3 top-level "bucket=" directories with 60 objects each = 180 total.
        // Shallow seed produces 4 ranges (3 cut-points + 1 final open range).
        List<byte[]> keyspace = new ArrayList<>();
        for (int b = 0; b < 3; b++) {
            for (int i = 0; i < 60; i++) {
                keyspace.add(("bucket=%01d/obj-%04d".formatted(b, i)).getBytes(StandardCharsets.UTF_8));
            }
        }
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        SeedStep step = SeedSteps.of(mock, NO_PREFIX, 8);

        // === BUG: separate insertNode calls; crash after committing only the FIRST spec ===
        Path bugDb = dir.resolve("bug.sqlite");
        List<byte[]> partialEmitted;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(bugDb)) {
            RunMeta run = store.openRun(key(), false, false);
            List<NodeSpec> specs = step.seedSpecs(run.id(), SeedMode.SHALLOW);
            assertThat(specs.size()).as("seed produces multiple ranges").isGreaterThan(3);
            // Old pattern: insertNode one-by-one. Crash (store.close()) after first commit.
            store.insertNode(specs.get(0));   // durably committed in its own transaction
            // --- CRASH: only 1 of N ranges is in the DB ---
        }
        // On --resume, only the 1 committed range is loaded → the rest of the keyspace is silently skipped.
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(bugDb)) {
            RunMeta run = store.openRun(key(), true, false);   // --resume
            List<Node> partial = store.loadResumable(run.id(), false);
            assertThat(partial).as("only 1 range survived the partial insert crash").hasSize(1);
            partialEmitted = runEngineFromNodes(mock, store, run.id(), NO_PREFIX, partial, dir.resolve("bug-engine"));
        }
        // BUG PROVEN: with only 1 range, the engine emits a strict subset of the keyspace.
        assertThat(partialEmitted.size())
                .as("partial seed causes silent key loss (BUG): %d/%d keys emitted",
                        partialEmitted.size(), keyspace.size())
                .isLessThan(keyspace.size());

        // === FIX: atomic insertNodes — all specs in one transaction, no partial partition ===
        Path fixDb = dir.resolve("fix.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(fixDb)) {
            RunMeta run = store.openRun(key(), false, false);
            List<NodeSpec> specs = step.seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);   // all-or-nothing atomic commit
            // All N ranges are durably committed: no partial state possible.
            List<Node> all = store.loadResumable(run.id(), false);
            assertThat(all).as("atomic seed: all specs committed together").hasSize(specs.size());
            List<byte[]> fullEmitted = runEngineFromNodes(mock, store, run.id(), NO_PREFIX, all, dir.resolve("fix-engine"));
            // FIX PROVEN: exactly-once coverage of the full keyspace.
            EngineHarness.assertExactlyOnce(fullEmitted, keyspace);
        }
    }

    // ---- engine harness --------------------------------------------------------

    private record EngineRun(List<byte[]> emitted, int seedCount, int peakInFlight) {
    }

    private static EngineRun runEngine(MockPageFetcher mock, byte[] prefix, SeedMode mode,
                                       int workers, int maxKeys, Path ckptDir) throws Exception {
        Files.createDirectories(ckptDir);
        InFlightFetcher fetcher = new InFlightFetcher(mock);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>();
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, prefix, workers).seedSpecs(run.id(), mode);
            store.insertNodes(specs);   // atomic batch (I2)
            seedCount = specs.size();
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), prefix, ListingMode.OBJECTS, metrics),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            RunContext ctx = RunContext.create();
            PipelineDrain.collectKeys(5000, ctx, engine, emitted);
        }
        return new EngineRun(emitted, seedCount, fetcher.peak.get());
    }

    /** Drive the engine with a caller-supplied list of already-committed seed nodes. */
    private static List<byte[]> runEngineFromNodes(MockPageFetcher mock, SqliteCheckpointStore store,
                                                   long runId, byte[] prefix,
                                                   List<Node> seeds, Path ckptDir) throws Exception {
        Files.createDirectories(ckptDir);
        List<byte[]> emitted = new ArrayList<>();
        WorkStealingScan engine = new WorkStealingScan(
                EngineContexts.of(runId, prefix, ListingMode.OBJECTS),
                mock, store, 8, 100, seeds, FilterChain.EMPTY);
        RunContext ctx = RunContext.create();
        PipelineDrain.collectKeys(5000, ctx, engine, emitted);
        return emitted;
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "seed-step-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** Records peak concurrent worker (bulk) fetches via the metric the engine itself reads. */
    private static final class InFlightFetcher implements PageFetcher {
        private final PageFetcher delegate;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();

        InFlightFetcher(PageFetcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            if (req.maxKeys() <= 1) {
                return delegate.fetchPage(req);   // thief probes excluded from the peak
            }
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                return delegate.fetchPage(req);
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public StoreCapabilities capabilities() {
            return delegate.capabilities();
        }
    }

    /**
     * Whether {@code cut} is a {@link StealMath#prefixCeil} of a {@code '/'}-terminated prefix — i.e.
     * the seed's scope-closing sentinel rather than an observed directory cut. Recognised structurally
     * (last byte is the successor of {@code '/'}, and the value equals the ceiling of the same bytes
     * with {@code '/'} restored) so the check need not be told which fixture it is running on.
     */
    private static boolean isScopeCeiling(byte[] cut) {
        if (cut.length == 0 || cut[cut.length - 1] != (byte) ('/' + 1)) {
            return false;
        }
        byte[] asPrefix = cut.clone();
        asPrefix[asPrefix.length - 1] = (byte) '/';
        return Arrays.equals(cut, StealMath.prefixCeil(asPrefix));
    }

}
