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
 * <b>The seed's last tile is {@code (c_last, null]}, and the owner-split governor refuses every
 * range whose {@code hi} is {@code null}</b> ({@code OwnerSplitSkipReason#OPEN_FRONTIER}) — so mass
 * landing past the final cut is drained by one worker, serially, however large it is. Measured on
 * {@code nrel-pds-porotomo}: 4,735,963 of 4,973,335 keys (95.2%) in that one unsplittable tile,
 * because the bucket's mass sits under the LAST top-level prefix.
 *
 * <p>When the top scope was listed to completion and its greatest returned item is a {@code
 * CommonPrefix p/}, every key in scope is strictly below {@code prefixCeil(p/)} — a bound already in
 * hand, costing no probe. Appending it as a cut empties the open tile and gives the mass-bearing
 * tile a finite {@code hi}, making it splittable at runtime.
 *
 * <p>These tests pin the mechanism AND every precondition that makes it safe. The preconditions are
 * the interesting part: a sentinel derived from a truncated top, or from a prefix that a direct
 * object sorts past, would cut the keyspace short — a correctness bug, not a performance one. The
 * tiling itself is verified to still cover {@code (⊥, null]} in every case.
 */
final class SeedOpenTileSentinelTest {

    private static final int WORKERS = 8;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The porotomo shape in miniature: two top-level prefixes, the mass under the LAST one, listed
     * to completion. Without the sentinel the final tile is {@code (Nodal/, null]} and holds
     * everything under {@code Nodal/}; with it, that tile is bounded at {@code Nodal0} and the open
     * tile is empty.
     */
    @Test
    void aCompleteTopWhoseGreatestItemIsACommonPrefixGetsItsScopeClosed() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        keys.add(b("DAS/a/obj"));
        keys.add(b("DAS/b/obj"));
        for (int i = 0; i < 200; i++) {
            keys.add(b("Nodal/sweep_%04d/obj".formatted(i)));
        }

        Seeded run = seed(keys);
        List<NodeSpec> tiles = run.tiles();

        assertThat(run.appended()).as("the sentinel fired exactly once").isEqualTo(1L);
        assertThat(lastHi(tiles)).as("the final tile is still the open one").isNull();
        byte[] penultimate = tiles.get(tiles.size() - 2).rangeEnd();
        assertThat(penultimate).as("the tile before it is bounded by the scope-closing sentinel")
                .isEqualTo(b("Nodal0"));
        // The point of the whole mechanism: the mass-bearing range now has a finite hi, so the
        // owner-split governor will consider it instead of skipping on OPEN_FRONTIER.
        assertThat(tilesCovering(tiles, b("Nodal/sweep_0100/obj")))
                .as("the range holding the mass has a finite upper bound")
                .allSatisfy(t -> assertThat(t.rangeEnd()).isNotNull());
        assertTilesCoverKeyspace(tiles);
    }

    /**
     * A truncated top is the precondition that protects correctness: unseen siblings may sort past
     * everything observed, so no upper bound is provable and the sentinel must not be invented. A
     * bound derived here would silently truncate the listing.
     */
    @Test
    void aTruncatedTopGetsNoSentinel() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        // MockPageFetcher's page cap is what makes the top truncated; a wide top forces it.
        for (int i = 0; i < 4000; i++) {
            keys.add(b("p%05d/obj".formatted(i)));
        }

        Seeded run = seed(keys);

        // NOT "the last tile is open" — that is true of every tiling and would assert nothing.
        // The real claim is that the mechanism looked and refused.
        assertThat(run.appended())
                .as("no bound is provable from a truncated top, so no sentinel may be appended")
                .isZero();
        assertThat(run.declined()).as("and the refusal is recorded, not silent").isEqualTo(1L);
        assertTilesCoverKeyspace(run.tiles());
    }

    /**
     * The correctness guard that is a comparison rather than an assumption: a direct object sorting
     * AFTER the last common prefix is not bounded by that prefix's ceiling, so the sentinel must be
     * declined. {@code zzz-tail.txt} sorts after {@code Nodal/} — a sentinel at {@code Nodal0} would
     * drop it.
     */
    @Test
    void aDirectObjectSortingPastTheLastPrefixDeclinesTheSentinel() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        keys.add(b("DAS/a/obj"));
        for (int i = 0; i < 50; i++) {
            keys.add(b("Nodal/sweep_%04d/obj".formatted(i)));
        }
        keys.add(b("zzz-tail.txt"));   // sorts after "Nodal/" — NOT below prefixCeil("Nodal/")

        Seeded run = seed(keys);
        List<NodeSpec> tiles = run.tiles();

        assertThat(run.appended())
                .as("a direct object past the last prefix must veto the sentinel").isZero();
        assertThat(run.declined()).as("and the veto is recorded").isEqualTo(1L);
        for (NodeSpec t : tiles) {
            assertThat(t.rangeEnd()).as("no sentinel may be derived when a direct object sorts past "
                    + "the last prefix — it would cut the keyspace short").isNotEqualTo(b("Nodal0"));
        }
        assertTilesCoverKeyspace(tiles);
    }

    /**
     * Whatever the preconditions decide, the result must remain an exact I2/I3 tiling — the same
     * check the sibling seed guards use. A sentinel that broke this would be a correctness bug.
     */
    private static void assertTilesCoverKeyspace(List<NodeSpec> tiles) {
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : tiles) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    private static byte[] lastHi(List<NodeSpec> tiles) {
        return tiles.get(tiles.size() - 1).rangeEnd();
    }

    private static List<NodeSpec> tilesCovering(List<NodeSpec> tiles, byte[] key) {
        List<NodeSpec> hits = new ArrayList<>();
        for (NodeSpec t : tiles) {
            boolean aboveLo = t.rangeStart() == null
                    || Arrays.compareUnsigned(key, t.rangeStart()) > 0;
            boolean atOrBelowHi = t.rangeEnd() == null
                    || Arrays.compareUnsigned(key, t.rangeEnd()) <= 0;
            if (aboveLo && atOrBelowHi) {
                hits.add(t);
            }
        }
        return hits;
    }

    /** One seed run: the tiling it produced plus the counters it emitted. */
    private record Seeded(List<NodeSpec> tiles, Map<String, Long> reasons) {
        long appended() {
            return reasons.getOrDefault("SEED.open_tile_sentinel_appended", 0L);
        }

        long declined() {
            return reasons.getOrDefault("SEED.open_tile_sentinel_declined", 0L);
        }
    }

    private static Seeded seed(List<byte[]> keys) throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        List<NodeSpec> tiles = SeedSteps.of(fetcher, new byte[0], WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        return new Seeded(tiles, metrics.diagnostics(Duration.ZERO).stealReasons());
    }
}
