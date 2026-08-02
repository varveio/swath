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
 * <p>These tests pin the mechanism AND every precondition. <b>Be precise about what the
 * preconditions protect:</b> NOT key loss. {@code SeedStep.tile} always retains a final
 * {@code (u, null]} range, so a bound that were too low would leave the keys above it covered by
 * that open tile — I2/I3 hold either way. What the preconditions protect is the bound's
 * <em>truth</em>, and therefore the EMPTINESS of that final tile: an unfounded sentinel cuts the
 * mass-bearing tile short and dumps the remainder back into the serial open tile, which is a
 * parallelism regression, not a correctness one. The tiling is asserted exactly in every case
 * regardless.
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
     * A truncated top proves nothing: unseen siblings may sort past everything observed, so no upper
     * bound is derivable and the sentinel must not be invented. (Inventing one would not lose keys —
     * the final open tile still covers them — it would strand them back on the serial tail, which is
     * the very thing this mechanism exists to prevent.)
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
        assertThat(run.declined("top_truncated"))
                .as("and the refusal names the precondition that refused, not a lumped 'declined'")
                .isEqualTo(1L);
        assertTilesCoverKeyspace(run.tiles());
    }

    /**
     * A comparison rather than an assumption: a direct object sorting AFTER the last common prefix is
     * not bounded by that prefix's ceiling, so the sentinel must be declined. {@code zzz-tail.txt}
     * sorts after {@code Nodal/}; a sentinel at {@code Nodal0} would leave it in the open tile rather
     * than inside the bounded one — no key lost, but the bound would be a lie and the tail would stay
     * serial.
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
        assertThat(run.declined("direct_object_tail"))
                .as("and the veto names the direct-object tail specifically — the shape an operator "
                        + "reading a slow run needs to be able to grep for")
                .isEqualTo(1L);
        for (NodeSpec t : tiles) {
            assertThat(t.rangeEnd()).as("no sentinel may be derived when a direct object sorts past "
                    + "the last prefix — the bound would not hold for it").isNotEqualTo(b("Nodal0"));
        }
        assertTilesCoverKeyspace(tiles);
    }

    /**
     * <b>A truncated top that the {@code TOP_EXTRA} continuation RECOVERED is complete.</b> The +1-page
     * pagination reads the overflow prefixes, so what the planner holds is the whole top level and the
     * bound is derivable. Gating on the first page's truncation alone (which the first version of this
     * mechanism did) declines every paginated-top bucket for no reason — and paginated tops are exactly
     * the wide-shaped buckets whose tails are worth bounding.
     *
     * <p>The fixture is direct objects sorting BELOW a handful of prefixes: {@code a0000_obj} through
     * {@code a0994_obj} fill the first page's 1000-item cap before any {@code p0/}…{@code p9/} common
     * prefix is reached, so page one is truncated with only the first few prefixes rolled up and
     * {@code TOP_EXTRA} recovers the rest. That leaves the fan-out at 10 cuts, far under the 32-seed
     * budget, so — unlike a fixture that pages solely because of prefix VOLUME — there is a spare slot
     * left for the sentinel to actually close the scope, and the appended case this test's name promises
     * is exercised for real rather than declined for a different reason.
     */
    @Test
    void aTruncatedTopRecoveredByPaginationStillGetsItsScopeClosed() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 995; i++) {
            keys.add(b("a%04d_obj".formatted(i)));
        }
        for (int i = 0; i < 10; i++) {
            keys.add(b("p%d/obj".formatted(i)));
        }

        Seeded run = seed(keys);
        List<NodeSpec> tiles = run.tiles();

        assertThat(run.reasons().getOrDefault("SEED.top_probe_paginated", 0L))
                .as("the fixture must actually paginate the top for this test to mean anything")
                .isEqualTo(1L);
        assertThat(run.declined("top_truncated"))
                .as("a RECOVERED top must not be refused as truncated — that was the original defect")
                .isZero();
        assertThat(run.appended())
                .as("with a spare slot and a bound in hand, the scope is actually closed, not merely "
                        + "not-declined-for-truncation")
                .isEqualTo(1L);
        assertThat(lastHi(tiles)).as("the final tile is still the open one").isNull();
        byte[] penultimate = tiles.get(tiles.size() - 2).rangeEnd();
        assertThat(penultimate)
                .as("the tile before it is bounded by prefixCeil of the greatest recovered prefix, "
                        + "p9/'s trailing 0x2F rolled to 0x30")
                .isEqualTo(b("p90"));
        assertTilesCoverKeyspace(tiles);
    }

    /**
     * <b>The structural limitation the mechanism cannot lift:</b> a top that is ALL prefixes pages
     * because there are simply more of them than the 32-seed budget, not because objects crowded the
     * first page. {@code subsampleEvenly}/{@code massWeightedSubsample} return exactly {@code
     * targetSeeds} cuts once the observed set is over-full, so by the time {@link
     * io.varve.swath.engine.policy.HybridSeedPlanner}'s scope-closing sentinel runs, every slot is
     * already spent on a real boundary. The sentinel's own spare-slot precondition — never evict a
     * real cut for an empty range — then declines it correctly: recovery made the bound provable, but
     * a fully-subscribed seed set leaves nothing to spend it on.
     */
    @Test
    void aRecoveredTopWhosePrefixFanoutFillsTheSeedBudgetGetsNoSentinel() throws Exception {
        // >1000 top-level prefixes truncates the first page; the sorted tail is recovered by TOP_EXTRA.
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 1400; i++) {
            keys.add(b("p%05d/obj".formatted(i)));
        }

        Seeded run = seed(keys);

        assertThat(run.reasons().getOrDefault("SEED.top_probe_paginated", 0L))
                .as("the fixture must actually paginate the top for this test to mean anything")
                .isEqualTo(1L);
        assertThat(run.declined("top_truncated"))
                .as("a RECOVERED top must not be refused as truncated — that was the original defect")
                .isZero();
        assertThat(run.declined("no_spare_slot"))
                .as("1400 recovered prefixes subsample down to exactly the 32-seed budget, so no slot "
                        + "remains for the sentinel")
                .isEqualTo(1L);
        assertThat(run.appended())
                .as("a full seed set cannot take a sentinel without evicting a real cut")
                .isZero();
        assertTilesCoverKeyspace(run.tiles());
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

        /** How many times the sentinel was refused for exactly {@code reason}. */
        long declined(String reason) {
            return reasons.getOrDefault("SEED.open_tile_sentinel_declined_" + reason, 0L);
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
