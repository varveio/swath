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
import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Issue #15's per-depth yield quota: unit coverage over synthetic trees, distinct from (and never
 * modifying) the pre-existing level-order guards this quota is layered on top of.
 */
final class SeedFrontierYieldQuotaTest {

    private static final byte[] NO_PREFIX = new byte[0];

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The porotomo shape (issue #15's own repro): a depth's early probes stop yielding new cuts before
     * the wide subtree behind it is ever reached. {@code zzhub/} sorts LAST among its depth-1 siblings
     * (no successor cut yet at offer time), so it scores the span-priority ceiling fallback and is
     * polled FIRST; it holds exactly one child, {@code zzhub/wide/} — a 100-way fan-out, the actual
     * mass. Ten {@code chaffNN/} siblings are pure dead ends (one direct object each, no
     * sub-directories, {@code cuts_kept == 0}). {@code workerCount = 3} pins {@code targetSeeds =
     * maxProbes = 12} and a descent ceiling of 6 total probes — of which the TOP probe consumes one,
     * leaving exactly 5 descent-loop probes: {@code zzhub/} (kept 1), three {@code chaffNN/} (kept 0
     * each, filling the 4-probe yield window at a total of 1 — at or below {@code
     * YIELD_WINDOW_MIN_CUTS}, so depth 1 is cut off), then the quota's redirect: depth 2 (holding only
     * {@code zzhub/wide/}) is not cut off and gets the fifth and final probe instead of a fourth
     * dead-end {@code chaffNN/} — reaching the 100-way fan-out and tiling it, instead of leaving it as
     * one un-subdivided range the way strict shallow-first (draining depth 1 to exhaustion first)
     * would have.
     */
    private static List<byte[]> wideNodeBehindDeadEndSiblings() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add(b("chaff%02d/obj".formatted(i)));
        }
        for (int i = 0; i < 100; i++) {
            keys.add(b("zzhub/wide/c%03d/obj".formatted(i)));
        }
        return keys;
    }

    @Test
    @Timeout(60)
    void deadEndSiblingsAreCutOffSoTheWideSubtreeBehindThemGetsReached() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<NodeSpec> specs = SeedSteps.of(
                        MockPageFetcher.builder().keys(wideNodeBehindDeadEndSiblings()).build(),
                        NO_PREFIX, 3, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED.yield_quota_cutoff", 0L))
                .as("depth 1's dead-end chaff run must cut it off")
                .isPositive();
        assertThat(reasons.getOrDefault("SEED.yield_quota_cutoff_shallow", 0L))
                .as("the cut-off depth (1) is at/under the shallow bucket threshold")
                .isPositive();
        assertThat(reasons.getOrDefault("SEED.yield_quota_cutoff_deep", 0L))
                .as("no DEEP cutoff in this fixture — only the shallow depth-1 run trips it")
                .isZero();

        RunSummary.SeedSummary seed = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        boolean wideNodeReached = seed.decisions().stream()
                .anyMatch(d -> "zzhub/wide/".equals(d.prefix()) && d.fanout() == 100);
        assertThat(wideNodeReached)
                .as("the quota's redirect must let the 100-way zzhub/wide/ subtree actually get probed "
                        + "within the tiny descent budget, instead of stranding it behind the ten "
                        + "dead-end chaffNN/ siblings the way strict shallow-first alone would")
                .isTrue();

        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);   // I2/I3 still exact under the redirected poll order
    }

    /**
     * Same keyspace, same toggles, same everything: the descent holds no ambient clock or randomness
     * (per {@link io.varve.swath.engine.policy.HybridSeedPlanner}'s own javadoc), so two independent
     * runs must produce byte-identical cut sets and decision traces.
     */
    @Test
    @Timeout(60)
    void seedDescentIsDeterministicAcrossRepeatedRuns() throws Exception {
        List<byte[]> keyspace = wideNodeBehindDeadEndSiblings();

        RunMetrics metrics1 = new RunMetrics(new SimpleMeterRegistry());
        List<NodeSpec> specs1 = SeedSteps.of(MockPageFetcher.builder().keys(keyspace).build(),
                        NO_PREFIX, 3, metrics1, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        RunMetrics metrics2 = new RunMetrics(new SimpleMeterRegistry());
        List<NodeSpec> specs2 = SeedSteps.of(MockPageFetcher.builder().keys(keyspace).build(),
                        NO_PREFIX, 3, metrics2, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs2).usingRecursiveFieldByFieldElementComparator().isEqualTo(specs1);
        assertThat(metrics2.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed().decisions())
                .as("the per-level decision trace (including this run's yield-quota cutoffs) is a pure "
                        + "function of the probe outcomes, never of wall-clock/thread scheduling")
                .usingRecursiveFieldByFieldElementComparator()
                .isEqualTo(metrics1.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed().decisions());
    }

    /**
     * The rejected round-robin/cover-then-rank designs fail because they hand a bottomless chain
     * (mints a new depth every step) a share of the budget proportional to how many depths it manages
     * to mint. The yield quota must not do this either — and it structurally can't: a chain that
     * carries exactly ONE queued entry per depth never accumulates {@code YIELD_WINDOW} (4) probes AT
     * ONE depth, so its judging window never fills and {@code SEED.yield_quota_cutoff} never fires for
     * it, whatever the chain's length. (A depth that mixes a chain's own continuation with OTHER,
     * unrelated dead-end siblings — as {@code
     * SeedDescentRightmostChainDoesNotStarveWideNonLastSiblingTest}'s {@code aaa/} partition does at
     * {@code zzz/}'s depth — is a different, correctly-engaging case: those extra siblings, not the
     * chain, drive that depth's window. Isolating the chain alone here is what makes this pin about
     * the chain's OWN starvation resistance specifically.)
     */
    @Test
    @Timeout(60)
    void quotaNeverEngagesOnABottomlessSingleChildChain() throws Exception {
        // A single-child-per-level chain only, deeper than the descent's own probe ceiling — nothing
        // else shares any of its depths.
        StringBuilder chain = new StringBuilder("zzz/");
        for (int d = 0; d < 40; d++) {
            chain.append("d%02d/".formatted(d));
        }
        List<byte[]> keys = List.of(b(chain + "obj"));

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(keys).build(), NO_PREFIX, 8, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED.yield_quota_cutoff", 0L))
                .as("a bottomless one-entry-per-depth chain never fills a single depth's yield window, "
                        + "so the quota must never engage for it — the rejected round-robin/"
                        + "cover-then-rank designs fail exactly this case")
                .isZero();
    }
}
