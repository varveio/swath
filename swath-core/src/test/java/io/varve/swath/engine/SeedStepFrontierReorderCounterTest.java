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
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code SEED.frontier_reordered} engagement counter unit tests.</b> {@link
 * SeedStep#collectCutPoints} picks a best-first descent frontier (a span-scored priority order,
 * maintained across every insertion — not a one-shot pre-pass) whenever {@code mass_aware_seed} is
 * ON and the top-level structure probe is not truncated. This counter records when that choice
 * actually had more than one candidate to rank — the four existing SEED counters (§5) stay silent
 * on this path. These tests pin it: it fires once per real engagement (mass-aware ON, non-truncated
 * top, frontier &gt; 1 entry at the point the descent loop starts) and stays silent with the toggle
 * OFF on the identical fixture.
 */
final class SeedStepFrontierReorderCounterTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final String FRONTIER_REORDERED = "frontier_reordered";

    /**
     * A narrow, non-truncated top with {@code n} sibling directories, each holding one object —
     * multiple expandable frontier entries at the top level, well under both the 1000-CommonPrefixes
     * truncation threshold and {@code targetSeeds}, so the descent loop's reorder call sees a
     * frontier size {@code > 1}.
     */
    private static List<byte[]> narrowTopManySiblings(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("dir%03d/obj".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static EngineToggles massAware(String value) throws Exception {
        return EngineToggles.parse(List.of("mass_aware_seed=" + value), false);
    }

    @Test
    void massAwareOn_nonTruncatedTop_multiEntryFrontier_firesFrontierReordered() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(narrowTopManySiblings(10)).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs).as("sanity: the fixture actually seeds ranges").isNotEmpty();
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + FRONTIER_REORDERED, 0L))
                .as("mass-aware ON + non-truncated top + multi-entry frontier must fire "
                        + "SEED.frontier_reordered")
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    void massAwareOff_sameFixture_doesNotFireFrontierReordered() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(narrowTopManySiblings(10)).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, massAware("off"))
                .seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs).as("sanity: the fixture actually seeds ranges").isNotEmpty();
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + FRONTIER_REORDERED, 0L))
                .as("toggle OFF must never fire SEED.frontier_reordered")
                .isZero();
    }

    @Test
    void massAwareOn_singleEntryFrontier_doesNotFireFrontierReordered() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // A single top-level directory: the frontier never exceeds 1 entry, so the reorder is a
        // no-op and must not be counted as an engagement.
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(narrowTopManySiblings(1)).build();

        SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, massAware("on")).seedSpecs(1L, SeedMode.SHALLOW);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + FRONTIER_REORDERED, 0L))
                .as("a <=1-entry frontier reorder is a no-op and must not fire the counter")
                .isZero();
    }

    /**
     * A single top-level directory ({@code only/}) whose ONE child fans into three siblings
     * ({@code only/a/}, {@code only/b/}, {@code only/c/}) — the frontier starts at exactly 1 entry
     * (so the descent loop is entered) but EXPANDS to 3 the moment {@code only/} itself is probed.
     * The re-gated check (queue size {@code > 1} at ANY poll, not just before the loop starts) must
     * still fire here — the original check (frontier size measured once, before the loop, when it
     * was still 1) would have missed this engagement entirely even though the priority order
     * genuinely has three candidates to rank on the very next poll.
     */
    private static List<byte[]> singleTopDirExpandsMidDescent() {
        return List.of(
                "only/a/obj".getBytes(StandardCharsets.UTF_8),
                "only/b/obj".getBytes(StandardCharsets.UTF_8),
                "only/c/obj".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void massAwareOn_singleEntryFrontierThatExpandsMidDescent_firesFrontierReordered() throws Exception {
        int workers = 64;
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(singleTopDirExpandsMidDescent()).build();

        List<NodeSpec> specs = SeedSteps.of(fetcher, NO_PREFIX, workers, metrics, massAware("on"))
                .seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs).as("sanity: the fixture actually seeds ranges").isNotEmpty();
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("SEED." + FRONTIER_REORDERED, 0L))
                .as("a frontier that starts with a single entry but expands to a real choice mid-descent "
                        + "must still fire SEED.frontier_reordered — checked at every poll, not just before "
                        + "the loop starts")
                .isGreaterThanOrEqualTo(1L);
    }
}
