/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>Guard: {@code frontier_level_ordered} must not fire on a frontier that only ever holds ONE
 * depth at a time.</b>
 *
 * <p>{@link SeedStep}'s {@code SpanPriorityFrontier} used to track {@code minDepthOffered}/{@code
 * maxDepthOffered} across the WHOLE descent to decide {@code spansMultipleDepths()}. A single-child
 * directory chain ({@code a/b/c/d/leaf.obj}) offers depth 1, then — once depth 1 is polled and
 * drained — offers depth 2, then depth 3, and so on: at any instant the frontier holds exactly ONE
 * entry, so the priority queue's depth key never actually decides between two candidates. But
 * historically-ever-offered depths climb 1, 2, 3, ... regardless, so the old check reported a
 * cross-level engagement that never happened — {@code frontier_level_ordered} fired on a run where
 * level ordering was strictly a no-op. The fix tracks CURRENTLY-queued depths (incremented on
 * {@code offer}, decremented/removed on {@code poll}), so {@code spansMultipleDepths()} is true only
 * while more than one depth is queued at once.
 */
final class SeedFrontierLevelOrderFalsePositiveTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 8;

    /** A strict single-child chain: each directory has exactly one sub-directory, so the frontier
     *  never holds more than one entry at a time and depth ordering never has a real choice to make. */
    private static List<byte[]> singleChildChain(int chainDepth) {
        StringBuilder path = new StringBuilder();
        for (int d = 0; d < chainDepth; d++) {
            path.append("lvl").append(d).append('/');
        }
        return List.of((path + "leaf.obj").getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @Timeout(60)
    void uniformDepthAtEveryInstantNeverFiresLevelOrdered() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        SeedSteps.of(MockPageFetcher.builder().keys(singleChildChain(6)).build(),
                        NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();

        assertThat(reasons.getOrDefault("SEED.frontier_level_ordered", 0L))
                .as("a single-child chain never queues more than one depth at once, so the level key "
                        + "never decides anything -- frontier_level_ordered must not fire even though "
                        + "depths 1..6 were each offered at some point over the course of the descent")
                .isZero();
    }
}
