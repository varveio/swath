/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedTiling;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * On the same saturated-shape harness {@link OwnerSplitDemandGateTest} uses (a large dense
 * flat bucket pre-tiled into many live ranges, so the owner-split demand gate genuinely engages),
 * asserts the T-vs-Tmax visibility fires at the real engine call site — {@code
 * OwnerSelfSplit#maybeOwnerSelfSplit} calling {@code RunMetrics#recordDemandGatedConcurrency}
 * alongside the {@code OWNER_SPLIT.demand_gated} steal-reason — not just the
 * {@code RunMetrics}-level unit coverage in {@code RunMetricsCallClassAndDemandGateTest}.
 */
final class OwnerSplitDemandGateConcurrencySummaryTest {

    private static final int MAX_KEYS = 100;
    private static final Duration PAGE_LATENCY = Duration.ofMillis(1);

    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static byte[] flatKey(int i) {
        return String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(120)
    void saturatedBucketPublishesDemandGateTAndTmaxToTheSummary(@TempDir Path dir) throws Exception {
        int workers = 8;
        int n = 40_000;
        int seedRanges = 8 * workers;
        List<byte[]> keyspace = denseFlat(n);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).pageDelay(PAGE_LATENCY).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("demand-gate-summary.sqlite"))) {
            RunMeta run = store.openRun(key("demand-gate-summary"), false, false);
            SeedTiling.seedTiled(store, run.id(), n, seedRanges,
                    "d/".getBytes(StandardCharsets.UTF_8), OwnerSplitDemandGateConcurrencySummaryTest::flatKey);
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.discard(2000, engine);
        }

        RunSummary summary = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 1L, 0L);
        RunSummary.DemandGateSummary dg = summary.demandGate();
        assertThat(dg).as("the saturated shape engages the demand gate at least once").isNotNull();
        assertThat(dg.events()).isGreaterThan(0L);
        assertThat(dg.tMax()).isEqualTo(workers);
        assertThat(dg.lastT()).isBetween(1, workers);
        assertThat(dg.minT()).isBetween(1, workers);
        assertThat(dg.minT()).isLessThanOrEqualTo(dg.lastT());

        Gauge lastTGauge = metrics.registry().find("swath.owner_split.demand_gated_t").gauge();
        assertThat(lastTGauge).isNotNull();
        assertThat(lastTGauge.value()).isEqualTo((double) dg.lastT());
    }
}
