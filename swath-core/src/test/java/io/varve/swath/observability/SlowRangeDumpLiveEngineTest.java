/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.sort.SortArm;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageGate;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the REAL {@code WorkStealingScan#snapshotLiveRanges()} wiring
 * (not a hand-built {@code RangeSnapshot}, as {@link SlowRangeDumpTest} does) against a genuinely
 * live open-frontier range, and proves the JSON writer renders its {@code est_remaining} as {@code
 * null} — never the non-finite double's Jackson default of the literal string {@code "Infinity"}
 * (verified empirically: {@code new ObjectMapper().createObjectNode().put("x",
 * Double.POSITIVE_INFINITY)} round-trips to {@code {"x":"Infinity"}}, a {@code TextNode}, on the
 * exact Jackson version this module depends on). Single worker, no thief (workerCount=1): the root
 * seed {@code (⊥, ⊤]} stays an open frontier ({@code hi == null}) for the worker's entire lifetime
 * — nothing ever splits it — so {@code StealMath#estRemaining} returns {@code
 * Double.POSITIVE_INFINITY} for as long as the range is live, exactly the shape that sorts to the
 * TOP of {@code slow_ranges[]} in production.
 */
final class SlowRangeDumpLiveEngineTest {

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "slow-range-live-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static JsonRunSummaryWriter.Config summaryConfig(Path path) {
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "jsonl", 1, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false);
        return new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "hash", runConfig,
                List.of("list", "s3://bucket"));
    }

    @Test
    @Timeout(30)
    void openFrontierLiveRangeRendersEstRemainingAsJsonNullNotTheStringInfinity(@TempDir Path dir) throws Exception {
        // Gate the SECOND bulk (maxKeys>1) page fetch, so the worker has already committed a
        // real, non-trivial first page (a genuine finite drain_rate) before parking in-flight with
        // its range STILL an open frontier (hi == null — a single worker with no thief never splits
        // it) and STILL live in WorkStealingScan#livePool.
        AtomicInteger bulkCalls = new AtomicInteger();
        PageGate gate = new PageGate(req -> req.maxKeys() > 1 && bulkCalls.incrementAndGet() >= 2);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.flatRandom(1L, 5_000))
                .interceptor(gate.interceptor())
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thread runnerThread;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, 1, 1000, seeds, FilterChain.EMPTY);

            runnerThread = new Thread(() -> {
                try {
                    PipelineDrain.discard(1000, engine);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "slow-range-live-engine-runner");
            runnerThread.setDaemon(true);
            runnerThread.start();

            // Block until the worker has committed page 1 and is parked in-flight fetching page 2 —
            // the range is still live (in livePool), open-frontier (hi == null), with keysEmitted > 0.
            gate.awaitFetched();
            try {
                RunSummary midRunSummary = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 0L, 0L);
                assertThat(midRunSummary.slowRanges()).as("the live open-frontier range is dumped").hasSize(1);
                RunSummary.SlowRange live = midRunSummary.slowRanges().get(0);
                assertThat(live.hi()).as("open frontier renders as the shared <none> display sentinel")
                        .isEqualTo("<none>");
                assertThat(live.drainRate()).as("a real, finite, positive drain rate from the committed page")
                        .isGreaterThan(0.0).isFinite();

                // Run this through JsonRunSummaryWriter for real, not just RunMetrics, and read the
                // JSON back.
                Path sidecar = dir.resolve("summary.json");
                JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                        summaryConfig(sidecar), metrics.registry(), Instant.now(), () -> midRunSummary);
                try {
                    writer.complete(midRunSummary);
                } finally {
                    writer.close();
                }
                JsonNode root = new ObjectMapper().readTree(sidecar.toFile());
                JsonNode slowRanges = root.get("slow_ranges");
                assertThat(slowRanges.isArray()).isTrue();
                assertThat(slowRanges).hasSize(1);
                JsonNode estRemaining = slowRanges.get(0).get("est_remaining");
                assertThat(estRemaining.isNull())
                        .as("est_remaining for an open-frontier (+Infinity) range must be JSON null, "
                                + "never the non-finite double's Jackson-default string \"Infinity\"")
                        .isTrue();
                assertThat(slowRanges.get(0).get("drain_rate").isNumber())
                        .as("drain_rate is a genuine finite number, unaffected by the est_remaining fix")
                        .isTrue();
            } finally {
                gate.release();
            }
        }
        runnerThread.join(10_000);
        assertThat(runnerThread.isAlive()).as("the gated worker resumed and the engine reached quiescence").isFalse();
    }
}
