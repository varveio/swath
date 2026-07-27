/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.JsonlFormatter;
import io.varve.swath.output.OutputStage;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Where the per-page CLIENT-SERVICE-COST spans are recorded, pinned against a real run of the
 * engine rather than against {@code RunMetrics} in isolation: a page's durable-commit wait comes
 * from the fetch worker's own await in {@link WorkStealingScan}, the checkpoint queue wait and batch
 * commit from the single-writer checkpoint thread, the emit span from the consumer stage's sink
 * write, and the writer-backpressure wait from the worker's handoff onto a full channel.
 *
 * <p>{@link io.varve.swath.observability.RunMetricsClientCostSpanTest} pins the readback shape; this
 * pins that each span is actually wired to its own site, so an engine change that stops paying (or
 * stops timing) one of them is caught here instead of showing up as a silently empty row.
 */
final class ClientCostSpanRecordingTest {

    private static final int PAGE = 4;
    private static final int KEYS = 40;
    /** One slot: the worker must block handing a page over, so the backpressure span is non-trivial. */
    private static final int CHANNEL_SLOTS = 1;
    /** One worker, owner-split off: the node stays whole, so page count is exactly {@code KEYS/PAGE}. */
    private static final long PAGES = KEYS / PAGE;

    private static byte[] key(int i) {
        return String.format("data/%05d", i).getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "client-cost-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(60)
    void everyClientCostSpanIsRecordedByTheRunThatPaidIt(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            keyspace.add(key(i));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunContext ctx = RunContext.create();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("client-cost.sqlite"), ctx.metrics())) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, ctx.metrics())
                            .withToggles(EngineToggles.DEFAULT.withOwnerSplit(false)),
                    fetcher, store, 1, PAGE, seeds, FilterChain.EMPTY);

            StringWriter out = new StringWriter();
            OutputStage output = new OutputStage(new JsonlFormatter(out));
            new Pipeline<PageBatch>(CHANNEL_SLOTS).run(ctx, engine, output);

            assertThat(out.toString().lines().count()).isEqualTo(KEYS);
        }

        Map<String, RunSummary.ClientCostSpan> spans =
                ctx.metrics().summary(Duration.ofSeconds(1), "work_stealing", 0L, 0L).clientCost().stream()
                        .collect(Collectors.toMap(RunSummary.ClientCostSpan::span, Function.identity()));

        assertThat(spans).containsKeys(
                RunMetrics.CLIENT_COST_SPAN_COMMIT_WAIT,
                RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_QUEUE_WAIT,
                RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_COMMIT,
                RunMetrics.CLIENT_COST_SPAN_EMIT,
                RunMetrics.CLIENT_COST_SPAN_WRITER_BACKPRESSURE);
        // Exactly one observation per page for each of the three PER-PAGE spans -- page-scoped, not
        // run-scoped and not batch-scoped.
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_EMIT).count()).isEqualTo(PAGES);
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_COMMIT_WAIT).count()).isEqualTo(PAGES);
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_WRITER_BACKPRESSURE).count()).isEqualTo(PAGES);
        // The two writer-thread spans are per-TASK and per-BATCH: the queue carries the run's
        // lifecycle writes (open/insert/complete) alongside the page commits, and a batch coalesces
        // whatever it drained -- so neither is page-scoped, and both are bounded below, not pinned.
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_QUEUE_WAIT).count())
                .isGreaterThanOrEqualTo(PAGES);
        assertThat(spans.get(RunMetrics.CLIENT_COST_SPAN_CHECKPOINT_COMMIT).count()).isPositive();
    }
}
