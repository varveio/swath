/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CancelledException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.StopReason;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A {@code --max-duration} cancel that burns the whole timebox with ZERO objects
 * emitted (a pathological/slow bucket, or a node that only ever attempt-times-out/gets
 * throttled) must be surfaced as a distinct {@code stop_reason=max_duration_no_progress} —
 * not conflated with a legit large timeboxed {@code max_duration} partial that actually made
 * headway. Exit-code mapping is untouched ({@link ListRunner}'s cancellation-token attribution
 * stays {@link StopReason#MAX_DURATION} either way; only the JSON summary's refined
 * classification differs) — see {@code ListCommand#timeboxExitOrRethrow}, unaffected by this.
 */
final class MaxDurationNoProgressTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static RunKey textKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "no-progress-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static JsonRunSummaryWriter.Config summaryConfig(Path path) {
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "jsonl", 4, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, false);
        return new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "no-progress-hash", runConfig,
                List.of("list", "s3://bucket"));
    }

    @Test
    void maxDurationWithZeroObjectsEmitted_isDistinctFromALegitPartial(@TempDir Path dir) throws Exception {
        // Pre-cancelled before any worker claims a seed (mirrors
        // ListRunnerWorkStealingTest#textSink_preCancelledBeforeClaimEscapesAsCancelledException):
        // the deterministic analog of a node that burns the whole --max-duration timebox never
        // successfully committing a single page (e.g. permanent attempt-timeout/throttle) — zero
        // API calls, zero objects emitted.
        RunContext ctx = RunContext.create();
        ctx.cancellation().cancel(StopReason.MAX_DURATION);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(500))
                .build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, new StringWriter(), spec, store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);
        }

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("objects").asLong()).isZero();
        assertThat(root.get("stop_reason").asText()).isEqualTo("max_duration_no_progress");
    }

    @Test
    void maxDurationWithProgress_staysPlainMaxDuration(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(500))
                .maxKeysCap(50)
                .interceptor((req, idx, page) -> {
                    // Let the first bulk page succeed (real progress), then cancel.
                    if (req.maxKeys() > 1 && idx > 0) {
                        ctx.cancellation().cancel(StopReason.MAX_DURATION);
                    }
                    return page;
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            ListRunner.Spec spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 50,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, new StringWriter(), spec, store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);
        }

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("objects").asLong()).as("this run made real progress before the cancel").isPositive();
        assertThat(root.get("stop_reason").asText()).isEqualTo("max_duration");
    }
}
