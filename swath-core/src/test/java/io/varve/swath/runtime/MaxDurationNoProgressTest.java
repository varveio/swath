/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

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
import java.util.concurrent.atomic.AtomicBoolean;
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
        // "The first bulk page succeeded" is a property of THIS fetcher, not of the global call
        // index: a thief's 1-key pivot probe / delimiter structure probe also consumes call indices,
        // so `idx > 0` does not mean "a bulk page already went out". A CAS on the first bulk page
        // says exactly what is meant, and — because the winner is never gated below — cannot deadlock.
        AtomicBoolean firstBulkPageServed = new AtomicBoolean();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(500))
                .maxKeysCap(50)
                .interceptor((req, idx, page) -> {
                    // Let the first bulk page succeed (real progress), then cancel.
                    //
                    // The cancel must not fire until that page's objects are COUNTED, which happens
                    // on the OUTPUT stage (OutputStage#writeBatch -> recordEntriesEmitted), several
                    // hand-offs downstream of this fetch: the owner still has to await its durable
                    // page commit and hand the batch to the channel. Cancelling as soon as a SECOND
                    // page was fetched raced all of that — an idle worker sees the token first, its
                    // CancelledException fails the engine's Scope, and shutdownNow() interrupts the
                    // owner while it is still parked in awaitCommit, so the first page's keys never
                    // reach the output stage and the run reports objects=0 (i.e. the summary refines
                    // to max_duration_no_progress and this test's own premise evaporates).
                    //
                    // So gate on the run's own committed-progress reading — sessionObjectsEmitted(),
                    // the EXACT quantity ListRunner#attributedStatus keys the max_duration /
                    // max_duration_no_progress split on. It is monotonic, so once it is positive the
                    // classification and the assertions below are pinned no matter how the workers
                    // interleave afterwards. Holding this fetch open until then also keeps the run
                    // from finishing before the cancel lands. Bounded (a failsafe, not a timing
                    // assumption): a run that never emits fails here loudly instead of hanging.
                    if (req.maxKeys() > 1 && !firstBulkPageServed.compareAndSet(false, true)) {
                        await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
                                .atMost(Duration.ofSeconds(30))
                                .until(() -> ctx.metrics().sessionObjectsEmitted() > 0L);
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
