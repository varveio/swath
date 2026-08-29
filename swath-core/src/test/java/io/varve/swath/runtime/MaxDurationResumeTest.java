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
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CancelledException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.StopReason;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A timebox stop cancels a mid-flight work-stealing
 * Parquet run cleanly (via {@link DeadlineCanceller}, the same seam the CLI arms), leaving
 * the checkpoint resumable; the first run's sidecar reads {@code stop_reason=max_duration,
 * completed:false}; the resumed run finishes and its sidecar reads {@code
 * stop_reason=completed}; and the byte-sorted union of both runs' Parquet parts equals a
 * clean non-timeboxed run over the same keyspace (no gaps, no duplicates).
 */
// Tagged at METHOD granularity, NOT class-level (this class has a single method). The one method is a
// wall-clock deadline test (arms a real 120ms DeadlineCanceller against a ~768ms fetch workload), so
// it is `@Tag("deep")`. The --max-duration deadline contract stays guarded per-commit by
// DeadlineCancellerTest + MaxDurationNoProgressTest.
final class MaxDurationResumeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    // 1024 pages * 3ms / 4 workers ~= 768ms of total fetch work vs. a 120ms deadline (~6.4x
    // margin) — deliberately wide so a loaded CI reliably lands the cancel mid-run instead of
    // racing the run to completion. Do not shrink this margin: a 4_096/256-page sizing left only
    // a ~72ms margin over the 120ms deadline, too tight for a loaded CI.
    private static final int OBJECTS = 16_384;
    private static final int WORKERS = 4;
    private static final int MAX_KEYS = 16;
    private static final String ARGS_HASH = "max-duration-hash";

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static ListRunner.ParquetSpec spec(Path sidecar) {
        // Small parts (1 KB target) so parts finalize frequently and the timebox stop lands with
        // a durable, resumable tail. summaryConfig lands the sidecar so we can read stop_reason.
        JsonRunSummaryWriter.RunConfig rc = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket", "us-east-1", "parquet", WORKERS, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, io.varve.swath.sort.SortArm.NONE, false);
        JsonRunSummaryWriter.Config summary =
                new JsonRunSummaryWriter.Config(sidecar, Duration.ofMinutes(10), ARGS_HASH, rc,
                        List.of("list", "s3://bucket"));
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "")
                        .withJsonSummary(summary);
    }

    @Tag("deep")   // wall-clock deadline test: arms a real 120ms DeadlineCanceller mid-run
    @Test
    @Timeout(60)
    void maxDurationStopsMidFlightThenResumesByteExact(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        Path resumedDir = tmp.resolve("resumed");
        Path cleanDir = tmp.resolve("clean");
        Path db = tmp.resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);
        Path firstSidecar = resumedDir.resolve("_swath_summary.json");
        Path resumedSidecar = resumedDir.resolve("_swath_summary_resumed.json");

        List<String> resumedUnion;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            // Slow fetcher so a short deadline reliably fires mid-run.
            MockPageFetcher slow = MockPageFetcher.builder()
                    .keys(keyspace).maxKeysCap(MAX_KEYS).pageDelay(Duration.ofMillis(3)).build();

            RunContext ctx = RunContext.create();
            assertThatThrownBy(() -> {
                try (DeadlineCanceller ignored = DeadlineCanceller.arm(
                        ctx.cancellation(), Duration.ofMillis(120))) {
                    new ListRunner().runToParquetWorkStealing(
                            ctx, slow, resumedDir, spec(firstSidecar), store, run.id(),
                            WORKERS, seeds, List.of());
                }
            }).as("the timebox cancel surfaces as the contractual CancelledException")
                    .isInstanceOf(CancelledException.class);

            assertThat(ctx.cancellation().stopReason())
                    .as("the deadline attributed the cancel as max_duration")
                    .isEqualTo(StopReason.MAX_DURATION);

            JsonNode first = MAPPER.readTree(firstSidecar.toFile());
            assertThat(first.get("schema_version").asInt()).isEqualTo(2);
            assertThat(first.get("completed").asBoolean()).isFalse();
            assertThat(first.get("exit_code").isNull()).isTrue();
            assertThat(first.get("stop_reason").asText()).isEqualTo("max_duration");

            // Resume: the run stayed RUNNING (never markRunFinished(COMPLETED)), so it is resumable.
            RunMeta resumed = store.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<PartInfo> existing = reconcileParquetResume(store, resumed.id(), resumedDir);
            List<Node> resumedSeeds = store.loadResumable(resumed.id(), true);
            assertThat(resumedSeeds).as("the timebox left a resumable tail").isNotEmpty();

            MockPageFetcher fast = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fast, resumedDir, spec(resumedSidecar), store, resumed.id(),
                    WORKERS, resumedSeeds, existing);

            assertThat(store.loadResumable(resumed.id(), true))
                    .as("resumed run is now output-complete").isEmpty();

            JsonNode resumedSummary = MAPPER.readTree(resumedSidecar.toFile());
            assertThat(resumedSummary.get("completed").asBoolean()).isTrue();
            assertThat(resumedSummary.get("stop_reason").asText()).isEqualTo("completed");

            resumedUnion = allPartKeys(resumedDir);
        }

        List<String> cleanUnion = runCleanRun(tmp.resolve("clean.sqlite"), cleanDir, keyspace);
        assertExactlyEqual(resumedUnion, cleanUnion);
        assertExactlyEqual(cleanUnion, expectedKeys(keyspace));
    }

    private static List<String> runCleanRun(Path db, Path dir, List<byte[]> keyspace) throws Exception {
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(runKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec(dir.resolve("_summary.json")),
                    store, run.id(), WORKERS, seeds, List.of());
            assertThat(store.loadResumable(run.id(), true)).as("clean run is output-complete").isEmpty();
        }
        return allPartKeys(dir);
    }

    private static List<PartInfo> reconcileParquetResume(SqliteCheckpointStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> names = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, names);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<String> expectedKeys(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
    }

    private static void assertExactlyEqual(List<String> actual, List<String> expected) {
        assertThat(actual).as("no duplicate rows").doesNotHaveDuplicates();
        assertThat(actual.stream().sorted().toList())
                .as("same key set under the unsorted Parquet part contract")
                .containsExactlyElementsOf(expected.stream().sorted().toList());
    }
}
