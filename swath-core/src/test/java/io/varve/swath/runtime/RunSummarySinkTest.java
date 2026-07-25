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
import io.varve.swath.engine.RetryConfig;
import io.varve.swath.error.CancelledException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.StopReason;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The terminal-summary sink seam: a presentation layer installed via {@link
 * RunMetrics#setSummarySink} sees every run's terminal {@link RunSummary} — the completed ones
 * from the lifecycle epilogue, the unwound ones from its teardown — without any {@code
 * ListRunner.run*} signature carrying it.
 */
final class RunSummarySinkTest {

    private static RunKey textKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "sink-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** One captured sink invocation — the three objects the sink is handed. */
    private record Emission(RunSummary summary, RunMetrics.RunDiagnostics diagnostics,
                            JsonRunSummaryWriter.TerminalStatus status) {
    }

    @Test
    @Timeout(30)
    void completedRunHandsTheTerminalSummaryToTheInstalledSink(@TempDir Path dir) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(1500);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        List<Emission> emissions = new ArrayList<>();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunContext ctx = RunContext.create();
            ctx.metrics().setSummarySink((summary, diagnostics, status) ->
                    emissions.add(new Emission(summary, diagnostics, status)));

            ListingStatistics stats = new ListRunner().runWorkStealing(
                    ctx, fetcher, new StringWriter(), spec(), store, run.id(), 4, seeds);

            assertThat(emissions).hasSize(1);
            Emission emission = emissions.getFirst();
            assertThat(emission.status().reason())
                    .as("a clean run is COMPLETED, the same disposition the JSON report records")
                    .isEqualTo(StopReason.COMPLETED);
            assertThat(emission.summary().objects()).isEqualTo(stats.objects());
            assertThat(emission.summary().apiCalls()).isEqualTo(stats.apiCalls());
            assertThat(emission.diagnostics().runId()).isEqualTo(emission.summary().runId());
        }
    }

    @Test
    @Timeout(30)
    void unwoundRunStillEmitsWithItsAttributedStopReason(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();
        List<Emission> emissions = new ArrayList<>();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunContext ctx = RunContext.create();
            ctx.metrics().setSummarySink((summary, diagnostics, status) ->
                    emissions.add(new Emission(summary, diagnostics, status)));
            ctx.cancellation().cancel(StopReason.SIGNAL);

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, new StringWriter(), spec(), store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);

            assertThat(emissions).hasSize(1);
            assertThat(emissions.getFirst().status().reason())
                    .as("an interrupted run reaches the sink through the lifecycle teardown, "
                            + "carrying the reason the JSON sidecar's partial record carries")
                    .isEqualTo(StopReason.SIGNAL);
        }
    }

    /**
     * The epilogue's disposition and the sidecar's terminal record are ONE decision. A cancel that
     * lands after the pipeline has drained and the completion chain has committed — here, from the
     * publisher that runs in the completion slot — is too late to change what the run was: the
     * sidecar writes {@code completed:true} and the process exits 0, so the block must not print
     * {@code INCOMPLETE (signal)} over a report that says completed.
     */
    @Test
    @Timeout(30)
    void aCancelLandingAfterTheDrainLeavesBothSurfacesSayingCompleted(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();
        List<Emission> emissions = new ArrayList<>();
        Path report = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunContext ctx = RunContext.create();
            ctx.metrics().setSummarySink((summary, diagnostics, status) ->
                    emissions.add(new Emission(summary, diagnostics, status)));
            OutputPublisher lateSignal = () -> ctx.cancellation().cancel(StopReason.SIGNAL);

            new ListRunner().runWorkStealing(ctx, fetcher, new StringWriter(),
                    spec().withJsonSummary(jsonSummaryConfig(report)), store, run.id(), 4, seeds,
                    EngineToggles.DEFAULT, TraceSink.NONE, RetryConfig.DEFAULT, lateSignal);

            assertThat(emissions).hasSize(1);
            assertThat(emissions.getFirst().status().reason()).isEqualTo(StopReason.COMPLETED);
            JsonNode written = new ObjectMapper().readTree(report.toFile());
            assertThat(written.get("completed").asBoolean()).isTrue();
            assertThat(written.get("stop_reason").asText()).isEqualTo(StopReason.COMPLETED.wireName());
        }
    }

    /**
     * The unwound path takes ONE snapshot: the block and the sidecar's {@code completed:false}
     * partial report the same {@code duration_ms} and the same {@code stop_reason}, rather than
     * each reading the counters a moment apart.
     */
    @Test
    @Timeout(30)
    void anUnwoundRunReportsOneSnapshotToBothSurfaces(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();
        List<Emission> emissions = new ArrayList<>();
        Path report = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(textKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunContext ctx = RunContext.create();
            ctx.metrics().setSummarySink((summary, diagnostics, status) ->
                    emissions.add(new Emission(summary, diagnostics, status)));
            ctx.cancellation().cancel(StopReason.SIGNAL);

            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    ctx, fetcher, new StringWriter(), spec().withJsonSummary(jsonSummaryConfig(report)),
                    store, run.id(), 4, seeds))
                    .isInstanceOf(CancelledException.class);

            assertThat(emissions).hasSize(1);
            Emission emission = emissions.getFirst();
            JsonNode written = new ObjectMapper().readTree(report.toFile());
            assertThat(written.get("completed").asBoolean()).isFalse();
            assertThat(written.get("stop_reason").asText())
                    .isEqualTo(emission.status().reason().wireName());
            assertThat(written.get("duration_ms").asLong())
                    .as("one snapshot feeds the block and the report, so neither can be fresher")
                    .isEqualTo(emission.summary().duration().toMillis());
            assertThat(written.get("objects").asLong()).isEqualTo(emission.summary().objects());
        }
    }

    private static JsonRunSummaryWriter.Config jsonSummaryConfig(Path path) {
        return new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "sink-hash",
                new JsonRunSummaryWriter.RunConfig("s3://bucket/", null, "jsonl", 4, false, null,
                        30_000L, List.of(), EngineToggles.DEFAULT, null, false, null, false),
                List.of("list", "s3://bucket/"));
    }

    private static ListRunner.Spec spec() {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                8000, 1000, FilterChain.EMPTY, null, null);
    }
}
