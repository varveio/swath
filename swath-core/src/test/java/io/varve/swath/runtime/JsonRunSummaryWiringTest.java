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
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.StopReason;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end (INT, {@code MockPageFetcher} harness): the JSON
 * run-summary sidecar is threaded through {@link ListRunner} for both a Parquet
 * sink and a text sink and lands as a {@code completed:true} artifact whose
 * {@code objects} count matches the run.
 */
class JsonRunSummaryWiringTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonRunSummaryWriter.Config summaryConfig(Path path) {
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-1", "parquet", 64, false, null, 30_000L, List.of(),
                EngineToggles.DEFAULT, null, false, null, false);
        return new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "argshash123", runConfig,
                List.of("list", "s3://bucket/prefix"));
    }

    @Test
    void parquetRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path dir) throws Exception {
        int n = 2_345;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path sidecar = dir.resolve("_swath_summary.json");

        var spec = new ListRunner.ParquetSpec(new byte[0], 16, 1000, FilterChain.EMPTY,
                3, 128 * 1024, 16, "argshash123", null, null, 0L, 0L, "")
                        .withJsonSummary(summaryConfig(sidecar));
        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);

        assertThat(stats.objects()).isEqualTo(n);
        assertThat(sidecar).exists();
        assertThat(sidecar.resolveSibling("_swath_summary.json.tmp")).doesNotExist();

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("schema_version").asInt()).isEqualTo(2);
        assertThat(root.get("completed").asBoolean()).isTrue();
        assertThat(root.get("exit_code").asInt()).isZero();
        assertThat(root.get("stop_reason").asText()).isEqualTo("completed");
        assertThat(root.get("objects").asLong()).isEqualTo(n);
        assertThat(root.get("output").get("files").asLong()).isPositive();
        JsonNode meters = root.get("meters");
        assertThat(meters.isArray()).isTrue();
        // Tail-occupancy screen (last-5%/10% window avg-in-flight + wall-time share) -- both
        // meter names, each tagged pct=5|10, must be present after any real scan, AND -- since
        // these 4 gauges register unconditionally regardless of whether the sampler ever actually
        // recorded a sample -- must carry a real (non-null) numeric value on a real completed run,
        // not just a null placeholder.
        List<JsonNode> avgInFlightMeters = tailOccupancyMeters(meters, "swath.tail_occupancy.avg_in_flight");
        List<JsonNode> wallShareMeters = tailOccupancyMeters(meters, "swath.tail_occupancy.wall_share");
        assertThat(tailOccupancyPcts(avgInFlightMeters)).containsExactlyInAnyOrder("5", "10");
        assertThat(tailOccupancyPcts(wallShareMeters)).containsExactlyInAnyOrder("5", "10");
        for (JsonNode m : avgInFlightMeters) {
            assertThat(m.get("value").isNumber())
                    .as(m.get("tags").toString() + " avg_in_flight must be a real value, not null").isTrue();
        }
        for (JsonNode m : wallShareMeters) {
            assertThat(m.get("value").isNumber())
                    .as(m.get("tags").toString() + " wall_share must be a real value, not null").isTrue();
        }
        // A clean completed run attributes no cancel, so both are absent/null.
        assertThat(root.get("stop_source").isNull()).isTrue();
        assertThat(root.get("error_class").isNull()).isTrue();
    }

    /** Every {@code meters[]} entry whose {@code name} matches. */
    private static List<JsonNode> tailOccupancyMeters(JsonNode meters, String name) {
        List<JsonNode> matches = new ArrayList<>();
        for (JsonNode m : meters) {
            if (m.get("name").asText().equals(name)) {
                matches.add(m);
            }
        }
        return matches;
    }

    /** The {@code pct} tag value off each of the given {@code meters[]} entries. */
    private static List<String> tailOccupancyPcts(List<JsonNode> meters) {
        List<String> pcts = new ArrayList<>();
        for (JsonNode m : meters) {
            pcts.add(m.get("tags").get("pct").asText());
        }
        return pcts;
    }

    @Test
    void textRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path dir) throws Exception {
        int n = 500;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path sidecar = dir.resolve("summary.json");

        var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                FilterChain.EMPTY, null, summaryConfig(sidecar));
        StringWriter out = new StringWriter();
        var stats = new ListRunner().run(RunContext.create(), fetcher, out, spec);

        assertThat(stats.objects()).isEqualTo(n);
        assertThat(sidecar).exists();

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isTrue();
        assertThat(root.get("objects").asLong()).isEqualTo(n);
        assertThat(root.get("output").get("files").asLong()).isEqualTo(1L);
    }

    /**
     * {@code jsonWriter} closes in an outer {@code finally}, not the {@code
     * try-with-resources} list, so it stays open past where {@code finish(...)} would run.
     * This guards the partial path specifically: when
     * the fetcher throws mid-run, {@link ListRunner#run} never reaches {@code
     * finish(jsonWriter, ...)} (which calls {@code complete()}), so the sidecar must
     * still land on disk — via {@code close()}'s partial fallback — with {@code
     * completed:false} instead of being silently dropped.
     */
    @Test
    void partialRunLeavesACompletedFalseSidecarOnDisk(@TempDir Path dir) throws Exception {
        // maxKeys=1000 forces multiple pages over a 2500-key space, so the callIndex==1
        // failure is a genuine mid-run crash: the first page's rows are already emitted
        // before the fetcher throws on the second page.
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2_500))
                .interceptor((req, callIndex, page) -> {
                    if (callIndex == 1) {
                        throw new ListingException("injected mid-run failure");
                    }
                    return page;
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                FilterChain.EMPTY, null, summaryConfig(sidecar));
        StringWriter out = new StringWriter();

        assertThatThrownBy(() -> new ListRunner().run(RunContext.create(), fetcher, out, spec))
                .isInstanceOf(SwathException.class);

        assertThat(sidecar).exists();
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("exit_code").isNull()).isTrue();
        assertThat(root.get("stop_reason").asText())
                .as("a mid-run exception is a crash").isEqualTo("crash");
    }

    /**
     * A {@code BOUNDED} transient-retry-cap STUCK stop
     * carries {@code stop_source=transient_retry_cap} AND the fetch-local {@code error_class} — the
     * SAME pair {@code ListCommand}'s {@code list_stuck_stop} marker prints, derived via the one
     * shared {@code RunMetrics#stuckErrorClass} helper. Mirrors {@code
     * SequentialPathStuckCancellationTest}'s cancel-then-throw shape: the interceptor wins the
     * token's attribution (recording its own local fault tally per {@code
     * RunMetrics#recordTransientRetryCapExhaustion}'s discipline), then throws so the pipeline
     * converts the resulting cooperative cancel into {@link CancelledException}.
     */
    @Test
    void terminalPartialForTransientRetryCapStuckCarriesStopSourceAndFetchLocalErrorClass(
            @TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(10))
                .interceptor((req, callIndex, page) -> {
                    boolean won = token.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP);
                    if (won) {
                        // Dominated by attempt-timeouts (9 vs. 0 voting) — the cap-tripping fetch's
                        // OWN local fault history, independent of any run-wide window.
                        ctx.metrics().recordTransientRetryCapExhaustion(9, 0);
                    }
                    throw ThrottleException.attemptTimeout("wedged");
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                FilterChain.EMPTY, null, summaryConfig(sidecar));
        StringWriter out = new StringWriter();

        assertThatThrownBy(() -> new ListRunner().run(ctx, fetcher, out, spec))
                .isInstanceOf(CancelledException.class);

        assertThat(sidecar).exists();
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("stuck");
        assertThat(root.get("stop_source").asText()).isEqualTo("transient_retry_cap");
        assertThat(root.get("error_class").asText()).isEqualTo("stuck_api_timeouts");
    }

    /**
     * A liveness-watchdog-attributed STUCK stop carries {@code
     * stop_source=liveness_watchdog} and the run-wide WINDOWED {@code error_class} (the watchdog's
     * own trip condition IS a global freeze, so {@code RunMetrics#classifyStuckErrorClass}'s
     * since-last-real-progress window is the honest signal — see that method's javadoc). Here a real
     * watchdog is not armed; the token is cancelled directly with {@code CancelSource
     * .LIVENESS_WATCHDOG} to pin the summary-writer wiring in isolation, the same way {@code
     * SequentialPathStuckCancellationTest} pins the STUCK→CancelledException conversion.
     */
    @Test
    void terminalPartialForLivenessWatchdogStuckCarriesStopSourceAndWindowedErrorClass(
            @TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(10))
                .interceptor((req, callIndex, page) -> {
                    // A sustained real-503/5xx storm (voting), dominating the run-wide window —
                    // classifyStuckErrorClass()'s STUCK_MIN_EVENTS floor is 8.
                    for (int i = 0; i < 9; i++) {
                        ctx.metrics().recordThrottleEvent(ThrottleType.SERVER_5XX);
                    }
                    token.cancel(StopReason.STUCK, CancelSource.LIVENESS_WATCHDOG);
                    throw ThrottleException.serverError("wedged");
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                FilterChain.EMPTY, null, summaryConfig(sidecar));
        StringWriter out = new StringWriter();

        assertThatThrownBy(() -> new ListRunner().run(ctx, fetcher, out, spec))
                .isInstanceOf(CancelledException.class);

        assertThat(sidecar).exists();
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("stuck");
        assertThat(root.get("stop_source").asText()).isEqualTo("liveness_watchdog");
        assertThat(root.get("error_class").asText()).isEqualTo("stuck_throttle");
    }

    /**
     * Guards the close-time broken-pipe path: unlike {@link BrokenPipeWriter} (which fails
     * inside {@code write()}, so {@code consume()} sets {@code brokenPipe} itself), {@link
     * BreakOnFlushWriter} lets every {@code write()} succeed and only breaks the pipe during
     * the final {@code formatter.close()} flush. {@code ListRunner.closeQuietly} must route
     * that close-time exception through {@code OutputStage.markBrokenPipe()}, not swallow it,
     * or a truncated run reports {@code completed:true}.
     */
    @Test
    void runOnCloseTimeBrokenPipeLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        int n = 100;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path sidecar = dir.resolve("summary.json");

        var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                FilterChain.EMPTY, null, summaryConfig(sidecar));
        var stats = new ListRunner().run(RunContext.create(), fetcher, new BreakOnFlushWriter(), spec);

        // All 100 entries were actually written (unlike the write-time-break case, where
        // stats.objects() is 0) — the break only happens on the terminal flush/close.
        assertThat(stats.objects()).isEqualTo(n);
        assertThat(sidecar).exists();
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean())
                .as("close-time broken pipe must not finalize the sidecar as completed:true")
                .isFalse();
        assertThat(root.get("exit_code").isNull()).isTrue();
        assertThat(root.get("stop_reason").isNull())
                .as("a clean broken-pipe termination is neither a crash nor a signal").isTrue();
    }

    // ---- checkpointed text (runCheckpointed) -----------------------------------

    private static RunKey checkpointedTextKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ck-text-hash",
                "SEQUENTIAL", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    void checkpointedTextRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path dir) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(500);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(checkpointedTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            StringWriter out = new StringWriter();
            var stats = new ListRunner().runCheckpointed(
                    RunContext.create(), fetcher, out, spec, store, run.id(), node);

            assertThat(stats.objects()).isEqualTo(keys.size());
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isTrue();
            assertThat(root.get("objects").asLong()).isEqualTo(keys.size());
        }
    }

    @Test
    void checkpointedTextRunOnMidRunThrowLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2_500))
                .interceptor((req, callIndex, page) -> {
                    if (callIndex == 1) {
                        throw new ListingException("injected mid-run failure");
                    }
                    return page;
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(checkpointedTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            StringWriter out = new StringWriter();

            assertThatThrownBy(() -> new ListRunner().runCheckpointed(
                    RunContext.create(), fetcher, out, spec, store, run.id(), node))
                    .isInstanceOf(SwathException.class);

            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    @Test
    void checkpointedTextRunOnBrokenPipeLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(100)).build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(checkpointedTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            var stats = new ListRunner().runCheckpointed(
                    RunContext.create(), fetcher, new BrokenPipeWriter(), spec, store, run.id(), node);

            assertThat(stats.objects()).isZero();
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean())
                    .as("truncated stdout must not finalize the sidecar as completed:true")
                    .isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    /**
     * Close-time broken pipe under the checkpointed path — same mechanism as {@link
     * #runOnCloseTimeBrokenPipeLeavesACompletedFalseSidecar}: {@link BreakOnFlushWriter}'s
     * {@code write()} always succeeds, so the pipe only breaks when {@code
     * EntryFormatter.close()} flushes the buffered tail. Here the close-time exception must
     * also mark the checkpoint run FAILED, not just the sidecar {@code completed:false}.
     */
    @Test
    void checkpointedTextRunOnCloseTimeBrokenPipeLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        int n = 100;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path sidecar = dir.resolve("summary.json");
        Path dbPath = dir.resolve("c.sqlite");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dbPath)) {
            RunMeta run = store.openRun(checkpointedTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            var stats = new ListRunner().runCheckpointed(
                    RunContext.create(), fetcher, new BreakOnFlushWriter(), spec, store, run.id(), node);

            // Unlike BrokenPipeWriter (write() throws, stats.objects()==0), every entry was
            // actually written here — proving this exercises the close/flush-time break, not
            // the write-time one already covered above.
            assertThat(stats.objects()).isEqualTo(n);

            SqliteCheckpointStore.RunIdentity identity = SqliteCheckpointStore.readLatestRun(dbPath).orElseThrow();
            assertThat(identity.status())
                    .as("close-time broken pipe must still mark the checkpoint run FAILED, not COMPLETED")
                    .isEqualTo(RunStatus.FAILED);
        }

        assertThat(sidecar).exists();
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean())
                .as("close-time broken pipe must not finalize the sidecar as completed:true")
                .isFalse();
        assertThat(root.get("exit_code").isNull()).isTrue();
    }

    // ---- checkpointed parquet (runToParquetCheckpointed) -----------------------

    private static RunKey checkpointedParquetKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ck-parquet-hash",
                "SEQUENTIAL", ListingMode.OBJECTS, "", "parquet");
    }

    @Test
    void checkpointedParquetRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path tmp) throws Exception {
        int n = 800;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path sidecar = dir.resolve("_swath_summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(checkpointedParquetKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            var spec = new ListRunner.ParquetSpec(new byte[0], 1000, 1000, FilterChain.EMPTY,
                    1, 256L * 1024, 64, "ck-parquet-hash", null, null, 0L, 0L, "")
                            .withJsonSummary(summaryConfig(sidecar));
            var stats = new ListRunner().runToParquetCheckpointed(
                    RunContext.create(), fetcher, dir, spec, store, run.id(), node, List.of());

            assertThat(stats.objects()).isEqualTo(n);
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isTrue();
            assertThat(root.get("objects").asLong()).isEqualTo(n);
        }
    }

    @Test
    void checkpointedParquetRunOnMidRunThrowLeavesACompletedFalseSidecar(@TempDir Path tmp) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2_500))
                .interceptor((req, callIndex, page) -> {
                    if (callIndex == 1) {
                        throw new ListingException("injected mid-run failure");
                    }
                    return page;
                })
                .build();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path sidecar = dir.resolve("_swath_summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(checkpointedParquetKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), true).getFirst();

            var spec = new ListRunner.ParquetSpec(new byte[0], 1000, 500, FilterChain.EMPTY,
                    1, 256L * 1024, 64, "ck-parquet-hash", null, null, 0L, 0L, "")
                            .withJsonSummary(summaryConfig(sidecar));

            assertThatThrownBy(() -> new ListRunner().runToParquetCheckpointed(
                    RunContext.create(), fetcher, dir, spec, store, run.id(), node, List.of()))
                    .isInstanceOf(SwathException.class);

            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    // ---- work-stealing text (runWorkStealing) -----------------------------------

    private static RunKey workStealingTextKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ws-text-json-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    void workStealingTextRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path dir) throws Exception {
        List<byte[]> keys = Keyspaces.exactly(1500);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(workStealingTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            StringWriter out = new StringWriter();
            var stats = new ListRunner().runWorkStealing(
                    RunContext.create(), fetcher, out, spec, store, run.id(), 4, seeds);

            assertThat(stats.objects()).isEqualTo(keys.size());
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isTrue();
            assertThat(root.get("objects").asLong()).isEqualTo(keys.size());
        }
    }

    @Test
    void workStealingTextRunOnMidRunThrowLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2_500))
                .interceptor((req, callIndex, page) -> {
                    if (callIndex == 1) {
                        throw new ListingException("injected mid-run failure");
                    }
                    return page;
                })
                .build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(workStealingTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            StringWriter out = new StringWriter();

            // Under the work-stealing engine's concurrent shutdown a sibling fork() can be
            // rejected during teardown, racing the injected ListingException;
            // WorkStealingScan#produce translates that spurious RejectedExecutionException
            // back into the recorded real cause, so the injected typed exception always
            // surfaces here.
            assertThatThrownBy(() -> new ListRunner().runWorkStealing(
                    RunContext.create(), fetcher, out, spec, store, run.id(), 1, seeds))
                    .isInstanceOf(ListingException.class)
                    .hasMessageContaining("injected mid-run failure");

            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    @Test
    void workStealingTextRunOnBrokenPipeLeavesACompletedFalseSidecar(@TempDir Path dir) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(100)).build();
        Path sidecar = dir.resolve("summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(workStealingTextKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            var spec = new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8000, 1000,
                    FilterChain.EMPTY, null, summaryConfig(sidecar));
            var stats = new ListRunner().runWorkStealing(
                    RunContext.create(), fetcher, new BrokenPipeWriter(), spec, store, run.id(), 1, seeds);

            assertThat(stats.objects()).isZero();
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean())
                    .as("truncated stdout must not finalize the sidecar as completed:true")
                    .isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    // ---- work-stealing parquet (runToParquetWorkStealing) -----------------------

    private static RunKey workStealingParquetKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "ws-parquet-json-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    @Test
    void workStealingParquetRunWritesACompletedSidecarMatchingObjectCount(@TempDir Path tmp) throws Exception {
        int n = 1500;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path sidecar = dir.resolve("_swath_summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(workStealingParquetKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            var spec = new ListRunner.ParquetSpec(new byte[0], 8000, 1000, FilterChain.EMPTY,
                    2, 256L * 1024, 64, "ws-parquet-json-hash", null, null, 0L, 0L, "")
                            .withJsonSummary(summaryConfig(sidecar));
            var stats = new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec, store, run.id(), 4, seeds, List.of());

            assertThat(stats.objects()).isEqualTo(n);
            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isTrue();
            assertThat(root.get("objects").asLong()).isEqualTo(n);
        }
    }

    @Test
    void workStealingParquetRunOnMidRunThrowLeavesACompletedFalseSidecar(@TempDir Path tmp) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(2_500))
                .interceptor((req, callIndex, page) -> {
                    if (callIndex == 1) {
                        throw new ListingException("injected mid-run failure");
                    }
                    return page;
                })
                .build();
        Path dir = tmp.resolve("out");
        Files.createDirectories(dir);
        Path sidecar = dir.resolve("_swath_summary.json");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(workStealingParquetKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            var spec = new ListRunner.ParquetSpec(new byte[0], 8000, 500, FilterChain.EMPTY,
                    1, 256L * 1024, 64, "ws-parquet-json-hash", null, null, 0L, 0L, "")
                            .withJsonSummary(summaryConfig(sidecar));

            // See the work-stealing-text case above: the engine translates a
            // spurious post-shutdown RejectedExecutionException back into the recorded real
            // cause, so the injected typed exception always surfaces here.
            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec, store, run.id(), 1, seeds, List.of()))
                    .isInstanceOf(ListingException.class)
                    .hasMessageContaining("injected mid-run failure");

            assertThat(sidecar).exists();
            JsonNode root = MAPPER.readTree(sidecar.toFile());
            assertThat(root.get("completed").asBoolean()).isFalse();
            assertThat(root.get("exit_code").isNull()).isTrue();
        }
    }

    /** Throws {@code IOException("Broken pipe")} on the very first write. */
    private static final class BrokenPipeWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            throw new IOException("Broken pipe");
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Every {@code write()} succeeds — entries are actually emitted — but {@code flush()}
     * throws {@code IOException("Broken pipe")}, so the pipe only breaks when {@link
     * io.varve.swath.output.EntryFormatter#close()} (which flushes) is called. Guards the
     * close/flush-time broken-pipe path, distinct from {@link BrokenPipeWriter}'s write-time
     * break.
     */
    private static final class BreakOnFlushWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) {
            // no-op: writes succeed.
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("Broken pipe");
        }

        @Override
        public void close() {
        }
    }
}
