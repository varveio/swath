/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.RetryPolicy;
import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.AccessDeniedException;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.NoSuchBucketException;
import io.varve.swath.error.RegionRedirectException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.UnauthorizedException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.StopReason;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Every pre-run early-exit path writes a JSON run-summary with the right {@code
 * stop_reason} and exits with the right code, and the internal sidecar-suppression seam suppresses the
 * sidecar on those paths. Also pins the timebox/signal exit-code mapping
 * ({@link ListCommand#timeboxExitOrRethrow}): a {@code max_duration} cancel → exit 124, a
 * SIGTERM-sourced {@code signal} cancel → 143, a SIGINT-sourced (or source-less) one → 130, and a
 * truly unattributed cancel re-throws so {@code CancelledException}'s 130 stands.
 */
final class EarlyExitSummaryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static void seedCompletedRun(Path db) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.JSONL.name(),
                SoftRestoreContext.NONE, false, storedIdentitySpec());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    /**
     * The {@code run_meta.identity_spec} a real creating run would persist: the registry's IDENTITY
     * fingerprint over a {@link ListCommand} mirroring the seeded run's fully-resolved state, so a
     * resume that changes an IDENTITY option (here the filter) is refused by the registry path.
     */
    private static String storedIdentitySpec() throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://" + BUCKET + "/" + PREFIX;
        original.connection.endpointUrl = ENDPOINT;
        original.output.format = OutputFormat.JSONL;
        FilterSpecCodec.Decoded filters = FilterSpecCodec.decode(NO_FILTER_SPEC);
        original.filters.include = filters.include();
        original.filters.exclude = filters.exclude();
        original.filters.minSize = filters.minSize();
        original.filters.maxSize = filters.maxSize();
        original.filters.modifiedAfter = filters.modifiedAfter();
        original.filters.modifiedBefore = filters.modifiedBefore();
        original.filters.storageClasses = filters.storageClasses();
        original.output.resolveOutput(false);   // resolves format + destination kind, as creation did
        return ResumeRegistry.identitySpec(original);
    }

    private static ListCommand resumeCommand(Path db, Path sidecar) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.JSONL;
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.output.summaryJson = sidecar.toString();   // explicit path ⇒ a sidecar even for a text/stdout run
        return cmd;
    }

    @Test
    void noOpResumeWritesACompletedSummary(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedCompletedRun(db);

        assertThat(resumeCommand(db, sidecar).call()).isEqualTo(ExitCodes.SUCCESS);

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isTrue();
        assertThat(root.get("exit_code").asInt()).isZero();
        assertThat(root.get("stop_reason").asText()).isEqualTo("completed");
    }

    @Test
    void resumeRefusalWritesAResumeRefusedSummaryAndExitsTwo(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedCompletedRun(db);

        ListCommand cmd = resumeCommand(db, sidecar);
        cmd.filters.include = "\\.parquet$";   // changed filter ⇒ resume refused (exit 2)

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("exit_code").isNull()).isTrue();
        assertThat(root.get("stop_reason").asText()).isEqualTo("resume_refused");
    }

    @Test
    void noSummaryJsonSuppressesTheEarlyExitSidecar(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedCompletedRun(db);

        ListCommand cmd = resumeCommand(db, sidecar);
        cmd.output.summaryJson = null;
        cmd.output.noSummaryJson = true;
        cmd.filters.include = "\\.parquet$";   // still refused (exit 2), but no sidecar must land

        assertThatThrownBy(cmd::call).isInstanceOf(InvalidArgsException.class);
        assertThat(Files.exists(sidecar)).isFalse();
    }

    @Test
    void seedFailureWritesASeedFailureSummary(@TempDir Path dir) throws Exception {
        // The seed-probe network failure path is a private wrap; verify the summary content the
        // handler emits directly (deterministic, no real S3), including the completed:false /
        // exit_code:null / stop_reason=seed_failure shape.
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();   // parquet dir ⇒ default sidecar next to the parts
        S3Config config = new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, config, "seedhash",
                RunContext.create(), 7L, System.nanoTime(), false, StopReason.SEED_FAILURE, "WORK_STEALING");

        Path sidecar = dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("schema_version").asInt()).isEqualTo(2);
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("exit_code").isNull()).isTrue();
        assertThat(root.get("stop_reason").asText()).isEqualTo("seed_failure");
        assertThat(root.get("strategy").asText()).isEqualTo("work_stealing");
        // An early-exit summary never ran the listing engine, so it carries NO `shape` block.
        assertThat(root.has("shape")).as("early-exit summary omits the shape block").isFalse();
    }

    /**
     * A pre-engine early exit reaches the operator-facing block too, not the sidecar alone: a seed
     * failure is precisely the run that stopped abnormally, which is what the auto gate's
     * stop-reason clause is for. The block carries no resume invitation — the run is marked fatal,
     * so a later resume would be refused.
     */
    @Test
    void seedFailureAlsoReachesTheSummarySink(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        RunContext ctx = RunContext.create();
        List<JsonRunSummaryWriter.TerminalStatus> emitted = new ArrayList<>();
        ctx.metrics().setSummarySink((summary, diagnostics, status) -> emitted.add(status));

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, anonymousConfig(), "seedhash",
                ctx, 7L, System.nanoTime(), false, StopReason.SEED_FAILURE, "WORK_STEALING");

        assertThat(emitted).hasSize(1);
        assertThat(emitted.getFirst().reason()).isEqualTo(StopReason.SEED_FAILURE);
        assertThat(SummaryRenderer.lines(
                new SummaryRenderer.Preferences(true, false, true, true, dir.toString(), false),
                ctx.metrics().summary(Duration.ZERO, "WORK_STEALING", 0L, 0L),
                ctx.metrics().diagnostics(Duration.ZERO), emitted.getFirst()))
                .first(STRING)
                .isEqualTo("INCOMPLETE (seed_failure)");
    }

    /**
     * Regression for a seed-phase interruption that lied about its own elapsed time: {@code
     * writeEarlyExitSummary} used to hand {@link RunMetrics#summary} a hardcoded {@code
     * Duration.ZERO} no matter how long the seed actually ran, so an operator killed mid-seed after
     * real wall-clock time (and real API calls) still saw {@code "0 objects in 0.0s"} — disagreeing
     * with the correct API-call count on the very next line. Both the operator-facing block and the
     * {@code --report} sidecar must instead carry the run's real elapsed time.
     */
    @Test
    void seedFailureReportsARealNonZeroDurationInBothSurfaces(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        RunContext ctx = RunContext.create();
        List<RunSummary> emittedSummaries = new ArrayList<>();
        List<JsonRunSummaryWriter.TerminalStatus> emittedStatus = new ArrayList<>();
        ctx.metrics().setSummarySink((summary, diagnostics, status) -> {
            emittedSummaries.add(summary);
            emittedStatus.add(status);
        });
        // A seed that ran for 22s before it was interrupted -- the reproduction's own numbers.
        long startedNs = System.nanoTime() - Duration.ofSeconds(22).toNanos();

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, anonymousConfig(), "seedhash",
                ctx, 7L, startedNs, false, StopReason.SEED_FAILURE, "WORK_STEALING");

        RunSummary summary = emittedSummaries.getFirst();
        assertThat(summary.duration())
                .as("the seed-phase early exit must report the run's real elapsed time, not zero")
                .isGreaterThan(Duration.ofSeconds(20));

        List<String> lines = SummaryRenderer.lines(
                new SummaryRenderer.Preferences(true, false, true, true, null, false),
                summary, ctx.metrics().diagnostics(summary.duration()), emittedStatus.getFirst());
        assertThat(lines.get(1))
                .as("the human block's elapsed figure must not read 0.0s for a 22s seed window")
                .doesNotContain("in 0.0s")
                .contains("objects in");

        Path sidecar = dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("duration_ms").asLong())
                .as("the JSON sidecar must carry the same real duration, not duration_ms:0")
                .isGreaterThan(20_000L);
    }

    /**
     * The one early exit the auto gate does not earn: a resume refusal never ran, and its own
     * {@code swath: …} line already names both the problem and the fix, so an unrequested block
     * would add nothing but a marker.
     */
    @Test
    void resumeRefusalStaysSilentOnTheSummarySinkUnderAuto(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        RunContext ctx = RunContext.create();
        List<JsonRunSummaryWriter.TerminalStatus> emitted = new ArrayList<>();
        ctx.metrics().setSummarySink((summary, diagnostics, status) -> emitted.add(status));

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, anonymousConfig(), "refusedhash",
                ctx, 7L, System.nanoTime(), false, StopReason.RESUME_REFUSED, "WORK_STEALING");

        assertThat(emitted).isEmpty();
        assertThat(dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME))
                .as("the sidecar still records the refusal for a machine consumer")
                .exists();
    }

    /**
     * An explicitly passed {@code --stats} forces the block past every gate, and a refusal is a
     * terminal path like any other — leaving it out would make the two surfaces disagree, since the
     * sidecar records the refusal regardless. What it renders is a one-line disposition: the run
     * never started, so a statistics body would be zeros describing nothing.
     */
    @Test
    void resumeRefusalReachesTheSummarySinkUnderAnExplicitStatsFlag(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        cmd.output.stats = true;
        RunContext ctx = RunContext.create();
        List<JsonRunSummaryWriter.TerminalStatus> emitted = new ArrayList<>();
        ctx.metrics().setSummarySink((summary, diagnostics, status) -> emitted.add(status));

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, anonymousConfig(), "refusedhash",
                ctx, 7L, System.nanoTime(), false, StopReason.RESUME_REFUSED, "WORK_STEALING");

        assertThat(emitted).hasSize(1);
        assertThat(emitted.getFirst().reason()).isEqualTo(StopReason.RESUME_REFUSED);
        SummaryRenderer.Preferences prefs =
                new SummaryRenderer.Preferences(true, false, true, true, dir.toString(), false);
        RunSummary summary = ctx.metrics().summary(Duration.ZERO, "WORK_STEALING", 0L, 0L);
        assertThat(SummaryRenderer.shouldRender(prefs, summary, emitted.getFirst()))
                .as("the explicit-flag branch admits the refusal like any other terminal path")
                .isTrue();
        assertThat(SummaryRenderer.lines(prefs, summary,
                ctx.metrics().diagnostics(Duration.ZERO), emitted.getFirst()))
                .as("the disposition is the whole record -- no zeroed statistics body")
                .containsExactly("INCOMPLETE (resume_refused)");
    }

    private static S3Config anonymousConfig() {
        return new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
    }

    private static Stream<Arguments> classifiedSeedFailures() {
        Throwable s3cause = new RuntimeException("s3 wire cause");
        return Stream.of(
                Arguments.of("access_denied", new AccessDeniedException(BUCKET, s3cause)),
                Arguments.of("unauthorized", new UnauthorizedException(BUCKET, s3cause)),
                Arguments.of("no_such_bucket", new NoSuchBucketException(BUCKET, s3cause)),
                Arguments.of("region_redirect", new RegionRedirectException(BUCKET, "eu-west-1", s3cause)));
    }

    /**
     * A classified fatal seed failure must yield a summary carrying the distinct greppable
     * {@code error_class} AND a real {@code exit_code:1} — never {@code error_class:null,
     * exit_code:null}, which leaves a supervisor unable to tell a permissions/config problem
     * from a crash. Threads the exit code + error_class exactly as the seed-failure catch does
     * ({@link ExitCodes#forThrowable} + the typed exception's {@link ListingException#errorClass()}),
     * then asserts the on-disk summary.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("classifiedSeedFailures")
    void classifiedSeedFailureWritesErrorClassAndExitOne(String expectedClass, ListingException seedFailure,
                                                         @TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();   // parquet dir ⇒ default sidecar next to the parts
        S3Config config = new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

        // Exactly the two derivations ListCommand's seed-failure catch performs on the thrown fault.
        int exitCode = ExitCodes.forThrowable(seedFailure);
        String errorClass = seedFailure.errorClass();

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, config, "seedhash",
                RunContext.create(), 7L, System.nanoTime(), false, StopReason.SEED_FAILURE, "WORK_STEALING",
                exitCode, errorClass);

        Path sidecar = dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("seed_failure");
        assertThat(root.get("error_class").asText())
                .as("a classified fatal seed failure names its error_class, not null")
                .isEqualTo(expectedClass);
        assertThat(root.get("exit_code").isNull())
                .as("the seed-failure summary must report the true exit code, not null").isFalse();
        assertThat(root.get("exit_code").asInt())
                .as("every permanent class is fatal — exit 1").isEqualTo(1);
    }

    @Test
    void seedRetryCapStuckSummaryCarriesStopSourceAndErrorClass(@TempDir Path dir) throws Exception {
        // A seed-time BOUNDED retry-cap STUCK must NOT write a null stop_source/error_class —
        // writeEarlyExitSummary must derive the same source-routed fields ListRunner's normal-run
        // terminal status does, via the shared ListRunner#stuckTerminalStatus helper. Drive the REAL
        // TransientRetryFetcher BOUNDED cap-exhaustion path (a permanent ATTEMPT_TIMEOUT storm) so the
        // token/metrics are populated exactly as the production seed path would leave them, then write
        // the early-exit summary from that same RunContext.
        Files.createDirectories(dir);
        RunContext ctx = RunContext.create();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8)))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("seed storm never heals");
                })
                .build();
        TransientRetryFetcher fetcher = new TransientRetryFetcher(delegate, ctx.cancellation(), ctx.metrics(),
                millis -> { /* no real sleep: deterministic */ }, RetryPolicy.BOUNDED);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("BOUNDED cap exhaustion unwinds resumably, attributing TRANSIENT_RETRY_CAP")
                .isInstanceOf(InterruptedException.class);
        assertThat(ctx.cancellation().stopReason()).isEqualTo(StopReason.STUCK);

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        S3Config config = new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, config, "seedhash",
                ctx, 7L, System.nanoTime(), false, StopReason.STUCK, "WORK_STEALING");

        Path sidecar = dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("completed").asBoolean()).isFalse();
        assertThat(root.get("stop_reason").asText()).isEqualTo("stuck");
        assertThat(root.get("stop_source").asText())
                .as("the retry cap must be named, not left null")
                .isEqualTo("transient_retry_cap");
        assertThat(root.get("error_class").asText())
                .as("the fetch-local error class the cap-exhausting fetch recorded")
                .isEqualTo("stuck_api_timeouts");
    }

    @Test
    void noOpResumeSummaryOmitsTheShapeBlock(@TempDir Path dir) throws Exception {
        // A no-op completed resume is an early exit too: it never fetches a listing page, so its
        // summary must omit `shape` — while still carrying the completed/stop_reason fields.
        Path db = dir.resolve("c.sqlite");
        Path sidecar = dir.resolve("summary.json");
        seedCompletedRun(db);

        assertThat(resumeCommand(db, sidecar).call()).isEqualTo(ExitCodes.SUCCESS);

        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("stop_reason").asText()).isEqualTo("completed");
        assertThat(root.has("shape")).as("no-op resume summary omits the shape block").isFalse();
    }

    @Test
    void writeEarlyExitSummaryCarriesTheCallerSuppliedStrategyNotAHardcodedOne(@TempDir Path dir) throws Exception {
        // writeEarlyExitSummary must not silently hardcode WORK_STEALING — it must report whatever
        // strategy the caller passes (e.g. a --checkpoint none/text early-exit path, which is
        // SEQUENTIAL, not WORK_STEALING).
        Files.createDirectories(dir);
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.toString();
        S3Config config = new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

        cmd.writeEarlyExitSummary(OutputFormat.PARQUET, config, "seedhash",
                RunContext.create(), 7L, System.nanoTime(), false, StopReason.RESUME_REFUSED, "SEQUENTIAL");

        Path sidecar = dir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        JsonNode root = MAPPER.readTree(sidecar.toFile());
        assertThat(root.get("strategy").asText()).isEqualTo("sequential");
    }

    @Test
    void maxDurationCancelMapsToExit124() throws Exception {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.MAX_DURATION);
        assertThat(ListCommand.timeboxExitOrRethrow(token, RunContext.create().metrics(),
                new CancelledException("timebox"), "hash"))
                .isEqualTo(ExitCodes.TIMEBOX);
    }

    @Test
    void sigtermSourcedSignalCancelMapsToExit143() throws Exception {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.SIGNAL, CancelSource.SIGTERM);
        assertThat(ListCommand.timeboxExitOrRethrow(token, RunContext.create().metrics(),
                new CancelledException("signal"), "hash"))
                .isEqualTo(ExitCodes.SIGTERM);
    }

    @Test
    void sigintSourcedSignalCancelMapsToExit130() throws Exception {
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.SIGNAL, CancelSource.SIGINT);
        assertThat(ListCommand.timeboxExitOrRethrow(token, RunContext.create().metrics(),
                new CancelledException("signal"), "hash"))
                .isEqualTo(ExitCodes.SIGINT);
    }

    @Test
    void sourcelessSignalCancelDefaultsToExit130() throws Exception {
        // The legacy cancel(SIGNAL) form carries no source — it must default to SIGINT/130, never
        // silently degrade to a rethrow or a wrong code.
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.SIGNAL);
        assertThat(ListCommand.timeboxExitOrRethrow(token, RunContext.create().metrics(),
                new CancelledException("signal"), "hash"))
                .isEqualTo(ExitCodes.SIGINT);
    }

    @Test
    void unattributedCancelRethrowsAsExit130() {
        // No attributed reason at all ⇒ the final rethrow stands, so CancelledException's own 130
        // governs.
        CancellationToken token = new CancellationToken();
        token.cancel();
        CancelledException cancelled = new CancelledException("cancelled");
        assertThatThrownBy(() -> ListCommand.timeboxExitOrRethrow(
                token, RunContext.create().metrics(), cancelled, "hash"))
                .isInstanceOf(CancelledException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(130));
    }

    @Test
    void stuckCancelMapsToDedicatedStuckExitCodeNotSignal() throws Exception {
        // A liveness-watchdog STUCK abort must map to the dedicated stuck code (75), NOT fall
        // through to the signal 130 — so a runner classifies the wedge as stop_reason=stuck, not a
        // user Ctrl-C.
        CancellationToken token = new CancellationToken();
        token.cancel(StopReason.STUCK);
        assertThat(ListCommand.timeboxExitOrRethrow(token, RunContext.create().metrics(),
                new CancelledException("stuck"), "hash"))
                .isEqualTo(ExitCodes.STUCK);
        assertThat(ExitCodes.STUCK).isNotEqualTo(130).isNotEqualTo(ExitCodes.SUCCESS);
    }
}
