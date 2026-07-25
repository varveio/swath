/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Machine report path and {@code --tune summary.interval} resolution.
 */
final class ListCommandSummaryJsonTest {

    @Test
    void defaultsToASidecarNextToTheParquetOutputDirectory() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.output.destination = "/tmp/swath-out";
        assertThat(cmd.output.resolveSummaryJsonPath(OutputFormat.PARQUET))
                .isEqualTo(Path.of("/tmp/swath-out", OutputOptions.DEFAULT_SUMMARY_JSON_NAME));
    }

    @Test
    void noSummaryJsonSuppressesTheDefaultSidecar() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.output.destination = "/tmp/swath-out";
        cmd.output.noSummaryJson = true;
        assertThat(cmd.output.resolveSummaryJsonPath(OutputFormat.PARQUET)).isNull();
    }

    @Test
    void explicitPathIsHonoredEvenForStdoutOutput() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.output.summaryJson = "/tmp/explicit-summary.json";
        assertThat(cmd.output.resolveSummaryJsonPath(OutputFormat.JSONL))
                .isEqualTo(Path.of("/tmp/explicit-summary.json"));
    }

    @Test
    void stdoutRunGetsNoDefaultSidecar() throws Exception {
        ListCommand cmd = new ListCommand();
        assertThat(cmd.output.resolveSummaryJsonPath(OutputFormat.JSONL)).isNull();
    }

    @Test
    void singleFileTextOutputGetsNoDefaultSidecar() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.output.destination = "/tmp/out.jsonl";
        assertThat(cmd.output.resolveSummaryJsonPath(OutputFormat.JSONL)).isNull();
    }

    @Test
    void summaryJsonAndNoSummaryJsonTogetherIsRejected() {
        ListCommand cmd = new ListCommand();
        cmd.output.summaryJson = "/tmp/x.json";
        cmd.output.noSummaryJson = true;
        assertThatThrownBy(() -> cmd.output.resolveSummaryJsonPath(OutputFormat.JSONL))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("sidecar suppression");
    }

    @Test
    void combinedFlagsAreRejectedAsExitTwoThroughTheFullCliPath() {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/prefix";
        cmd.connection.region = "us-east-1";
        cmd.checkpoint.location = "none";
        cmd.output.summaryJson = "/tmp/x.json";
        cmd.output.noSummaryJson = true;
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));
    }

    @Test
    void combinedFlagsAreRejectedBeforeAnyNetworkOrStoreWorkOnTheDefaultCheckpointedPath(
            @TempDir Path dir) throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/prefix";
        cmd.connection.region = "us-east-1";
        // A directory-dataset output with checkpoint.location at its field default ("auto") is the
        // co-located checkpointed path, where buildJsonSummaryConfig() used to run only after the
        // SQLite store was opened, the S3Client was created, and (for a fresh run) the seedFreshRun
        // network probe ran. The validation must now be hoisted ahead of all of that.
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = dir.toString();
        cmd.output.summaryJson = "/tmp/x.json";
        cmd.output.noSummaryJson = true;

        Path checkpointPath = colocatedCheckpoint(dir);

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

        assertThat(checkpointPath)
                .as("no SQLite checkpoint store was opened before the pure-validation failure")
                .doesNotExist();
    }

    /**
     * Guards: {@code --progress-interval} is validated by the same early,
     * pre-network {@link ListCommand#validateSummaryJsonFlags()} hoist as {@code
     * --tune summary.interval}, since the JSON flush cadence falls back to it
     * ({@link ListCommand#resolveSummaryJsonInterval}). Before the fix this only
     * surfaced late, inside {@code buildJsonSummaryConfig}, well after the SQLite
     * checkpoint store would have been opened on the default {@code --checkpoint
     * auto} path.
     */
    @Test
    void badProgressIntervalFailsBeforeAnyNetworkOrStoreWorkOnTheDefaultCheckpointedPath(
            @TempDir Path dir) throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/prefix";
        cmd.connection.region = "us-east-1";
        // checkpoint.location at its field default ("auto") + a directory output = the co-located
        // checkpointed path.
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = dir.toString();
        cmd.liveness.progressInterval = "not-a-duration";

        Path checkpointPath = colocatedCheckpoint(dir);

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2));

        assertThat(checkpointPath)
                .as("no SQLite checkpoint store was opened before the pure-validation failure")
                .doesNotExist();
    }

    /** The co-located run-handle checkpoint for a directory-dataset output: {@code <dir>/.swath/checkpoint.sqlite}. */
    private static Path colocatedCheckpoint(Path outputDir) {
        return CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
    }

    @Test
    void intervalDefaultsToTheResolvedProgressInterval() throws Exception {
        ListCommand cmd = new ListCommand();
        assertThat(cmd.resolveSummaryJsonInterval()).isEqualTo(RunProgressReporter.nonTtyInterval());

        cmd.liveness.progressInterval = "5s";
        assertThat(cmd.resolveSummaryJsonInterval()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void explicitSummaryJsonIntervalOverridesTheProgressInterval() throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.liveness.progressInterval = "5s";
        cmd.output.summaryJsonInterval = "500ms";
        assertThat(cmd.resolveSummaryJsonInterval()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void invalidSummaryJsonIntervalIsRejected() {
        ListCommand cmd = new ListCommand();
        cmd.output.summaryJsonInterval = "not-a-duration";
        assertThatThrownBy(cmd::resolveSummaryJsonInterval).isInstanceOf(InvalidConfigException.class);
    }

    @Test
    void reportAndSummaryIntervalParseFromTheCommandLine() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--report", "/tmp/x.json", "--tune", "summary.interval=2s");
        cmd.syncArgGroups();
        assertThatCode(() -> cmd.tune.apply(cmd.output, cmd.engine, cmd.sorting,
                new PrintWriter(new StringWriter()))).doesNotThrowAnyException();
        assertThat(cmd.output.summaryJson).isEqualTo("/tmp/x.json");
        assertThat(cmd.output.summaryJsonInterval).isEqualTo("2s");
    }

    /**
     * {@code --checkpoint none} must no longer switch ENGINE — it still drives {@code
     * WorkStealingScan} (the summary's {@code strategy} field reads {@code work_stealing}), just
     * with nothing durable behind it. Full end-to-end through {@link ListCommand#call()} with an
     * injected {@link MockPageFetcher} (no real S3), mirroring the adversarial tests' pattern
     * (e.g. {@code SeedRideOutHealsContractTest}).
     */
    @Test
    void checkpointNoneStillRunsTheWorkStealingEngine(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("out.jsonl");
        Path sidecar = dir.resolve("summary.json");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.endpointUrl = "http://localhost:4566";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = out.toString();
        cmd.output.summaryJson = sidecar.toString();
        cmd.fetcherOverride = fetcher;

        int exit = cmd.call();

        assertThat(exit).isEqualTo(ExitCodes.SUCCESS);
        JsonNode root = new ObjectMapper().readTree(sidecar.toFile());
        assertThat(root.get("strategy").asText())
                .as("--checkpoint none must still report the WorkStealingScan engine, not SEQUENTIAL")
                .isEqualTo("work_stealing");
        assertThat(root.get("completed").asBoolean()).isTrue();
        assertThat(Files.readAllLines(out)).hasSize(2);
    }

    private static S3Config anonymousConfig() {
        return new S3Config(Region.US_EAST_1, null, false,
                S3Config.DEFAULT_MAX_PARALLEL, S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30), S3Config.DEFAULT_API_CALL_TIMEOUT, AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
    }

    // ---- engine_flags + argv + max_duration identifiability -----------

    @Test
    void engineFlagsDefaultToOwnerSplitOn() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json");

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.runConfig().engineToggles().ownerSplit()).isTrue();
        assertThat(config.runConfig().maxDurationMs()).isNull();
    }

    @Test
    void summaryConfigPersistsRedactedEndpointArgv() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json",
                "--endpoint-url=https://user:summary-secret@example.test");

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.argv()).containsExactly("s3://bucket/prefix", "--report", "/tmp/x.json",
                "--endpoint-url=<redacted endpoint>");
        assertThat(String.join(" ", config.argv())).doesNotContain("summary-secret");
    }

    @Test
    void noOwnerSplitReflectsAsOwnerSplitFalse() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json",
                "--engine-toggle", "owner_split=off");
        cmd.syncArgGroups();
        // call() resolves this in production (before buildJsonSummaryConfig is ever reached); these
        // tests exercise buildJsonSummaryConfig() directly, via parseArgs() only, so resolve it here.
        cmd.engine.toggles = cmd.engine.resolveToggles();

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.runConfig().engineToggles().ownerSplit()).isFalse();
    }

    @Test
    void engineToggleOffReflectsInEngineFlags() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json",
                "--engine-toggle", "structure_probes=off", "--engine-toggle", "far_ahead=off");
        cmd.syncArgGroups();
        cmd.engine.toggles = cmd.engine.resolveToggles();

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        EngineToggles toggles = config.runConfig().engineToggles();
        assertThat(toggles.structureProbes()).isFalse();
        assertThat(toggles.farAhead()).isFalse();
        // every other toggle stays at its supported default
        assertThat(toggles.ownerSplit()).isTrue();
        assertThat(toggles.densityEwma()).isTrue();
        assertThat(toggles.radixBands()).isTrue();
        assertThat(toggles.alphabetPivots()).isTrue();
    }

    @Test
    void maxDurationValueIsRecordedInEngineFlags() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json",
                "--max-duration", "5m");
        cmd.syncArgGroups();

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.runConfig().maxDurationMs()).isEqualTo(Duration.ofMinutes(5).toMillis());
    }

    @Test
    void safeArgvPreservesNonSensitiveCommandLineArguments() throws Exception {
        ListCommand cmd = new ListCommand();
        String[] args = {"s3://bucket/prefix", "--report", "/tmp/x.json",
                "--engine-toggle", "owner_split=off", "--max-duration", "5m"};
        new CommandLine(cmd).parseArgs(args);
        cmd.syncArgGroups();

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.argv()).containsExactly(args);
    }

    @Test
    void argvIsEmptyWhenBuiltWithoutGoingThroughPicocli() throws Exception {
        // Several other tests in this suite construct ListCommand directly and set fields
        // without a picocli parse; buildJsonSummaryConfig must not NPE in that case.
        ListCommand cmd = new ListCommand();
        cmd.output.summaryJson = "/tmp/x.json";

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.argv()).isEmpty();
    }

    // ---- sort observability polish: buildJsonSummaryConfig's effective_fan_in wiring ----

    /**
     * Regression guard for {@code buildJsonSummaryConfig}'s {@code sort ?
     * SortConfig.fromSystemProperties().effectiveFanIn() : null} wiring (ListCommand ~938-943) was
     * previously untested — {@code JsonRunSummaryWriterTest} only ever constructed {@code
     * RunConfig} manually. This exercises the real seam: {@code --sort} on the command line must
     * produce a {@code RunConfig} whose {@code sortEffectiveFanIn()} matches {@code
     * SortConfig.fromSystemProperties().effectiveFanIn()} exactly (same knobs, same formula).
     */
    @Test
    void sortEnabledEchoesTheEffectiveFanInFromSortConfig() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json", "--sort");
        cmd.syncArgGroups();

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.PARQUET, anonymousConfig(), "hash");

        assertThat(config.runConfig().sortEnabled()).isTrue();
        assertThat(config.runConfig().sortEffectiveFanIn())
                .isEqualTo(SortConfig.fromSystemProperties().effectiveFanIn());
    }

    /** {@code --sort} off (the default): {@code sortEffectiveFanIn()} stays {@code null} (renders JSON null). */
    @Test
    void sortDisabledLeavesEffectiveFanInNull() throws Exception {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--report", "/tmp/x.json");

        JsonRunSummaryWriter.Config config =
                cmd.buildJsonSummaryConfig(OutputFormat.JSONL, anonymousConfig(), "hash");

        assertThat(config.runConfig().sortEnabled()).isFalse();
        assertThat(config.runConfig().sortEffectiveFanIn()).isNull();
    }
}
