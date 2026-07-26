/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.testkit.MockObject;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * End-to-end coverage for {@link ResumeCommand#call()} via the {@code <dir>} run handle. Proves that
 * the 7-field filter restore block in {@code call()} assigns every field to the correct slot: if any
 * field is swapped or dropped, the rebuilt {@link ListCommand} computes a different {@code filter_spec}
 * and the resume-safety check would throw {@link InvalidArgsException} instead of
 * returning {@code ExitCodes.SUCCESS}. Every run is resumed by opening the checkpoint co-located inside
 * its directory dataset ({@code <dir>/.swath/checkpoint.sqlite}) — the output directory is the whole
 * run handle.
 */
final class ResumeCommandTest {

    private static final String BUCKET   = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX   = "data/";

    /**
     * All seven filter fields set to distinct non-null values so that dropping or
     * swapping any single field during {@link ResumeCommand}'s restore causes the
     * rebuilt {@link ListCommand} to compute a different {@code filter_spec}, which
     * trips the refusal instead of returning {@code ExitCodes.SUCCESS}.
     * Fields that were previously null gave the restore block a free pass on bugs
     * that dropped a null — every field being non-null eliminates that blind spot.
     */
    private static final String DISTINCTIVE_FILTER_SPEC = FilterSpecCodec.encode(
            "\\.log$",              // include
            "tmp/",                 // exclude
            "1k",                   // minSize
            "1g",                   // maxSize
            "2024-01-01T00:00:00Z", // modifiedAfter
            "2025-01-01T00:00:00Z", // modifiedBefore
            List.of("STANDARD", "GLACIER")); // storageClasses

    /** 2024-06-01T00:00:00Z in micros — inside {@link #DISTINCTIVE_FILTER_SPEC}'s modified window. */
    private static final long WITHIN_FILTER_WINDOW_MICROS = 1_717_200_000_000_000L;

    /**
     * A recorded relative destination for the refusal tests, distinctive enough that the cwd-relative
     * reading of it can only be something a test just created — the assertion there is about a path
     * nothing else in the working directory owns.
     */
    private static final String RELATIVE_RECORDING = "swath-refused-resume-dataset";

    /**
     * A stored {@code identity_spec} that does not parse: it cannot be confirmed to match this
     * invocation, so every identity column reads as changed and the resume is refused.
     */
    private static final String UNCONFIRMABLE_IDENTITY_SPEC = "not-an-identity-spec";

    /** The co-located checkpoint path for a directory dataset, with its {@code .swath/} parent created. */
    private static Path colocated(Path outputDir) throws Exception {
        Path db = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        Files.createDirectories(db.getParent());
        return db;
    }

    /** Seed a COMPLETED filtered run co-located in a fresh directory dataset and return that directory. */
    private static Path seedCompletedRunDir(Path parent) throws Exception {
        Path outputDir = parent.resolve("dataset");
        seedCompletedRun(colocated(outputDir));
        return outputDir;
    }

    /** Seed a COMPLETED run with the distinctive filter into {@code db}. */
    private static void seedCompletedRun(Path db) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, OutputFormat.JSONL.name());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    /** Seed a RUNNING legacy/foreign text FILE checkpoint with a recorded destination. */
    private static void seedRunningTextFileRun(Path db, Path output) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8), argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, OutputFormat.JSONL.name(),
                new SoftRestoreContext(false, null, null, false, false, output.toString(), false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k1".getBytes(StandardCharsets.UTF_8), false));
        }
    }

    /**
     * A COMPLETED filtered run resumes cleanly (no-op, no S3 contact) when the 7
     * filter fields are correctly restored onto the rebuilt {@link ListCommand}.
     */
    @Test
    void filteredCompletedRun_resumesClean(@TempDir Path tempDir) throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    void resumeAcceptsDiskCheckTuneButRejectsRunShapeTune(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);

        ResumeCommand allowed = new ResumeCommand();
        new CommandLine(allowed).parseArgs(outputDir.toString(),
                "--tune", "sort.ignore-disk-check=on");
        assertThat(allowed.call()).isEqualTo(ExitCodes.SUCCESS);

        // A run-shape tune is rejected during validation, before any checkpoint I/O — so a run
        // handle that does not exist yet still surfaces the tune error first.
        ResumeCommand rejected = new ResumeCommand();
        new CommandLine(rejected).parseArgs(tempDir.resolve("missing").toString(),
                "--tune", "seed.mode=none");
        assertThatThrownBy(rejected::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("seed.mode")
                .hasMessageContaining("run-shape")
                .hasMessageContaining("cannot be changed by swath resume");
    }

    /**
     * A resumed run renders the same end-of-run block a fresh one does, so it must accept the same
     * switch for forcing/silencing it — {@code swath resume --stats <dir>} was an exit-2 unknown
     * option. The flag is forwarded to the delegated {@link ListCommand} the way {@code
     * -v}/{@code -q}/{@code --color} already are.
     */
    @Test
    void statsIsAcceptedOnResumeAndForcesTheBlockOnACompletedNoOp(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        int exit;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            exit = App.commandLine().execute("resume", outputDir.toString(), "--stats");
        } finally {
            System.setErr(originalErr);
        }

        assertThat(exit).isZero();
        assertThat(captured.toString(StandardCharsets.UTF_8))
                .as("--stats forces the block past the auto gate on a resume too")
                .contains("objects in")
                .contains("API calls");
    }

    @Test
    void noStatsIsAcceptedOnResume(@TempDir Path tempDir) throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);
        ResumeCommand resume = new ResumeCommand();
        new CommandLine(resume).parseArgs(outputDir.toString(), "--no-stats");

        assertThat(resume.stats).isFalse();
    }

    @Test
    void verboseResumeEchoesAcceptedEffectiveTuneExactlyOnceViaRootCli(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);
        CommandLine cli = App.commandLine();
        StringWriter err = new StringWriter();
        cli.setErr(new PrintWriter(err));

        int exit = cli.execute("resume", outputDir.toString(), "-v",
                "--tune", "sort.ignore-disk-check=on");

        assertThat(exit).isZero();
        assertThat(err.toString())
                .contains("sort.ignore-disk-check=on")
                .containsOnlyOnce("tune effective:");
    }

    @Test
    void resumeTuneValidationAndHelpPrecedeMissingCheckpointIo(@TempDir Path tempDir)
            throws Exception {
        Path missing = tempDir.resolve("does-not-exist");

        ResumeCommand rejectedIdentity = new ResumeCommand();
        new CommandLine(rejectedIdentity).parseArgs(missing.toString(),
                "--tune", "seed.mode=none");
        assertThatThrownBy(rejectedIdentity::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("seed.mode")
                .hasMessageContaining("run-shape");

        ResumeCommand rejectedValue = new ResumeCommand();
        new CommandLine(rejectedValue).parseArgs(missing.toString(),
                "--tune", "sort.ignore-disk-check=maybe");
        assertThatThrownBy(rejectedValue::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("sort.ignore-disk-check")
                .hasMessageContaining("on|off");

        ResumeCommand help = new ResumeCommand();
        CommandLine helpCli = new CommandLine(help);
        StringWriter out = new StringWriter();
        helpCli.setOut(new PrintWriter(out));
        helpCli.parseArgs(missing.toString(), "--tune", "help");
        assertThat(help.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(out.toString()).contains("Tune keys:").contains("sort.ignore-disk-check");
        assertThat(missing).doesNotExist();
    }

    @Test
    void freshOnlyFreeTuneKeyUsesApplicabilityWordingBeforeCheckpointIo(@TempDir Path tempDir) {
        ResumeCommand cmd = new ResumeCommand();
        new CommandLine(cmd).parseArgs(tempDir.resolve("missing").toString(),
                "--tune", "parquet.writers=4");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("parquet.writers")
                .hasMessageContaining("not applicable during swath resume")
                .hasMessageNotContaining("run-shape");
    }

    /** A sorted checkpoint must regain its persisted mode before ListCommand's mismatch guard. */
    @Test
    void publishedSortedRun_resumesCleanWithoutNetwork(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = colocated(outputDir);
        seedPublishedSortedRun(db, outputDir);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    void preSortSchemaResumesAsUnsortedAndMigratesColumn(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = colocated(outputDir);
        seedCompletedRun(db);
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement dropEnabled = c.prepareStatement(
                     "ALTER TABLE run_meta DROP COLUMN sort_enabled");
             PreparedStatement dropPhase = c.prepareStatement(
                     "ALTER TABLE run_meta DROP COLUMN sort_phase")) {
            dropEnabled.executeUpdate();
            dropPhase.executeUpdate();
        }

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("SELECT sort_enabled FROM run_meta");
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isZero();
            assertThat(rs.wasNull()).isFalse();
        }
    }

    @Test
    void malformedCheckpointOutputFormat_mapsToCheckpointExit(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = colocated(outputDir);
        seedCompletedRun(db);
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("UPDATE run_meta SET output_format=?")) {
            ps.setString(1, "BROKEN");
            ps.executeUpdate();
        }

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThatThrownBy(cmd::call)
                .isInstanceOf(CheckpointException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(1));
    }

    @Test
    void recordedTextFileRefusalPrecedesMalformedFormatViaResumeCommand(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = colocated(outputDir);
        Path recordedOutput = tempDir.resolve("out.jsonl");
        seedRunningTextFileRun(db, recordedOutput);
        overwriteOutputFormat(db, "CORRUPTED");

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("FILE-kind text destination")
                .hasMessageContaining(recordedOutput.toString())
                .hasMessageNotContaining("output_format");
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
    }

    @Test
    void recordedAbsentParquetPathIsRefusedViaResumeCommand(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = colocated(outputDir);
        Path recordedOutput = tempDir.resolve("absent.parquet");
        seedRunningFileRun(db, recordedOutput, OutputFormat.PARQUET);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("ambiguous .parquet destination")
                .hasMessageContaining(recordedOutput.toString());
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
    }

    // ---- swath resume <dir>: the directory-dataset run handle ----------------------------------

    /**
     * {@code swath resume <dir>} opens the co-located {@code <dir>/.swath/checkpoint.sqlite} directly
     * and resumes that directory-dataset run — here a run with nothing left to do, so the resume is a
     * clean, network-free no-op driven entirely by the run handle it was handed.
     */
    @Test
    void resumeByDirectoryOpensTheCoLocatedRunHandle(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path db = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        Files.createDirectories(db.getParent());
        seedCompletedDirectoryRun(db, outputDir, outputDir.toString());

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    /**
     * The run handle and the checkpoint's recorded destination may be spelled differently — a
     * trailing separator, a {@code ..} detour — as long as they name the same directory. Those
     * resume normally: only a genuinely different location is refused.
     */
    @Test
    void resumeByDirectoryAcceptsAnEquivalentSpellingOfTheRecordedDestination(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        seedCompletedDirectoryRun(colocated(outputDir), outputDir, outputDir + "/");

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir.resolve("..").resolve("dataset");

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    /**
     * {@code swath resume <dir>} treats {@code <dir>} as the whole run handle, so a checkpoint whose
     * recorded destination is a different directory — a dataset that was moved, or a checkpoint whose
     * stored path was rewritten — is refused (exit 2) naming both directories, and the recorded
     * location is never written to.
     */
    @Test
    void resumeByDirectoryRefusesCheckpointRecordingAnotherDirectory(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path elsewhere = Files.createDirectories(tempDir.resolve("elsewhere"));
        seedCompletedDirectoryRun(colocated(outputDir), outputDir, elsewhere.toString());

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("swath resume refused")
                .hasMessageContaining(outputDir.toString())
                .hasMessageContaining(elsewhere.toString());
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
        try (var recorded = Files.list(elsewhere)) {
            assertThat(recorded).isEmpty();
        }
    }

    /**
     * A run started as {@code swath list … -o dataset} records that relative spelling verbatim, and it
     * only ever meant anything against the working directory of THAT run — which the string does not
     * carry and {@code swath resume <dir>}, typed from anywhere, need not share. So the recorded
     * spelling is not compared at all: the run handle is bound in its place before anything writes.
     * Driven with real listing work outstanding, so the resumed run genuinely opens its sink and
     * publishes its dataset — under {@code <dir>}, and nowhere the recorded spelling points to here.
     */
    @Test
    void resumeByDirectoryWithARelativeRecordedDestinationWritesOnlyUnderTheRunHandle(
            @TempDir Path tempDir) throws Exception {
        // The recorded spelling names nothing from the working directory this test runs in.
        assertThat(Path.of("dataset")).doesNotExist();

        Path outputDir = tempDir.resolve("dataset");
        seedResumableDirectoryRun(colocated(outputDir), outputDir, "dataset");

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;
        cmd.fetcherOverride = matchingObjects(20);

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        assertThat(layout.success()).exists();
        assertThat(layout.dataParts()).isNotEmpty();
        assertThat(Path.of("dataset")).doesNotExist();
    }

    /**
     * A resume that is about to be REFUSED writes its early-exit summary too — and for Parquet that
     * report defaults to {@code <destination>/summary.json}, creating the parent directory and
     * replacing the file. So the run handle must already be bound when a refusal writes: a checkpoint
     * recording a relative destination must not have its summary land wherever THIS process reads
     * that spelling. Covers the fatal-run refusal (the checkpoint records a run the fatal-error guard
     * marked FAILED).
     */
    @Test
    void refusedFatalRunResumeWritesNothingAtTheRelativeReadingOfTheRecordedDestination(
            @TempDir Path tempDir) throws Exception {
        Path cwdReading = Path.of(RELATIVE_RECORDING);
        assertThat(cwdReading).doesNotExist();

        Path outputDir = tempDir.resolve("dataset");
        seedFatalFailedDirectoryRun(colocated(outputDir), outputDir, RELATIVE_RECORDING);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        try {
            assertThatThrownBy(cmd::call)
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("recorded FAILED");
            assertNothingWrittenAt(cwdReading);
        } finally {
            deleteRecursively(cwdReading);
        }
        assertThat(outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME))
                .as("the refusal's report belongs inside the run handle the operator named")
                .exists();
    }

    /**
     * The same for the identity refusal, which is the LAST of the pre-run refusals: a checkpoint whose
     * stored identity fingerprint cannot be confirmed to match this invocation is refused, and that
     * refusal must not have written outside the run handle on its way out either.
     */
    @Test
    void refusedIdentityResumeWritesNothingAtTheRelativeReadingOfTheRecordedDestination(
            @TempDir Path tempDir) throws Exception {
        Path cwdReading = Path.of(RELATIVE_RECORDING);
        assertThat(cwdReading).doesNotExist();

        Path outputDir = tempDir.resolve("dataset");
        seedResumableDirectoryRun(colocated(outputDir), outputDir, RELATIVE_RECORDING,
                UNCONFIRMABLE_IDENTITY_SPEC);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        try {
            assertThatThrownBy(cmd::call)
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("changed since the checkpointed run");
            assertNothingWrittenAt(cwdReading);
        } finally {
            deleteRecursively(cwdReading);
        }
        assertThat(outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME)).exists();
    }

    /**
     * A {@code -o ../dataset} recording resumes exactly like any other relative one. Nothing about
     * the original working directory is recoverable from such a string — a run in {@code /root/sub}
     * wrote to {@code /root/dataset} — so reading it anywhere, against this process's directory or
     * against the run handle's place in the tree, could only refuse a perfectly legitimate resume.
     */
    @Test
    void resumeByDirectoryAcceptsARelativeRecordedDestinationThatClimbsOut(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        seedCompletedDirectoryRun(colocated(outputDir), outputDir, "../dataset");

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    /**
     * The {@code .parquet} disambiguation — a recorded {@code .parquet} path is resumable only when a
     * directory dataset is really there — probes the directory this resume will WRITE to. For a
     * relative recording that is the run handle, so a {@code -o dataset.parquet} directory dataset
     * resumes instead of being called ambiguous because that spelling happens to name nothing here.
     */
    @Test
    void resumeByDirectoryAcceptsARelativeRecordedParquetDirectory(@TempDir Path tempDir)
            throws Exception {
        assertThat(Path.of("dataset.parquet")).doesNotExist();

        Path outputDir = tempDir.resolve("dataset.parquet");
        seedCompletedDirectoryRun(colocated(outputDir), outputDir, "dataset.parquet");

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(Path.of("dataset.parquet")).doesNotExist();
    }

    /**
     * A recorded destination that no longer exists cannot be resolved to a real path, so the
     * comparison falls back to its absolute normalized form: the refusal is still the ordinary
     * exit-2 one naming both directories, never an escaping I/O failure.
     */
    @Test
    void resumeByDirectoryRefusesRecordedDestinationThatNoLongerExists(@TempDir Path tempDir)
            throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        Path vanished = tempDir.resolve("vanished");
        seedCompletedDirectoryRun(colocated(outputDir), outputDir, vanished.toString());

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("swath resume refused")
                .hasMessageContaining(outputDir.toString())
                .hasMessageContaining(vanished.toString());
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
        assertThat(vanished).doesNotExist();
    }

    /**
     * A checkpoint that records no destination at all names no location to be redirected to, so the
     * run-handle guard admits it — the refusal is reserved for an absolute recorded path naming
     * somewhere else. What such a checkpoint cannot supply is the output directory Parquet needs, so a
     * run with listing work left stops on ordinary config validation, before any listing and without
     * publishing anything under the run handle.
     */
    @Test
    void resumeByDirectoryAcceptsCheckpointWithNoRecordedDestinationThenLacksAnOutputDir(
            @TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        seedResumableDirectoryRun(colocated(outputDir), outputDir, null);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Parquet output requires -o <dir>")
                .hasMessageNotContaining("swath resume refused");
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
        assertThat(DatasetLayout.of(outputDir).success()).doesNotExist();
    }

    /**
     * {@code swath resume <completed-dir>} — a dataset whose {@code _SUCCESS} + manifest are present
     * but whose co-located checkpoint was deleted on completion — is a clean no-op: it prints "already
     * complete" and exits 0, never treating the finished dir as a run needing work.
     */
    @Test
    void resumeByDirectoryOfCompletedDatasetIsAlreadyComplete(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("dataset");
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Files.createDirectories(layout.dataDir());
        Files.writeString(layout.manifest(),
                "{\"sourceBucket\":\"" + BUCKET + "\",\"files\":[]}");
        Files.writeString(layout.success(), "");
        // No co-located checkpoint: completion deleted it.
        assertThat(CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir)).doesNotExist();

        ResumeCommand cmd = new ResumeCommand();
        StringWriter out = new StringWriter();
        CommandLine cli = new CommandLine(cmd);
        cli.setOut(new PrintWriter(out));
        cli.parseArgs(outputDir.toString());

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(out.toString()).contains("already complete");
    }

    /** {@code swath resume <dir>} with neither a checkpoint nor a completed dataset is a clear error. */
    @Test
    void resumeByDirectoryWithNoRunErrors(@TempDir Path tempDir) throws Exception {
        Path outputDir = Files.createDirectories(tempDir.resolve("empty-dir"));

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("no run to resume")
                .hasMessageContaining(outputDir.toString());
    }

    /**
     * Seed a COMPLETED directory-dataset run whose checkpoint records {@code recordedDestination} as
     * its output, so resuming it by its {@code <dir>} run handle is a clean, network-free no-op.
     */
    private static void seedCompletedDirectoryRun(Path db, Path outputDir, String recordedDestination)
            throws Exception {
        Files.createDirectories(outputDir);
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8), argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false,
                        recordedDestination, false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            // Parquet resume is exactly-once: a node is only output-complete once durable_cursor has
            // caught up to cursor. Without this the resume would re-list the non-durable tail (network).
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    /**
     * Seed a directory-dataset run with listing work still outstanding: the single node's page is not
     * the last one, so resuming it by its {@code <dir>} run handle really does open an output sink
     * rather than returning early on an empty worklist.
     */
    private static void seedResumableDirectoryRun(Path db, Path outputDir, String recordedDestination)
            throws Exception {
        seedResumableDirectoryRun(db, outputDir, recordedDestination, null);
    }

    /** As {@link #seedResumableDirectoryRun(Path, Path, String)}, with a stored identity fingerprint. */
    private static void seedResumableDirectoryRun(Path db, Path outputDir, String recordedDestination,
                                                  String identitySpec) throws Exception {
        Files.createDirectories(outputDir);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(
                    directoryRunKey(recordedDestination, identitySpec), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k1".getBytes(StandardCharsets.UTF_8), false));
        }
    }

    /**
     * Seed a directory-dataset run recorded FAILED with the fatal-error flag the CLI's own guard sets
     * — the state {@code swath resume} refuses rather than re-attempting a deterministic failure.
     */
    private static void seedFatalFailedDirectoryRun(Path db, Path outputDir, String recordedDestination)
            throws Exception {
        Files.createDirectories(outputDir);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(directoryRunKey(recordedDestination, null), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            store.markRunFatalUnlessFinished(run.id());
        }
    }

    /** The Parquet directory-dataset run key these seeds share, varying only in what it records. */
    private static RunKey directoryRunKey(String recordedDestination, String identitySpec) {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        return new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8), argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false,
                        recordedDestination, false, null, null), false, identitySpec);
    }

    /**
     * Nothing may appear at {@code path}. Reported as the absence it is, rather than by comparing a
     * directory listing, so the failure names the stray path a refusal was never allowed to create.
     */
    private static void assertNothingWrittenAt(Path path) {
        assertThat(path)
                .as("a refused resume must not write outside the run handle it was given")
                .doesNotExist();
    }

    /** Remove a path a failing run may have left behind, so the working directory stays clean. */
    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }

    /**
     * A fetcher whose {@code count} objects all pass {@link #DISTINCTIVE_FILTER_SPEC} — matching name,
     * size, modification time and storage class — so a resumed run does not merely list, it emits rows
     * and publishes part files.
     */
    private static MockPageFetcher matchingObjects(int count) {
        List<MockObject> objects = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            objects.add(new MockObject(
                    String.format(PREFIX + "key-%05d.log", i).getBytes(StandardCharsets.UTF_8),
                    2048L, WITHIN_FILTER_WINDOW_MICROS, String.format("%08x", i), "STANDARD"));
        }
        return MockPageFetcher.builder().objects(objects).build();
    }

    private static void seedRunningFileRun(Path db, Path output, OutputFormat format) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8), argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, format.name(),
                new SoftRestoreContext(false, null, null, false, false, output.toString(), false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k1".getBytes(StandardCharsets.UTF_8), false));
        }
    }

    /** Seed the fully published sorted state whose resume path performs no S3 listing work. */
    private static void seedPublishedSortedRun(Path db, Path outputDir) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Files.createDirectories(outputDir);
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET,
                PREFIX.getBytes(StandardCharsets.UTF_8), argsHash, "auto", ListingMode.OBJECTS,
                DISTINCTIVE_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false,
                        outputDir.toString(), false, null, null), true);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);

            DatasetLayout layout =
                    DatasetLayout.of(outputDir);
            Files.writeString(layout.state(),
                    "{\"args_hash\":\"" + argsHash + "\",\"run_id\":" + run.id() + "}");
            Files.writeString(layout.manifest(),
                    "{\"sourceBucket\":\"" + BUCKET
                            + "\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
            Files.writeString(layout.success(), "");
        }
    }

    /**
     * {@code --bearer-token-command} must never reach the checkpoint: a persisted row would make
     * {@code run_meta} decide which command a later {@code swath resume} executes, so whoever can
     * write a checkpoint file could choose that command. FREE (no row) is the security property;
     * {@link ResumeRegistryDriftTest#everyFreeOptionHasNoRegistryRow()} enforces the same invariant
     * generically, but this names the reason so the classification is not "simplified" back to
     * STICKY to match the {@code --profile}/{@code --region} block it sits in.
     */
    @Test
    void bearerTokenOptionsAreNeverPersistedToTheCheckpoint() {
        assertThat(ResumeRegistry.hasPersistedRow("--bearer-token-command")).isFalse();
        assertThat(ResumeRegistry.hasPersistedRow("--bearer-token-refresh-interval")).isFalse();
    }

    /**
     * Because they are never persisted (above), re-passing them on {@code swath resume} is the ONLY
     * way a resumed run against a bearer-auth endpoint can authenticate — so the forwarding onto the
     * delegated {@link ListCommand} is load-bearing, not a convenience.
     *
     * <p>One malformed-duration assertion pins BOTH forwardings at once, because
     * {@code ConnectionOptions#resolveBearerTokenSupplier()} parses the interval only when the
     * command is non-null: drop the command forwarding and it returns early (no throw); drop the
     * interval forwarding and the 45m default is used (no throw).
     */
    @Test
    void bearerTokenOptionsForwardOntoTheDelegatedListCommand(@TempDir Path tempDir) throws Exception {
        Path outputDir = seedCompletedRunDir(tempDir);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = outputDir;
        cmd.bearerTokenCommand = "printf token";
        cmd.bearerTokenRefreshInterval = "not-a-duration";

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("bearer-token-refresh-interval");
    }

    private static void overwriteOutputFormat(Path db, String format) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("UPDATE run_meta SET output_format=?")) {
            ps.setString(1, format);
            ps.executeUpdate();
        }
    }
}
