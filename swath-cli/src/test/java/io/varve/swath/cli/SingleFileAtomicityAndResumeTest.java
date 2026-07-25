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
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.ListingException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Single-file
 * destinations are published ATOMICALLY (a hidden temp sibling, renamed into place
 * only on success) and are NON-RESUMABLE (only a directory dataset carries a resume handle).
 * Supersedes {@code ListCommandResumeAppendTest} (retired): its "resumed openSink appends" model
 * is exactly the behavior this replaces with an outright resume refusal.
 */
final class SingleFileAtomicityAndResumeTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    // ---- atomic publish ------------------------------------------------------------------------

    @Test
    void successfulRunPublishesTheRealFileAndLeavesNoTempSibling(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("listing.jsonl");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = freshCommand(out);
        cmd.fetcherOverride = fetcher;

        int exit = cmd.call();

        assertThat(exit).isEqualTo(ExitCodes.SUCCESS);
        assertThat(out).exists();
        assertThat(Files.readAllLines(out)).hasSize(2);
        assertThat(stagingSiblings(dir, out)).as("unique temp sibling cleaned up by the rename").isEmpty();
    }

    @Test
    void successfulRunAtomicallyReplacesAPreExistingDestination(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("listing.jsonl");
        Files.writeString(out, "PRE-EXISTING\n");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/replacement".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = freshCommand(out);
        cmd.fetcherOverride = fetcher;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(Files.readAllLines(out)).hasSize(1);
        assertThat(Files.readString(out)).contains("data/replacement").doesNotContain("PRE-EXISTING");
        assertThat(stagingSiblings(dir, out)).isEmpty();
    }

    @Test
    void crashMidRunLeavesNoPartialFileAtTheRealDestination(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("listing.jsonl");
        // A pre-existing file at the real path must survive untouched too -- the crash never
        // even reaches the rename, so an old completed run's file is never clobbered.
        Files.writeString(out, "PRE-EXISTING\n");

        MockPageFetcher faulty = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8),
                        "data/c".getBytes(StandardCharsets.UTF_8)))
                .interceptor((req, idx, page) -> {
                    if (idx == 1) {
                        throw new ListingException("injected crash mid-run");
                    }
                    return page;
                })
                .build();

        ListCommand cmd = freshCommand(out);
        cmd.fetcherOverride = faulty;

        assertThatThrownBy(cmd::call).isInstanceOf(ListingException.class);

        // The real destination is untouched (still the pre-existing content) -- the crash never
        // reached commitFileSink()'s rename.
        assertThat(Files.readString(out)).isEqualTo("PRE-EXISTING\n");
        assertThat(stagingSiblings(dir, out)).as("failed attempt cleans its unique temp sibling").isEmpty();
    }

    private static List<Path> stagingSiblings(Path dir, Path out) throws Exception {
        String prefix = "." + out.getFileName() + ".swath.";
        try (var paths = Files.list(dir)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)
                            && path.getFileName().toString().endsWith(".tmp"))
                    .toList();
        }
    }

    // ---- FILE-kind creation is structurally ephemeral ---------------------------------------

    @Test
    void fileKindDestinationRequiresCheckpointNoneAtCreation(@TempDir Path dir) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.resolve("listing.jsonl").toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("FILE-kind destination")
                .hasMessageContaining("--checkpoint none")
                .hasMessageContaining("directory-dataset destination");
    }

    @Test
    void parquetFileKindGuardDescribesItsCurrentPhysicalDatasetLayout(@TempDir Path dir) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.output.destination = dir.resolve("listing.parquet").toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--checkpoint none")
                .hasMessageContaining("one-writer dataset directory under the path")
                .hasMessageContaining("still non-resumable");
    }

    /**
     * The same hazard as the FILE-kind guard above, for stdout: an explicit non-none
     * {@code --checkpoint <path>} against a stdout destination opens a real checkpoint file that
     * nothing can ever resume (stdout is ephemeral, one-shot) -- an orphan on disk. An explicit
     * {@code --checkpoint auto} stays unaffected: {@code auto} against stdout resolves to no
     * durable file at all (see {@link CheckpointOptions.CheckpointMode#resolve}), so there is
     * nothing to orphan and the default run must keep succeeding exactly as before.
     */
    @Test
    void stdoutDestinationRefusesAnExplicitCheckpointRequest(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("checkpoint.sqlite");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.output.destination = "-";
        cmd.checkpoint.location = db.toString();
        cmd.fetcherOverride = fetcher;

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("stdout")
                .hasMessageContaining("--checkpoint none")
                .hasMessageContaining("directory-dataset destination");
        assertThat(db).doesNotExist();
    }

    /**
     * The FILE-kind physical-layout note ("writes a one-writer dataset directory under the path")
     * describes a real {@code -o} path -- it must not leak onto the stdout refusal, which has no
     * path to describe. {@code --format parquet} is reachable here: the stdout-vs-parquet sink
     * rejection itself only fires later, in {@code openParquetDir}.
     */
    @Test
    void stdoutParquetRefusalOmitsTheFileKindPhysicalLayoutNote() {
        ListCommand cmd = new ListCommand();
        cmd.output.destination = "-";

        String refusal = cmd.checkpointRefusalForEphemeralSink(OutputOptions.DestinationKind.STDOUT,
                CheckpointOptions.CheckpointMode.parse("/tmp/some.sqlite"), OutputFormat.PARQUET);

        assertThat(refusal)
                .contains("the stdout destination")
                .doesNotContain("one-writer dataset directory")
                .doesNotContain("still non-resumable");
    }

    /**
     * The stdout {@code auto} default must keep succeeding exactly as today: {@code auto} against
     * stdout resolves to no durable checkpoint file at all, so there is nothing to orphan and no
     * explicit request was ever made. Exercised directly against the extracted guard method rather
     * than a full {@code cmd.call()} run: a completed stdout run writes through the raw process
     * stdout file descriptor, which closing it would take down for the rest of the test JVM.
     */
    @Test
    void stdoutDestinationWithDefaultAutoCheckpointIsNotRefused() {
        ListCommand cmd = new ListCommand();

        assertThat(cmd.checkpointRefusalForEphemeralSink(OutputOptions.DestinationKind.STDOUT,
                CheckpointOptions.CheckpointMode.parse("auto"), OutputFormat.TSV)).isNull();
    }

    private static ListCommand freshCommand(Path destination) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = "none";
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = destination.toString();
        return cmd;
    }

    // ---- refuse-resume onto a FILE-kind destination, including the restored-path round trip
    // ---- that also pins recomputeKindAfterRestore -----------------------------------------

    @Test
    void recordedTextFileResumeCannotBeRedirectedToStdout(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out.jsonl");
        seedResumableRun(db, out.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.JSONL;
        cmd.output.destination = "-";   // must not hide the checkpoint's recorded FILE origin
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("non-resumable")
                .hasMessageContaining(out.toString())
                .hasMessageContaining("changing the resume invocation's -o");
    }

    @Test
    void recordedTextRefusalPrecedesMalformedStoredOutputFormat(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out.jsonl");
        seedResumableRun(db, out.toString());
        overwriteStoredOutputFormat(db, "BROKEN");

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.destination = "-";
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("FILE-kind text destination")
                .hasMessageContaining(out.toString())
                .hasMessageNotContaining("unknown output format");
    }

    @Test
    void invalidRecordedParquetPathRefusesWithExitTwoInsteadOfUncheckedPathFailure(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        String invalidPath = "\u0000legacy.parquet";
        seedCompletedRun(db, OutputFormat.PARQUET, invalidPath);

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("ambiguous .parquet destination")
                .hasMessageContaining("directory dataset must exist");
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
    }

    @Test
    void bareResumeTreatsUnrecognizedRecordedExtensionAsDirectoryDataset(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("legacy-output");
        seedCompletedRun(db, OutputFormat.JSONL, out.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        // Deliberately no -o/--format: both are restored from the checkpoint. Destination-kind
        // inference defines an unrecognized extension as a directory dataset, not a FILE.
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(cmd.output.resolvedKind).isEqualTo(OutputOptions.DestinationKind.DIRECTORY);
    }

    /**
     * The recomputeKindAfterRestore round trip: a BARE {@code swath resume} with NO explicit {@code -o} only learns
     * its destination from {@link ListCommand#restoreRunContext} -- {@code resolvedKind} must be
     * re-derived against that RESTORED destination (not stuck at the pre-restore stdout default),
     * or this refusal would never fire and the run would silently drift to directory-dataset
     * (multi-writer) config instead. The refusal message naming the restored path IS the proof
     * the restore + recompute both landed correctly.
     */
    @Test
    void bareResumeWithNoExplicitDashORefusesUsingTheRestoredDestination(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("original-run.jsonl");
        seedResumableRun(db, out.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        // NO cmd.output.destination set -- must come from the checkpoint's SoftRestoreContext.
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("non-resumable")
                .hasMessageContaining(out.toString());   // proves resolvedKind saw the RESTORED path
    }

    /** A directory-dataset destination resumes normally -- the refusal is FILE-kind only. */
    @Test
    void directoryDatasetDestinationResumeIsNotRefused(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = dir.resolve("out-dataset");
        Files.createDirectories(outDir);
        // COMPLETED is a no-op only for a valid directory dataset; FILE-kind resumes are refused
        // unconditionally because publication may have failed after the checkpoint said complete.
        seedCompletedRun(db, OutputFormat.PARQUET, outDir.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outDir.toString();
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        // Must NOT throw the single-file refusal -- proceeds (and completes) normally.
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    @Test
    void recognizedParquetExtensionForcedToDirectoryRoundTripsThroughBareResume(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path outDir = dir.resolve("out.parquet");
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand fresh = new ListCommand();
        fresh.uri = "s3://" + BUCKET + "/" + PREFIX;
        fresh.connection.endpointUrl = ENDPOINT;
        fresh.connection.region = "us-east-1";
        fresh.connection.noSignRequest = true;
        fresh.checkpoint.location = db.toString();
        fresh.output.format = OutputFormat.PARQUET;
        fresh.output.destination = outDir.toString();
        fresh.output.outputType = "dir";
        fresh.fetcherOverride = fetcher;

        assertThat(fresh.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(fresh.output.resolvedKind).isEqualTo(OutputOptions.DestinationKind.DIRECTORY);
        assertThat(outDir).isDirectory();

        ListCommand resume = new ListCommand();
        resume.uri = "s3://" + BUCKET + "/" + PREFIX;
        resume.connection.endpointUrl = ENDPOINT;
        resume.checkpoint.location = db.toString();
        resume.checkpoint.resume = true;
        // Deliberately no -o/--output-type/--format: all three must reconstruct safely.

        assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(resume.output.destination).isEqualTo(outDir.toString());
        assertThat(resume.output.resolvedKind).isEqualTo(OutputOptions.DestinationKind.DIRECTORY);
    }

    @Test
    void recordedParquetPathThatIsARegularFileIsRefusedAsAmbiguous(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("legacy.parquet");
        Files.writeString(out, "legacy file");
        seedCompletedRun(db, OutputFormat.PARQUET, out.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("ambiguous .parquet destination")
                .hasMessageContaining("directory dataset must exist")
                .hasMessageContaining(out.toString());
    }

    @Test
    void recordedParquetDirectoryProbeExceptionFailsClosed(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = Files.createDirectories(dir.resolve("out.parquet"));
        seedCompletedRun(db, OutputFormat.PARQUET, out.toString());

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.checkpoint.resume = true;
        cmd.checkpoint.location = db.toString();
        cmd.recordedDestinationDirectoryProbeOverride = path -> {
            throw new SecurityException("injected directory-probe denial");
        };

        Throwable thrown = Assertions.catchThrowable(cmd::call);
        assertThat(thrown)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("ambiguous .parquet destination")
                .hasMessageContaining(out.toString());
        assertThat(ExitCodes.forThrowable(thrown)).isEqualTo(2);
    }

    /** Seeds a RUNNING (resumable) run whose checkpoint records {@code outputPath}. */
    private static void seedResumableRun(Path db, String outputPath) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.JSONL.name(),
                new SoftRestoreContext(false, null, null, false, false, outputPath, false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            // A committed-but-not-terminal page: the node stays resumable (status RUNNING).
            store.commitPage(new PageCommit(node, "k1".getBytes(StandardCharsets.UTF_8), false));
            assertThat(run.status()).isEqualTo(RunStatus.RUNNING);
        }
    }

    /** Seeds a COMPLETED (no-op-resumable) run whose checkpoint records {@code outputPath}. */
    private static void seedCompletedRun(Path db, OutputFormat format, String outputPath) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, format.name(),
                new SoftRestoreContext(false, null, null, false, false, outputPath, false, null, null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            // File-sink (Parquet) "output-complete" ALSO requires durable_cursor IS cursor
            // (loadResumable's fileSink branch) -- commitPage alone only advances cursor/status;
            // markOutputComplete is the separate call that catches durable_cursor up (mirrors
            // SortResumeMismatchContractTest#seedPublishedSortRun's identical seeding need).
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    private static void overwriteStoredOutputFormat(Path db, String format) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement update = connection.prepareStatement("UPDATE run_meta SET output_format=?")) {
            update.setString(1, format);
            update.executeUpdate();
        }
    }
}
