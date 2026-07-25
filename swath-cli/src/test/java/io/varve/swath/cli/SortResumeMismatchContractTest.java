/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * The authoritative {@code --sort} resume-refusal matrix, going beyond
 * a basic pass/fail check with exit-code assertions and a checkpoint-integrity guard:
 *
 * <ul>
 *   <li>a {@code --sort} run resumed {@code --no-sort} is REFUSED at exit 2 with a message naming
 *       the {@code --sort/--no-sort} mismatch; the inverse direction too;</li>
 *   <li>{@code --sort} with a text format is a clear {@link InvalidArgsException} (exit 2);</li>
 *   <li>the refused attempt does NOT corrupt the checkpoint: {@code sort_enabled} is unchanged and a
 *       subsequent CORRECT resume still completes.</li>
 * </ul>
 */
final class SortResumeMismatchContractTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    // ---------------------------------------------------------------------
    // Refusal matrix + exit code.
    // ---------------------------------------------------------------------

    @Test
    void sortRunResumedNoSort_refusedAtExitTwo(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedPublishedSortRun(db, dir, true);

        InvalidArgsException ex = catchThrowableOfType(InvalidArgsException.class,
                () -> resumeCommand(db, dir, false).call());
        assertThat(ex).isNotNull();
        assertThat(ex).hasMessageContaining("sort_enabled changed since the checkpointed run");
        assertThat(ex.exitCode()).as("bad-flags refusal is exit 2").isEqualTo(2);
    }

    @Test
    void plainRunResumedWithSort_refusedAtExitTwo(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        seedPublishedSortRun(db, dir, false);

        InvalidArgsException ex = catchThrowableOfType(InvalidArgsException.class,
                () -> resumeCommand(db, dir, true).call());
        assertThat(ex).isNotNull();
        assertThat(ex).hasMessageContaining("sort_enabled changed since the checkpointed run");
        assertThat(ex.exitCode()).isEqualTo(2);
    }

    @Test
    void sortWithTextFormat_isAClearErrorAtExitTwo() {
        // Stdout is a valid checkpointed text destination. It keeps this matrix focused on the
        // --sort/text-format refusal instead of the earlier FILE+checkpoint structural refusal.
        for (OutputFormat text : List.of(
                OutputFormat.JSONL, OutputFormat.TSV, OutputFormat.TABLE)) {
            ListCommand cmd = new ListCommand();
            new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--sort", "--format", text.name(),
                    "-o", "-");
            InvalidArgsException ex = catchThrowableOfType(InvalidArgsException.class, cmd::call);
            assertThat(ex).as("--sort with --format %s must be a clear error", text).isNotNull();
            assertThat(ex).hasMessageContaining("--sort requires --format parquet");
            assertThat(ex.exitCode()).isEqualTo(2);
        }
    }

    // ---------------------------------------------------------------------
    // Checkpoint integrity: a refused attempt leaves a correct resume workable.
    // ---------------------------------------------------------------------

    @Test
    void refusedResumeDoesNotCorruptCheckpoint_subsequentCorrectResumeSucceeds(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        long runId = seedPublishedSortRun(db, dir, true);
        assertThat(sortEnabled(db, runId)).isTrue();

        // A mismatched --no-sort resume is refused ...
        InvalidArgsException refused = catchThrowableOfType(InvalidArgsException.class,
                () -> resumeCommand(db, dir, false).call());
        assertThat(refused).isNotNull();

        // ... and leaves the checkpoint intact: sort_enabled unchanged.
        assertThat(sortEnabled(db, runId)).as("the refused attempt did not flip sort_enabled").isTrue();

        // A subsequent CORRECT --sort resume still completes (PUBLISHED re-entry, no LIST work).
        assertThat(resumeCommand(db, dir, true).call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("COMPLETED");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /**
     * Seed a fully PUBLISHED sort run: the node is output-complete and a {@code manifest.json} exists
     * that correctly identifies as THIS run's ({@code args_hash} + {@code run_id} must both match, or
     * the manifest is untrusted and the state machine falls through to merge-pending instead), so a
     * correct sorted resume takes the no-network PUBLISHED re-entry path. Returns the run id.
     */
    private static long seedPublishedSortRun(Path db, Path outputDir, boolean sortEnabled) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Files.createDirectories(outputDir);
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outputDir.toString(), false, null, null),
                sortEnabled, storedIdentitySpec(outputDir, sortEnabled));
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());   // durable_cursor catches up ⇒ loadResumable empty
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
            // Written AFTER openRun so run.id() is known — must match exactly for the PUBLISHED
            // dispatch to trust this run. Identity lives in .swath-state.json (what
            // isPublishedByThisRun/readIdentity consult); the consumer manifest.json exists too.
            DatasetLayout layout =
                    DatasetLayout.of(outputDir);
            Files.writeString(layout.state(),
                    "{\"args_hash\":\"" + argsHash + "\",\"run_id\":" + run.id() + "}");
            Files.writeString(layout.manifest(),
                    "{\"sourceBucket\":\"" + BUCKET + "\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
            return run.id();
        }
    }

    /**
     * The {@code run_meta.identity_spec} a real creating run would persist: the registry's IDENTITY
     * fingerprint over a {@link ListCommand} mirroring the seeded run's fully-resolved state. Reusing
     * {@link ResumeRegistry#identitySpec} (not a hand-written string) keeps the fixture in lockstep
     * with the single source of truth the drift guard protects.
     */
    private static String storedIdentitySpec(Path outputDir, boolean sortEnabled) throws Exception {
        ListCommand original = new ListCommand();
        original.uri = "s3://" + BUCKET + "/" + PREFIX;
        original.connection.endpointUrl = ENDPOINT;
        original.output.destination = outputDir.toString();
        original.output.format = OutputFormat.PARQUET;
        original.sorting.sort = sortEnabled;
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

    private static ListCommand resumeCommand(Path db, Path outputDir, boolean sort) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = sort;
        return cmd;
    }

    private static boolean sortEnabled(Path db, long runId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("SELECT sort_enabled FROM run_meta WHERE id=?")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getInt(1) != 0;
            }
        }
    }
}
