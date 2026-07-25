/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.checkpoint.SqliteCheckpointStore.RunIdentity;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.store.PageFetcher;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * {@code swath resume <dir>} — resume the run whose checkpoint is co-located in a directory-dataset
 * output ({@code <dir>/.swath/checkpoint.sqlite}): the output directory is the whole run handle. It
 * reads the run's listing identity (scheme, bucket, prefix, endpoint, format) from the checkpoint DB
 * and re-drives {@link ListCommand} in resume mode, continuing each non-COMPLETED node from its
 * cursor (text) / durable_cursor (Parquet). A {@code <dir>} whose dataset is already complete (its
 * checkpoint was deleted on completion) reports "already complete" and exits 0. Credentials and
 * region resolve from the environment, exactly as a fresh {@code list} would. A directory-dataset
 * output is the only resumable regime.
 */
@Command(name = "resume", mixinStandardHelpOptions = true,
        description = "Resume a run by its output directory: swath resume <dir>.")
public final class ResumeCommand implements Callable<Integer>, GlobalOptions.Carrier {

    @Parameters(index = "0", arity = "0..1", paramLabel = "<dir>",
            description = "The directory-dataset run handle to resume "
                    + "(opens <dir>/.swath/checkpoint.sqlite).")
    Path directory;

    final TuneOptions tune = new TuneOptions();

    @Resume(ResumeClass.FREE)
    @Option(names = "--tune", paramLabel = "KEY=VALUE",
            description = "Set a resume-safe typed expert option (repeatable).")
    void tune(String[] values) {
        tune.entries = new ArrayList<>(List.of(values));
    }

    @Mixin
    final GlobalOptions global = new GlobalOptions();

    /**
     * Test-only seam (package-private, null in production): handed to the rebuilt {@link ListCommand}
     * so a resume can be driven to its real output sink without S3. Mirrors
     * {@link ListCommand#fetcherOverride}.
     */
    PageFetcher fetcherOverride;

    @Spec
    CommandSpec spec;

    @Override
    public GlobalOptions globalOptions() {
        return global;
    }

    @Override
    public Integer call() throws Exception {
        ListCommand list = new ListCommand();
        CommandLine delegated = new CommandLine(list);
        if (spec != null) {
            delegated.setOut(spec.commandLine().getOut());
            delegated.setErr(spec.commandLine().getErr());
        }
        PrintWriter commandOut = delegated.getOut();
        if (tune.applyForResume(list.sorting, commandOut)) {
            return ExitCodes.SUCCESS;
        }

        if (directory != null) {
            Path colocated = CheckpointOptions.CheckpointMode.colocatedCheckpoint(directory);
            if (!Files.isRegularFile(colocated) || Files.size(colocated) == 0L) {
                // No live checkpoint under the run handle: a COMPLETE dataset (its checkpoint was
                // deleted on completion) is a clean no-op, exit 0; anything else has no run to resume.
                if (DatasetDirGuard.isCompletedDataset(directory)) {
                    commandOut.println(directory + " is already complete — nothing to resume.");
                    commandOut.flush();
                    return ExitCodes.SUCCESS;
                }
                throw new InvalidArgsException("no run to resume at " + directory + " (no checkpoint at "
                        + colocated + " and no completed dataset); run `swath list -o " + directory
                        + " …` first");
            }
        }

        CheckpointCandidate candidate = resumeCheckpoint().orElseThrow(this::missingCheckpoint);
        Path db = candidate.db();
        RunIdentity id = candidate.id();

        list.uri = id.scheme() + "://" + id.bucket() + "/"
                + new String(id.prefix() == null ? new byte[0] : id.prefix(), StandardCharsets.UTF_8);
        list.connection.endpointUrl = id.endpoint();
        // Important: `list` is driven directly via #call(), never through picocli's
        // own parse of the "list" subcommand -- its OWN @Mixin GlobalOptions instance never sees
        // whatever -q/-v the user actually passed to `swath resume`. Propagate explicitly,
        // merging root+leaf the same way ListCommand#call() does for its own invocation.
        int verbosity = spec != null ? GlobalOptions.effectiveVerbosity(spec.commandLine()) : global.verbosity.length;
        list.global.verbosity = new boolean[verbosity];
        int quietLevel = spec != null ? GlobalOptions.effectiveQuietLevel(spec.commandLine()) : global.quiet.length;
        list.global.quiet = new boolean[quietLevel];
        list.global.color = spec != null ? GlobalOptions.effectiveColor(spec.commandLine()) : global.color;
        // Do not parse checkpoint output_format here: ListCommand must first classify the
        // checkpoint's recorded destination and let a FILE-origin refusal win with exit 2. The
        // checkpoint path is also the marker that preserves the ordinary malformed-format exit-1
        // mapping once that safety gate has passed.
        list.resumeCommandCheckpoint = db;
        // The run handle is the output directory, so ListCommand refuses a checkpoint whose recorded
        // destination is somewhere else rather than publishing the dataset outside <dir>.
        list.resumeCommandRunHandle = directory;
        list.checkpoint.resume = true;
        list.checkpoint.location = db.toString();
        list.fetcherOverride = fetcherOverride;
        list.sorting.sort = readLatestSortEnabled(db, id);
        list.tune.copyEffectiveValuesFrom(tune);

        // Restore filter fields from the stored filter_spec so the rebuilt ListCommand
        // computes the same filter_spec and the resume-safety check passes.
        FilterSpecCodec.Decoded filters = FilterSpecCodec.decode(id.filterSpec());
        list.filters.include = filters.include();
        list.filters.exclude = filters.exclude();
        list.filters.minSize = filters.minSize();
        list.filters.maxSize = filters.maxSize();
        list.filters.modifiedAfter = filters.modifiedAfter();
        list.filters.modifiedBefore = filters.modifiedBefore();
        list.filters.storageClasses = filters.storageClasses();

        return list.call();
    }

    /**
     * Restore the selected run's persisted sort mode. The identity predicates make the second,
     * post-selection read fail closed if the latest row changed between checkpoint discovery and
     * reconstruction instead of combining one run's identity with another run's sort mode.
     */
    private static boolean readLatestSortEnabled(Path db, RunIdentity selected)
            throws CheckpointException {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath())) {
            boolean hasSortEnabled = false;
            try (PreparedStatement columns = c.prepareStatement("PRAGMA table_info(run_meta)");
                 ResultSet rs = columns.executeQuery()) {
                while (rs.next()) {
                    if ("sort_enabled".equals(rs.getString("name"))) {
                        hasSortEnabled = true;
                        break;
                    }
                }
            }

            // The only two projections are fixed tokens. A checkpoint from before sorted output
            // has no sort_enabled column and therefore means the historical default: unsorted.
            String projection = hasSortEnabled ? "sort_enabled" : "0";
            String sql = """
                    SELECT %s
                    FROM run_meta
                    WHERE id = (
                        SELECT id FROM run_meta ORDER BY started_at DESC, id DESC LIMIT 1
                    )
                      AND store_scheme IS ?
                      AND endpoint IS ?
                      AND bucket IS ?
                      AND prefix IS ?
                      AND output_format IS ?
                      AND filter_spec IS ?
                      AND status IS ?
                      AND started_at = ?
                    """.formatted(projection);
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, selected.scheme());
                ps.setString(2, selected.endpoint());
                ps.setString(3, selected.bucket());
                ps.setBytes(4, selected.prefix());
                ps.setString(5, selected.outputFormat());
                ps.setString(6, selected.filterSpec());
                ps.setString(7, selected.status().name());
                ps.setLong(8, selected.startedAt());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new CheckpointException("failed to read sort mode from " + db
                                + ": the selected run is no longer the latest checkpoint row");
                    }
                    int value = rs.getInt(1);
                    boolean wasNull = rs.wasNull();
                    if (hasSortEnabled && (wasNull || (value != 0 && value != 1))) {
                        throw new CheckpointException("checkpoint row has invalid sort_enabled in " + db
                                + ": " + (wasNull ? "null" : value));
                    }
                    return value != 0;
                }
            }
        } catch (SQLException e) {
            throw new CheckpointException("failed to read sort mode from " + db, e);
        }
    }

    record CheckpointCandidate(Path db, RunIdentity id) {
    }

    Optional<CheckpointCandidate> resumeCheckpoint() throws IOException, CheckpointException {
        // The <dir> run handle opens its co-located checkpoint directly (a single DB, no scan). The
        // call()-level positional guard has already handled a completed/absent <dir>, so when a
        // directory is given here its co-located file exists.
        if (directory == null) {
            return Optional.empty();
        }
        Path colocated = CheckpointOptions.CheckpointMode.colocatedCheckpoint(directory);
        if (!Files.isRegularFile(colocated) || Files.size(colocated) == 0L) {
            return Optional.empty();
        }
        return SqliteCheckpointStore.readLatestRun(colocated)
                .map(id -> new CheckpointCandidate(colocated, id));
    }

    private InvalidArgsException missingCheckpoint() {
        return new InvalidArgsException("no checkpointed run to resume — pass the run directory as "
                + "`swath resume <dir>`");
    }
}
