/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins where the checkpoint <em>lives</em> and how {@code swath resume} <em>finds</em> a run, so that
 * any later move of either is a visible, deliberate change rather than a silent one.
 *
 * <p>Two facts are held here: {@code --checkpoint auto} co-locates the DB inside a directory-dataset
 * output at {@code <dir>/.swath/checkpoint.sqlite} (and keeps nothing at all for a stdout / single-file
 * run — the zero-litter default); and {@code swath resume <dir>} discovers that run by opening the
 * co-located checkpoint under the directory it is handed, not by scanning a shared directory.
 */
final class CheckpointLocationPreImageTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    /** {@code auto} + a DIRECTORY dataset co-locates the checkpoint at {@code <dir>/.swath/checkpoint.sqlite}. */
    @Test
    void checkpointAutoCoLocatesInsideTheDirectoryDataset() {
        Path outputDir = Path.of("results");

        Path resolved = CheckpointOptions.CheckpointMode.parse("auto")
                .resolve(outputDir.toString(), OutputOptions.DestinationKind.DIRECTORY);

        assertThat(resolved).isEqualTo(outputDir.resolve(".swath").resolve("checkpoint.sqlite"));
    }

    /** {@code auto} + stdout keeps nothing on disk — the zero-litter default (no {@code .swath-checkpoint/}). */
    @Test
    void checkpointAutoIsEphemeralForStdout() {
        Path resolved = CheckpointOptions.CheckpointMode.parse("auto")
                .resolve(null, OutputOptions.DestinationKind.STDOUT);

        assertThat(resolved).isNull();
    }

    /** {@code auto} + a single-file destination is likewise ephemeral (single files are non-resumable). */
    @Test
    void checkpointAutoIsEphemeralForSingleFile() {
        Path resolved = CheckpointOptions.CheckpointMode.parse("auto")
                .resolve("out.parquet", OutputOptions.DestinationKind.FILE);

        assertThat(resolved).isNull();
    }

    /**
     * {@code swath resume <dir>} locates a run by opening {@code <dir>/.swath/checkpoint.sqlite}
     * directly — the output directory is the whole run handle, with no shared checkpoint-directory scan.
     */
    @Test
    void resumeDiscoversARunByItsDirectoryRunHandle(@TempDir Path tempDir) throws Exception {
        Path db = CheckpointOptions.CheckpointMode.colocatedCheckpoint(tempDir);
        Files.createDirectories(db.getParent());
        seedCompletedRun(db);

        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = tempDir;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
    }

    /** Seed a COMPLETED run so a same-args resume is a clean, network-free no-op. */
    private static void seedCompletedRun(Path db) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.JSONL.name());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }
}
