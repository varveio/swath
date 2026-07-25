/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The directory-as-run-handle behaviour end to end: with the default {@code --checkpoint auto}, a
 * {@code list -o <dir>} co-locates its checkpoint at {@code <dir>/.swath/checkpoint.sqlite}, deletes
 * it on clean completion, and thereafter protects the finished dataset using the on-disk completion
 * markers alone — proving the refusal no longer depends on a surviving checkpoint.
 */
final class ColocatedCheckpointRunHandleTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    private static ListCommand autoCommand(Path outputDir, MockPageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        // checkpoint.location left at its "auto" field default — the co-located run handle.
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.fetcherOverride = fetcher;
        return cmd;
    }

    private static MockPageFetcher fetcher(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("data/key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        return MockPageFetcher.builder().keys(keys).build();
    }

    /**
     * A completed {@code auto} run deletes its co-located checkpoint, and a second fresh run over the
     * same dir is then refused by the on-disk markers alone (no checkpoint left to consult), leaving
     * the finished dataset byte-for-byte intact until {@code --overwrite} re-lists it.
     */
    @Test
    void completedRunDeletesItsCheckpointAndTheMarkersAloneRefuseAReRun(@TempDir Path root)
            throws Exception {
        Path outputDir = root.resolve("out");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(outputDir);
        Set<String> cwdLitterBefore = cwdCheckpointDirSnapshot();

        // Drive a real listing all the way to a COMPLETED dataset via the co-located auto checkpoint.
        assertThat(autoCommand(outputDir, fetcher(50)).call()).isEqualTo(ExitCodes.SUCCESS);

        DatasetLayout layout = DatasetLayout.of(outputDir);
        assertThat(layout.success()).exists();
        assertThat(layout.manifest()).exists();
        assertThat(layout.dataParts()).isNotEmpty();
        // The run handle is complete, so the checkpoint (and the emptied .swath/ dir) are gone.
        assertThat(checkpoint).doesNotExist();
        assertThat(outputDir.resolve(".swath")).doesNotExist();
        // The co-located checkpoint was isolated under the output dir: the run added nothing to the
        // old ./.swath-checkpoint/ default location.
        assertThat(cwdCheckpointDirSnapshot()).isEqualTo(cwdLitterBefore);

        String manifestBefore = Files.readString(layout.manifest());
        List<Path> partsBefore = layout.dataParts();

        // With NO checkpoint on disk, a second fresh run must still refuse — the completed-dataset
        // guard reads _SUCCESS + a valid manifest, not any checkpoint state — and steer to --overwrite.
        assertThatThrownBy(() -> autoCommand(outputDir, fetcher(50)).call())
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("completed")
                .hasMessageContaining("--overwrite");
        assertThat(checkpoint).as("a refused re-run never recreates the checkpoint").doesNotExist();

        // The finished dataset is preserved exactly.
        assertThat(layout.success()).exists();
        assertThat(Files.readString(layout.manifest())).isEqualTo(manifestBefore);
        assertThat(partsBefore).allMatch(Files::exists);

        // --overwrite discards the completed run and re-lists cleanly, again deleting its checkpoint.
        ListCommand overwrite = autoCommand(outputDir, fetcher(50));
        overwrite.checkpoint.overwrite = true;
        assertThat(overwrite.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(DatasetLayout.of(outputDir).dataParts()).isNotEmpty();
        assertThat(checkpoint).doesNotExist();
    }

    /** A plain {@code auto} stdout run keeps nothing on disk — no {@code ./.swath-checkpoint/} litter. */
    @Test
    void plainStdoutRunDropsNoCheckpointLitter() throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8),
                        "data/b".getBytes(StandardCharsets.UTF_8)))
                .build();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        // checkpoint.location "auto" + stdout (no -o) = ephemeral.
        cmd.output.format = OutputFormat.JSONL;
        cmd.fetcherOverride = fetcher;

        Set<String> cwdLitterBefore = cwdCheckpointDirSnapshot();
        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        // The ephemeral run wrote no checkpoint file at all — the old ./.swath-checkpoint/ default is
        // untouched (compared as a snapshot so this holds regardless of unrelated pre-existing files).
        assertThat(cwdCheckpointDirSnapshot()).isEqualTo(cwdLitterBefore);
    }

    /** Snapshot of the file names in the working-directory {@code .swath-checkpoint} dir (empty if absent). */
    private static Set<String> cwdCheckpointDirSnapshot() throws Exception {
        Path dir = Path.of(".swath-checkpoint");
        if (!Files.isDirectory(dir)) {
            return Set.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            Set<String> names = new TreeSet<>();
            entries.forEach(p -> names.add(p.getFileName().toString()));
            return names;
        }
    }
}
