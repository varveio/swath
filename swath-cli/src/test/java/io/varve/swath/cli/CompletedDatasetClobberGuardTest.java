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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The data-loss guard, pinned end-to-end and independently of how completion is detected: once a
 * {@code swath list -o <dir>} has run all the way to a COMPLETED dataset, a second fresh {@code swath
 * list -o <dir>} for the same listing MUST refuse rather than clobber the finished output; only
 * {@code --overwrite} may re-list.
 *
 * <p>Unlike {@link DirectoryLifecycleCharacterizationTest#completedPriorRunIsRefusedUnlessOverwrite},
 * which plants a completed state by hand, this drives a real run to completion so the on-disk dataset
 * markers ({@code _SUCCESS} + a valid {@code manifest.json}) and the {@code data/} parts are the run's
 * own genuine output. Every assertion is on the user-visible dataset — the refusal message, the exit
 * code, and the finished output being left byte-for-byte intact — never on any internal checkpoint
 * bookkeeping. That keeps the guarantee anchored to what the user observes, so it holds however the
 * completed state happens to be recognized.
 */
final class CompletedDatasetClobberGuardTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    private static ListCommand freshCommand(Path outputDir, Path checkpointDb, MockPageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = checkpointDb.toString();
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

    @Test
    void freshRunOverACompletedDatasetRefusesUntilOverwrite(@TempDir Path root) throws Exception {
        Path outputDir = root.resolve("out");
        Path db = root.resolve("cp.sqlite");

        // Drive a real listing all the way to a COMPLETED dataset at -o <dir>.
        ListCommand first = freshCommand(outputDir, db, fetcher(50));
        assertThat(first.call()).isEqualTo(ExitCodes.SUCCESS);

        DatasetLayout layout = DatasetLayout.of(outputDir);
        assertThat(layout.success()).exists();
        assertThat(layout.manifest()).exists();
        assertThat(layout.dataParts()).isNotEmpty();
        // Snapshot the finished output so we can prove a refused re-run left it untouched.
        String manifestBefore = Files.readString(layout.manifest());
        List<Path> partsBefore = layout.dataParts();

        // A second fresh run for the same listing must refuse — it may not silently overwrite the
        // finished dataset — and must steer the user to the --overwrite escape hatch.
        ListCommand refused = freshCommand(outputDir, db, fetcher(50));
        assertThatThrownBy(refused::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("completed")
                .hasMessageContaining("--overwrite");

        // The completed dataset is preserved exactly: markers present, manifest byte-for-byte
        // unchanged, and the run's own parts still on disk.
        assertThat(layout.success()).exists();
        assertThat(Files.readString(layout.manifest())).isEqualTo(manifestBefore);
        assertThat(partsBefore).allMatch(Files::exists);

        // --overwrite is required to proceed: it discards the completed run and re-lists cleanly.
        ListCommand overwrite = freshCommand(outputDir, db, fetcher(50));
        overwrite.checkpoint.overwrite = true;
        assertThat(overwrite.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(DatasetLayout.of(outputDir).dataParts()).isNotEmpty();
    }
}
