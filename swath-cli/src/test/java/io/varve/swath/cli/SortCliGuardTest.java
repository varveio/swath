/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Pins the {@code --sort} CLI guards: parquet-only, checkpoint-required, and
 * that {@code --sort} requires a directory-dataset destination -- a single-file {@code -o}
 * (formerly {@code --single-file}, now a {@code .parquet}-extension path) is rejected.
 */
final class SortCliGuardTest {

    @Test
    void sortWithExplicitTextFormat_isRejected() {
        for (OutputFormat text : new OutputFormat[]{OutputFormat.JSONL, OutputFormat.TSV, OutputFormat.TABLE}) {
            ListCommand cmd = new ListCommand();
            cmd.sorting.sort = true;
            cmd.output.format = text;
            assertThatThrownBy(() -> cmd.validateSortFlags(text))
                    .as("--sort with --format %s must be rejected", text)
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("--sort requires --format parquet");
        }
    }

    @Test
    void sortWithParquetDirectoryDestination_isAccepted() {
        ListCommand cmd = new ListCommand();
        cmd.sorting.sort = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = "out";
        cmd.output.resolvedKind = OutputOptions.DestinationKind.DIRECTORY;   // as resolveOutput() would set it
        assertThatCode(() -> cmd.validateSortFlags(OutputFormat.PARQUET)).doesNotThrowAnyException();
    }

    @Test
    void sortWithSingleFileDestination_isRejected() {
        ListCommand cmd = new ListCommand();
        cmd.sorting.sort = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = "out.parquet";
        cmd.output.resolvedKind = OutputOptions.DestinationKind.FILE;   // as resolveOutput() would set it
        // --sort requires a directory dataset -- a single-file -o is rejected outright
        // (this replaces the old --single-file flag, which --sort used to silently subsume).
        assertThatThrownBy(() -> cmd.validateSortFlags(OutputFormat.PARQUET))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--sort requires a directory dataset destination");
    }

    @Test
    void sortWithExplicitDashDestinationCallsItStdout() {
        ListCommand cmd = new ListCommand();
        cmd.sorting.sort = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = "-";
        cmd.output.resolvedKind = OutputOptions.DestinationKind.STDOUT;

        assertThatThrownBy(() -> cmd.validateSortFlags(OutputFormat.PARQUET))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("destination is stdout")
                .hasMessageNotContaining("resolves to a single file");
    }

    @Test
    void sortWithOmittedDestinationCallsItStdoutWithoutPrintingNull() {
        ListCommand cmd = new ListCommand();
        cmd.sorting.sort = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.resolvedKind = OutputOptions.DestinationKind.STDOUT;

        assertThatThrownBy(() -> cmd.validateSortFlags(OutputFormat.PARQUET))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("destination is stdout")
                .hasMessageNotContaining("-o null");
    }

    @Test
    void sortThroughCallWithTextFormat_failsFastAtExitTwo() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--sort", "--format", "JSONL");
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--sort requires --format parquet");
    }

    @Test
    void sortWithCheckpointNone_isRejected() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--sort", "--format", "PARQUET",
                "--checkpoint", "none", "-o", "out");
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--sort needs a checkpoint");
    }

    @Test
    void publishedReEntryCleanup_removesStagingAndStaleTmpButKeepsManifest(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        // Finals + their stale *.tmp live under <root>/data/; manifest is at the root.
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Path dataDir = Files.createDirectories(layout.dataDir());
        Files.createFile(stagingDir.resolve("seg-1-0.parquet"));
        Files.createFile(dataDir.resolve("part-00001.parquet.tmp"));
        Path manifest = Files.createFile(layout.manifest());
        Path finalFile = Files.createFile(dataDir.resolve("part-00001.parquet"));

        ListCommand.cleanSortStagingAndStaleTmp(outputDir, stagingDir);

        assertThat(Files.exists(stagingDir)).as("staging dir removed").isFalse();
        assertThat(Files.exists(dataDir.resolve("part-00001.parquet.tmp"))).as("stale tmp removed").isFalse();
        assertThat(Files.exists(manifest)).as("published manifest kept").isTrue();
        assertThat(Files.exists(finalFile)).as("published final file kept").isTrue();
    }
}
