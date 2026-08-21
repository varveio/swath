/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.luben.zstd.ZstdInputStream;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.text.TextCompression;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

final class TextCompressionTest {

    @Test
    void compoundExtensionInfersTextFormatFileKindAndGzip(@TempDir Path dir) throws Exception {
        OutputOptions options = new OutputOptions();
        Path output = dir.resolve("listing.jsonl.gz");
        options.destination = output.toString();

        OutputOptions.Resolved resolved = options.resolveOutput(false);
        assertThat(resolved).isEqualTo(new OutputOptions.Resolved(
                OutputFormat.JSONL, OutputOptions.DestinationKind.FILE));
        assertThat(options.resolvedCompression).isEqualTo(TextCompression.GZIP);

        try (Writer writer = options.openSink()) {
            writer.write("{\"key\":\"value\"}\n");
            options.commitFileSink();
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(output)), StandardCharsets.UTF_8))) {
            assertThat(reader.readLine()).isEqualTo("{\"key\":\"value\"}");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    void trailingSeparatorDoesNotHideCompressionExtension() throws Exception {
        OutputOptions options = new OutputOptions();
        options.destination = "listing.jsonl.gz/";

        assertThat(options.resolveOutput(false)).isEqualTo(new OutputOptions.Resolved(
                OutputFormat.JSONL, OutputOptions.DestinationKind.FILE));
        assertThat(options.resolvedCompression).isEqualTo(TextCompression.GZIP);
    }

    @Test
    void zstdFrameIsFinishedBeforeAtomicPublication(@TempDir Path dir) throws Exception {
        OutputOptions options = new OutputOptions();
        Path output = dir.resolve("listing.tsv.zst");
        options.destination = output.toString();
        options.resolveOutput(false);

        try (Writer writer = options.openSink()) {
            writer.write("key\t42\n");
            options.commitFileSink();
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ZstdInputStream(Files.newInputStream(output)), StandardCharsets.UTF_8))) {
            assertThat(reader.readLine()).isEqualTo("key\t42");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    void explicitCompressionSupportsStdoutButRejectsParquetAndSuffixConflicts() throws Exception {
        OutputOptions stdout = new OutputOptions();
        new CommandLine(stdout).parseArgs("--compression", "GZIP");
        assertThat(stdout.resolveOutput(false).format()).isEqualTo(OutputFormat.TSV);
        assertThat(stdout.resolvedCompression).isEqualTo(TextCompression.GZIP);

        OutputOptions parquet = new OutputOptions();
        parquet.destination = "listing.parquet";
        parquet.setCompression(TextCompression.ZSTD);
        assertThatThrownBy(() -> parquet.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("applies only to text output formats");

        OutputOptions conflict = new OutputOptions();
        conflict.destination = "listing.jsonl.gz";
        conflict.setCompression(TextCompression.ZSTD);
        assertThatThrownBy(() -> conflict.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("conflicts with the compression extension");
    }
}
