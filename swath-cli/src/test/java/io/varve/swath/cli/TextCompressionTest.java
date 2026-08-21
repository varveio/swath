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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
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
    void explicitCompressionSupportsStdoutAndFileStartupEchoNamesIt() throws Exception {
        OutputOptions stdout = new OutputOptions();
        new CommandLine(stdout).parseArgs("--compression", "GZIP");
        OutputOptions.Resolved resolved = stdout.resolveOutput(false);
        assertThat(resolved.format()).isEqualTo(OutputFormat.TSV);
        assertThat(stdout.resolvedCompression).isEqualTo(TextCompression.GZIP);

        // Stdout is intentionally not echoed. Exercise the operator-facing startup echo on the
        // real-destination path without invoking ListCommand or contacting S3.
        OutputOptions file = new OutputOptions();
        file.destination = "listing.jsonl.gz";
        OutputOptions.Resolved fileResolved = file.resolveOutput(false);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        file.echoResolvedOutput(
                fileResolved, new PrintStream(bytes, true, StandardCharsets.UTF_8), false);
        assertThat(bytes.toString(StandardCharsets.UTF_8))
                .contains("jsonl (gzip) to listing.jsonl.gz");
    }

    @Test
    void explicitCompressionParquetErrorNamesTheFlag() {
        OutputOptions parquet = new OutputOptions();
        parquet.destination = "listing.parquet";
        parquet.setCompression(TextCompression.ZSTD);
        assertThatThrownBy(() -> parquet.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--compression zstd")
                .hasMessageContaining("applies only to text output formats");
    }

    @Test
    void inferredCompressionParquetErrorNamesTheExtensionNotTheFlag() {
        OutputOptions parquet = new OutputOptions();
        parquet.destination = "listing.parquet.gz";
        assertThatThrownBy(() -> parquet.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("compression extension of -o listing.parquet.gz")
                .hasMessageNotContaining("--compression gzip");
    }

    @Test
    void explicitCompressionConflictNamesBothSources() {
        OutputOptions conflict = new OutputOptions();
        conflict.destination = "listing.jsonl.gz";
        conflict.setCompression(TextCompression.ZSTD);
        assertThatThrownBy(() -> conflict.resolveOutput(false))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--compression zstd")
                .hasMessageContaining("compression extension")
                .hasMessageContaining("--compression gzip");
    }

    @Test
    void compressionWrapperConstructionFailureLeavesNoOutputOrTemporaryFile(@TempDir Path dir)
            throws Exception {
        OutputOptions options = new OutputOptions();
        Path output = dir.resolve("listing.jsonl.gz");
        options.destination = output.toString();
        options.resolveOutput(false);
        options.encodedWriterFactoryOverride = stream -> {
            stream.write(0x1f); // model a native/wrapper constructor that emits a header, then fails
            throw new IOException("wrapper construction failed");
        };

        assertThatThrownBy(options::openSink)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("wrapper construction failed");
        assertThat(output).doesNotExist();
        try (var entries = Files.list(dir)) {
            assertThat(entries).isEmpty();
        }
    }
}
