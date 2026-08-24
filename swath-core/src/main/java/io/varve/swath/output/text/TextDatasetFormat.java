/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.text;

import com.github.luben.zstd.ZstdOutputStream;
import io.varve.swath.model.ListEntry;
import io.varve.swath.output.CountingWriter;
import io.varve.swath.output.EntryFormatter;
import io.varve.swath.output.Formatters;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.Utf8TsvFormatter;
import io.varve.swath.output.dataset.DatasetFormat;
import io.varve.swath.output.dataset.DatasetPartWriter;
import io.varve.swath.output.dataset.DigestingOutputStream;
import io.varve.swath.output.dataset.DurableFiles;
import io.varve.swath.output.dataset.PartDigest;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

/** TSV/JSONL encoding adapter; scheduling, rotation and publication stay in the shared pool. */
public record TextDatasetFormat(OutputFormat format, TextCompression compression, boolean escape)
        implements DatasetFormat {

    public TextDatasetFormat {
        if (format != OutputFormat.TSV && format != OutputFormat.JSONL) {
            throw new IllegalArgumentException("partitioned text output requires tsv or jsonl");
        }
    }

    @Override public String partSuffix() {
        return "." + format.name().toLowerCase(Locale.ROOT) + switch (compression) {
            case NONE -> "";
            case GZIP -> ".gz";
            case ZSTD -> ".zst";
        };
    }

    @Override public String manifestFormat() { return format.name(); }

    @Override public String manifestSchema() {
        return format == OutputFormat.TSV
                ? "key,size,last_modified,etag,storage_class,row_type"
                : "canonical swath listing JSON object per line";
    }

    @Override public DatasetPartWriter openPart(Path path) throws IOException {
        PartEncoderFactory factory = format == OutputFormat.TSV
                ? TextDatasetFormat::openTsvEncoder
                : TextDatasetFormat::openWriterEncoder;
        return new TextPartWriter(path, factory);
    }

    DatasetPartWriter openPart(Path path, EncoderFactory encoderFactory) throws IOException {
        return new TextPartWriter(path, (stream, compression, escape) ->
                new WriterPartEncoder(encoderFactory.open(stream, compression), format, escape));
    }

    private final class TextPartWriter implements DatasetPartWriter {
        private final Path path;
        private final PartEncoder encoder;
        private final PartDigest digest;
        private long rows;

        TextPartWriter(Path path, PartEncoderFactory encoderFactory) throws IOException {
            this.path = path;
            digest = new PartDigest();
            encoder = open(path, encoderFactory, digest);
        }

        @Override public Path path() { return path; }
        @Override public long rows() { return rows; }
        @Override public long bufferedDataSize() { return encoder.bytesWritten(); }
        @Override public void write(ListEntry entry) throws IOException { encoder.write(entry); rows++; }
        @Override public void close() throws IOException {
            IOException failure = null;
            try {
                encoder.finish();
            } catch (IOException e) {
                failure = e;
            }
            try {
                encoder.close();
            } catch (IOException e) {
                failure = retain(failure, e);
            }
            if (failure == null) {
                try {
                    DurableFiles.fileAndParent(path);
                    digest.markDurable();
                } catch (IOException e) {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override public void discard() throws IOException {
            encoder.close();
        }

        @Override public String md5() { return digest.md5(); }
        @Override public long digestNanos() { return digest.digestNanos(); }
    }

    /** Open the whole encoder stack transactionally so constructor failure cannot leak the file. */
    private PartEncoder open(Path path, PartEncoderFactory encoderFactory, PartDigest digest) throws IOException {
        OutputStream stream = null;
        PartEncoder encoder = null;
        try {
            stream = new BufferedOutputStream(new DigestingOutputStream(Files.newOutputStream(path), digest));
            encoder = encoderFactory.open(stream, compression, escape);
            encoder.writeHeader();
            return encoder;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (encoder != null) {
                    encoder.close();
                } else if (stream != null) {
                    stream.close();
                }
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            try {
                Files.deleteIfExists(path);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static PartEncoder openWriterEncoder(
            OutputStream stream, TextCompression compression, boolean escape)
            throws IOException {
        CountingWriter writer = openEncoder(stream, compression);
        return new WriterPartEncoder(writer, OutputFormat.JSONL, escape);
    }

    private static PartEncoder openTsvEncoder(
            OutputStream stream, TextCompression compression, boolean escape)
            throws IOException {
        OutputStream compressed = compress(stream, compression);
        return new TsvPartEncoder(new Utf8TsvFormatter(compressed, escape));
    }

    private static CountingWriter openEncoder(OutputStream stream, TextCompression compression)
            throws IOException {
        OutputStream compressed = compress(stream, compression);
        return new CountingWriter(new BufferedWriter(
                new OutputStreamWriter(compressed, StandardCharsets.UTF_8)));
    }

    private static OutputStream compress(OutputStream stream, TextCompression compression)
            throws IOException {
        OutputStream compressed = switch (compression) {
            case NONE -> stream;
            case GZIP -> new GZIPOutputStream(stream);
            case ZSTD -> new ZstdOutputStream(stream);
        };
        return compressed;
    }

    @FunctionalInterface
    interface EncoderFactory {
        CountingWriter open(OutputStream stream, TextCompression compression) throws IOException;
    }

    @FunctionalInterface
    private interface PartEncoderFactory {
        PartEncoder open(OutputStream stream, TextCompression compression, boolean escape)
                throws IOException;
    }

    private interface PartEncoder {
        void writeHeader() throws IOException;
        void write(ListEntry entry) throws IOException;
        long bytesWritten();
        void finish() throws IOException;
        void close() throws IOException;
    }

    private static final class WriterPartEncoder implements PartEncoder {
        private final CountingWriter writer;
        private final EntryFormatter formatter;

        WriterPartEncoder(CountingWriter writer, OutputFormat format, boolean escape) {
            this.writer = writer;
            formatter = Formatters.text(format, writer, escape);
        }

        @Override public void writeHeader() throws IOException { formatter.writeHeader(); }
        @Override public void write(ListEntry entry) throws IOException { formatter.write(entry); }
        @Override public long bytesWritten() { return writer.bytesWritten(); }
        @Override public void finish() throws IOException { formatter.close(); }
        @Override public void close() throws IOException { writer.close(); }
    }

    private static final class TsvPartEncoder implements PartEncoder {
        private final Utf8TsvFormatter formatter;

        TsvPartEncoder(Utf8TsvFormatter formatter) {
            this.formatter = formatter;
        }

        @Override public void writeHeader() throws IOException { formatter.writeHeader(); }
        @Override public void write(ListEntry entry) throws IOException { formatter.write(entry); }
        @Override public long bytesWritten() { return formatter.bytesWritten(); }
        @Override public void finish() throws IOException { formatter.flush(); }
        @Override public void close() throws IOException { formatter.close(); }
    }

    private static IOException retain(IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

}
