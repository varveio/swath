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
        return openPart(path, TextDatasetFormat::openEncoder);
    }

    DatasetPartWriter openPart(Path path, EncoderFactory encoderFactory) throws IOException {
        return new TextPartWriter(path, encoderFactory);
    }

    private final class TextPartWriter implements DatasetPartWriter {
        private final Path path;
        private final CountingWriter counting;
        private final EntryFormatter formatter;
        private final PartDigest digest;
        private long rows;

        TextPartWriter(Path path, EncoderFactory encoderFactory) throws IOException {
            this.path = path;
            digest = new PartDigest();
            OpenedTextPart opened = open(path, encoderFactory, digest);
            counting = opened.counting();
            formatter = opened.formatter();
        }

        @Override public Path path() { return path; }
        @Override public long rows() { return rows; }
        @Override public long bufferedDataSize() { return counting.bytesWritten(); }
        @Override public void write(ListEntry entry) throws IOException { formatter.write(entry); rows++; }
        @Override public void close() throws IOException {
            IOException failure = null;
            try {
                formatter.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                counting.close();
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
            counting.close();
        }

        @Override public String md5() { return digest.md5(); }
        @Override public long digestNanos() { return digest.digestNanos(); }
    }

    /** Open the whole encoder stack transactionally so constructor failure cannot leak the file. */
    private OpenedTextPart open(Path path, EncoderFactory encoderFactory, PartDigest digest) throws IOException {
        OutputStream stream = null;
        CountingWriter counting = null;
        try {
            stream = new BufferedOutputStream(new DigestingOutputStream(Files.newOutputStream(path), digest));
            counting = encoderFactory.open(stream, compression);
            EntryFormatter formatter = Formatters.text(format, counting, escape);
            formatter.writeHeader();
            return new OpenedTextPart(counting, formatter);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (counting != null) {
                    counting.close();
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

    private static CountingWriter openEncoder(OutputStream stream, TextCompression compression)
            throws IOException {
        OutputStream compressed = switch (compression) {
            case NONE -> stream;
            case GZIP -> new GZIPOutputStream(stream);
            case ZSTD -> new ZstdOutputStream(stream);
        };
        return new CountingWriter(new BufferedWriter(
                new OutputStreamWriter(compressed, StandardCharsets.UTF_8)));
    }

    @FunctionalInterface
    interface EncoderFactory {
        CountingWriter open(OutputStream stream, TextCompression compression) throws IOException;
    }

    private static IOException retain(IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private record OpenedTextPart(CountingWriter counting, EntryFormatter formatter) {
    }
}
