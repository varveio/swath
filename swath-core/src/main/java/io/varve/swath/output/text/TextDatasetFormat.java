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
                ? "key,size,last_modified,etag,storage_class,row_type,owner_id,owner_display_name,checksum_type"
                : "canonical swath listing JSON object per line";
    }

    @Override public DatasetPartWriter openPart(Path path) throws IOException {
        return new TextPartWriter(path);
    }

    private final class TextPartWriter implements DatasetPartWriter {
        private final Path path;
        private final CountingWriter counting;
        private final EntryFormatter formatter;
        private long rows;

        TextPartWriter(Path path) throws IOException {
            this.path = path;
            OutputStream buffered = new BufferedOutputStream(Files.newOutputStream(path));
            OutputStream compressed = switch (compression) {
                case NONE -> buffered;
                case GZIP -> new GZIPOutputStream(buffered);
                case ZSTD -> new ZstdOutputStream(buffered);
            };
            counting = new CountingWriter(new BufferedWriter(
                    new OutputStreamWriter(compressed, StandardCharsets.UTF_8)));
            formatter = Formatters.text(format, counting, escape);
            formatter.writeHeader();
        }

        @Override public Path path() { return path; }
        @Override public long rows() { return rows; }
        @Override public long bufferedDataSize() { return counting.bytesWritten(); }
        @Override public void write(ListEntry entry) throws IOException { formatter.write(entry); rows++; }
        @Override public void close() throws IOException {
            formatter.close();
            counting.close();
        }

        @Override public void discard() throws IOException {
            counting.close();
        }
    }
}
