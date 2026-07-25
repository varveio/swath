/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * One rotating Parquet part file (I6): pinned writer settings
 * (ZSTD-3, 64 MB row group, 1 MB page, dictionary on). A part's rows are durable
 * <b>iff</b> it is finalized — {@link #close()} writes the footer and fsyncs the
 * file before the part is recorded in the manifest.
 */
public final class PartWriter implements AutoCloseable {

    /** Pinned writer settings. */
    public static final long ROW_GROUP_BYTES = 64L * 1024 * 1024;
    public static final int PAGE_BYTES = 1024 * 1024;
    public static final int ZSTD_LEVEL = 3;

    private final Path path;
    private final ParquetWriter<ListEntry> writer;
    private long rows;

    public PartWriter(Path path, MessageType schema) throws IOException {
        this.path = path;
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", ZSTD_LEVEL);
        this.writer = new Builder(new LocalOutputFile(path), schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(ROW_GROUP_BYTES)
                .withPageSize(PAGE_BYTES)
                .withDictionaryEncoding(true)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    public void write(ListEntry e) throws IOException {
        writer.write(e);
        rows++;
    }

    public long rows() {
        return rows;
    }

    /** Uncompressed bytes buffered so far — the rotation trigger. */
    public long bufferedDataSize() {
        return writer.getDataSize();
    }

    public Path path() {
        return path;
    }

    /**
     * Write the footer, then fsync the file <b>and its parent directory</b> so the part is
     * durable (I6): the bytes and the directory entry that names the new part must both reach
     * disk before the part can be recorded in the manifest.
     */
    @Override
    public void close() throws IOException {
        writer.close();
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Fsync.directory(path.getParent());
    }

    /**
     * Release this part's resources <b>without finalizing it durably</b> — for the
     * abort/discard path (I6/RES-4), where the caller deletes the file immediately
     * afterwards. We close the underlying parquet-mr writer (the only way to free
     * its open file handle) but deliberately <b>skip the {@code fsync}</b> that
     * {@link #close()} performs: an un-fsynced part is not promised to survive a
     * crash, so we never produce a durably-finalized file that we then intend to
     * throw away. The part is also never recorded in {@code manifest.json} (the
     * durability source of truth), so resume ignores any byte that does linger.
     * Best-effort: an {@link IOException} from the writer close is swallowed since
     * the file is about to be deleted.
     */
    public void discard() {
        try {
            writer.close();
        } catch (IOException ignored) {
            // about to be deleted — releasing the handle is all that matters
        }
    }

    private static final class Builder extends ParquetWriter.Builder<ListEntry, Builder> {
        private final MessageType schema;

        Builder(OutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")   // getWriteSupport(Configuration) is the abstract method in parquet 1.15
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return new ListEntryWriteSupport(schema);
        }
    }
}
