/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

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

/**
 * Shared {@link ParquetWriter} scaffolding for {@link SegmentParquetSink} and
 * {@link SortedParquetWriter} — extracted so the parquet property construction and the
 * close/fsync durability discipline (I6) exist in exactly ONE place: splitting it back into two
 * copies risks a future durability fix landing in one copy and not the other. The two writers
 * still differ in what actually matters — their {@link WriteSupport} (plain vs. sortedness-stamped)
 * and their row-group-size knob — both of which are supplied by the caller.
 */
final class ListEntryParquetWriters {

    static final int PAGE_BYTES = 1024 * 1024;
    static final int ZSTD_LEVEL = 3;

    private ListEntryParquetWriters() {
    }

    /** Builds a {@link ParquetWriter} for {@code path} with the shared knobs, given a caller-supplied
     * {@link WriteSupport} and row-group size (the only two axes the two callers differ on). */
    static ParquetWriter<ListEntry> build(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes)
            throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", ZSTD_LEVEL);
        return new Builder(new LocalOutputFile(path), writeSupport)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(PAGE_BYTES)
                .withDictionaryEncoding(true)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    /**
     * Finalizes the footer, then fsyncs the file and its parent directory (I6): a file is durable
     * <b>iff</b> this has returned. The one durability sequence shared by both writers.
     */
    static void closeWithDurability(Path path, ParquetWriter<ListEntry> writer) throws IOException {
        writer.close();
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Durability.directory(path.getParent());
    }

    private static final class Builder extends ParquetWriter.Builder<ListEntry, Builder> {
        private final WriteSupport<ListEntry> writeSupport;

        Builder(OutputFile file, WriteSupport<ListEntry> writeSupport) {
            super(file);
            this.writeSupport = writeSupport;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")   // getWriteSupport(Configuration) is the abstract method in parquet 1.15
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return writeSupport;
        }
    }
}
