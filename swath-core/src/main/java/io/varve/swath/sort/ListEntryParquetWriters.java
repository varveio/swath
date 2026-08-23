/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.DigestingOutputFile;
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
 * and their page/row-group geometry — all of which are supplied by the caller.
 */
final class ListEntryParquetWriters {

    static final int PAGE_BYTES = 1024 * 1024;
    static final int ZSTD_LEVEL = 3;
    static final int SERVED_DICTIONARY_BYTES = 8 * 1024;
    static final int SEGMENT_PAGE_ROWS = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT;

    record PageLayout(int pageRows, int dictionaryBytes) {
        static PageLayout staging() {
            return new PageLayout(SEGMENT_PAGE_ROWS, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
        }

        static PageLayout served(int pageRows) {
            return new PageLayout(pageRows, SERVED_DICTIONARY_BYTES);
        }
    }

    private ListEntryParquetWriters() {
    }

    /** Builds a writer with caller-supplied write support and page/row-group geometry. */
    static ParquetWriter<ListEntry> build(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes,
                                          PageLayout layout)
            throws IOException {
        return build(writeSupport, rowGroupBytes, layout, new LocalOutputFile(path));
    }

    /** As {@link #build(Path, WriteSupport, long, PageLayout)}, with emitted-byte tracking. */
    static TrackedWriter buildTracked(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes,
                                      PageLayout layout)
            throws IOException {
        DigestingOutputFile output = new DigestingOutputFile(new LocalOutputFile(path));
        return new TrackedWriter(build(writeSupport, rowGroupBytes, layout, output), output);
    }

    private static ParquetWriter<ListEntry> build(WriteSupport<ListEntry> writeSupport,
                                                   long rowGroupBytes, PageLayout layout, OutputFile output)
            throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", ZSTD_LEVEL);
        return new Builder(output, writeSupport)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(PAGE_BYTES)
                .withPageRowCountLimit(layout.pageRows())
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(layout.dictionaryBytes())
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    record TrackedWriter(ParquetWriter<ListEntry> writer, DigestingOutputFile output) {
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
