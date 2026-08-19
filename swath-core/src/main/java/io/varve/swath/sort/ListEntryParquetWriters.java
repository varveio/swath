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
 * and their page/row-group geometry — all of which are supplied by the caller.
 */
final class ListEntryParquetWriters {

    static final int PAGE_BYTES = 1024 * 1024;
    static final int ZSTD_LEVEL = 3;

    /**
     * The dictionary a <b>served</b> file's column chunk may carry, in bytes.
     *
     * <p>A dictionary must be read and decoded <em>in full</em> before a single value of its column
     * chunk can be read, so its cost falls on every request that touches the column however few rows
     * that request wants — while its benefit is proportional to how often values repeat. For a
     * seek-served file that is the wrong way round, and the damage is proportional to the
     * dictionary's size. Parquet already falls back to a plain/delta encoding once a chunk's
     * dictionary outgrows this cap; setting the cap low is what makes the fallback fire on the
     * columns that deserve it.
     *
     * <p>8&nbsp;KiB is about a page's worth of entries — the point at which decoding the dictionary
     * stops being cheaper than decoding the page it exists to serve. Measured on a real fixture, it
     * is both the fastest and the smallest of the settings tried: {@code size}, with ~200,000 distinct
     * values per row group, had been carrying a dictionary that cost <b>2.9 ms to decode</b> on every
     * request — by itself the largest single cost in a 1,000-row page read — and that
     * {@code DELTA_BINARY_PACKED} then stored ~10 % smaller anyway. The enum-like columns (storage
     * class, checksum pair, row type, owner, version) sit far under the cap and keep their
     * dictionaries untouched.
     *
     * <p>A cap, not a column list: which columns repeat is a property of the <em>bucket</em>, not of
     * the schema, so a hardcoded list would be right for one fixture and wrong for the next. Measured
     * a further 362&nbsp;KB smaller than the equivalent list, on the fixture the list was chosen for.
     */
    static final int SERVED_DICTIONARY_BYTES = 8 * 1024;

    static final int SEGMENT_PAGE_ROWS = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT;

    /**
     * How a written file's pages and encodings are laid out for the way it will be <em>read</em> —
     * the axis, beyond row-group size, on which the two callers of {@link #build} differ.
     *
     * @param pageRows       cap on a data page's rows; see {@link #build}
     * @param dictionaryBytes cap on a column chunk's dictionary; see {@link #SERVED_DICTIONARY_BYTES}
     */
    record PageLayout(int pageRows, int dictionaryBytes) {

        /** Read once, front to back, by the merge: parquet's defaults are right. */
        static PageLayout staging() {
            return new PageLayout(SEGMENT_PAGE_ROWS, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
        }

        /** Seek-served a page of keys at a time: pay at write time for what a cold read would pay. */
        static PageLayout served(int pageRows) {
            return new PageLayout(pageRows, SERVED_DICTIONARY_BYTES);
        }
    }

    private ListEntryParquetWriters() {
    }

    /**
     * Builds a {@link ParquetWriter} for {@code path} with the shared knobs, given a caller-supplied
     * {@link WriteSupport}, row-group size and {@link PageLayout} (the axes the two callers differ on).
     *
     * <p>{@link PageLayout#pageRows} caps a data page's <b>rows</b>; {@link #PAGE_BYTES} caps its
     * bytes. The two are not interchangeable — the byte cap binds only on columns wide enough to reach
     * it, so it is the row cap that decides what a narrow column's page costs to decode.
     */
    static ParquetWriter<ListEntry> build(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes,
                                          PageLayout layout) throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", ZSTD_LEVEL);
        return new Builder(new LocalOutputFile(path), writeSupport)
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
