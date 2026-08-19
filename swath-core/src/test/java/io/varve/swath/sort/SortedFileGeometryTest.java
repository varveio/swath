/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The <b>on-disk geometry</b> {@link SortedParquetWriter} gives a served file, pinned from the
 * footer rather than from a stopwatch.
 *
 * <p>Both properties here are the write-side half of what a cold bounded range read costs, and
 * neither is visible in the file's contents — a fixture written the old way and one written the new
 * way serve byte-identical answers. What separates them is how much a reader must decode to produce
 * one: a page is Parquet's smallest addressable unit and a dictionary is decoded in full before its
 * column yields a value, so a page's row count and a dictionary's size are the floor on a seek. Both
 * defaults were chosen against a real fixture (a 1,000-key page went from 21.7 ms to ~4 ms); a
 * regression in either would be a large, silent serving cost with nothing else in the tree to catch
 * it.
 */
class SortedFileGeometryTest {

    /** Wide enough that the byte cap, not the row cap, would bind on the key column if it could. */
    private static final int ROWS = 12_000;

    @Test
    void noDataPageHoldsMoreRowsThanTheConfiguredCap(@TempDir Path dir) throws IOException {
        Path file = write(dir.resolve("capped.parquet"), SortConfigs.pagesOf(1024));

        forEachColumn(file, (column, maxRowsPerPage) ->
                assertThat(maxRowsPerPage)
                        .as("column %s: a page a reader must decode whole", column)
                        .isLessThanOrEqualTo(1024L));
    }

    /**
     * The cap has to bind on the <b>narrow</b> columns, which is the whole point: parquet's byte cap
     * never reaches them, so before this knob existed {@code size}, {@code last_modified} and every
     * enum-like column sat at parquet's 20,000-row default while {@code key} self-limited far below
     * it. A 1,000-row read therefore decoded ~20,000 values per column to return 1,000, and measured
     * flat from {@code max-keys=1} to {@code max-keys=1000}.
     */
    @Test
    void theCapBindsOnNarrowColumnsWhereTheByteCapNeverReaches(@TempDir Path dir) throws IOException {
        Path uncapped = write(dir.resolve("uncapped.parquet"), SortConfigs.pagesOf(20_000));
        Path capped = write(dir.resolve("capped.parquet"), SortConfigs.pagesOf(1024));

        assertThat(maxRowsPerPageOf(uncapped, "size"))
                .as("without the row cap a narrow column runs to parquet's own 20,000-row default")
                .isGreaterThan(1024L);
        assertThat(maxRowsPerPageOf(capped, "size")).isLessThanOrEqualTo(1024L);
        assertThat(maxRowsPerPageOf(capped, "last_modified")).isLessThanOrEqualTo(1024L);
    }

    /**
     * A high-cardinality column must fall out of dictionary encoding. The served dictionary cap is
     * what makes that happen without naming columns: which values repeat is a property of the bucket,
     * not of the schema, so the rule is "a dictionary stops being worth it once decoding it costs
     * more than the page it serves", not a list.
     */
    @Test
    void aHighCardinalityColumnIsNotDictionaryEncodedInAServedFile(@TempDir Path dir) throws IOException {
        Path file = write(dir.resolve("served.parquet"), SortConfigs.base());

        assertThat(encodingsOf(file, "size"))
                .as("a near-unique int64 column carries a dictionary no seek should have to decode")
                .noneMatch(e -> e.toString().contains("DICTIONARY"));
        assertThat(encodingsOf(file, "storage_class"))
                .as("an enum-like column's dictionary is a few bytes and stays")
                .anyMatch(e -> e.toString().contains("DICTIONARY"));
    }

    private static Path write(Path file, SortConfig config) throws IOException {
        try (SortedFileWriter writer = new SortedParquetWriter(file, config, SortMode.OBJECTS, 1)) {
            writer.markFinal();
            for (int i = 0; i < ROWS; i++) {
                writer.write(row(i));
            }
        }
        return file;
    }

    /** Distinct size and last_modified per row, so both columns are genuinely high-cardinality. */
    private static ListEntry row(int i) {
        return new ObjectEntry(
                KeyBytes.ofUtf8(String.format("prefix/%08d/object-with-a-realistic-length-name", i)),
                1_000_000L + i,
                1_700_000_000_000_000L + i * 1_000L,
                String.format("%032x", i),
                "STANDARD",
                null, true, "owner-id", "owner-display", "CRC32", "FULL_OBJECT");
    }

    private interface ColumnCheck {
        void check(String column, long maxRowsPerPage);
    }

    private static void forEachColumn(Path file, ColumnCheck check) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    check.check(column.getPath().toDotString(), maxRowsPerPage(reader, block, column));
                }
            }
        }
    }

    private static long maxRowsPerPageOf(Path file, String column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            long max = 0;
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    if (chunk.getPath().toDotString().equals(column)) {
                        max = Math.max(max, maxRowsPerPage(reader, block, chunk));
                    }
                }
            }
            return max;
        }
    }

    private static List<Object> encodingsOf(Path file, String column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            List<Object> encodings = new ArrayList<>();
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    if (chunk.getPath().toDotString().equals(column)) {
                        encodings.addAll(chunk.getEncodings());
                    }
                }
            }
            return encodings;
        }
    }

    private static long maxRowsPerPage(ParquetFileReader reader, BlockMetaData block,
                                       ColumnChunkMetaData column) throws IOException {
        OffsetIndex offsets = reader.readOffsetIndex(column);
        if (offsets == null) {
            return block.getRowCount();   // no page index: the whole chunk is one addressable unit
        }
        long max = 0;
        for (int page = 0; page < offsets.getPageCount(); page++) {
            long first = offsets.getFirstRowIndex(page);
            long next = page + 1 < offsets.getPageCount()
                    ? offsets.getFirstRowIndex(page + 1) : block.getRowCount();
            max = Math.max(max, next - first);
        }
        return max;
    }
}
