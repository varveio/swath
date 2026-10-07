/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SortedParquetSinglePageBoundaryOrderTest {

    @ParameterizedTest
    @EnumSource(value = BoundaryOrder.class, names = {"UNORDERED", "DESCENDING"})
    void singlePageFlagsDoNotPreventExactCursorAndObjectRangeReads(BoundaryOrder order,
            @TempDir Path dir) throws IOException {
        Path path = dir.resolve("single.parquet");
        write(path, List.of("aaa", "bbb", "ccc"), 1024);
        rewriteKeyIndex(path, index -> index.setBoundary_order(order));
        assertIndexShape(path, 1, 1, order);

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            try (var cursor = reader.openKeyCursor(0, bytes("bbb"), true, bytes("ccc"))) {
                cursor.advanceTo(bytes("bbb"), true);
                assertThat(cursor.currentKey()).isEqualTo(bytes("bbb"));
                assertThat(cursor.position()).isEqualTo(1);
                cursor.advanceTo(bytes("bbb"), false);
                // Page pruning retains the whole page; the cursor still sees its final key.
                assertThat(cursor.currentKey()).isEqualTo(bytes("ccc"));
                cursor.advanceTo(bytes("ccc"), false);
                assertThat(cursor.hasCurrent()).isFalse();
            }
            assertThat(reader.objectRange(0, bytes("bbb"), true, bytes("ccc"), 2, true))
                    .extracting(row -> new String(row.key(), StandardCharsets.UTF_8))
                    .containsExactly("bbb");
            assertThat(reader.objectRange(0, bytes("bbb"), false, null, 2, false))
                    .extracting(row -> new String(row.key(), StandardCharsets.UTF_8))
                    .containsExactly("ccc");
        }
    }

    @ParameterizedTest
    @EnumSource(value = BoundaryOrder.class, names = {"UNORDERED", "DESCENDING"})
    void singlePageAcceptanceStillRejectsInvertedAndDuplicateDecodedKeys(BoundaryOrder order,
            @TempDir Path dir) throws IOException {
        for (List<String> keys : List.of(List.of("aaa", "ccc", "bbb"), List.of("aaa", "aaa"))) {
            Path path = dir.resolve("disordered-" + keys.size() + ".parquet");
            write(path, keys, 1024);
            rewriteKeyIndex(path, index -> index.setBoundary_order(order));
            assertIndexShape(path, 1, 1, order);
            try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path);
                    var cursor = reader.openKeyCursor(0)) {
                assertThatThrownBy(() -> cursor.advanceTo(bytes("zzz"), true))
                        .isInstanceOfSatisfying(RowGroupOrderException.class, failure -> {
                            assertThat(failure.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
                            assertThat(failure.rowGroup()).isZero();
                            assertThat(failure.row()).isEqualTo(keys.size() - 1L);
                        });
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = BoundaryOrder.class, names = {"UNORDERED", "DESCENDING"})
    void multiPageFlagsStillRejectBeforePruningThroughBothReaderTiers(BoundaryOrder order,
            @TempDir Path dir) throws IOException {
        Path path = dir.resolve("multiple.parquet");
        write(path, multiplePageKeys(), 1024);
        rewriteKeyIndex(path, index -> index.setBoundary_order(order));
        assertIndexShape(path, 2, 2, order);
        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            assertThatThrownBy(() -> reader.openKeyCursor(0, bytes("00001024"), true, null))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            failure -> assertThat(failure.row()).isEqualTo(-1));
            assertThatThrownBy(() -> reader.objectRange(0, bytes("00001024"), true, null, 1, false))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            failure -> assertThat(failure.row()).isEqualTo(-1));
        }
    }

    @Test
    void oneColumnIndexPageCannotBypassTwoOffsetIndexPages(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("mismatched.parquet");
        write(path, multiplePageKeys(), 1024);
        rewriteKeyIndex(path, index -> {
            index.setBoundary_order(BoundaryOrder.UNORDERED);
            index.setNull_pages(List.of(index.getNull_pages().getFirst()));
            index.setMin_values(List.of(index.getMin_values().getFirst()));
            index.setMax_values(List.of(index.getMax_values().getFirst()));
            index.setNull_counts(List.of(index.getNull_counts().getFirst()));
            index.unsetRepetition_level_histograms();
            index.unsetDefinition_level_histograms();
        });
        assertIndexShape(path, 1, 2, BoundaryOrder.UNORDERED);
        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            assertThatThrownBy(() -> reader.openKeyCursor(0))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            failure -> assertThat(failure.row()).isEqualTo(-1));
        }
    }

    private static void write(Path path, List<String> keys, int pageRows) throws IOException {
        var config = SortConfigs.base().withFinalPageRows(pageRows);
        try (var writer = new SortedParquetWriter(path, config, SortMode.OBJECTS, 1)) {
            for (String key : keys) {
                writer.write(new ObjectEntry(KeyBytes.ofUtf8(key), 11L, 0L, "etag", "STANDARD",
                        null, true, null, null, null, null));
            }
            writer.markFinal();
        }
    }

    private static List<String> multiplePageKeys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2048; i++) {
            keys.add(String.format(java.util.Locale.ROOT, "%08d", i));
        }
        return keys;
    }

    private static void assertIndexShape(Path path, int columns, int offsets, BoundaryOrder order)
            throws IOException {
        try (var reader = ParquetFileReader.open(new LocalInputFile(path))) {
            assertThat(reader.getRowGroups()).hasSize(1);
            var key = reader.getRowGroups().getFirst().getColumns().getFirst();
            var index = reader.readColumnIndex(key);
            assertThat(index.getNullPages()).hasSize(columns);
            assertThat(index.getBoundaryOrder().name()).isEqualTo(order.name());
            assertThat(reader.readOffsetIndex(key).getPageCount()).isEqualTo(offsets);
        }
    }

    /** Change only test index metadata; generated pages and footer offsets remain untouched. */
    private static void rewriteKeyIndex(Path path, Consumer<ColumnIndex> mutation) throws IOException {
        IndexReference reference;
        try (var reader = ParquetFileReader.open(new LocalInputFile(path))) {
            reference = reader.getRowGroups().getFirst().getColumns().getFirst().getColumnIndexReference();
        }
        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            byte[] bytes = new byte[reference.getLength()];
            file.seek(reference.getOffset());
            file.readFully(bytes);
            ColumnIndex index = Util.readColumnIndex(new ByteArrayInputStream(bytes));
            mutation.accept(index);
            var output = new ByteArrayOutputStream();
            Util.writeColumnIndex(index, output);
            assertThat(output.size()).isLessThanOrEqualTo(reference.getLength());
            file.seek(reference.getOffset());
            file.write(output.toByteArray());
            // Thrift's STOP self-delimits the index; pad shortened adversarial metadata to keep
            // its existing reference length and every later physical offset unchanged.
            file.write(new byte[reference.getLength() - output.size()]);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
