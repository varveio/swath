/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedRowGroupReader} — the replay server's delimiter skip-scan reads a sorted fixture
 * through exactly this class, so its per-row-group decode must be exact at a row-group boundary
 * (never bleed a neighboring group's rows in or drop the group's own), for both of its tiers ({@link
 * SortedRowGroupReader.KeyCursor} key-only, {@link SortedRowGroupReader#rows} full row).
 */
class SortedRowGroupReaderTest {

    private static SortConfig config(Map<String, String> overrides) {
        return SortConfig.fromProperties(key -> overrides.get(key.substring("swath.sort.".length())));
    }

    @Test
    void keyCursorAndRowsMatchWrittenOrderAcrossEveryRowGroupBoundary(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));   // fixed-width, ascending, ~200 B
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }

        List<SortedFileIndex.RowGroupSpan> spans = SortedFileIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(1);   // the multi-group path is actually exercised

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            int offset = 0;
            for (SortedFileIndex.RowGroupSpan span : spans) {
                List<byte[]> decodedKeys = drain(reader.openKeyCursor(span.blockIndex()));
                assertThat(decodedKeys).hasSize((int) span.rowCount());
                for (int i = 0; i < decodedKeys.size(); i++) {
                    assertThat(new String(decodedKeys.get(i), StandardCharsets.UTF_8)).isEqualTo(keys.get(offset + i));
                }

                List<SortedRowGroupReader.ObjectRow> rows = reader.rows(span.blockIndex(), false);
                assertThat(rows).hasSize((int) span.rowCount());
                for (int i = 0; i < rows.size(); i++) {
                    SortedRowGroupReader.ObjectRow row = rows.get(i);
                    String key = keys.get(offset + i);
                    assertThat(new String(row.key(), StandardCharsets.UTF_8)).isEqualTo(key);
                    assertThat(row.size()).isEqualTo(11L);
                    assertThat(row.etag()).isEqualTo("etag-" + key);
                    assertThat(row.storageClass()).isEqualTo("STANDARD");
                    assertThat(row.checksumAlgorithm()).isEqualTo("CRC32");
                    assertThat(row.checksumType()).isEqualTo("FULL_OBJECT");
                    // Owner columns were not requested for this row group: never decoded, always null.
                    assertThat(row.ownerId()).isNull();
                    assertThat(row.ownerDisplayName()).isNull();
                }
                offset += (int) span.rowCount();
            }
            assertThat(offset).isEqualTo(keys.size());
        }
    }

    @Test
    void rowsDecodesOwnerColumnsOnlyWhenAsked(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            writer.write(object("a"));
            writer.write(object("b"));
        }

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            List<SortedRowGroupReader.ObjectRow> withOwner = reader.rows(0, true);
            assertThat(withOwner).extracting(SortedRowGroupReader.ObjectRow::ownerId)
                    .containsExactly("owner-id", "owner-id");
            assertThat(withOwner).extracting(SortedRowGroupReader.ObjectRow::ownerDisplayName)
                    .containsExactly("owner-display", "owner-display");

            List<SortedRowGroupReader.ObjectRow> withoutOwner = reader.rows(0, false);
            assertThat(withoutOwner).extracting(SortedRowGroupReader.ObjectRow::ownerId)
                    .containsOnlyNulls();
            assertThat(withoutOwner).extracting(SortedRowGroupReader.ObjectRow::ownerDisplayName)
                    .containsOnlyNulls();
            // Non-owner columns are unaffected by the projection.
            assertThat(withoutOwner).extracting(SortedRowGroupReader.ObjectRow::etag)
                    .containsExactly("etag-a", "etag-b");
        }
    }

    /**
     * A key cursor is forward-only and stops as soon as it reaches the target — {@link
     * SortedRowGroupReader.KeyCursor#advanceTo} must land exactly on the first row at/after the target
     * (inclusive) or strictly after it (exclusive), never one row short or one row past, and repeated
     * calls with an advancing target must keep landing correctly (the same reuse the skip-scan's own
     * hop loop relies on: several hops into the same group, each resuming from the last position).
     */
    @Test
    void advanceToLandsExactlyOnTheFirstRowAtOrAfterTheTarget(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            keys.add(String.format("%08d", i * 2));   // even-numbered, so odd targets test "no exact match"
        }
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }
        assertThat(SortedFileIndex.rowGroupSpans(path)).hasSize(1);   // single group: exercises pure cursor logic

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            SortedRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

            // No lower bound: starts at row 0.
            cursor.advanceTo(null, true);
            assertThat(utf8(cursor.currentKey())).isEqualTo(keys.get(0));
            assertThat(cursor.position()).isZero();

            // Exact match, inclusive: lands ON it.
            cursor.advanceTo(bytes(keys.get(10)), true);
            assertThat(utf8(cursor.currentKey())).isEqualTo(keys.get(10));
            assertThat(cursor.position()).isEqualTo(10);

            // Exact match, exclusive: lands past it.
            cursor.advanceTo(bytes(keys.get(10)), false);
            assertThat(utf8(cursor.currentKey())).isEqualTo(keys.get(11));

            // No exact match (an odd number): lands on the next even key past it.
            cursor.advanceTo(bytes(String.format("%08d", 2 * 20 + 1)), true);
            assertThat(utf8(cursor.currentKey())).isEqualTo(keys.get(21));

            // Advancing to a target before the current position is a no-op (the cursor never rewinds).
            cursor.advanceTo(bytes(keys.get(5)), true);
            assertThat(utf8(cursor.currentKey())).isEqualTo(keys.get(21));

            // Advancing past the group's last key exhausts it.
            cursor.advanceTo(bytes("99999999"), true);
            assertThat(cursor.hasCurrent()).isFalse();
        }
    }

    @Test
    void keyCursorAndRowsCanInterleaveAgainstTheSameOpenReader(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }

        List<SortedFileIndex.RowGroupSpan> spans = SortedFileIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(2);

        // A key cursor opened on one group, a full-row decode of a DIFFERENT group, then re-opening the
        // first group's key cursor again — re-access to an already-visited group on the same open reader
        // must still return the exact same content (no leaked/mutated projection state).
        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            List<byte[]> firstGroupKeys = drain(reader.openKeyCursor(spans.get(0).blockIndex()));
            reader.rows(spans.get(2).blockIndex(), true);
            List<byte[]> firstGroupKeysAgain = drain(reader.openKeyCursor(spans.get(0).blockIndex()));
            assertThat(firstGroupKeysAgain).hasSize(firstGroupKeys.size());
            for (int i = 0; i < firstGroupKeys.size(); i++) {
                assertThat(firstGroupKeysAgain.get(i)).isEqualTo(firstGroupKeys.get(i));
            }
        }
    }

    /** Steps a key cursor to exhaustion, collecting every key it visits — a test-only bulk drain. */
    private static List<byte[]> drain(SortedRowGroupReader.KeyCursor cursor) {
        List<byte[]> out = new ArrayList<>();
        while (cursor.hasCurrent()) {
            out.add(cursor.currentKey());
            cursor.advanceTo(successor(cursor.currentKey()), true);
        }
        return out;
    }

    /** The least key strictly greater than {@code key} in this test's fixed-width all-ASCII keyspace. */
    private static byte[] successor(byte[] key) {
        byte[] next = key.clone();
        next[next.length - 1]++;
        return next;
    }

    private static String utf8(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 11L, 0L, "etag-" + key, "STANDARD", null, true,
                "owner-id", "owner-display", "CRC32", "FULL_OBJECT");
    }
}
