/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

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
 * (never bleed a neighboring group's rows in or drop the group's own), for all three of its tiers
 * ({@link SortedRowGroupReader.KeyCursor} key-only resumable, {@link SortedRowGroupReader#forEachKey}
 * key-only bulk, {@link SortedRowGroupReader#rows} full row).
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

    /**
     * The bulk key tier and the cursor tier are two decoders over the same column, so a caller
     * choosing between them on speed must not be choosing between two answers: {@link
     * SortedRowGroupReader#forEachKey} must visit exactly the keys {@link
     * SortedRowGroupReader.KeyCursor} steps through, in the same order, group for group — including
     * across a row-group boundary, where a column-API reader that mis-scoped its page store would
     * bleed a neighbour's rows in.
     */
    @Test
    void forEachKeyVisitsExactlyTheKeysTheCursorSteps(@TempDir Path dir) throws IOException {
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
        assertThat(spans.size()).isGreaterThan(1);

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            int offset = 0;
            for (SortedFileIndex.RowGroupSpan span : spans) {
                List<byte[]> stepped = drain(reader.openKeyCursor(span.blockIndex()));
                List<byte[]> bulk = new ArrayList<>();
                long visited = reader.forEachKey(span.blockIndex(), bulk::add);

                assertThat(visited).isEqualTo(span.rowCount());
                assertThat(bulk).hasSize(stepped.size());
                for (int i = 0; i < bulk.size(); i++) {
                    assertThat(bulk.get(i)).isEqualTo(stepped.get(i));
                    assertThat(utf8(bulk.get(i))).isEqualTo(keys.get(offset + i));
                }
                offset += (int) span.rowCount();
            }
            assertThat(offset).isEqualTo(keys.size());
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

    /**
     * A row group whose rows are not in ascending order must fail the read that steps over them, not
     * be reported as positions the skip-scan can trust. Nothing upstream can catch this: the writer
     * stamps whatever it is handed, and index derive proves the ascent of row-group FIRST keys only —
     * so a fixture built by anything other than the sorter (an older producer, a hand-rolled writer)
     * can be stamped, eligible, and internally disordered all at once. The failure must name the file
     * and the row group, because the caller that hits it is a sweep over a corpus of fixtures — and it
     * must do so in {@link RowGroupOrderException}'s typed fields, because that sweep classifies the
     * exclusion by reason and location, not by matching substrings of a message.
     */
    @Test
    void keyCursorRefusesARowGroupWhoseRowsAreNotAscending(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            writer.write(object("aaa"));
            writer.write(object("ccc"));
            writer.write(object("bbb"));   // below its predecessor: row 2 of row group 0
            writer.write(object("ddd"));
        }
        assertThat(SortedFileIndex.rowGroupSpans(path)).hasSize(1);

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            SortedRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

            assertThatThrownBy(() -> cursor.advanceTo(bytes("zzz"), true))
                    .isInstanceOfSatisfying(RowGroupOrderException.class, e -> {
                        assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
                        assertThat(e.file()).isEqualTo(path);
                        assertThat(e.rowGroup()).isZero();
                        assertThat(e.row()).isEqualTo(2);
                    })
                    .hasMessageContaining("row group 0 of " + path)
                    .hasMessageContaining("strictly ascending")
                    .hasMessageContaining("row 2");
            cursor.close();
        }
    }

    /**
     * The same failure as a surface that must not publish the server's filesystem layout renders it:
     * the reason and the row group survive, the file is reduced to its NAME, and the directory that
     * held it is gone — while {@link Throwable#getMessage()}, which is what a server logs, keeps the
     * whole path. Pinned here rather than at the HTTP seam alone, because it is a property of the
     * exception every caller reads.
     */
    @Test
    void theRedactedReportKeepsTheReasonAndRowGroupButNotTheFixturePath(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            writer.write(object("aaa"));
            writer.write(object("aaa"));
        }

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            SortedRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);
            RowGroupOrderException thrown = catchThrowableOfType(RowGroupOrderException.class,
                    () -> cursor.advanceTo(bytes("zzz"), true));

            assertThat(thrown.redactedMessage())
                    .contains(RowGroupOrderException.ROW_GROUP_DISORDER)
                    .contains("row group 0 of part-00001.parquet")
                    .doesNotContain(dir.toString());
            assertThat(thrown.getMessage()).contains(path.toString());
            cursor.close();
        }
    }

    /**
     * A duplicate is as corrupting as an inversion here — a hop that lands on the first of two equal
     * keys and advances "past" it exclusively would step onto the same key again — so the check is
     * strict ascent, not non-descent.
     */
    @Test
    void keyCursorRefusesADuplicateKeyWithinARowGroup(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        try (SortedFileWriter writer = new SortedParquetWriter(path, config(Map.of()), SortMode.OBJECTS, 1)) {
            writer.write(object("aaa"));
            writer.write(object("aaa"));
        }

        try (SortedRowGroupReader reader = new SortedRowGroupReader(path)) {
            SortedRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

            assertThatThrownBy(() -> cursor.advanceTo(bytes("zzz"), true))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            e -> assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER))
                    .hasMessageContaining("row group 0 of " + path)
                    .hasMessageContaining("row 1");
            cursor.close();
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
