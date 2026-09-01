/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedParquetRowGroupReader} — the replay server's delimiter skip-scan reads a sorted fixture
 * through exactly this class, so its per-row-group decode must be exact at a row-group boundary
 * (never bleed a neighboring group's rows in or drop the group's own), for all four of its tiers
 * ({@link SortedParquetRowGroupReader.KeyCursor} key-only resumable, {@link SortedParquetRowGroupReader#forEachKey}
 * key-only bulk, {@link SortedParquetRowGroupReader#objectRange} bounded full row, and {@link
 * SortedParquetRowGroupReader#rows} whole-group full row).
 */
class SortedParquetRowGroupReaderTest {

    private static SortConfig config(Map<String, String> overrides) {
        SortConfig config = SortConfigs.base();
        String rowGroupBytes = overrides.get("final-row-group-bytes");
        if (rowGroupBytes != null) {
            config = config.withFinalRowGroupBytes(Long.parseLong(rowGroupBytes));
        }
        String pageRows = overrides.get("final-page-rows");
        return pageRows == null ? config : config.withFinalPageRows(Integer.parseInt(pageRows));
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

        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(1);   // the multi-group path is actually exercised

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            int offset = 0;
            for (SortedParquetIndex.RowGroupSpan span : spans) {
                List<byte[]> decodedKeys = drain(reader.openKeyCursor(span.blockIndex()));
                assertThat(decodedKeys).hasSize((int) span.rowCount());
                for (int i = 0; i < decodedKeys.size(); i++) {
                    assertThat(new String(decodedKeys.get(i), StandardCharsets.UTF_8)).isEqualTo(keys.get(offset + i));
                }

                List<SortedParquetRowGroupReader.ObjectRow> rows = reader.rows(span.blockIndex(), false);
                assertThat(rows).hasSize((int) span.rowCount());
                for (int i = 0; i < rows.size(); i++) {
                    SortedParquetRowGroupReader.ObjectRow row = rows.get(i);
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

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            List<SortedParquetRowGroupReader.ObjectRow> withOwner = reader.rows(0, true);
            assertThat(withOwner).extracting(SortedParquetRowGroupReader.ObjectRow::ownerId)
                    .containsExactly("owner-id", "owner-id");
            assertThat(withOwner).extracting(SortedParquetRowGroupReader.ObjectRow::ownerDisplayName)
                    .containsExactly("owner-display", "owner-display");

            List<SortedParquetRowGroupReader.ObjectRow> withoutOwner = reader.rows(0, false);
            assertThat(withoutOwner).extracting(SortedParquetRowGroupReader.ObjectRow::ownerId)
                    .containsOnlyNulls();
            assertThat(withoutOwner).extracting(SortedParquetRowGroupReader.ObjectRow::ownerDisplayName)
                    .containsOnlyNulls();
            // Non-owner columns are unaffected by the projection.
            assertThat(withoutOwner).extracting(SortedParquetRowGroupReader.ObjectRow::etag)
                    .containsExactly("etag-a", "etag-b");
        }
    }

    /**
     * A key cursor is forward-only and stops as soon as it reaches the target — {@link
     * SortedParquetRowGroupReader.KeyCursor#advanceTo} must land exactly on the first row at/after the target
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
        assertThat(SortedParquetIndex.rowGroupSpans(path)).hasSize(1);   // single group: exercises pure cursor logic

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

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
     * SortedParquetRowGroupReader#forEachKey} must visit exactly the keys {@link
     * SortedParquetRowGroupReader.KeyCursor} steps through, in the same order, group for group — including
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

        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(1);

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            int offset = 0;
            for (SortedParquetIndex.RowGroupSpan span : spans) {
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

        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(2);

        // A key cursor opened on one group, a full-row decode of a DIFFERENT group, then re-opening the
        // first group's key cursor again — re-access to an already-visited group on the same open reader
        // must still return the exact same content (no leaked/mutated projection state).
        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            List<byte[]> firstGroupKeys = drain(reader.openKeyCursor(spans.get(0).blockIndex()));
            reader.rows(spans.get(2).blockIndex(), true);
            List<byte[]> firstGroupKeysAgain = drain(reader.openKeyCursor(spans.get(0).blockIndex()));
            assertThat(firstGroupKeysAgain).hasSize(firstGroupKeys.size());
            for (int i = 0; i < firstGroupKeys.size(); i++) {
                assertThat(firstGroupKeysAgain.get(i)).isEqualTo(firstGroupKeys.get(i));
            }
        }
    }

    @Test
    void objectRangeCanFollowAKeyOnlyCursorInALaterRowGroup(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        SplittableRandom random = new SplittableRandom(0x5A17CA5EL);
        for (int i = 0; i < 8_000; i++) {
            // The numeric prefix preserves order; the incompressible suffix forces several key pages
            // inside each row group instead of letting parquet collapse this into a one-page fixture.
            StringBuilder key = new StringBuilder(908).append(String.format("%08d", i));
            for (int j = 0; j < 900; j++) {
                key.append((char) ('a' + random.nextInt(26)));
            }
            keys.add(key.toString());
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig multiPageGroups = config(Map.of(
                "final-page-rows", "1024",
                "final-row-group-bytes", Long.toString(4L << 20)));
        try (SortedFileWriter writer = new SortedParquetWriter(path, multiPageGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }

        List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(1);
        assertThat(spans.get(1).rowCount()).isGreaterThan(600);
        int targetBlock = spans.get(1).blockIndex();
        try (ParquetFileReader parquet = ParquetFileReader.open(new LocalInputFile(path))) {
            assertThat(parquet.getColumnIndexStore(targetBlock)
                    .getOffsetIndex(ColumnPath.get("key")).getPageCount())
                    .as("key pages in target row group with %d rows", spans.get(1).rowCount())
                    .isGreaterThan(1);
        }

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            // Loading group 0 leaves the mutable Parquet reader on its key-only projection. Before
            // maximal-projection priming, opening group 1 then poisoned that group's permanent index
            // cache with key-only paths before a multi-hop delimiter scan discovered a bare object.
            try (SortedParquetRowGroupReader.KeyCursor ignored = reader.openKeyCursor(spans.get(0).blockIndex())) {
                // Opening the cursor is enough to select and decode the key projection.
            }
            int groupOneStart = Math.toIntExact(spans.get(0).rowCount());
            String from = keys.get(groupOneStart + 500);
            String to = keys.get(groupOneStart + 502);
            try (SortedParquetRowGroupReader.KeyCursor ignored = reader.openKeyCursor(targetBlock)) {
                List<SortedParquetRowGroupReader.ObjectRow> rows = reader.objectRange(
                        targetBlock, bytes(from), true, bytes(to), 2, true);
                assertThat(rows).extracting(row -> utf8(row.key()))
                        .containsExactly(from, keys.get(groupOneStart + 501));
                assertThat(rows).extracting(SortedParquetRowGroupReader.ObjectRow::ownerId)
                        .containsExactly("owner-id", "owner-id");
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
        assertThat(SortedParquetIndex.rowGroupSpans(path)).hasSize(1);

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

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

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);
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

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0);

            assertThatThrownBy(() -> cursor.advanceTo(bytes("zzz"), true))
                    .isInstanceOfSatisfying(RowGroupOrderException.class,
                            e -> assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER))
                    .hasMessageContaining("row group 0 of " + path)
                    .hasMessageContaining("row 1");
            cursor.close();
        }
    }

    /** Steps a key cursor to exhaustion, collecting every key it visits — a test-only bulk drain. */
    /**
     * A cursor that seeks by the page index must <b>refuse</b> a row group whose key pages are not in
     * ascending order, rather than quietly skip the pages it was misled about.
     *
     * <p>This is the failure mode the per-row ascent check cannot see. That check proves what it steps
     * over, which was everything while a cursor read the whole row group. A pruning cursor never reads
     * the pages it prunes — and on a disordered group the page index is exactly what misleads it: a
     * page whose keys sort below the target also has a {@code max} below the target, so it is pruned,
     * and its rows leave the listing with nothing having read them and nothing to check. Measured
     * before the fix on this very fixture: a seek to the {@code d…} run returned 4,096 keys and no
     * exception, with the 2,048 {@code a…} keys silently gone. Through {@code delimitedRollup} that is
     * a {@code 200 OK} listing missing a third of a directory.
     *
     * <p>The guard is {@code BoundaryOrder} off the column index — a footer read, cached per row
     * group, no I/O per request — and it is complementary to the per-row check rather than a
     * replacement: a single page is trivially "ascending" whatever its rows do, which is why
     * {@link #keyCursorRefusesARowGroupWhoseRowsAreNotAscending} still has to exist.
     */
    @Test
    void keyCursorRefusesAGroupWhosePagesAreOutOfOrderRatherThanSkippingThem(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("part-00001.parquet");
        // Three whole pages, written out of order: d… then a… then g…. Every page is internally
        // ascending, so only the page BOUNDARIES are wrong — invisible to a per-row check that never
        // reads the pruned page.
        SortConfig smallPages = config(Map.of("final-page-rows", "1024"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, smallPages, SortMode.OBJECTS, 1)) {
            for (String prefix : List.of("d", "a", "g")) {
                for (int i = 0; i < 2048; i++) {
                    writer.write(object(prefix + String.format("%08d", i)));
                }
            }
        }
        assertThat(SortedParquetIndex.rowGroupSpans(path)).hasSize(1);

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            assertThatThrownBy(() -> reader.openKeyCursor(0, bytes("d00000000"), true, null))
                    .isInstanceOfSatisfying(RowGroupOrderException.class, e -> {
                        assertThat(e.reason()).isEqualTo(RowGroupOrderException.ROW_GROUP_DISORDER);
                        assertThat(e.file()).isEqualTo(path);
                        assertThat(e.rowGroup()).isZero();
                    })
                    .hasMessageContaining("strictly ascending");
        }
    }

    /**
     * A cursor must read a row group correctly across its own window boundaries — the stretch of
     * pages it loads at a time — not just within the first one.
     *
     * <p>Every other fixture in this class is small enough to fit one window, so the second load and
     * the position re-seat that goes with it were never exercised. The arithmetic there is the kind
     * that is right or silently off by a window: a cursor that mis-seats its position after a reload
     * reports a row index the skip-scan then trusts.
     */
    @Test
    void keyCursorIsExactAcrossItsOwnWindowBoundaries(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 30_000; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(60));
        }
        Path path = dir.resolve("part-00001.parquet");
        // One row group, 1,024-row pages: several windows' worth of pages inside a single group.
        SortConfig manyPagesOneGroup = config(Map.of("final-page-rows", "1024",
                "final-row-group-bytes", Long.toString(64L << 20)));
        try (SortedFileWriter writer = new SortedParquetWriter(path, manyPagesOneGroup, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }
        assertThat(SortedParquetIndex.rowGroupSpans(path)).hasSize(1);

        try (SortedParquetRowGroupReader reader = new SortedParquetRowGroupReader(path)) {
            List<byte[]> bulk = new ArrayList<>();
            reader.forEachKey(0, bulk::add);
            assertThat(bulk).hasSize(keys.size());

            // A full drain from the start must cross every window and agree with the bulk tier.
            try (SortedParquetRowGroupReader.KeyCursor cursor = reader.openKeyCursor(0)) {
                List<byte[]> stepped = drain(cursor);
                assertThat(stepped).hasSize(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    assertThat(stepped.get(i)).isEqualTo(bulk.get(i));
                }
            }

            // And a seek to either side of a window boundary must land on the right row, with
            // position() still meaning "row index within the row group".
            for (int at : new int[] {0, 1, 1023, 1024, 8191, 8192, 8193, 9000, 20_000, 29_999}) {
                try (SortedParquetRowGroupReader.KeyCursor cursor =
                             reader.openKeyCursor(0, bytes(keys.get(at)), true, null)) {
                    // The cursor opens at the first row of the PAGE that can hold the target, never at
                    // the target itself — the page filter prunes pages, never rows — so it must still
                    // be stepped there, exactly as the skip-scan steps it.
                    assertThat(cursor.hasCurrent()).isTrue();
                    assertThat(cursor.position()).as("page start at/below %d", at).isLessThanOrEqualTo(at);
                    cursor.advanceTo(bytes(keys.get(at)), true);
                    assertThat(cursor.currentKey()).as("key at %d", at).isEqualTo(bytes(keys.get(at)));
                    assertThat(cursor.position()).as("position at %d", at).isEqualTo(at);
                    // Step past the next boundary from wherever it landed.
                    int ahead = Math.min(at + 2500, keys.size() - 1);
                    cursor.advanceTo(bytes(keys.get(ahead)), true);
                    assertThat(cursor.currentKey()).as("key at %d after stepping", ahead)
                            .isEqualTo(bytes(keys.get(ahead)));
                    assertThat(cursor.position()).as("position at %d after stepping", ahead)
                            .isEqualTo(ahead);
                }
            }
        }
    }

    private static List<byte[]> drain(SortedParquetRowGroupReader.KeyCursor cursor) {
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
