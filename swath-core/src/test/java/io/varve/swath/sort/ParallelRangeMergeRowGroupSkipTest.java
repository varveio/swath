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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Row-group skip mechanism (off-by-default parallel range-merge path): the pure overlap/skip
 * predicate ({@link ParallelRangeMerge#selectRowGroups}) that maps a range {@code [lo, hi)} to the
 * physical Parquet block indices it must decode, and the row-group-scoped {@link SegmentReader} that
 * reads exactly those blocks. This proves the mechanism's building blocks are correct and CONSERVATIVE (a
 * maybe-covered block is never skipped).
 */
class ParallelRangeMergeRowGroupSkipTest {

    private static SortConfig config(Map<String, String> overrides) {
        return SortConfig.fromProperties(key -> overrides.get(key.substring("swath.sort.".length())));
    }

    private static SortedFileIndex.RowGroupSpan span(String firstKey, int blockIndex) {
        return new SortedFileIndex.RowGroupSpan(firstKey.getBytes(StandardCharsets.UTF_8), blockIndex, 1L);
    }

    private static byte[] key(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    // --- selectRowGroups: contiguous groups a<d<g at physical 0,1,2 ---

    @Test
    void unboundedRangeReadsEveryGroup() {
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, null, null);
        assertThat(sel.blockIndices()).containsExactly(0, 1, 2);
        assertThat(sel.groupsRead()).isEqualTo(3);
        assertThat(sel.groupsSkipped()).isZero();
    }

    @Test
    void loEqualToAGroupFirstKeyStillReadsThePredecessorGroup() {
        // lo="d" is inclusive and equals group 1's first key. group 0's decoded first key is "a", but
        // selectRowGroups cannot prove from first keys alone that group 0 holds no trailing "d" (a
        // duplicate/straddle can put a key == firstKey_1 in group 0). Its max possible key is
        // upper_0 = firstKey_1 = "d" >= lo, so the LOSS-FREE rule reads group 0 too (predecessor-
        // inclusive); RangeFilteredStream trims any below-lo rows per-row. Do not skip group 0 here —
        // a rule that does drops those rows.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("d"), null);
        assertThat(sel.blockIndices()).containsExactly(0, 1, 2);
        assertThat(sel.groupsRead()).isEqualTo(3);
        assertThat(sel.groupsSkipped()).isZero();
    }

    // --- duplicate / straddle keys: a single key can OVERFLOW several physical row groups that all
    // decode to the SAME firstKey (versions / cross row_types). Do not prune by firstKey alone — it
    // silently drops the earlier equal-first-key blocks. ---

    @Test
    void equalFirstKeyOverflowBlocksAreNotSkippedWhenLoEqualsThatKey() {
        // Key "d" overflows physical blocks 1 and 2 (identical decoded firstKey). A range with lo="d"
        // must read BOTH. Selecting firstCovered = largest i with firstKey_i <= "d" would land on 2 and
        // drop block 1's "d" rows; the selection reads the whole [0..3] predecessor-inclusive span
        // instead.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(
                span("a", 0), span("d", 1), span("d", 2), span("g", 3));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("d"), null);
        assertThat(sel.blockIndices()).contains(1, 2);            // the critical never-skip
        assertThat(sel.blockIndices()).containsExactly(0, 1, 2, 3);
    }

    @Test
    void straddlingPredecessorBlockIsReadWhenLoEqualsTheBoundaryKey() {
        // Straddle: block 0 decodes firstKey "a" but its TRAILING rows are "d"; block 1 starts at "d".
        // Spans [a@0, d@1], lo="d". Returning [1] only would drop block 0's trailing "d" rows.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("d"), null);
        assertThat(sel.blockIndices()).containsExactly(0, 1);
    }

    @Test
    void loStrictlyAboveEqualFirstKeyOverflowSkipsThoseProvablyBelowBlocks() {
        // Spans [a@0, d@1, d@2, g@3], lo="e" (strictly between d and g): every block whose max key
        // (upper) is provably below "e" is skipped. upper_0=d, upper_1=d, upper_2=g(>=e) ⇒ start at 2.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(
                span("a", 0), span("d", 1), span("d", 2), span("g", 3));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("e"), null);
        assertThat(sel.blockIndices()).containsExactly(2, 3);
    }

    @Test
    void hiExclusiveAtAGroupFirstKeySkipsThatGroupAndLater() {
        // hi="d" is exclusive ⇒ group 1 (starts at d, all keys >= d >= hi) is out; only group 0 stays.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, null, key("d"));
        assertThat(sel.blockIndices()).containsExactly(0);
        assertThat(sel.groupsRead()).isEqualTo(1);
        assertThat(sel.groupsSkipped()).isEqualTo(2);
    }

    @Test
    void interiorRangeReadsOnlyTheStraddledGroup() {
        // [e,f) falls entirely inside group 1's span [d,g): only that group can hold in-range keys.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("e"), key("f"));
        assertThat(sel.blockIndices()).containsExactly(1);
        assertThat(sel.groupsRead()).isEqualTo(1);
        assertThat(sel.groupsSkipped()).isEqualTo(2);
    }

    @Test
    void rangeWhollyBelowTheFirstGroupReadsNothing() {
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("b", 0), span("d", 1), span("g", 2));
        // hi="a" <= every group's first key ⇒ no group overlaps ⇒ read nothing, all skipped.
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, null, key("a"));
        assertThat(sel.blockIndices()).isEmpty();
        assertThat(sel.groupsRead()).isZero();
        assertThat(sel.groupsSkipped()).isEqualTo(3);
    }

    @Test
    void loAboveEveryGroupStillReadsTheLastGroup() {
        // The last group's upper bound is +inf, so a lo past every first key must still read it (its
        // rows >= lo are trimmed per-row by RangeFilteredStream).
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("z"), null);
        assertThat(sel.blockIndices()).containsExactly(2);
        assertThat(sel.groupsRead()).isEqualTo(1);
        assertThat(sel.groupsSkipped()).isEqualTo(2);
    }

    @Test
    void singleGroupIsAlwaysReadWhenTheRangeTouchesIt() {
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("c"), key("x"));
        assertThat(sel.blockIndices()).containsExactly(0);
        assertThat(sel.groupsRead()).isEqualTo(1);
        assertThat(sel.groupsSkipped()).isZero();
    }

    @Test
    void emptySpansSelectsNothing() {
        ParallelRangeMerge.RangeSelection sel =
                ParallelRangeMerge.selectRowGroups(List.of(), key("a"), key("z"));
        assertThat(sel.blockIndices()).isEmpty();
        assertThat(sel.groupsRead()).isZero();
        assertThat(sel.groupsSkipped()).isZero();
    }

    @Test
    void coveredPhysicalSpanIncludesOmittedEmptyBlocksBetweenCoveredGroups() {
        // Physical blocks 1 and 2 are empty (omitted from spans); the covered inclusive physical span
        // [0..3] still reads them (harmless, zero rows) — conservative: never skip a maybe-covered block.
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("g", 3));
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, key("c"), null);
        assertThat(sel.blockIndices()).containsExactly(0, 1, 2, 3);
        assertThat(sel.groupsRead()).isEqualTo(2);   // counted over NON-empty groups only
        assertThat(sel.groupsSkipped()).isZero();
    }

    // --- SegmentReader row-group-scoped read ---

    @Test
    void segmentReaderReadsOnlyTheSelectedPhysicalBlocks(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(String.format("%08d", i) + "x".repeat(190));
        }
        Path path = dir.resolve("part-00001.parquet");
        SortConfig tinyRowGroups = config(Map.of("final-row-group-bytes", "4096"));
        try (SortedFileWriter writer = new SortedParquetWriter(path, tinyRowGroups, SortMode.OBJECTS, 1)) {
            for (String k : keys) {
                writer.write(new ObjectEntry(KeyBytes.ofUtf8(k), 1L, 0L, null, null, null, false, null, null, null, null));
            }
        }
        List<SortedFileIndex.RowGroupSpan> spans = SortedFileIndex.rowGroupSpans(path);
        assertThat(spans.size()).isGreaterThan(2);

        // Read only physical block 1: its rows are exactly keys[rowCount(0) .. rowCount(0)+rowCount(1)).
        int start = (int) spans.get(0).rowCount();
        int len = (int) spans.get(1).rowCount();
        List<String> read = new ArrayList<>();
        try (SegmentReader r = new SegmentReader(path, new int[] {spans.get(1).blockIndex()})) {
            while (r.hasNext()) {
                read.add(r.next().key().asString());
            }
        }
        assertThat(read).isEqualTo(keys.subList(start, start + len));

        // An empty selection reads nothing.
        try (SegmentReader empty = new SegmentReader(path, new int[0])) {
            assertThat(empty.hasNext()).isFalse();
        }

        // The full block list reproduces the whole file, in order (skip is byte-identical to no-skip).
        int[] all = new int[spans.size()];
        for (int i = 0; i < spans.size(); i++) {
            all[i] = spans.get(i).blockIndex();
        }
        List<String> whole = new ArrayList<>();
        try (SegmentReader r = new SegmentReader(path, all)) {
            while (r.hasNext()) {
                whole.add(r.next().key().asString());
            }
        }
        assertThat(whole).isEqualTo(keys);
    }

    // --- end-to-end: no row of a multi-row-group key is dropped through the full parallel path ---

    @Test
    void parallelMergeKeepsEveryRowOfAKeySpanningMultipleRowGroups(@TempDir Path root) throws IOException {
        // Key "d…" carries 60 distinct-but-equal-key rows (differing version_id) that overflow several
        // tiny row groups, bracketed by a single "a…" and "g…". Distinct sample first-keys are exactly
        // {a, d, g} ⇒ R=2 samples the boundary EXACTLY on "d", so range 1's lower bound lands on the
        // overflowing key — the row-group skip's critical case. Skipping the earlier "d" blocks here
        // would drop those rows (totalRows < expected).
        ListEntryComparator cmp = new ListEntryComparator();
        int dRows = 500;   // > Parquet's ~100-record row-group-size check interval, so "d" actually splits
        List<ListEntry> seg = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        String aKey = pad("a");
        String dKey = pad("d");
        String gKey = pad("g");
        seg.add(obj(aKey, null));
        expected.add(aKey);
        for (int i = 0; i < dRows; i++) {
            // Identical key, distinct + bulky version_id: the row payload (not the dictionary-collapsing
            // repeated key) grows the buffered row group past segmentRowGroupBytes, so the SAME "d" key
            // overflows several physical row groups — the exact shape that drops rows if the selection
            // prunes by firstKey alone.
            seg.add(obj(dKey, ("v" + i + "-").repeat(60)));
            expected.add(dKey);
        }
        seg.add(obj(gKey, null));
        expected.add(gKey);
        long expectedRows = dRows + 2;

        // Pin the scenario: the sampled boundary is exactly the overflowing key, and the written
        // segment really splits "d" across >= 2 physical row groups (else the test is vacuous).
        Path probe = writeSegment(cmp, Files.createDirectories(root.resolve("probe")), seg, 4096L);
        List<byte[]> boundaries = ParallelRangeMerge.boundaries(List.of(probe), 2);
        assertThat(boundaries).hasSize(1);
        assertThat(boundaries.get(0)).isEqualTo(dKey.getBytes(StandardCharsets.UTF_8));
        long dBlocks = SortedFileIndex.rowGroupSpans(probe).stream()
                .filter(s -> Arrays.equals(s.firstKey(), dKey.getBytes(StandardCharsets.UTF_8)))
                .count();
        assertThat(dBlocks).isGreaterThanOrEqualTo(2);

        // Serial baseline (untouched path — no row-group skip).
        Path serialOut = Files.createDirectories(root.resolve("serial"));
        Path serialStaging = Files.createDirectories(root.resolve("serial/_staging"));
        Path serialSeg = writeSegment(cmp, serialStaging, seg, 4096L);
        SortTransformResult serial = transform(cmp, config(Map.of("merge-parallelism", "1")))
                .transform(List.of(serialSeg), serialOut, serialStaging, PublishListener.NO_OP);
        assertThat(keys(serial.finalFiles())).containsExactlyElementsOf(expected);
        assertThat(serial.totalRows()).isEqualTo(expectedRows);

        // Parallel path with R=2 (boundary on "d"). Must keep ALL 62 rows and equal the serial sort.
        Path parOut = Files.createDirectories(root.resolve("parallel"));
        Path parStaging = Files.createDirectories(root.resolve("parallel/_staging"));
        Path parSeg = writeSegment(cmp, parStaging, seg, 4096L);
        SortTransformResult parallel = transform(cmp, config(Map.of("merge-parallelism", "2")))
                .transform(List.of(parSeg), parOut, parStaging, PublishListener.NO_OP);

        assertThat(parallel.totalRows()).isEqualTo(expectedRows);
        assertThat(keys(parallel.finalFiles())).containsExactlyElementsOf(expected);
    }

    private static String pad(String prefix) {
        return prefix + "x".repeat(200);
    }

    private static ObjectEntry obj(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, versionId, false, null, null, null, null);
    }

    private static SortTransform transform(ListEntryComparator cmp, SortConfig config) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
    }

    private static Path writeSegment(ListEntryComparator cmp, Path dir, List<ListEntry> sorted,
                                     long segmentRowGroupBytes) throws IOException {
        Path path = dir.resolve("seg.parquet");
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, segmentRowGroupBytes);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    private static List<String> keys(List<Path> files) throws IOException {
        List<String> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader r = new SegmentReader(f)) {
                while (r.hasNext()) {
                    out.add(r.next().key().asString());
                }
            }
        }
        return out;
    }
}
