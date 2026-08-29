/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ordinary confidence tests for {@link PageAwareMerger}: the page-whole fast path engages on
 * globally-disjoint pages, the overlap guard falls back to a correct key-merge when pages interleave,
 * and the merged output is byte-identical (entry-for-entry) to the entry-typed {@link StreamingMerger}
 * over the SAME page-run segments. The ADVERSARIAL edge cases (mis-ordered pages WITHIN one segment,
 * equal-key page boundaries, version tiebreaks, multi-segment overlap storms) are the separate
 * adversarial suite's job — not covered here.
 */
class PageAwareMergerTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void disjointPagesAcrossSegmentsAllEmitWhole(@TempDir Path dir) throws IOException {
        // Four segments, each in its OWN key band (s0-*, s1-*, ...), 2500 entries each ⇒ writeIntermediate
        // pages at 1000 ⇒ pages of 1000/1000/500 per segment, all globally range-disjoint.
        int segCount = 4;
        int perSeg = 2500;
        List<List<ListEntry>> segs = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (int s = 0; s < segCount; s++) {
            List<ListEntry> seg = new ArrayList<>();
            for (int i = 0; i < perSeg; i++) {
                ListEntry e = object(String.format("s%d-k%06d", s, i));
                seg.add(e);
                all.add(e);
            }
            segs.add(seg);
        }
        List<Path> files = stage(dir, segs);

        CountingMetrics metrics = new CountingMetrics();
        List<String> merged = drainKeys(files, metrics);

        List<String> expected = all.stream().map(e -> e.key().asString()).sorted().toList();
        assertThat(merged).containsExactlyElementsOf(expected);
        assertThat(merged).isSorted();
        // pages: 3 per segment × 4 segments = 12, ALL emitted whole; zero overlap fallback.
        assertThat(metrics.get("SORT.page_whole_emitted")).isEqualTo(12);
        assertThat(metrics.get("SORT.page_overlap_keymerge")).isZero();
        assertThat(metrics.overlapClusters).isZero();
        assertThat(metrics.overlapPagesPeak).isZero();
        assertThat(metrics.overlapRowsPeak).isZero();
    }

    @Test
    void interleavingSegmentsFallBackToKeyMerge(@TempDir Path dir) throws IOException {
        // Two single-page segments whose key ranges overlap: [a..g] and [b..h] interleave.
        List<List<ListEntry>> segs = List.of(
                sorted("a", "c", "e", "g"),
                sorted("b", "d", "f", "h"));
        List<Path> files = stage(dir, segs);

        CountingMetrics metrics = new CountingMetrics();
        long[] classification = new long[2];
        List<String> merged = new ArrayList<>();
        try (PageAwareMerger merger = new PageAwareMerger(frontiers(files), cmp,
                MergeScope.CROSS_SEGMENT, metrics,
                (copyable, interleaved) -> {
                    classification[0] = copyable;
                    classification[1] = interleaved;
                })) {
            while (merger.hasNext()) {
                merged.add(merger.next().key().asString());
            }
        }

        assertThat(merged).containsExactly("a", "b", "c", "d", "e", "f", "g", "h");
        assertThat(merged).isSorted();
        // The overlap guard fired (loud alarm on a genuinely-interleaving input); no whole page possible.
        assertThat(metrics.get("SORT.page_overlap_keymerge")).isGreaterThan(0);
        assertThat(metrics.get("SORT.page_whole_emitted")).isZero();
        assertThat(metrics.get("SORT.merge_overlap_cluster")).isEqualTo(1);
        assertThat(metrics.overlapClusters).isEqualTo(1);
        assertThat(metrics.overlapPagesPeak).isEqualTo(2);
        assertThat(metrics.overlapRowsPeak).isEqualTo(8);
        assertThat(classification).containsExactly(0L, 2L);
    }

    @Test
    void equalBoundaryReportsOneDuplicateWithoutChangingOverlapAccounting(@TempDir Path dir)
            throws IOException {
        List<Path> files = stage(dir, List.of(sorted("dup"), sorted("dup")));
        CountingMetrics metrics = new CountingMetrics();
        List<String> duplicates = new ArrayList<>();

        List<String> merged = new ArrayList<>();
        try (SortedCursor cursor = new DuplicateReporting(
                new PageAwareMerger(frontiers(files), cmp, MergeScope.CROSS_SEGMENT, metrics), cmp,
                (previous, duplicate) -> duplicates.add(duplicate.key().asString()))) {
            while (cursor.hasNext()) {
                merged.add(cursor.next().key().asString());
            }
        }

        assertThat(merged).containsExactly("dup", "dup");
        assertThat(duplicates).containsExactly("dup");
        assertThat(metrics.overlapClusters).isEqualTo(1);
        assertThat(metrics.overlapPagesPeak).isEqualTo(2);
        assertThat(metrics.overlapRowsPeak).isEqualTo(2);
    }

    @Test
    void comparatorEqualMultiPageSourcesKeepTheirActualRunClassification(@TempDir Path dir)
            throws IOException {
        List<ListEntry> left = new ArrayList<>();
        List<ListEntry> right = new ArrayList<>();
        for (int i = 0; i < 1_001; i++) {
            // These fields deliberately distinguish source and ordinal but are outside the sort
            // comparator. This catches a route changing equal-key tie emission while retaining the
            // genuine comparator-equal input needed to exercise DuplicateReporting.
            left.add(taggedDuplicate(0, i));
            right.add(taggedDuplicate(1, i));
        }
        List<Path> files = stage(dir, List.of(left, right));
        // Page-run staging is itself allowed to choose an arbitrary stable order for comparator
        // ties. Read its physical source order once as the merge oracle; do not accidentally make
        // this test assume an order that the comparator never promised.
        List<ListEntry> stagedLeft = readStaged(files.getFirst());
        List<ListEntry> stagedRight = readStaged(files.getLast());
        long[] pageRuns = new long[2];
        long[] streamingRuns = new long[2];
        List<String> pageDuplicates = new ArrayList<>();
        List<String> streamingDuplicates = new ArrayList<>();
        List<ListEntry> page;
        List<ListEntry> streaming;
        try (SortedCursor cursor = new DuplicateReporting(new PageAwareMerger(frontiers(files), cmp,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP,
                (copyable, interleaved) -> { pageRuns[0] = copyable; pageRuns[1] = interleaved; }),
                cmp, (previous, current) -> pageDuplicates.add(pair(previous, current)))) {
            page = drainEntries(cursor);
        }
        try (SortedCursor cursor = new DuplicateReporting(new StreamingMerger(entryStreams(files), cmp, n -> { },
                (copyable, interleaved) -> { streamingRuns[0] = copyable; streamingRuns[1] = interleaved; }),
                cmp, (previous, current) -> streamingDuplicates.add(pair(previous, current)))) {
            streaming = drainEntries(cursor);
        }
        List<ListEntry> streamingExpected = new ArrayList<>(2_002);
        streamingExpected.addAll(stagedLeft);
        streamingExpected.addAll(stagedRight);

        // PageAware's heap does not impose a tie-breaker between comparator-equal pages, so its
        // encounter interleaving is intentionally not a contract. The independently-derived
        // source/ordinal oracle below is exact after source/ordinal normalization: every tagged
        // row must appear once (and no row may be substituted or lost) even when equal-key pages
        // are interleaved in a different legal order.
        assertSourceOrdinalMembership(page, stagedLeft);
        assertSourceOrdinalMembership(page, stagedRight);
        assertThat(tags(page)).containsExactlyInAnyOrderElementsOf(tags(streamingExpected));
        assertThat(streaming).containsExactlyElementsOf(streamingExpected);
        assertThat(tags(streaming)).containsExactlyElementsOf(tags(streamingExpected));
        assertThat(page).hasSize(2_002);
        assertThat(streaming).hasSize(2_002);
        assertThat(pageDuplicates).hasSize(2_001).containsExactlyElementsOf(pairs(page));
        assertThat(streamingDuplicates).hasSize(2_001).containsExactlyElementsOf(pairs(streamingExpected));
        assertThat(pageRuns).containsExactly(0L, 2L);
        assertThat(streamingRuns).containsExactly(2L, 0L);
    }

    @Test
    void partialConsumerAbortReportsNoSourceRunClassificationOnEitherMerger(
            @TempDir Path dir) throws IOException {
        List<Path> files = stage(dir, List.of(
                sorted("a", "c", "e", "g"),
                sorted("b", "d", "f", "h")));
        long[] pageReports = new long[1];
        long[] streamingReports = new long[1];

        try (PageAwareMerger page = new PageAwareMerger(frontiers(files), cmp,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP,
                (copyable, interleaved) -> pageReports[0]++)) {
            assertThat(page.next().key().asString()).isEqualTo("a");
        }
        try (StreamingMerger streaming = new StreamingMerger(
                entryStreams(files), cmp, ignored -> { },
                (copyable, interleaved) -> streamingReports[0]++)) {
            assertThat(streaming.next().key().asString()).isEqualTo("a");
        }

        assertThat(pageReports).containsExactly(0L);
        assertThat(streamingReports).containsExactly(0L);
    }

    @Test
    void expectedRangeCutoffDelegatesLogicalCompletionThroughDuplicateWrapper(
            @TempDir Path dir) throws IOException {
        List<Path> files = stage(dir, List.of(
                sorted("a", "c", "e", "g"),
                sorted("b", "d", "f", "h")));
        long[] pageClassification = new long[3];
        long[] streamingClassification = new long[3];

        List<ListEntry> page;
        try (SortedCursor cursor = new RangeFilteredCursor(new DuplicateReporting(
                new PageAwareMerger(frontiers(files), cmp, MergeScope.CROSS_SEGMENT,
                        SortMetrics.NO_OP, (copyable, interleaved) -> {
                            pageClassification[0]++;
                            pageClassification[1] = copyable;
                            pageClassification[2] = interleaved;
                        }), cmp, DuplicateHook.NO_OP), null, "d".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            page = drainOpen(cursor);
        }
        List<ListEntry> streaming;
        try (SortedCursor cursor = new RangeFilteredCursor(new DuplicateReporting(
                new StreamingMerger(entryStreams(files), cmp, ignored -> { },
                        (copyable, interleaved) -> {
                            streamingClassification[0]++;
                            streamingClassification[1] = copyable;
                            streamingClassification[2] = interleaved;
                        }), cmp, DuplicateHook.NO_OP), null,
                "d".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            streaming = drainOpen(cursor);
        }

        assertThat(page).extracting(entry -> entry.key().asString())
                .containsExactly("a", "b", "c");
        assertThat(streaming).isEqualTo(page);
        assertThat(pageClassification[0]).isEqualTo(1);
        assertThat(pageClassification[1] + pageClassification[2]).isPositive();
        assertThat(streamingClassification[0]).isEqualTo(1);
        assertThat(streamingClassification[1] + streamingClassification[2]).isPositive();
    }

    @Test
    void constructorCancellationReportsNoSourceRunClassificationOnEitherMerger(
            @TempDir Path dir) throws IOException {
        List<Path> files = stage(dir, List.of(sorted("a", "b"), sorted("c", "d")));
        long[] reports = new long[2];
        List<PageFrontierStream> pageFrontiers = frontiers(files);

        try {
            Thread.currentThread().interrupt();
            assertThat(catchThrowable(() -> new PageAwareMerger(pageFrontiers, cmp,
                    MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP,
                    (copyable, interleaved) -> reports[0]++)))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }
        List<EntryStream> streams = entryStreams(files);
        try {
            Thread.currentThread().interrupt();
            assertThat(catchThrowable(() -> new StreamingMerger(
                    streams, cmp, ignored -> { },
                    (copyable, interleaved) -> reports[1]++)))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }

        assertThat(reports).containsExactly(0L, 0L);
    }

    @Test
    void streamingCloseFailureAfterNaturalDrainReportsNoSourceRunClassification() {
        ListEntry row = object("only");
        EntryStream failingClose = new EntryStream() {
            private boolean available = true;

            @Override
            public boolean hasNext() {
                return available;
            }

            @Override
            public ListEntry peek() {
                return available ? row : null;
            }

            @Override
            public ListEntry next() {
                available = false;
                return row;
            }

            @Override
            public void close() throws IOException {
                throw new IOException("injected close failure");
            }
        };
        long[] reports = new long[1];
        StreamingMerger merger = new StreamingMerger(List.of(failingClose), cmp, ignored -> { },
                (copyable, interleaved) -> reports[0]++);

        assertThat(drainOpen(merger)).containsExactly(row);
        assertThat(catchThrowable(merger::close))
                .isInstanceOf(java.io.UncheckedIOException.class)
                .hasMessageContaining("closing entry streams failed");
        assertThat(reports).containsExactly(0L);
    }

    @Test
    void nestedPagesFormOneBoundedOverlapCluster(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), cmp);
        buffer.admit(1, sorted("a", "z"));
        buffer.admit(2, sorted("b", "c"));
        buffer.admit(3, sorted("d", "e"));
        Path segment = dir.resolve("nested.pageseg");
        new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), segment);
        CountingMetrics metrics = new CountingMetrics();

        List<String> merged = drainKeys(List.of(segment), metrics);

        assertThat(merged).containsExactly("a", "b", "c", "d", "e", "z");
        assertThat(metrics.overlapClusters).isEqualTo(1);
        assertThat(metrics.overlapPagesPeak).isEqualTo(3);
        assertThat(metrics.overlapRowsPeak).isEqualTo(6);
    }

    @Test
    void byteIdenticalToEntryTypedMergeOnMixedInput(@TempDir Path dir) throws IOException {
        // A mix: two globally-disjoint bands (fast path) and two interleaving segments (key-merge), so
        // BOTH paths run in one merge — the output must equal the entry-typed StreamingMerger exactly.
        List<List<ListEntry>> segs = List.of(
                sorted("a", "c", "e", "g"),          // interleaves with the next
                sorted("b", "d", "f", "h"),
                sorted("m", "n", "o"),               // disjoint band
                sorted("w", "x", "y", "z"));         // disjoint band
        List<Path> files = stage(dir, segs);

        List<ListEntry> viaPageAware = drainEntries(new PageAwareMerger(frontiers(files), cmp,
                MergeScope.CROSS_SEGMENT, new CountingMetrics()));
        List<ListEntry> viaEntryMerge = drainEntries(new StreamingMerger(entryStreams(files), cmp,
                n -> { }));

        assertThat(viaPageAware).isEqualTo(viaEntryMerge);
        assertThat(viaPageAware.stream().map(e -> e.key().asString()).toList()).isSorted();
    }

    // --- helpers ---

    private List<ListEntry> sorted(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(object(k));
        }
        out.sort(cmp);
        return out;
    }

    private List<Path> stage(Path dir, List<List<ListEntry>> segs) throws IOException {
        Files.createDirectories(dir);
        PageRunSegmentWriter writer = new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            Path path = dir.resolve("seg-" + i + ".pageseg");
            try (SortedCursor cursor = new InMemoryCursor(segs.get(i), cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
    }

    private List<String> drainKeys(List<Path> files, SortMetrics metrics) throws IOException {
        List<String> out = new ArrayList<>();
        for (ListEntry e : drainEntries(new PageAwareMerger(
                frontiers(files), cmp, MergeScope.CROSS_SEGMENT, metrics))) {
            out.add(e.key().asString());
        }
        return out;
    }

    private List<ListEntry> drainEntries(SortedCursor cursor) {
        List<ListEntry> out = new ArrayList<>();
        try (cursor) {
            out.addAll(drainOpen(cursor));
        }
        return out;
    }

    private static List<ListEntry> drainOpen(SortedCursor cursor) {
        List<ListEntry> out = new ArrayList<>();
        while (cursor.hasNext()) {
            out.add(cursor.next());
        }
        return out;
    }

    private static ObjectEntry taggedDuplicate(int source, int ordinal) {
        return new ObjectEntry(KeyBytes.ofUtf8("dup"), source * 1_000_000L + ordinal,
                ordinal, "source-%d/ordinal-%04d".formatted(source, ordinal), null, null,
                false, null, null, null, null);
    }

    private static void assertSourceOrdinalMembership(List<ListEntry> entries, List<ListEntry> source) {
        String sourcePrefix = tag(source.getFirst()).substring(0, "source-0".length());
        List<String> expected = tags(source).stream().sorted().toList();
        assertThat(entries.stream().map(PageAwareMergerTest::tag)
                .filter(tag -> tag.startsWith(sourcePrefix + "/")).sorted().toList())
                .containsExactlyElementsOf(expected);
    }

    private static List<String> tags(List<ListEntry> entries) {
        return entries.stream().map(PageAwareMergerTest::tag).toList();
    }

    private static List<String> pairs(List<ListEntry> entries) {
        List<String> pairs = new ArrayList<>(entries.size() - 1);
        for (int i = 1; i < entries.size(); i++) {
            pairs.add(pair(entries.get(i - 1), entries.get(i)));
        }
        return pairs;
    }

    private static String pair(ListEntry previous, ListEntry current) {
        return tag(previous) + " -> " + tag(current);
    }

    private static String tag(ListEntry entry) {
        return ((ObjectEntry) entry).etag();
    }

    private List<PageFrontierStream> frontiers(List<Path> files) throws IOException {
        List<PageFrontierStream> out = new ArrayList<>();
        for (Path f : files) {
            out.add(new PageFrontierReader(f, SortMetrics.NO_OP));
        }
        return out;
    }

    private List<EntryStream> entryStreams(List<Path> files) throws IOException {
        List<EntryStream> out = new ArrayList<>();
        for (Path f : files) {
            out.add(PageRunReads.open(f));
        }
        return out;
    }

    private List<ListEntry> readStaged(Path file) throws IOException {
        try (EntryStream stream = PageRunReads.open(file)) {
            List<ListEntry> entries = new ArrayList<>();
            while (stream.hasNext()) {
                entries.add(stream.next());
            }
            return entries;
        }
    }

    private static final class CountingMetrics implements SortMetrics {
        private final Map<String, Integer> counts = new HashMap<>();
        private int overlapClusters;
        private long overlapPagesPeak;
        private long overlapRowsPeak;

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.merge(outcome + "." + reason, 1, Integer::sum);
        }

        @Override
        public void markProgress() {
        }

        @Override
        public void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        }

        @Override
        public void recordPageAwareOverlapCluster() {
            overlapClusters++;
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
            overlapPagesPeak = Math.max(overlapPagesPeak, activePages);
            overlapRowsPeak = Math.max(overlapRowsPeak, retainedRows);
        }

        @Override
        public void recordRangeIndexBytes(long bytes) {
        }

        @Override
        public void recordRangeFramedBytes(long bytes) {
        }

        @Override
        public void recordProofSpool(long logicalExtentBytes, long preallocationOperations,
                long preallocationAttemptedBytes, long mappedOperations, long mappedBytes,
                long serviceNanos) {
        }

        int get(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
