/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
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
        List<Path> files = stage(dir, List.of(sorted("dup", "dup"), sorted("dup", "dup")));
        long[] pageRuns = new long[2];
        long[] streamingRuns = new long[2];
        List<ListEntry> page;
        List<ListEntry> streaming;
        try (SortedCursor cursor = new DuplicateReporting(new PageAwareMerger(frontiers(files), cmp,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP,
                (copyable, interleaved) -> { pageRuns[0] = copyable; pageRuns[1] = interleaved; }),
                cmp, DuplicateHook.NO_OP)) {
            page = drainEntries(cursor);
        }
        try (SortedCursor cursor = new DuplicateReporting(new StreamingMerger(entryStreams(files), cmp, n -> { },
                (copyable, interleaved) -> { streamingRuns[0] = copyable; streamingRuns[1] = interleaved; }),
                cmp, DuplicateHook.NO_OP)) {
            streaming = drainEntries(cursor);
        }
        assertThat(page).containsExactlyElementsOf(streaming);
        assertThat(page).hasSize(4);
        assertThat(pageRuns).containsExactly(0L, 2L);
        assertThat(streamingRuns).containsExactly(2L, 0L);
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
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        }
        return out;
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

        int get(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
