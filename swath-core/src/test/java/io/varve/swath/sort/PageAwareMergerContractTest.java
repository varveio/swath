/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial suite for {@link PageAwareMerger}'s page-whole fast path and overlap guards. Every
 * test drives the merger against a
 * <b>differential oracle</b>: the same page-run segments, every entry decoded (order-independently) via
 * {@link PageRunSegmentReader} and globally sorted under {@link ListEntryComparator}. Because every
 * constructed entry is DISTINCT under the comparator (unique {@code (key, version, row_type)}), the
 * sorted order is unique, so the oracle is exact and the merger output must equal it entry-for-entry.
 *
 * <p>Where each segment is itself a valid sorted stream (no intra-segment overlap) the entry-typed
 * {@link StreamingMerger} is ALSO run as a second oracle. For intra-segment-overlap cases the
 * {@link PageRunSegmentReader}+{@link StreamingMerger} path is deliberately NOT used as an oracle: a
 * physically-misordered segment makes that reader a non-sorted stream, so only the resort oracle is
 * sound there.
 *
 * <p>This class does not touch product code. A failing assertion here means a real defect in the
 * merger, not in the test.
 */
class PageAwareMergerContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();
    private int seq;

    // ------------------------------------------------------------------ case 1: disjoint baseline

    @Test
    void disjointObjectsBaselineAllEmitWholeNoOverlap(@TempDir Path dir) throws IOException {
        // Many segments, each in its own key band, several pages each — every page range-disjoint.
        List<Path> files = new ArrayList<>();
        for (int s = 0; s < 5; s++) {
            List<List<ListEntry>> pages = new ArrayList<>();
            for (int p = 0; p < 4; p++) {
                List<ListEntry> page = new ArrayList<>();
                for (int i = 0; i < 7; i++) {
                    page.add(obj(String.format("s%d-p%d-k%02d", s, p, i)));
                }
                pages.add(page);
            }
            files.add(writeSegment(dir, "seg-" + s + ".pageseg", pages));
        }

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertThat(out).isEqualTo(streamingMerge(files));   // segments individually sorted -> valid oracle
        assertGloballySorted(out);
        long records = totalRecords(files);
        assertThat(metrics.count("SORT.page_whole_emitted")).isEqualTo((int) records);   // ALL whole
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isZero();
    }

    // ------------------------------------------------------------- case 2: intra-segment overlap

    @Test
    void intraSegmentAdjacentPageOverlapIsCaughtNotMisordered(@TempDir Path dir) throws IOException {
        // ONE segment, two node runs whose ranges INTERLEAVE. flush() orders pages by firstKey, so the
        // file holds two adjacent pages [a..m] then [c..z]: minKeys monotone (a<c) but ranges overlap
        // (maxKey(page0)="m" >= minKey(page1)="c"). A plain sequential reader would emit a,m,c,z —
        // misordered. The intra-segment guard MUST fall back to a key-merge.
        List<ListEntry> pageLow = List.of(obj("a"), obj("m"));
        List<ListEntry> pageHigh = List.of(obj("c"), obj("z"));
        Path file = writeSegment(dir, "overlap.pageseg", List.of(pageLow, pageHigh));

        assertThat(hasOverlappingAdjacentPages(file))
                .as("constructed segment must physically contain overlapping adjacent pages")
                .isTrue();

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(List.of(file)));   // a,c,m,z — NOT the file order a,m,c,z
        assertGloballySorted(out);
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("intra-segment overlap must trigger the key-merge guard")
                .isGreaterThan(0);
    }

    @Test
    void intraSegmentOverlapChainAcrossManyPages(@TempDir Path dir) throws IOException {
        // A long intra-segment overlap chain: every page spans nearly the whole keyspace, so each
        // adjacent pair overlaps and the fallback must chain-pull the segment's own successors.
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int p = 0; p < 8; p++) {
            List<ListEntry> page = new ArrayList<>();
            // firstKey grows with p (keeps minKeys monotone) but maxKey reaches "z" every time.
            page.add(obj(String.format("b%02d", p)));   // distinct low anchor per page
            page.add(obj("z" + (char) ('a' + p)));       // high anchor, distinct
            pages.add(page);
        }
        Path file = writeSegment(dir, "chain.pageseg", pages);
        assertThat(hasOverlappingAdjacentPages(file)).isTrue();

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(List.of(file)));
        assertGloballySorted(out);
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // ------------------------------------------------------------ case 3: maxKey == minKey boundary

    @Test
    void equalKeyAtPageEdgeIsMergedNotEmittedWhole(@TempDir Path dir) throws IOException {
        // Within ONE segment: page0 ends with key "m" and page1 begins with key "m" (same key bytes at
        // the edge, distinct versions). maxKey(page0) == minKey(page1) — the STRICT (<) whole-emit check
        // must fail, so the equal-key entries merge in version order (x before y), never a whole emit.
        List<ListEntry> page0 = List.of(obj("a"), obj("m", "x"));
        List<ListEntry> page1 = List.of(obj("m", "y"), obj("z"));
        Path file = writeSegment(dir, "edge.pageseg", List.of(page0, page1));

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(List.of(file)));   // a, m/x, m/y, z
        assertGloballySorted(out);
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("a page whose maxKey equals the next page's minKey must NOT emit whole")
                .isGreaterThan(0);
    }

    @Test
    void equalKeyAtBoundaryAcrossTwoSegments(@TempDir Path dir) throws IOException {
        // maxKey==minKey across DIFFERENT segments: seg A ends at "m"/x, seg B starts at "m"/y.
        Path a = writeSegment(dir, "a.pageseg", List.of(List.of(obj("d"), obj("m", "x"))));
        Path b = writeSegment(dir, "b.pageseg", List.of(List.of(obj("m", "y"), obj("t"))));
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertThat(out).isEqualTo(streamingMerge(files));   // each segment individually sorted
        assertGloballySorted(out);
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // -------------------------------------------- case 4: version + row_type tiebreaks over a boundary

    @Test
    void versionAndRowTypeTiebreaksStraddlingPageAndSegmentBoundaries(@TempDir Path dir) throws IOException {
        // A dense equal-key cluster on key "m" that spans a page boundary in seg A and a segment boundary
        // into seg B, mixing all three row types and null/absent versions. cmp resolves:
        //   version absent(null) first (obj rank0 < commonPrefix rank1 < deleteMarker rank2),
        //   then present versions "a" < "b" in byte order (obj/del interleave by version then rank).
        List<ListEntry> segAPage0 = List.of(obj("f"), objNull("m"), prefix("m"));
        List<ListEntry> segAPage1 = List.of(del("m", null), obj("m", "a"));
        Path a = writeSegment(dir, "a.pageseg", List.of(segAPage0, segAPage1));

        List<ListEntry> segBPage0 = List.of(del("m", "a"), obj("m", "b"), del("m", "b"), obj("s"));
        Path b = writeSegment(dir, "b.pageseg", List.of(segBPage0));
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
    }

    // ------------------------------------------------------------- case 5: cross-segment overlap storm

    @Test
    void crossSegmentOverlapStormAtModerateScale(@TempDir Path dir) throws IOException {
        // N segments whose pages all mutually overlap: round-robin a large sorted keyspace across
        // segments, then chunk each segment's (whole-range-spanning) keys into small pages. Every page
        // overlaps many others, so `ceiling` grows and closeActiveUnderFrontier pulls many pages at once.
        int n = 5;
        int keys = 300;
        int pageSize = 12;
        List<List<ListEntry>> perSeg = new ArrayList<>();
        for (int s = 0; s < n; s++) {
            perSeg.add(new ArrayList<>());
        }
        for (int i = 0; i < keys; i++) {
            perSeg.get(i % n).add(obj(String.format("k%04d", i)));
        }
        List<Path> files = new ArrayList<>();
        for (int s = 0; s < n; s++) {
            List<List<ListEntry>> pages = chunk(perSeg.get(s), pageSize);
            files.add(writeSegment(dir, "storm-" + s + ".pageseg", pages));
        }

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
        assertThat(out).hasSize(keys);   // no loss/dup at scale
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // --------------------------------------------------- case 6: !orderedUnderFullComparator (versions)

    @Test
    void versionsArrivingLastModifiedDescendingAreRepackedByM3aAndMergeCorrectly(@TempDir Path dir)
            throws IOException {
        // Pages whose entries arrive NOT in full-comparator order (S3 delivers a key's versions
        // last-modified-descending). pack() flags them !orderedUnderFullComparator, so flush drains
        // + re-sorts + re-packs them. Feed the result through and assert it merges to the true sorted order.
        List<ListEntry> descA = new ArrayList<>(List.of(
                obj("k1", "v3"), obj("k1", "v2"), obj("k1", "v1"),   // descending versions
                obj("k2", "v9"), obj("k2", "v5")));
        List<ListEntry> descB = new ArrayList<>(List.of(
                obj("k1", "v6"), obj("k1", "v4"),
                obj("k3", "v2"), obj("k3", "v1")));
        Path a = writeSegment(dir, "descA.pageseg", List.of(descA));
        Path b = writeSegment(dir, "descB.pageseg", List.of(descB));
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
    }

    // ------------------------------------------------------------------------ case 8: minimal shapes

    @Test
    void singleSegmentSinglePageEmitsWhole(@TempDir Path dir) throws IOException {
        Path file = writeSegment(dir, "one.pageseg", List.of(List.of(obj("only"))));
        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(List.of(file)));
        assertThat(out).hasSize(1);
        assertThat(metrics.count("SORT.page_whole_emitted")).isEqualTo(1);
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isZero();
    }

    @Test
    void allEmptySegmentsDrainToNothing(@TempDir Path dir) throws IOException {
        Path empty = writeSegment(dir, "empty.pageseg", List.of());
        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(empty), metrics);
        assertThat(out).isEmpty();
    }

    @Test
    void mixOfEmptyAndPopulatedSegments(@TempDir Path dir) throws IOException {
        Path empty = writeSegment(dir, "empty.pageseg", List.of());
        Path a = writeSegment(dir, "a.pageseg", List.of(List.of(obj("a"), obj("c"))));
        Path b = writeSegment(dir, "b.pageseg", List.of(List.of(obj("b"), obj("d"))));
        List<Path> files = List.of(empty, a, b);

        List<ListEntry> out = drainPageAware(files, new CountingMetrics());
        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
    }

    // ------------------------------------------------------------------------ case 7: PROPERTY (adversarial)

    @Test
    void propertyRandomShapesAlwaysEqualTheSortedOracle(@TempDir Path dir) throws IOException {
        for (long seed = 0; seed < 60; seed++) {
            Path caseDir = dir.resolve("seed-" + seed);
            Files.createDirectories(caseDir);
            Random rnd = new Random(seed);

            int keyspace = 5 + rnd.nextInt(14);        // small alphabet -> heavy equal-key clustering
            int nEntries = 20 + rnd.nextInt(140);
            int nSegs = 2 + rnd.nextInt(5);

            // Every entry gets a globally-unique version -> distinct under the comparator -> the sorted
            // oracle is unique and the differential comparison is exact (no tie ambiguity).
            List<List<ListEntry>> bySeg = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                bySeg.add(new ArrayList<>());
            }
            for (int i = 0; i < nEntries; i++) {
                String key = String.format("k%03d", rnd.nextInt(keyspace));
                String version = String.format("v%08d", seq++);
                ListEntry e = rnd.nextInt(5) == 0 ? del(key, version) : obj(key, version);
                bySeg.get(rnd.nextInt(nSegs)).add(e);
            }

            List<Path> files = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                List<ListEntry> entries = bySeg.get(s);
                Collections.shuffle(entries, rnd);
                // Live pages require non-decreasing raw keys. Stable raw sorting preserves randomized
                // version/row-type order within equal keys, so safe full-comparator repair still engages.
                entries.sort((a, b) -> KeyBytes.compareUnsigned(
                        a.key().rawUnsafe(), b.key().rawUnsafe()));
                List<List<ListEntry>> pages = new ArrayList<>();
                int idx = 0;
                while (idx < entries.size()) {
                    int len = 1 + rnd.nextInt(8);
                    pages.add(new ArrayList<>(entries.subList(idx, Math.min(entries.size(), idx + len))));
                    idx += len;
                }
                files.add(writeSegment(caseDir, "seg-" + s + ".pageseg", pages));
            }

            CountingMetrics metrics = new CountingMetrics();
            List<ListEntry> out = drainPageAware(files, metrics);
            List<ListEntry> oracle = sortedOracle(files);

            assertThat(out).as("seed %d: merger output must equal the sorted oracle", seed).isEqualTo(oracle);
            assertGloballySorted(out);

            // Page accounting: whole-emitted pages never exceed total pages, and the whole-emit fast path
            // engages on exactly all pages iff no overlap event fired (every page accounted for).
            long records = totalRecords(files);
            int whole = metrics.count("SORT.page_whole_emitted");
            int overlapEvents = metrics.count("SORT.page_overlap_keymerge");
            assertThat(whole).as("seed %d: whole <= totalPages", seed).isLessThanOrEqualTo((int) records);
            if (overlapEvents == 0) {
                assertThat(whole).as("seed %d: no overlap => every page whole", seed).isEqualTo((int) records);
            } else {
                assertThat(whole).as("seed %d: overlap => not all pages whole", seed).isLessThan((int) records);
            }
        }
    }

    // ------------------------------------------------------------------------------------- helpers

    private ObjectEntry obj(String key) {
        return obj(key, "v" + String.format("%08d", seq));   // unique version keeps distinct-under-cmp
    }

    /** An object with an ABSENT (null) version — sorts first among a key's rows. */
    private ObjectEntry objNull(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, null, false, null, null, null, null);
    }

    private ObjectEntry obj(String key, String version) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, version, false, null, null, null, null);
    }

    private DeleteMarkerEntry del(String key, String version) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), version, false, seq++, null);
    }

    private CommonPrefixEntry prefix(String key) {
        return new CommonPrefixEntry(KeyBytes.ofUtf8(key));
    }

    private static List<List<ListEntry>> chunk(List<ListEntry> entries, int size) {
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int i = 0; i < entries.size(); i += size) {
            pages.add(new ArrayList<>(entries.subList(i, Math.min(entries.size(), i + size))));
        }
        return pages;
    }

    /** Write a page-run segment where each inner list is exactly ONE page (one framed record). */
    private Path writeSegment(Path dir, String name, List<List<ListEntry>> pages) throws IOException {
        SortBuffer buffer = new SortBuffer(config, cmp);
        long node = 0;
        for (List<ListEntry> page : pages) {
            if (!page.isEmpty()) {
                buffer.admit(node++, page);
            }
        }
        Path path = dir.resolve(name);
        new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private List<ListEntry> drainPageAware(List<Path> files, SortMetrics metrics) throws IOException {
        List<PageFrontierStream> frontiers = new ArrayList<>();
        for (Path f : files) {
            frontiers.add(new PageFrontierReader(f, SortMetrics.NO_OP));
        }
        List<ListEntry> out = new ArrayList<>();
        try (PageAwareMerger merger = new PageAwareMerger(
                frontiers, cmp, MergeScope.CROSS_SEGMENT, metrics)) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    /** Entry-typed merge over {@link PageRunSegmentReader} — valid oracle ONLY when each segment is a
     *  well-formed (internally sorted, non-overlapping) sorted stream. */
    private List<ListEntry> streamingMerge(List<Path> files) throws IOException {
        List<EntryStream> streams = new ArrayList<>();
        for (Path f : files) {
            streams.add(PageRunReads.open(f));
        }
        List<ListEntry> out = new ArrayList<>();
        try (StreamingMerger merger = new StreamingMerger(streams, cmp, n -> { })) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    /** Universal oracle: decode every entry (order-independently) and globally sort. Exact because every
     *  constructed entry is distinct under the comparator. */
    private List<ListEntry> sortedOracle(List<Path> files) throws IOException {
        List<ListEntry> all = new ArrayList<>();
        for (Path f : files) {
            try (PageRunSegmentReader reader = PageRunReads.open(f)) {
                while (reader.hasNext()) {
                    all.add(reader.next());
                }
            }
        }
        all.sort(cmp);
        return all;
    }

    private long totalRecords(List<Path> files) throws IOException {
        long total = 0;
        for (Path f : files) {
            total += PageRunTrailer.read(f).totalRecords();
        }
        return total;
    }

    private void assertGloballySorted(List<ListEntry> out) {
        for (int i = 1; i < out.size(); i++) {
            assertThat(cmp.compare(out.get(i - 1), out.get(i)))
                    .as("output must be globally non-decreasing at index %d", i)
                    .isLessThanOrEqualTo(0);
        }
    }

    /** True iff the physical segment holds two adjacent pages whose ranges overlap (minKeys still
     *  monotone, but maxKey(page[i]) >= minKey(page[i+1])) — proves the overlap hazard is actually present. */
    private boolean hasOverlappingAdjacentPages(Path file) throws IOException {
        try (PageFrontierReader reader = new PageFrontierReader(file, SortMetrics.NO_OP)) {
            byte[] prevMax = null;
            while (reader.hasPage()) {
                if (prevMax != null && Arrays.compareUnsigned(reader.minKey(), prevMax) <= 0) {
                    return true;
                }
                prevMax = reader.maxKey().clone();
                reader.advance();
            }
        }
        return false;
    }

    private static final class CountingMetrics implements SortMetrics {
        private final Map<String, Integer> counts = new HashMap<>();

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
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
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

        int count(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
