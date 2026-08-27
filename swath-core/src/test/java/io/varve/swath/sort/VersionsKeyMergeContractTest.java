/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial suite for the {@link PageAwareMerger} <b>key-merge (overlap) fallback</b> under
 * <b>VERSIONS-shaped</b> input — distinct in scope from the page-whole/overlap case in
 * {@link PageAwareMergerContractTest} (no overlapping coverage between the two).
 *
 * <p><b>Why VERSIONS stresses the key-merge path.</b> In OBJECTS mode every key is unique, sort pages
 * are range-disjoint, and the page-whole fast path always fires — the overlap fallback is effectively
 * prod-unreachable. In VERSIONS mode (<code>--all-versions</code>) a single key carries MANY versions;
 * S3 delivers them last-modified-descending (≠ the {@code (key, version_id, row_type)} sort order), so
 * after flush's per-page re-sort the equal-key clusters straddle page (and segment) boundaries. Those
 * equal-key spans are exactly the {@code maxKey >= minKey} overlap case that routes through
 * {@link PageAwareMerger}'s key-merge fallback. This suite hammers that path for VERSIONS risks:
 * single-key clusters larger than a page, per-key version-count preservation (the data-loss guard),
 * delete markers interleaved with object versions, null-vs-present {@code version_id} transitions, and
 * a seeded VERSIONS-biased differential-oracle property test.
 *
 * <p><b>Oracle.</b> Every constructed entry is DISTINCT under {@link ListEntryComparator} (unique
 * {@code (key, version_id, row_type)}), so the globally-sorted decode of all input entries is the exact,
 * unique expected output; the merger output must equal it entry-for-entry. Where each segment is itself
 * a well-formed sorted stream, the entry-typed {@link StreamingMerger} is run as a second oracle.
 *
 * <p>This class touches no product code. A failing assertion here is a real defect in the merger.
 *
 * <p><b>Scope note.</b> VERSIONS listing does NOT flow end-to-end today — the CLI has no
 * {@code --all-versions} option (it hardcodes {@link io.varve.swath.model.ListingMode#OBJECTS}),
 * and {@code S3PageFetcher} throws {@code UnsupportedOperationException("VERSIONS listing lands in Phase
 * 9")}. The {@code (key, version_id, row_type)} sort/merge machinery is fully present, so this suite
 * covers the key-merge path at the merger/page-run level with VERSIONS-shaped synthetic input. An
 * end-to-end VERSIONS sort test remains until {@code ListObjectVersions} is wired.
 */
class VersionsKeyMergeContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();
    private int seq;

    // ----------------------------------------- risk 1: a single key's versions span MULTIPLE pages

    @Test
    void singleKeyVersionsSpanManyPagesWithinOneSegment(@TempDir Path dir) throws IOException {
        // ONE key "k" with 12 versions packed as FOUR internally-sorted pages of 3 versions each. Adjacent
        // pages share the key at the edge (maxKey==minKey=="k"), so the strict whole-emit check fails on
        // every boundary and the overlap fallback must chain-pull the whole equal-key closure and emit all
        // 12 versions in version-id order with none dropped or duplicated.
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            List<ListEntry> page = new ArrayList<>();
            for (int v = 0; v < 3; v++) {
                page.add(obj("k", ver()));
            }
            pages.add(page);
        }
        Path file = writeSegment(dir, "single-key.pageseg", pages);
        assertThat(hasOverlappingAdjacentPages(file))
                .as("the single key's versions must physically straddle page boundaries")
                .isTrue();

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(List.of(file)));
        assertGloballySorted(out);
        assertThat(out).hasSize(12);
        assertVersionCountPreserved(List.of(file), out);
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("a >1-page equal-key cluster must route through the key-merge fallback")
                .isGreaterThan(0);
    }

    @Test
    void singleKeyVersionsSpanPagesAcrossSegments(@TempDir Path dir) throws IOException {
        // The same equal-key cluster split ACROSS THREE segments: each segment holds a slice of key "k"'s
        // versions (plus a disjoint neighbour key so the segment isn't degenerate). The transitive
        // equal-key closure now spans segment frontiers, not just pages within one file.
        Path a = writeSegment(dir, "segA.pageseg",
                List.of(List.of(obj("a", ver())), List.of(obj("k", ver()), obj("k", ver()))));
        Path b = writeSegment(dir, "segB.pageseg",
                List.of(List.of(obj("k", ver()), obj("k", ver())), List.of(obj("k", ver()))));
        Path c = writeSegment(dir, "segC.pageseg",
                List.of(List.of(obj("k", ver()), obj("k", ver())), List.of(obj("z", ver()))));
        List<Path> files = List.of(a, b, c);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertThat(out).isEqualTo(streamingMerge(files));   // each segment individually sorted -> valid
        assertGloballySorted(out);
        assertVersionCountPreserved(files, out);
        assertThat(byKey(out).get("k"))
                .as("all 7 versions of the cross-segment key must survive").isEqualTo(7);
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // -------------------------------------- risk 2: per-key version-count preservation (data-loss guard)

    @Test
    void everyKeysVersionCountIsPreservedAcrossMixedRouting(@TempDir Path dir) throws IOException {
        // Several keys, each with a different version fan-out, arranged so the router mixes whole-emit
        // (the lone/disjoint keys) with key-merge (the fat multi-page keys) in one drain. The guard: for
        // EVERY key, |versions out| == |versions in| — nothing dropped by the page-whole vs key-merge
        // routing decision.
        List<List<ListEntry>> segA = new ArrayList<>();
        segA.add(List.of(obj("aaa", ver())));                                  // singleton -> whole-emit
        segA.add(pageOf("fat", 5));                                            // 5 versions -> multi-page
        segA.add(pageOf("fat", 5));
        Path a = writeSegment(dir, "a.pageseg", segA);

        List<List<ListEntry>> segB = new ArrayList<>();
        segB.add(pageOf("fat", 4));                                            // same key, another segment
        segB.add(List.of(obj("mmm", ver()), obj("mmm", ver())));              // 2 versions, one page
        segB.add(List.of(obj("zzz", ver())));
        Path b = writeSegment(dir, "b.pageseg", segB);
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
        Map<String, Integer> outCounts = byKey(out);
        assertThat(outCounts.get("aaa")).isEqualTo(1);
        assertThat(outCounts.get("fat")).isEqualTo(14);
        assertThat(outCounts.get("mmm")).isEqualTo(2);
        assertThat(outCounts.get("zzz")).isEqualTo(1);
        assertVersionCountPreserved(files, out);
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("the fat multi-page key must exercise the fallback").isGreaterThan(0);
    }

    // ----------------------------------------- risk 3: delete markers interleaved with object versions

    @Test
    void deleteMarkersInterleavedWithVersionsSpanningBoundaries(@TempDir Path dir) throws IOException {
        // Key "k" mixes object versions and delete markers, spanning a page boundary in seg A and a
        // segment boundary into seg B. A delete marker holds both the OLDEST version-id ("v01") and the
        // NEWEST ("v99"); a same-version cross-type tie (obj "v50" vs del "v50") exercises the row_type
        // tail (OBJECT < DELETE_MARKER). Nothing may be dropped and the order must match the oracle.
        List<ListEntry> pageA0 = List.of(obj("a", ver()), del("k", "v01"), obj("k", "v20"));
        List<ListEntry> pageA1 = List.of(obj("k", "v50"), del("k", "v50"), obj("k", "v70"));
        Path a = writeSegment(dir, "a.pageseg", List.of(pageA0, pageA1));

        List<ListEntry> pageB0 = List.of(del("k", "v80"), del("k", "v99"), obj("s", ver()));
        Path b = writeSegment(dir, "b.pageseg", List.of(pageB0));
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
        assertVersionCountPreserved(files, out);
        // 8 rows at key "k": v01(del), v20, v50(obj), v50(del), v70, v80(del), v99(del) -> 7. plus a,s.
        assertThat(byKey(out).get("k")).isEqualTo(7);
        long deleteMarkers = out.stream().filter(e -> e instanceof DeleteMarkerEntry).count();
        assertThat(deleteMarkers).as("all 4 delete markers must survive").isEqualTo(4);
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // -------------------------- risk 4: null-vs-present version_id transition + isLatest combinations

    @Test
    void nullVersionTransitionAndIsLatestCombosOrderDeterministically(@TempDir Path dir)
            throws IOException {
        // Key "k": an unversioned row (null version_id — the pre-versioning object) plus present versions
        // and a null-version delete marker, mixing isLatest true/false. The comparator sorts null version
        // FIRST (obj rank 0 before del rank 2), then present versions by byte order. The equal-key cluster
        // straddles a page boundary, so it routes through the fallback; order must equal the oracle.
        List<ListEntry> page0 = List.of(objNull("k", false), delNull("k"), obj("k", "v10", true));
        List<ListEntry> page1 = List.of(obj("k", "v20", false), obj("k", "v30", true));
        Path a = writeSegment(dir, "a.pageseg", List.of(page0, page1));
        List<Path> files = List.of(a);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
        assertVersionCountPreserved(files, out);
        assertThat(byKey(out).get("k")).isEqualTo(5);
        // Deterministic head: null-version object first, then null-version delete marker, then present.
        assertThat(out.get(0)).isInstanceOf(ObjectEntry.class);
        assertThat(((ObjectEntry) out.get(0)).versionId()).isNull();
        assertThat(out.get(1)).isInstanceOf(DeleteMarkerEntry.class);
        assertThat(((DeleteMarkerEntry) out.get(1)).versionId()).isNull();
        assertThat(metrics.count("SORT.page_overlap_keymerge")).isGreaterThan(0);
    }

    // ------------------ risk 4b: versions arriving last-modified-DESCENDING with delete markers, >1 page

    @Test
    void versionsArrivingDescendingWithDeleteMarkersAcrossPages(@TempDir Path dir) throws IOException {
        // Pages whose rows arrive in last-modified-DESCENDING order (as S3 delivers a key's versions),
        // NOT sort order — so pack() flags them !orderedUnderFullComparator and flush re-sorts and
        // re-packs them. Beyond PageAwareMergerContractTest's DESC case: delete markers are interleaved and the key
        // spans multiple pages/segments. The re-packed, merged output must still equal the sorted oracle.
        List<ListEntry> descA = new ArrayList<>(List.of(
                obj("k", "v90"), del("k", "v80"), obj("k", "v70"), del("k", "v60"), obj("k", "v50")));
        List<ListEntry> descB = new ArrayList<>(List.of(
                obj("k", "v40"), obj("k", "v30"), del("k", "v20"), obj("k", "v10")));
        Path a = writeSegment(dir, "descA.pageseg", List.of(descA));
        Path b = writeSegment(dir, "descB.pageseg", List.of(descB));
        List<Path> files = List.of(a, b);

        CountingMetrics metrics = new CountingMetrics();
        List<ListEntry> out = drainPageAware(files, metrics);

        assertThat(out).isEqualTo(sortedOracle(files));
        assertGloballySorted(out);
        assertVersionCountPreserved(files, out);
        assertThat(byKey(out).get("k")).isEqualTo(9);
        assertThat(out.stream().filter(e -> e instanceof DeleteMarkerEntry).count()).isEqualTo(3);
    }

    // ----------------------------------------------- risk 5: VERSIONS-biased seeded property (adversarial)

    @Test
    void propertyVersionsBiasedAlwaysEqualsOraclePreservingEveryVersion(@TempDir Path dir)
            throws IOException {
        int totalKeymerge = 0;
        for (long seed = 0; seed < 60; seed++) {
            Path caseDir = dir.resolve("seed-" + seed);
            Files.createDirectories(caseDir);
            Random rnd = new Random(seed);

            int nKeys = 2 + rnd.nextInt(3);            // 2..4 distinct keys -> heavy equal-key clustering
            int nSegs = 2 + rnd.nextInt(3);            // 2..4 segments
            int pageMax = 2 + rnd.nextInt(5);          // page size cap 2..6

            List<List<ListEntry>> bySeg = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                bySeg.add(new ArrayList<>());
            }
            Set<String> nullUsed = new HashSet<>();    // keep null-version rows distinct under the comparator

            // Guaranteed overlap engagement: one fat key gets >pageMax versions ALL in segment 0, so it
            // must span >=2 pages within a single segment -> the key-merge fallback fires every seed.
            int fatCount = pageMax + 6 + rnd.nextInt(6);
            for (int i = 0; i < fatCount; i++) {
                bySeg.get(0).add(randomRow(rnd, "k00", nullUsed));
            }
            // Scatter the rest across all keys/segments.
            int nOther = 20 + rnd.nextInt(60);
            for (int i = 0; i < nOther; i++) {
                String key = String.format("k%02d", rnd.nextInt(nKeys));
                bySeg.get(rnd.nextInt(nSegs)).add(randomRow(rnd, key, nullUsed));
            }

            List<Path> files = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                List<ListEntry> entries = bySeg.get(s);
                if (entries.isEmpty()) {
                    continue;
                }
                entries.sort(cmp);                     // sort-then-chunk -> internally-sorted, well-formed
                List<List<ListEntry>> pages = new ArrayList<>();
                int idx = 0;
                while (idx < entries.size()) {
                    int len = 1 + rnd.nextInt(pageMax);
                    pages.add(new ArrayList<>(entries.subList(idx, Math.min(entries.size(), idx + len))));
                    idx += len;
                }
                files.add(writeSegment(caseDir, "seg-" + s + ".pageseg", pages));
            }

            CountingMetrics metrics = new CountingMetrics();
            List<ListEntry> out = drainPageAware(files, metrics);
            List<ListEntry> oracle = sortedOracle(files);

            assertThat(out).as("seed %d: merger output must equal the sorted oracle", seed).isEqualTo(oracle);
            assertThat(out).as("seed %d: well-formed segments -> streaming oracle agrees", seed)
                    .isEqualTo(streamingMerge(files));
            assertGloballySorted(out);
            assertThat(out).as("seed %d: no version dropped or duplicated", seed).hasSize(oracle.size());
            assertVersionCountPreservedSeed(files, out, seed);
            int keymerge = metrics.count("SORT.page_overlap_keymerge");
            assertThat(keymerge)
                    .as("seed %d: the fat equal-key cluster must engage the key-merge fallback", seed)
                    .isGreaterThan(0);
            totalKeymerge += keymerge;
        }
        assertThat(totalKeymerge)
                .as("the VERSIONS-biased property must exercise the key-merge path across the suite")
                .isGreaterThan(0);
    }

    // ------------------------------------------------------------------------------------- generators

    private ListEntry randomRow(Random rnd, String key, Set<String> nullUsed) {
        boolean delete = rnd.nextInt(6) == 0;                    // ~1/6 delete markers
        boolean isLatest = rnd.nextBoolean();
        boolean wantNull = rnd.nextInt(10) == 0;                 // occasional unversioned/null-version row
        String nullTag = key + "|" + delete;
        if (wantNull && nullUsed.add(nullTag)) {                 // at most one null-version row per (key,type)
            return delete ? delNull(key) : objNull(key, isLatest);
        }
        String version = ver();
        return delete ? del(key, version) : obj(key, version, isLatest);
    }

    /** A page of {@code n} distinct object versions of {@code key}. */
    private List<ListEntry> pageOf(String key, int n) {
        List<ListEntry> page = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            page.add(obj(key, ver()));
        }
        return page;
    }

    private String ver() {
        return String.format("v%08d", seq++);
    }

    private ObjectEntry obj(String key, String version) {
        return obj(key, version, false);
    }

    private ObjectEntry obj(String key, String version, boolean isLatest) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, version,
                isLatest, null, null, null, null);
    }

    private ObjectEntry objNull(String key, boolean isLatest) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, null,
                isLatest, null, null, null, null);
    }

    private DeleteMarkerEntry del(String key, String version) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), version, false, seq++, null);
    }

    private DeleteMarkerEntry delNull(String key) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), null, false, seq++, null);
    }

    // ------------------------------------------------------------------------------------- harness

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
        try (PageAwareMerger merger = new PageAwareMerger(frontiers, cmp, DuplicateHook.NO_OP, metrics)) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    private List<ListEntry> streamingMerge(List<Path> files) throws IOException {
        List<EntryStream> streams = new ArrayList<>();
        for (Path f : files) {
            streams.add(PageRunReads.open(f));
        }
        List<ListEntry> out = new ArrayList<>();
        try (StreamingMerger merger = new StreamingMerger(streams, cmp, DuplicateHook.NO_OP, n -> { })) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    private List<ListEntry> sortedOracle(List<Path> files) throws IOException {
        List<ListEntry> all = decodeAll(files);
        all.sort(cmp);
        return all;
    }

    private List<ListEntry> decodeAll(List<Path> files) throws IOException {
        List<ListEntry> all = new ArrayList<>();
        for (Path f : files) {
            try (PageRunSegmentReader reader = PageRunReads.open(f)) {
                while (reader.hasNext()) {
                    all.add(reader.next());
                }
            }
        }
        return all;
    }

    /** Per-key row count, keyed by the (ASCII) key string. */
    private Map<String, Integer> byKey(List<ListEntry> entries) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (ListEntry e : entries) {
            counts.merge(e.key().asString(), 1, Integer::sum);
        }
        return counts;
    }

    /** The VERSIONS data-loss guard: every key's version count OUT must equal its count IN. */
    private void assertVersionCountPreserved(List<Path> files, List<ListEntry> out) throws IOException {
        assertThat(byKey(out)).isEqualTo(byKey(decodeAll(files)));
    }

    private void assertVersionCountPreservedSeed(List<Path> files, List<ListEntry> out, long seed)
            throws IOException {
        assertThat(byKey(out))
                .as("seed %d: every key's version count must be preserved (no version dropped)", seed)
                .isEqualTo(byKey(decodeAll(files)));
    }

    private void assertGloballySorted(List<ListEntry> out) {
        for (int i = 1; i < out.size(); i++) {
            assertThat(cmp.compare(out.get(i - 1), out.get(i)))
                    .as("output must be globally non-decreasing at index %d", i)
                    .isLessThanOrEqualTo(0);
        }
    }

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

        int count(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
