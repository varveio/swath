/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.drain;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every cascade reduction pass ({@link KWayMerge#onePass}) selects its merger through the SAME
 * {@code openMerger} factory {@link KWayMerge}'s final pass uses: {@link PageAwareMerger} guards
 * the final pass, and this factory keeps {@link PageAwareMerger}'s guards in force on every
 * cascade pass too. A cascade group that instead opened its page-run inputs through the trusting
 * {@link StreamingMerger} + {@link PageRunSegmentReader} would read a segment's pages in FILE order
 * and apply neither the intra-segment nor cross-segment overlap guard: a segment with intra-segment
 * overlapping adjacent pages would then be merged into a mis-ordered intermediate
 * <em>silently</em>, with {@code SORT.page_overlap_keymerge} never firing for the cascade.
 *
 * <p>These tests drive real {@code .pageseg} files through {@link KWayMerge} with the fan-in pinned to
 * 2 so a cascade fires, against a decode-all-then-sort oracle. The guard-engagement assertion fails
 * ({@code page_overlap_keymerge == 0} during the cascade) if any cascade pass bypasses the guard.
 */
class KWayMergeCascadePageGuardTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();
    private int seq;

    @Test
    void cascadeOverIntraSegmentOverlapEngagesTheKeyMergeGuardAndStaysSorted(@TempDir Path dir)
            throws IOException {
        // seg0 holds two node runs whose ranges INTERLEAVE: flush orders pages by firstKey, so the file
        // holds [a..m] then [c..z] — minKeys monotone (a<c) but maxKey(page0)="m" >= minKey(page1)="c".
        // A file-order reader emits a,m,c,z (mis-ordered); the intra-segment guard MUST fall back to a
        // key-merge. Two further disjoint segments make three inputs, so fan-in 2 forces a cascade whose
        // first group [seg0, seg1] contains the overlapping segment.
        Path seg0 = writeSegment(dir, "overlap.pageseg",
                List.of(List.of(obj("a"), obj("m")), List.of(obj("c"), obj("z"))));
        Path seg1 = writeSegment(dir, "mid.pageseg", List.of(List.of(obj("n"), obj("o"))));
        Path seg2 = writeSegment(dir, "high.pageseg", List.of(List.of(obj("r"), obj("s"))));
        List<Path> originals = List.of(seg0, seg1, seg2);

        assertThat(hasOverlappingAdjacentPages(seg0))
                .as("seg0 must physically contain overlapping adjacent pages to exercise the cascade "
                        + "overlap-guard hazard")
                .isTrue();

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Path> merge = new KWayMerge<>(cmp, 2, pageRunIo(dir, metrics), DuplicateHook.NO_OP, metrics);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(originals)));

        assertThat(merge.cascadedPasses()).as("fan-in 2 over 3 segments must cascade").isGreaterThan(0);
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isGreaterThan(0);
        // The overlap guard must engage DURING the cascade: a cascade group that instead merges seg0
        // through the trusting StreamingMerger leaves this counter at 0.
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("intra-segment overlap in a cascade group must trigger the key-merge guard")
                .isGreaterThan(0);
        // And the final output is globally correct (== the decode-all-then-sort oracle).
        assertThat(out).isEqualTo(sortedOracle(originals));
        assertGloballySorted(out);
    }

    @Test
    void cascadeOverDisjointPageRunsStaysOnTheWholePageFastPath(@TempDir Path dir) throws IOException {
        // Three fully range-disjoint page-run segments: fan-in 2 still cascades, but every page is
        // range-disjoint so the guard never fires — the whole-page fast path carries every pass.
        Path a = writeSegment(dir, "a.pageseg", List.of(List.of(obj("a01"), obj("a02"), obj("a03"))));
        Path b = writeSegment(dir, "b.pageseg", List.of(List.of(obj("b01"), obj("b02"))));
        Path c = writeSegment(dir, "c.pageseg", List.of(List.of(obj("c01"), obj("c02"), obj("c03"))));
        List<Path> originals = List.of(a, b, c);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        KWayMerge<Path> merge = new KWayMerge<>(cmp, 2, pageRunIo(dir, metrics), DuplicateHook.NO_OP, metrics);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(originals)));

        assertThat(merge.cascadedPasses()).isGreaterThan(0);
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isGreaterThan(0);
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("disjoint page runs never trigger the guard")
                .isZero();
        assertThat(metrics.count("SORT.page_whole_emitted"))
                .as("disjoint page runs emit whole pages on the fast path")
                .isGreaterThan(0);
        assertThat(out).isEqualTo(sortedOracle(originals));
        assertGloballySorted(out);
    }

    @Test
    void cascadeCarriesDuplicateKeysAcrossPassesWithoutLosingMultiplicity(@TempDir Path dir) throws IOException {
        // Every row here has a value-EQUAL twin (identical key, version and payload),
        // so each pass's PageAwareMerger fires the DuplicateHook on adjacent equals AND the intermediate is
        // re-staged through PageRunSegmentWriter#writeIntermediate and merged AGAIN. §0.5 says swath --sort
        // is a sorter, never a deduper: multiplicity must survive EVERY pass. The oracle is the GENERATED
        // input multiset — not a re-read of the staged files, which would hide a writer-side drop.
        List<ListEntry> s0 = List.of(dup("a"), dup("a"), dup("m"));
        List<ListEntry> s1 = List.of(dup("a"), dup("b"), dup("m"));
        List<ListEntry> s2 = List.of(dup("b"), dup("m"), dup("z"));
        List<ListEntry> generated = new ArrayList<>();
        generated.addAll(s0);
        generated.addAll(s1);
        generated.addAll(s2);

        List<Path> originals = List.of(
                writeSegment(dir, "d0.pageseg", List.of(s0)),
                writeSegment(dir, "d1.pageseg", List.of(s1)),
                writeSegment(dir, "d2.pageseg", List.of(s2)));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        AtomicInteger duplicatesReported = new AtomicInteger();
        KWayMerge<Path> merge = new KWayMerge<>(cmp, 2, pageRunIo(dir, metrics),
                (previous, current) -> duplicatesReported.incrementAndGet(), metrics);
        List<ListEntry> out = drain(merge.merge(new ArrayList<>(originals)));

        assertThat(merge.cascadedPasses()).as("fan-in 2 over 3 segments must cascade").isGreaterThan(0);
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isGreaterThan(0);
        assertThat(out)
                .as("no user entry is dropped by ANY pass — the output is the generated input multiset")
                .containsExactlyInAnyOrderElementsOf(generated);
        // Absolute per-key multiplicity, so a symmetric drop (one "m" lost in each of two passes) cannot hide.
        assertThat(out).hasSize(9);
        assertThat(countKey(out, "a")).as("all three \"a\" rows survive the cascade").isEqualTo(3);
        assertThat(countKey(out, "b")).as("both \"b\" rows survive the cascade").isEqualTo(2);
        assertThat(countKey(out, "m")).as("all three \"m\" rows survive the cascade").isEqualTo(3);
        assertThat(countKey(out, "z")).isEqualTo(1);
        assertThat(duplicatesReported.get())
                .as("adjacent equals are REPORTED to the hook (never dropped)").isGreaterThan(0);
        assertGloballySorted(out);
        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("neither flush() nor writeIntermediate() may emit a page-min regression").isZero();
    }

    // ------------------------------------------------------------------------------------- helpers

    /** Page-run {@link KWayMerge.SegmentIo}: every segment (original + cascade intermediate) is a
     *  {@code .pageseg} exposing a decode-free page frontier, so the guarded merger is always eligible. */
    private KWayMerge.SegmentIo<Path> pageRunIo(Path dir, SortMetrics metrics) {
        return new KWayMerge.SegmentIo<>() {
            private int seq;

            @Override
            public EntryStream open(Path segment) throws IOException {
                return new PageRunSegmentReader(segment);
            }

            @Override
            public Path writeIntermediate(SortedCursor sorted) throws IOException {
                Path dest = dir.resolve("merge-" + (seq++) + ".pageseg");
                new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, metrics, PageCodec.NONE).writeIntermediate(sorted, dest);
                return dest;
            }

            @Override
            public void delete(Path segment) throws IOException {
                Files.deleteIfExists(segment);
            }

            @Override
            public boolean supportsPageFrontier(Path segment) {
                return true;
            }

            @Override
            public PageFrontierStream openFrontier(Path segment) throws IOException {
                return new PageFrontierReader(segment, metrics);   // counter visible to the test
            }
        };
    }

    private ObjectEntry obj(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null,
                "v" + String.format("%08d", seq), false, null, null, null, null);
    }

    /** A row with a FIXED payload: two {@code dup(k)} rows are equal by value AND under the comparator —
     *  a real user duplicate, the thing §0.5 forbids the sort from swallowing. */
    private static ObjectEntry dup(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, "v1",
                false, null, null, null, null);
    }

    private static int countKey(List<ListEntry> entries, String key) {
        int n = 0;
        for (ListEntry e : entries) {
            if (e.key().asString().equals(key)) {
                n++;
            }
        }
        return n;
    }

    /** Each inner list is one node run (one page after flush's firstKey ordering). */
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

    /** Universal oracle: decode every entry (order-independently) and globally sort. Exact because
     *  every constructed entry is distinct under the comparator (unique version). */
    private List<ListEntry> sortedOracle(List<Path> files) throws IOException {
        List<ListEntry> all = new ArrayList<>();
        for (Path f : files) {
            try (PageRunSegmentReader reader = new PageRunSegmentReader(f)) {
                while (reader.hasNext()) {
                    all.add(reader.next());
                }
            }
        }
        all.sort(cmp);
        return all;
    }

    private void assertGloballySorted(List<ListEntry> out) {
        for (int i = 1; i < out.size(); i++) {
            assertThat(cmp.compare(out.get(i - 1), out.get(i)))
                    .as("output must be globally non-decreasing at index %d", i)
                    .isLessThanOrEqualTo(0);
        }
    }

    /** True iff the physical segment holds two adjacent pages whose ranges overlap. */
    private boolean hasOverlappingAdjacentPages(Path file) throws IOException {
        try (PageFrontierReader reader = new PageFrontierReader(file)) {
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
}
