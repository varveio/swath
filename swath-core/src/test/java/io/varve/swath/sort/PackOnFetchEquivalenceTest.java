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
import io.varve.swath.model.PackedPage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pack-on-fetch behavior-preservation guard. Packing a page on the fetch worker
 * ({@link SortPagePacker} → {@link SortLane#admit(long, PackedPage)}) must produce byte-identical sorted
 * staging to the old pack-on-drain path ({@link SortLane#admit(long, java.util.List)} which packs inside
 * {@link SortBuffer}), and concurrent packing across threads must still yield correct per-node sorted runs.
 */
final class PackOnFetchEquivalenceTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** Small entry gate so multi-page input seals several segments in both A/B lanes identically. */
    private static SortConfig gate3() {
        return SortConfigs.base().withSegmentEntries(3);
    }

    @Test
    void fetchPackedOutputEqualsDrainPacked(@TempDir Path tmp) throws Exception {
        // Two disjoint per-node ascending runs (the work-stealing ownership invariant).
        List<PageOnNode> input = List.of(
                new PageOnNode(1L, page("a", "b", "c")),
                new PageOnNode(1L, page("d", "e", "f")),
                new PageOnNode(2L, page("g", "h", "i", "j")),
                new PageOnNode(2L, page("k", "l")));

        List<String> drainPacked = runLane(tmp.resolve("drain"), input, false);
        List<String> fetchPacked = runLane(tmp.resolve("fetch"), input, true);

        // Identical sorted staging content — pack location does not change what gets sealed.
        assertThat(fetchPacked).containsExactlyElementsOf(drainPacked);
        assertThat(fetchPacked).containsExactly("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l");
    }

    /** Drive one lane either via the raw admit (packs on the drain thread) or via SortPagePacker (fetch-pack). */
    private List<String> runLane(Path dir, List<PageOnNode> input, boolean fetchPack) throws Exception {
        Path staging = Files.createDirectories(dir.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        SortPagePacker packer = new SortPagePacker(cmp, gate3());
        try (SortLane lane = new SortLane(gate3(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                new SortLaneMeters() { }, staging, "seg-" + (fetchPack ? "fetch" : "drain"), finalized::add)) {
            for (PageOnNode p : input) {
                if (fetchPack) {
                    lane.admit(p.node(), packer.pack(p.page()));
                } else {
                    lane.admit(p.node(), p.page());
                }
            }
        }
        List<String> all = new ArrayList<>();
        for (SegmentResult r : finalized) {
            List<String> segKeys = keys(r.path());
            assertThat(segKeys).isSorted();
            all.addAll(segKeys);
        }
        return all;
    }

    @Test
    void concurrentPackingProducesCorrectPerNodeRuns(@TempDir Path tmp) throws Exception {
        SortPagePacker packer = new SortPagePacker(cmp, gate3());
        int nodes = 8;
        int pagesPerNode = 25;
        int perPage = 20;

        // Concurrently pack every page on a shared packer (the fetch-thread contention this unit introduces).
        List<Callable<PageOnPacked>> tasks = new ArrayList<>();
        for (int n = 0; n < nodes; n++) {
            long node = n;
            for (int pg = 0; pg < pagesPerNode; pg++) {
                int base = pg * perPage;
                tasks.add(() -> {
                    List<ListEntry> page = new ArrayList<>();
                    for (int i = 0; i < perPage; i++) {
                        // node-prefixed, zero-padded so each node's keys are a disjoint ascending range.
                        page.add(SortTestSupport.object(String.format("n%02d/%08d", node, base + i)));
                    }
                    return new PageOnPacked(node, packer.pack(page));
                });
            }
        }
        List<PageOnPacked> packed = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<PageOnPacked>> futures = pool.invokeAll(tasks);
            for (Future<PageOnPacked> f : futures) {
                packed.add(f.get());
            }
        } finally {
            pool.shutdownNow();
        }

        // Each packed page decodes to exactly its perPage objects (no cross-thread corruption).
        for (PageOnPacked p : packed) {
            assertThat(p.packed().entryCount()).isEqualTo(perPage);
            assertThat(p.packed().objectCount()).isEqualTo(perPage);
        }

        // Feed the concurrently-packed pages into a single lane (admit is single-caller) → sorted segments.
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        try (SortLane lane = new SortLane(gate3(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                new SortLaneMeters() { }, staging, "seg-conc", finalized::add)) {
            for (PageOnPacked p : packed) {
                lane.admit(p.node(), p.packed());
            }
        }

        List<String> all = new ArrayList<>();
        for (SegmentResult r : finalized) {
            List<String> segKeys = keys(r.path());
            assertThat(segKeys).isSorted();   // each staging segment is internally sorted per page-run merge
            all.addAll(segKeys);
        }
        assertThat(all).hasSize(nodes * pagesPerNode * perPage);
        assertThat(all).doesNotHaveDuplicates();
    }

    @Test
    void packedSummaryMatchesEntryTallies() {
        SortPagePacker packer = new SortPagePacker(cmp, gate3());
        List<ListEntry> page = new ArrayList<>();
        page.add(new ObjectEntry(KeyBytes.ofUtf8("a"), 10L, 0L, null, null, null, false, null, null, null, null));
        page.add(new ObjectEntry(KeyBytes.ofUtf8("b"), 25L, 0L, null, null, null, false, null, null, null, null));
        page.add(new CommonPrefixEntry(KeyBytes.ofUtf8("c/")));
        page.add(new DeleteMarkerEntry(KeyBytes.ofUtf8("d"), "v1", true, 0L, null));

        PackedPage packed = packer.pack(page);

        assertThat(packed.entryCount()).isEqualTo(4);
        assertThat(packed.objectCount()).isEqualTo(2);
        assertThat(packed.commonPrefixCount()).isEqualTo(1);
        assertThat(packed.deleteMarkerCount()).isEqualTo(1);
        assertThat(packed.totalObjectSize()).isEqualTo(35L);
    }

    private static List<ListEntry> page(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(SortTestSupport.object(k));
        }
        return out;
    }

    private List<String> keys(Path segment) throws IOException {
        List<String> out = new ArrayList<>();
        try (PageRunSegmentReader r = new PageRunSegmentReader(segment)) {
            while (r.hasNext()) {
                out.add(r.next().key().asString());
            }
        }
        return out;
    }

    private record PageOnNode(long node, List<ListEntry> page) { }

    private record PageOnPacked(long node, PackedPage packed) { }
}
