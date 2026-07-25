/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit coverage for the ordered {@link SortLane}: a small entry gate seals multiple segments, each
 * finalized in seal order via {@link SegmentSink}, and every admitted entry round-trips through the
 * staging segments. The strict-seal-order / durable_cursor-correctness properties are exercised by
 * the resume adversarial suite; this pins the lane's own mechanics + the meters seam.
 */
final class SortLaneTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** segment-entries=3 ⇒ a fresh segment roughly every 3 admitted entries. */
    private static SortConfig gate3() {
        return SortConfigs.base().withSegmentEntries(3);
    }

    @Test
    void sealsMultipleSegmentsInOrderAndRoundTripsEveryEntry(@TempDir Path tmp) throws Exception {
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        AtomicLong entries = new AtomicLong();
        AtomicLong segments = new AtomicLong();

        SortLaneMeters meters = new SortLaneMeters() {
            @Override
            public void entriesAccepted(long n) {
                entries.addAndGet(n);
            }

            @Override
            public void segmentFinalized(long bytes, int pageRuns) {
                segments.incrementAndGet();
            }
        };

        SortLane lane = new SortLane(gate3(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, meters,
                staging, "seg-test", finalized::add);

        // node 1 ascending a..f, node 2 ascending g..j — disjoint ranges (the work-stealing invariant).
        lane.admit(1L, page("a", "b", "c"));
        lane.admit(1L, page("d", "e", "f"));
        lane.admit(2L, page("g", "h", "i", "j"));
        lane.close();

        // Every admitted entry landed in a durable segment, and each segment is internally sorted.
        List<String> all = new ArrayList<>();
        for (SegmentResult r : finalized) {
            assertThat(Files.exists(r.path())).isTrue();
            List<String> segKeys = keys(r.path());
            assertThat(segKeys).isSorted();
            all.addAll(segKeys);
        }
        assertThat(all).containsExactlyInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        assertThat(finalized.size()).as("the small gate sealed more than one segment").isGreaterThan(1);
        assertThat(entries.get()).isEqualTo(10);
        assertThat(segments.get()).isEqualTo(finalized.size());

        // Per-node max keys are captured at ingest (the durable_cursor seam).
        boolean sawNode1 = finalized.stream().anyMatch(r -> r.perNodeMaxKeys().containsKey(1L));
        boolean sawNode2 = finalized.stream().anyMatch(r -> r.perNodeMaxKeys().containsKey(2L));
        assertThat(sawNode1 && sawNode2).isTrue();
    }

    @Test
    void wiresTheConfiguredSegmentCodecIntoAnOutOfOrderReSortedPage(@TempDir Path tmp) throws Exception {
        // SortLane must thread SortConfig#segmentCodec() into its PageRunSegmentWriter so an
        // out-of-order page's re-pack (at flush time) honors it too, not just the page's original
        // admission-time pack.
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        SortConfig zstdConfig = SortConfigs.base().withSegmentCodec(PageCodec.ZSTD1);

        SortLane lane = new SortLane(zstdConfig, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, NO_OP_METERS,
                staging, "seg-test", finalized::add);
        lane.admit(1L, page("d", "b", "a", "c"));   // out of order: forces the re-pack at flush
        lane.close();

        assertThat(finalized).hasSize(1);
        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(finalized.get(0).path());
        assertThat(dump.records()).hasSize(1);
        assertThat(dump.records().get(0).codec()).isEqualTo("ZSTD1");
    }

    private static final SortLaneMeters NO_OP_METERS = new SortLaneMeters() {
        @Override
        public void entriesAccepted(long n) {
        }

        @Override
        public void segmentFinalized(long bytes, int pageRuns) {
        }
    };

    private static List<ListEntry> page(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(SortTestSupport.object(k));
        }
        return out;
    }

    private List<String> keys(Path segment) throws IOException {
        // The lane stages page-run (.pageseg) segments; read them back with the page-run reader.
        List<String> out = new ArrayList<>();
        try (PageRunSegmentReader r = new PageRunSegmentReader(segment)) {
            while (r.hasNext()) {
                out.add(r.next().key().asString());
            }
        }
        return out;
    }
}
