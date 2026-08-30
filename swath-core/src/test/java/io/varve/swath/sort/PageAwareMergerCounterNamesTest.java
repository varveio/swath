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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Pins the exact engagement literals selected by each {@link MergeScope}. */
class PageAwareMergerCounterNamesTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private int seq;

    @Test
    void theScopeSwitchEmitsRouteSpecificReasonsAndTheOverlapClusterReason(@TempDir Path dir)
            throws IOException {
        // Three pages that force BOTH paths in one run: cross-segment [a..c] and [b..d] overlap,
        // while [x..z] is the disjoint successor of [a..c]. Each persisted segment remains disjoint.
        Path first = dir.resolve("first.pageseg");
        PageRunRawFixtures.writeRawPageRun(
                first,
                List.of(
                        List.of(entry("a"), entry("c")),
                        List.of(entry("x"), entry("z"))),
                cmp);
        Path second = dir.resolve("second.pageseg");
        PageRunRawFixtures.writeRawPageRun(
                second, List.of(List.of(entry("b"), entry("d"))), cmp);
        List<Path> segments = List.of(first, second);

        assertScope(segments, MergeScope.CROSS_SEGMENT,
                "page_whole_emitted", "page_overlap_keymerge");
        assertScope(segments, MergeScope.INTRA_SEGMENT,
                "page_run_entry_whole_page", "page_run_entry_overlap_keymerge");
    }

    private void assertScope(List<Path> segments, MergeScope scope, String wholeReason,
                             String overlapReason) throws IOException {
        List<String> reasons = new ArrayList<>();
        SortMetrics capturing = new SortMetrics() {
            @Override
            public void recordStealReason(String outcome, String reason) {
                assertThat(outcome).isEqualTo("SORT");
                reasons.add(reason);
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
        };

        List<String> merged = new ArrayList<>();
        List<PageFrontierStream> frontiers = new ArrayList<>();
        for (Path segment : segments) {
            frontiers.add(new PageFrontierReader(segment, capturing));
        }
        try (PageAwareMerger merger = new PageAwareMerger(frontiers, cmp, scope, capturing)) {
            while (merger.hasNext()) {
                merged.add(merger.next().key().asString());
            }
        }

        // Sanity: the fixture really did exercise the merger (and produced a sorted run).
        assertThat(merged).containsExactly("a", "b", "c", "d", "x", "z");

        assertThat(reasons)
                .containsOnly(wholeReason, overlapReason, "merge_overlap_cluster")
                .contains(wholeReason)
                .contains(overlapReason, "merge_overlap_cluster");
    }

    private ListEntry entry(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null,
                "v" + String.format("%08d", seq), false, null, null, null, null);
    }
}
