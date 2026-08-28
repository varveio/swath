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
        // Three pages that force BOTH paths in one run: [a..c] and [b..d] OVERLAP (key-merge fallback),
        // while [x..z] is disjoint from everything ahead of it (whole-page fast path). Page mins ascend
        // (a, b, x), so this is a perfectly legal segment — the min-regression guard stays silent.
        Path segment = dir.resolve("seg.pageseg");
        PageRunRawFixtures.writeRawPageRun(
                segment,
                List.of(
                        List.of(entry("a"), entry("c")),
                        List.of(entry("b"), entry("d")),
                        List.of(entry("x"), entry("z"))),
                cmp);

        assertScope(segment, MergeScope.CROSS_SEGMENT,
                "page_whole_emitted", "page_overlap_keymerge");
        assertScope(segment, MergeScope.INTRA_SEGMENT,
                "page_run_entry_whole_page", "page_run_entry_overlap_keymerge");
    }

    private void assertScope(Path segment, MergeScope scope, String wholeReason,
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
        };

        List<String> merged = new ArrayList<>();
        try (PageFrontierReader frontier = new PageFrontierReader(segment, capturing);
                PageAwareMerger merger =
                        new PageAwareMerger(List.of(frontier), cmp, scope, capturing)) {
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
