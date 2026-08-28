/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRunSeekPlanTest {

    @Test
    void planningRetainsOnlyPrimitiveRangeSeamsAndPositionsAtChosenPage(@TempDir Path dir)
            throws IOException {
        Path path = SortTestSupport.writeIndexedPages(dir.resolve("indexed.pageseg"), 8, 0);
        Prepared prepared = prepared(path);

        PageRunSeekPlan plan = PageRunSeekPlan.plan(prepared.descriptors(),
                List.of(bytes("k00004")), SortMetrics.NO_OP);
        PageRunSeekPlan.SegmentPlan segment = plan.segment(path);

        assertThat(segment.start(0).pageOrdinal()).isZero();
        assertThat(segment.start(1).pageOrdinal()).isEqualTo(3);
        assertThat(segment.zone(0).end().pageOrdinal()).isEqualTo(3);
        assertThat(segment.zone(1).end().pageOrdinal()).isEqualTo(8);
        assertThat(java.util.Arrays.stream(PageRunSeekPlan.SegmentPlan.class.getDeclaredFields())
                .map(java.lang.reflect.Field::getType))
                .allMatch(type -> type.isPrimitive() || type.isArray()
                        || type == PageRunSegmentDescriptor.class);

        try (PageFrontierReader frontier = new PageFrontierReader(
                path, SortMetrics.NO_OP, segment, 1)) {
            assertThat(frontier.currentPosition().pageOrdinal()).isEqualTo(3);
            assertThat(frontier.currentPosition().frameOffset())
                    .isEqualTo(segment.start(1).frameOffset());
            assertThat(frontier.minKey()).containsExactly(bytes("k00003"));
        }
    }

    @Test
    void nextPageRejectsWrongIndexedOffsetOrMinimumWithTypedMismatch(@TempDir Path dir)
            throws IOException {
        Path path = SortTestSupport.writeIndexedPages(dir.resolve("mismatch.pageseg"), 4, 0);
        Prepared prepared = prepared(path);
        PageRunSegmentDescriptor descriptor = prepared.descriptors().getFirst();
        List<PageRunPageIndex.IndexEntry> entries = new ArrayList<>();
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            PageRunPageIndex.Cursor cursor = PageRunPageIndex.cursor(io, descriptor.extension());
            while (cursor.hasNext()) {
                entries.add(cursor.next().entry());
            }
        }
        PageRunPageIndex.IndexEntry first = entries.getFirst();
        PageRunPageIndex.IndexEntry second = entries.get(1);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, metrics)) {
            io.seekToPage(new PageRunPageIndex.IndexEntry(first.pageOrdinal(),
                    second.fileOffset(), first.cumulativeEntries(),
                    second.cumulativeFramedBytes(), first.minKey(), first.prefixMax()));
            assertThatThrownBy(io::nextPage)
                    .isInstanceOf(SegmentCorruptionException.class)
                    .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                    .isEqualTo(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH);
        }
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isEqualTo(1);

        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, metrics)) {
            io.seekToPage(new PageRunPageIndex.IndexEntry(first.pageOrdinal(), first.fileOffset(),
                    first.cumulativeEntries(), first.cumulativeFramedBytes(),
                    bytes("not-the-page-min"), first.prefixMax()));
            assertThatThrownBy(io::nextPage)
                    .isInstanceOf(SegmentCorruptionException.class)
                    .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                    .isEqualTo(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH);
        }
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isEqualTo(2);
    }

    @Test
    void repeatedStartsAreExplicitEmptyZonesAndLegacyStartsStayAtTheHeader(@TempDir Path dir)
            throws IOException {
        Path indexed = SortTestSupport.writeIndexedPages(dir.resolve("indexed.pageseg"), 4, 0);
        Path legacy = SortTestSupport.writePageRun(dir.resolve("legacy.pageseg"), List.of(
                SortTestSupport.object("k00100"), SortTestSupport.object("k00101")),
                new ListEntryComparator());
        Prepared prepared = prepared(List.of(indexed, legacy));

        PageRunSeekPlan plan = PageRunSeekPlan.plan(prepared.descriptors(),
                List.of(bytes("k00000"), bytes("k00001")), SortMetrics.NO_OP);
        PageRunSeekPlan.SegmentPlan indexedPlan = plan.segment(indexed);
        PageRunSeekPlan.SegmentPlan legacyPlan = plan.segment(legacy);

        assertThat(indexedPlan.zone(0).empty()).isTrue();
        assertThat(indexedPlan.zone(1).empty()).isTrue();
        assertThat(indexedPlan.zone(2).start().pageOrdinal()).isZero();
        assertThat(indexedPlan.zone(2).end().pageOrdinal()).isEqualTo(4);
        for (int range = 0; range < 3; range++) {
            assertThat(legacyPlan.start(range).indexed()).isFalse();
            assertThat(legacyPlan.start(range).pageOrdinal()).isZero();
            assertThat(legacyPlan.start(range).frameOffset())
                    .isEqualTo(PageRunSegmentWriter.HEADER_BYTES);
        }
        assertThat(legacyPlan.zone(0).empty()).isTrue();
        assertThat(legacyPlan.zone(1).empty()).isTrue();
        assertThat(legacyPlan.zone(2).end().pageOrdinal()).isEqualTo(1);
    }

    private static Prepared prepared(Path path) throws IOException {
        return prepared(List.of(path));
    }

    private static Prepared prepared(List<Path> paths) throws IOException {
        ParallelRangeMerge.BoundaryCandidates candidates =
                new ParallelRangeMerge.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunSegmentDescriptor.readAll(
                paths, candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add));
        return new Prepared(descriptors, candidates);
    }

    private static byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private record Prepared(List<PageRunSegmentDescriptor> descriptors,
                            ParallelRangeMerge.BoundaryCandidates candidates) {
    }
}
