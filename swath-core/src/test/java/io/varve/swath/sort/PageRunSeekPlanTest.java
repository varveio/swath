/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunSeekPlan plan = PageRunSeekPlan.plan(prepared.descriptors(),
                List.of(bytes("k00004")), metrics);
        PageRunSeekPlan.SegmentPlan segment = plan.segment(path);

        assertThat(segment.start(0).pageOrdinal()).isZero();
        assertThat(segment.start(1).pageOrdinal()).isEqualTo(3);
        assertThat(segment.zone(0).end().pageOrdinal()).isEqualTo(3);
        assertThat(segment.zone(1).end().pageOrdinal()).isEqualTo(8);
        assertThat(java.util.Arrays.stream(PageRunSeekPlan.SegmentPlan.class.getDeclaredFields())
                .map(java.lang.reflect.Field::getType))
                .allMatch(type -> type.isPrimitive() || type.isArray()
                        || type == PageRunSegmentDescriptor.class);
        assertThat(metrics.rangeIndexBytes.sum()).isEqualTo(8L * 48L);

        try (PageFrontierReader frontier = new PageFrontierReader(
                path, SortMetrics.NO_OP, segment, 1)) {
            assertThat(frontier.currentPageOrdinal()).isEqualTo(3);
            assertThat(frontier.currentFrameOffset())
                    .isEqualTo(segment.start(1).frameOffset());
            assertThat(frontier.minKey()).containsExactly(bytes("k00003"));
        }
    }

    @Test
    void serialFrontierAllocatesNoPagePositionAndTracksNoProofOrIndexBytes(@TempDir Path dir)
            throws IOException {
        Path path = SortTestSupport.writeIndexedPages(dir.resolve("serial.pageseg"), 8, 0);
        long trailerStart;
        try (PageRunSegmentIo metadata = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            trailerStart = metadata.trailerStart;
        }

        assertThat(java.util.Arrays.stream(PageRunSegmentIo.class.getDeclaredClasses())
                .map(Class::getSimpleName))
                .doesNotContain("PagePosition");
        try (PageFrontierReader frontier = new PageFrontierReader(path, SortMetrics.NO_OP)) {
            while (frontier.hasPage()) {
                frontier.advance();
            }
            assertThat(frontier.proofTracking()).isFalse();
            assertThat(frontier.framedBytesRead())
                    .isEqualTo(trailerStart - PageRunSegmentWriter.HEADER_BYTES);
            assertThat(frontier.indexBytesRead()).isZero();
            assertThat(frontier.nextFrameOffset()).isEqualTo(trailerStart);
        }
    }

    @Test
    void proofTopologyRetainsPrimitiveTablesAndOnlyBoundedExactKeyCachesPerRange() {
        assertThat(java.util.Arrays.stream(PageRunZoneVerifier.Tracker.class.getDeclaredFields())
                .map(java.lang.reflect.Field::getType))
                .noneMatch(type -> type == byte[].class);
        assertThat(java.util.Arrays.stream(PageRunZoneVerifier.RangeSummary.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getType))
                .doesNotContain(byte[].class, Object[].class);
        assertThat(java.util.Arrays.stream(PageRunZoneVerifier.RangeBuilder.class.getDeclaredFields())
                .filter(field -> field.getType().getSimpleName().equals("KeyCache")))
                .hasSize(3);
        assertThat(java.util.Arrays.stream(PageRunZoneVerifier.RangeBuilder.class.getDeclaredClasses())
                .filter(type -> type.getSimpleName().equals("KeyCache"))
                .flatMap(type -> java.util.Arrays.stream(type.getDeclaredFields()))
                .filter(field -> field.getType() == byte[].class))
                .hasSize(1);
        assertThat(PageRunProofSpool.logicalBytes(10_000 * 16)).isEqualTo(993_920_000L);
        int beyondSingleBuffer = Math.toIntExact(
                Math.floorDiv(Integer.MAX_VALUE, PageRunProofSpool.slotBytes()) + 1);
        assertThat(PageRunProofSpool.logicalBytes(beyondSingleBuffer))
                .isGreaterThan(Integer.MAX_VALUE);
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
    void planningRejectsSameSizeInPlaceIndexKeyRewriteWithTypedMismatch(@TempDir Path dir)
            throws IOException {
        Path path = SortTestSupport.writeIndexedPages(dir.resolve("rewritten.pageseg"), 4, 0);
        Prepared prepared = prepared(path);
        PageRunSegmentDescriptor descriptor = prepared.descriptors().getFirst();
        long originalSize = Files.size(path);
        long firstMinLength = descriptor.extension().locator().payloadStart() + 4L * Long.BYTES;
        ByteBuffer overlong = ByteBuffer.allocate(Short.BYTES).putShort((short) 0xffff).flip();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            while (overlong.hasRemaining()) {
                firstMinLength += channel.write(overlong, firstMinLength);
            }
        }

        assertThat(Files.size(path)).isEqualTo(originalSize);
        assertThatThrownBy(() -> PageRunSeekPlan.plan(
                prepared.descriptors(), List.of(bytes("k00002")), SortMetrics.NO_OP))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("page-index cursor entry key exceeds the key limit")
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH);
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
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                paths, candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();
        return new Prepared(descriptors, candidates);
    }

    private static byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private record Prepared(List<PageRunSegmentDescriptor> descriptors,
                            MergePlanner.BoundaryCandidates candidates) {
    }
}
