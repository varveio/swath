/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Type-2 page-index format, bounded parser, and non-retaining planning cursor. */
class PageRunPageIndexTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void listingIndexRoundTripsEverySampledFieldAndCursorRetainsNoEntryList(@TempDir Path dir)
            throws IOException {
        Path segment = writePages(dir.resolve("indexed.pageseg"), 5);
        List<byte[]> minima = new ArrayList<>();

        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
            PageRunPageIndex.ReadResult result = PageRunPageIndex.read(io, trailer, minima::add);

            assertThat(result.status()).isEqualTo(PageRunPageIndex.Status.EMBEDDED);
            assertThat(result.extensionType()).isEqualTo(PageRunPageIndex.TYPE);
            assertThat(result.entryCount()).isEqualTo(5);
            assertThat(result.firstOffset()).isEqualTo(PageRunSegmentWriter.HEADER_BYTES);
            assertThat(result.lastOffset()).isGreaterThan(result.firstOffset());
            assertThat(result.locator()).isNotNull();
            assertThat(minima).extracting(bytes -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
                    .containsExactly("k00000", "k00001", "k00002", "k00003", "k00004");

            PageRunPageIndex.Cursor cursor = PageRunPageIndex.cursor(io, result);
            long previousOffset = -1;
            long previousPayloadOffset = -1;
            int ordinal = 0;
            while (cursor.hasNext()) {
                PageRunPageIndex.LocatedEntry located = cursor.next();
                PageRunPageIndex.IndexEntry entry = located.entry();
                assertThat(located.payloadOffset()).isGreaterThan(previousPayloadOffset);
                assertThat(entry.pageOrdinal()).isEqualTo(ordinal);
                assertThat(entry.cumulativeEntries()).isEqualTo(ordinal);
                assertThat(entry.cumulativeFramedBytes())
                        .isEqualTo(entry.fileOffset() - PageRunSegmentWriter.HEADER_BYTES);
                assertThat(entry.fileOffset()).isGreaterThan(previousOffset);
                assertThat(entry.prefixMax()).containsExactly(entry.minKey());
                previousOffset = entry.fileOffset();
                previousPayloadOffset = located.payloadOffset();
                ordinal++;
            }
            assertThat(ordinal).isEqualTo(5);
        }
    }

    @Test
    void emittedType2BytesAndSegmentMetadataAgree(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        buffer.admit(1, List.of(object("a")));
        Path segment = dir.resolve("typed.pageseg");
        SegmentResult result = new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                PageCodec.NONE).flush(buffer.seal(SealTrigger.DRAIN), segment);

        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunPageIndex.ReadResult index = PageRunPageIndex.read(
                    io, PageRunTrailer.read(io), ignored -> { });
            assertThat(index.extensionType()).isEqualTo((short) PageRunFormat.PAGE_INDEX_EXTENSION);
            assertThat(result.pageRunFormat()).isEqualTo(PageRunFormat.currentListing());
            assertThat(result.pageRunFormat().extensionType()).isEqualTo(index.extensionType());
        }
    }

    @Test
    void serialPreflightReadsOnlyTheFixedHeaderOfAWorstShapeType3Index(@TempDir Path dir)
            throws IOException {
        Path segment = writeLongKeyPages(dir.resolve("long-index.pageseg"),
                PageRunBoundarySample.MAX_ENTRIES);

        PageRunSegmentDescriptor serial = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty())
                .descriptors().getFirst();
        long fixedTailStart = serial.fileSize()
                - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long extensionBytes = fixedTailStart - serial.trailer().extensionStart();

        assertThat(extensionBytes).isGreaterThan(8L << 20);
        assertThat(serial.extension().status()).isEqualTo(PageRunPageIndex.Status.SKIPPED);
        assertThat(serial.extension().bytesRead())
                .isEqualTo(PageRunBoundarySample.HEADER_BYTES);
        assertThat(serial.hasDecodedPageMaximum()).isFalse();

        PageRunSegmentDescriptor parallel = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { })).descriptors().getFirst();
        assertThat(parallel.extension().status()).isEqualTo(PageRunPageIndex.Status.EMBEDDED);
        assertThat(parallel.extension().bytesRead()).isGreaterThan(extensionBytes);
        assertThat(parallel.hasDecodedPageMaximum()).isTrue();
    }

    @Test
    void checkpointFormatMustMatchPhysicalExtensionAndHeaderVersion(@TempDir Path dir)
            throws IOException {
        Path segment = writePages(dir.resolve("format.pageseg"), 3);
        PageRunFormat physical = PageRunFormat.currentListing();
        SortTestSupport.CountingMetrics extensionMetrics =
                new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, extensionMetrics), Optional.empty(),
                java.util.Map.of(segment, new PageRunFormat(
                        PageRunFormat.CURRENT_FORMAT_VERSION,
                        PageRunFormat.LEGACY_PAGE_INDEX_EXTENSION)), extensionMetrics))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_format_mismatch");
        assertThat(extensionMetrics.count("SORT.page_run_format_mismatch")).isEqualTo(1);

        PageRunCatalog legacyUnrecorded = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty());
        assertThat(legacyUnrecorded.descriptors().getFirst().physicalFormat())
                .isEqualTo(physical);

        for (int extensionType : List.of(
                PageRunFormat.LEGACY_PAGE_INDEX_EXTENSION, 99)) {
            Path changedExtension = writePages(
                    dir.resolve("physical-extension-" + extensionType + ".pageseg"), 3);
            byte[] changed = Files.readAllBytes(changedExtension);
            ByteBuffer.wrap(changed).putShort(
                    layout(changed).extensionStart + Integer.BYTES, (short) extensionType);
            Files.write(changedExtension, changed);
            assertThatThrownBy(() -> PageRunCatalog.preflight(List.of(changedExtension),
                    path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty(),
                    java.util.Map.of(changedExtension, physical)))
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_format_mismatch");
        }

        byte[] bytes = Files.readAllBytes(segment);
        ByteBuffer.wrap(bytes).putShort(Integer.BYTES, (short) 1);
        Files.write(segment, bytes);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        assertThatThrownBy(() -> PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, metrics), Optional.empty(),
                java.util.Map.of(segment, physical), metrics))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_format_mismatch")
                .hasMessageContaining("unsupported page-run format version 1");
        assertThat(metrics.count("SORT.page_run_format_mismatch")).isEqualTo(1);
    }

    @Test
    void structurallyInvalidType2FieldsAreRejectedTransactionally(@TempDir Path dir)
            throws IOException {
        for (Mutation mutation : Mutation.values()) {
            Path segment = writePages(dir.resolve(mutation + ".pageseg"), 3);
            mutate(segment, mutation);
            List<byte[]> minima = new ArrayList<>();

            PageRunPageIndex.ReadResult result;
            try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
                result = PageRunPageIndex.read(io, PageRunTrailer.read(io), minima::add);
            }

            assertThat(result.status()).as(mutation.name()).isEqualTo(mutation.expected);
            assertThat(result.locator()).as(mutation.name()).isNull();
            assertThat(minima).as("no provisional minima from %s", mutation).isEmpty();
        }
    }

    @Test
    void cumulativeEntriesMustStrictlyIncreaseAfterTheFirstSample(@TempDir Path dir)
            throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        for (int page = 0; page < 3; page++) {
            buffer.admit(page, List.of(object("k" + page + "a"), object("k" + page + "b")));
        }
        Path segment = dir.resolve("equal-cumulative.pageseg");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), segment);
        byte[] bytes = Files.readAllBytes(segment);
        Layout layout = layout(bytes);
        int secondEntry = entryPosition(bytes, layout, 1);
        int thirdEntry = entryPosition(bytes, layout, 2);
        long secondCumulative = ByteBuffer.wrap(bytes).getLong(secondEntry + 16);
        assertThat(secondCumulative).isEqualTo(2L);
        assertThat(ByteBuffer.wrap(bytes).getLong(thirdEntry)).isEqualTo(2L);
        ByteBuffer.wrap(bytes).putLong(thirdEntry + 16, secondCumulative);
        rewriteExtensionCrc(bytes, layout);
        Files.write(segment, bytes);

        assertThat(readIndex(segment).status()).isEqualTo(PageRunPageIndex.Status.INVALID_CUMULATIVE);
    }

    @Test
    void cumulativeEntriesCannotFallBelowSampledPageOrdinal(@TempDir Path dir)
            throws IOException {
        Path segment = writePages(dir.resolve("below-ordinal.pageseg"), 4_097);
        byte[] bytes = Files.readAllBytes(segment);
        Layout layout = layout(bytes);
        int secondEntry = entryPosition(bytes, layout, 1);
        assertThat(ByteBuffer.wrap(bytes).getLong(secondEntry)).isEqualTo(2L); // stride=2
        ByteBuffer.wrap(bytes).putLong(secondEntry + 16, 1L); // increasing from zero, but < ordinal
        rewriteExtensionCrc(bytes, layout);
        Files.write(segment, bytes);

        assertThat(readIndex(segment).status()).isEqualTo(PageRunPageIndex.Status.INVALID_CUMULATIVE);
    }

    @Test
    void badCrcIsRejectedBeforeAnOversizedKeyCanBeAllocated(@TempDir Path dir) throws IOException {
        Path segment = writePages(dir.resolve("oversized-bad-crc.pageseg"), 32);
        byte[] bytes = Files.readAllBytes(segment);
        Layout layout = layout(bytes);
        int firstEntry = entryPosition(bytes, layout, 0);
        ByteBuffer.wrap(bytes).putShort(firstEntry + 32, (short) 1_025);
        Files.write(segment, bytes); // deliberately leave the old CRC
        List<byte[]> minima = new ArrayList<>();

        PageRunPageIndex.ReadResult result = readIndex(segment, minima);

        assertThat(result.status()).isEqualTo(PageRunPageIndex.Status.INVALID_CRC);
        assertThat(minima).isEmpty();
    }

    @Test
    void crcValidOversizedIndexKeyIsRejectedByTheS3Ceiling(@TempDir Path dir) throws IOException {
        Path segment = writePages(dir.resolve("oversized-valid-crc.pageseg"), 32);
        byte[] bytes = Files.readAllBytes(segment);
        Layout layout = layout(bytes);
        int firstEntry = entryPosition(bytes, layout, 0);
        ByteBuffer.wrap(bytes).putShort(firstEntry + 32, (short) 1_025);
        rewriteExtensionCrc(bytes, layout);
        Files.write(segment, bytes);

        assertThat(readIndex(segment).status()).isEqualTo(PageRunPageIndex.Status.INVALID_LENGTH);
    }

    @Test
    void type2OffsetAndCumulativeFallbacksKeepDistinctRuntimeReasons(@TempDir Path dir)
            throws IOException {
        assertFallbackReason(dir.resolve("offset.pageseg"), Mutation.OFFSET,
                "SORT.merge_boundary_fallback_invalid_offset");
        assertFallbackReason(dir.resolve("cumulative.pageseg"), Mutation.CUMULATIVE,
                "SORT.merge_boundary_fallback_invalid_cumulative");
    }

    @Test
    void prefixMaximumAndCumulativeEntriesCoverDisjointPages(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        buffer.admit(1, List.of(object("a"), object("b")));
        buffer.admit(2, List.of(object("c"), object("d")));
        buffer.admit(3, List.of(object("e"), object("f")));
        Path segment = dir.resolve("disjoint.pageseg");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), segment);

        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunPageIndex.ReadResult result = PageRunPageIndex.read(
                    io, PageRunTrailer.read(io), ignored -> { });
            PageRunPageIndex.Cursor cursor = PageRunPageIndex.cursor(io, result);
            List<Long> cumulativeEntries = new ArrayList<>();
            List<String> prefixMaxima = new ArrayList<>();
            while (cursor.hasNext()) {
                PageRunPageIndex.IndexEntry entry = cursor.next().entry();
                cumulativeEntries.add(entry.cumulativeEntries());
                prefixMaxima.add(new String(entry.prefixMax(),
                        java.nio.charset.StandardCharsets.UTF_8));
            }
            assertThat(cumulativeEntries).containsExactly(0L, 2L, 4L);
            assertThat(prefixMaxima).containsExactly("b", "d", "f");
        }
    }

    @Test
    void emptyListingCarriesAValidEmptyType2Index(@TempDir Path dir) throws IOException {
        Path segment = writePages(dir.resolve("empty.pageseg"), 0);

        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunPageIndex.ReadResult result = PageRunPageIndex.read(
                    io, PageRunTrailer.read(io), ignored -> { });
            assertThat(result.status()).isEqualTo(PageRunPageIndex.Status.EMBEDDED);
            assertThat(result.entryCount()).isZero();
            assertThat(result.firstOffset()).isEqualTo(-1);
            assertThat(result.lastOffset()).isEqualTo(-1);
            assertThat(result.locator()).isNotNull();
        }
    }

    private static Path writePages(Path path, int pages) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        for (int i = 0; i < pages; i++) {
            buffer.admit(i, List.of(object(String.format("k%05d", i))));
        }
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static Path writeLongKeyPages(Path path, int pages) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        for (int page = 0; page < pages; page++) {
            byte[] key = new byte[io.varve.swath.model.ByteMidpoint.MAX_KEY_LEN];
            key[0] = (byte) (page >>> 8);
            key[1] = (byte) page;
            buffer.admit(page, List.of(new CommonPrefixEntry(KeyBytes.of(key))));
        }
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static void mutate(Path path, Mutation mutation) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        Layout layout = layout(bytes);
        int firstEntry = layout.extensionStart + PageRunBoundarySample.HEADER_BYTES;
        boolean rewriteCrc = true;
        switch (mutation) {
            case LENGTH -> {
                ByteBuffer.wrap(bytes).putInt(layout.extensionStart + 8,
                        ByteBuffer.wrap(bytes).getInt(layout.extensionStart + 8) + 1);
                rewriteCrc = false;
            }
            case COUNT -> {
                ByteBuffer.wrap(bytes).putInt(layout.extensionStart + 12,
                        ByteBuffer.wrap(bytes).getInt(layout.extensionStart + 12) + 1);
                rewriteCrc = false;
            }
            case ORDINAL -> ByteBuffer.wrap(bytes).putLong(firstEntry, 1);
            case OFFSET -> {
                ByteBuffer.wrap(bytes).putLong(firstEntry + 8, PageRunSegmentWriter.HEADER_BYTES + 1L);
                ByteBuffer.wrap(bytes).putLong(firstEntry + 24, 1L);
            }
            case CUMULATIVE -> ByteBuffer.wrap(bytes).putLong(firstEntry + 16, 1L);
            case CUMULATIVE_BYTES -> ByteBuffer.wrap(bytes).putLong(firstEntry + 24, 1L);
            case BOUNDS -> {
                int minLength = ByteBuffer.wrap(bytes).getShort(firstEntry + 32) & 0xFFFF;
                int prefixLengthPosition = firstEntry + 34 + minLength;
                int prefixLength = ByteBuffer.wrap(bytes).getShort(prefixLengthPosition) & 0xFFFF;
                assertThat(prefixLength).isPositive();
                bytes[prefixLengthPosition + 2] = 'j';
            }
            case CRC -> {
                bytes[layout.fixedTailStart - 1] ^= 0x7F;
                rewriteCrc = false;
            }
        }
        if (rewriteCrc) {
            rewriteExtensionCrc(bytes, layout);
        }
        Files.write(path, bytes);
    }

    private static PageRunPageIndex.ReadResult readIndex(Path segment) throws IOException {
        return readIndex(segment, new ArrayList<>());
    }

    private static PageRunPageIndex.ReadResult readIndex(Path segment, List<byte[]> minima)
            throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            return PageRunPageIndex.read(io, PageRunTrailer.read(io), minima::add);
        }
    }

    private static void assertFallbackReason(Path segment, Mutation mutation, String reason)
            throws IOException {
        writePages(segment, 3);
        mutate(segment, mutation);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                List.of(segment), path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();

        MergePlanner.boundaries(descriptors, candidates, 2, metrics);

        assertThat(metrics.count(reason)).isEqualTo(1);
    }

    private static int entryPosition(byte[] bytes, Layout layout, int target) {
        int position = layout.extensionStart + PageRunBoundarySample.HEADER_BYTES;
        for (int i = 0; i < target; i++) {
            position += 32;
            int minLength = ByteBuffer.wrap(bytes).getShort(position) & 0xFFFF;
            position += 2 + minLength;
            int prefixLength = ByteBuffer.wrap(bytes).getShort(position) & 0xFFFF;
            position += 2 + prefixLength;
        }
        return position;
    }

    private static void rewriteExtensionCrc(byte[] bytes, Layout layout) {
        int crcPosition = layout.fixedTailStart - PageRunBoundarySample.CRC_BYTES;
        CRC32C crc = new CRC32C();
        crc.update(bytes, layout.extensionStart, crcPosition - layout.extensionStart);
        ByteBuffer.wrap(bytes).putInt(crcPosition, (int) crc.getValue());
    }

    private static Layout layout(byte[] bytes) {
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int trailerStart = Math.toIntExact(ByteBuffer.wrap(bytes).getLong(fixedTailStart));
        int position = trailerStart;
        int minLength = ByteBuffer.wrap(bytes).getShort(position) & 0xFFFF;
        position += 2 + minLength;
        int maxLength = ByteBuffer.wrap(bytes).getShort(position) & 0xFFFF;
        position += 2 + maxLength;
        return new Layout(position, fixedTailStart);
    }

    private enum Mutation {
        LENGTH(PageRunPageIndex.Status.INVALID_LENGTH),
        COUNT(PageRunPageIndex.Status.INVALID_COUNT),
        ORDINAL(PageRunPageIndex.Status.INVALID_COUNT),
        OFFSET(PageRunPageIndex.Status.INVALID_OFFSET),
        CUMULATIVE(PageRunPageIndex.Status.INVALID_CUMULATIVE),
        CUMULATIVE_BYTES(PageRunPageIndex.Status.INVALID_CUMULATIVE),
        BOUNDS(PageRunPageIndex.Status.INVALID_BOUNDS),
        CRC(PageRunPageIndex.Status.INVALID_CRC);

        private final PageRunPageIndex.Status expected;

        Mutation(PageRunPageIndex.Status expected) {
            this.expected = expected;
        }
    }

    private record Layout(int extensionStart, int fixedTailStart) {
    }
}
