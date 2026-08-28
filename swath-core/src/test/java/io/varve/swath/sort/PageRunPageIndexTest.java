/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    void prefixMaximumAndCumulativeEntriesCoverNestedPages(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfigs.base(), CMP);
        buffer.admit(1, List.of(object("a"), object("z")));
        buffer.admit(2, List.of(object("b"), object("c")));
        buffer.admit(3, List.of(object("d"), object("e")));
        Path segment = dir.resolve("nested.pageseg");
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
            assertThat(prefixMaxima).containsExactly("z", "z", "z");
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
