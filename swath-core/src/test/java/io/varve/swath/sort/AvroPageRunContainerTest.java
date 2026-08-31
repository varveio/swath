/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AvroPageRunContainerTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void roundTripsPagesAndRequiresExactSeal(@TempDir Path dir) throws IOException {
        List<PageBlock> pages = pages(3, 4, PageCodec.LZ4);
        Path path = dir.resolve("roundtrip.avro");
        writeAvro(path, pages, PageCodec.LZ4);

        List<ListEntry> decoded = new ArrayList<>();
        try (AvroPageRunContainer.Reader reader = AvroPageRunContainer.openReader(path)) {
            PageBlock page;
            while ((page = reader.nextPage()) != null) {
                PageBlockCursor cursor = page.cursor();
                while (cursor.hasNext()) {
                    decoded.add(cursor.next());
                }
            }
        }

        assertThat(decoded).hasSize(12);
        assertThat(decoded.getFirst().key().asString()).isEqualTo(key(0));
        assertThat(decoded.getLast().key().asString()).isEqualTo(key(11));
        assertThat(AvroPageRunContainer.scanHeaders(path))
                .isEqualTo(new AvroPageRunContainer.HeaderSummary(3, 12));
        assertThat(AvroPageRunContainer.inspect(path))
                .contains("avro.codec=null", "kind=SEAL", "blocks=4 records=4");
    }

    @Test
    void bothArmsRejectMidHeaderMidBlockAndBlockBoundaryTruncation(@TempDir Path dir)
            throws IOException {
        List<PageBlock> pages = pages(3, 5, PageCodec.NONE);
        Path custom = dir.resolve("complete.pageseg");
        writeCustom(custom, pages);
        Path avro = dir.resolve("complete.avro");
        AvroPageRunContainer.WriteResult avroLayout = writeAvro(avro, pages, PageCodec.NONE);

        byte[] customBytes = Files.readAllBytes(custom);
        int firstCustomLength = ByteBuffer.wrap(customBytes, PageRunSegmentWriter.HEADER_BYTES, 4)
                .getInt();
        int customBoundary = PageRunSegmentWriter.HEADER_BYTES + 8 + firstCustomLength;
        byte[] avroBytes = Files.readAllBytes(avro);
        long avroBoundary = avroLayout.pageBoundaries().getFirst();

        Path customMidHeader = writePrefix(dir.resolve("custom-mid-header"), customBytes,
                PageRunSegmentWriter.HEADER_BYTES / 2);
        Path avroMidHeader = writePrefix(dir.resolve("avro-mid-header"), avroBytes,
                avroLayout.headerEnd() / 2);
        assertThatThrownBy(() -> PageRunSegmentIo.open(customMidHeader, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> AvroPageRunContainer.openReader(avroMidHeader))
                .isInstanceOf(IOException.class);

        Path customMidBlock = writePrefix(dir.resolve("custom-mid-block"), customBytes,
                PageRunSegmentWriter.HEADER_BYTES + 8L + firstCustomLength / 2L);
        Path avroMidBlock = writePrefix(dir.resolve("avro-mid-block"), avroBytes,
                avroLayout.headerEnd() + (avroBoundary - avroLayout.headerEnd()) / 2);
        assertThatThrownBy(() -> PageRunSegmentIo.open(customMidBlock, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> AvroPageRunContainer.scanHeaders(avroMidBlock))
                .isInstanceOf(IOException.class);

        Path customAtBoundary = writePrefix(dir.resolve("custom-at-boundary"), customBytes,
                customBoundary);
        Path avroAtBoundary = writePrefix(dir.resolve("avro-at-boundary"), avroBytes,
                avroBoundary);
        assertThatThrownBy(() -> PageRunSegmentIo.open(customAtBoundary, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> AvroPageRunContainer.scanHeaders(avroAtBoundary))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing final SEAL");
    }

    @Test
    void avroSyncMarkerRecoversAlignmentAfterDamagedBlock(@TempDir Path dir) throws IOException {
        List<PageBlock> pages = pages(3, 3, PageCodec.NONE);
        byte[] firstBody = pages.getFirst().serialize();
        Path path = dir.resolve("resync.avro");
        writeAvro(path, pages, PageCodec.NONE);
        byte[] bytes = Files.readAllBytes(path);
        int bodyOffset = indexOf(bytes, firstBody);
        assertThat(bodyOffset).isPositive();
        bytes[bodyOffset + 4] ^= 0x40;
        Files.write(path, bytes);

        assertThatThrownBy(() -> drainAvro(path)).isInstanceOf(IOException.class);

        AvroPageRunContainer.ResyncResult recovered =
                AvroPageRunContainer.resync(path, bodyOffset + firstBody.length / 2L);
        assertThat(recovered.kind()).isEqualTo("PAGE");
        assertThat(recovered.minKey()).containsExactly(pages.get(1).firstKey());
    }

    @Test
    void syncOnlyAvroSilentlyAcceptsLz4PayloadBitFlipThatCustomCrcRejects(@TempDir Path dir)
            throws IOException {
        List<PageBlock> pages = pages(1, 20, PageCodec.LZ4);
        PageBlock page = pages.getFirst();
        byte[] body = page.serialize();
        PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
        List<ListEntry> original = drainPage(page);
        SilentMutation mutation = findSilentMutation(body, header);

        Path custom = dir.resolve("bitflip.pageseg");
        writeCustom(custom, pages);
        byte[] customBytes = Files.readAllBytes(custom);
        customBytes[PageRunSegmentWriter.HEADER_BYTES + 8 + mutation.offset()] ^=
                mutation.mask();
        Files.write(custom, customBytes);
        try (PageRunSegmentIo io = PageRunSegmentIo.open(custom, SortMetrics.NO_OP)) {
            assertThatThrownBy(io::nextPage)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CRC32C mismatch");
        }

        Path avro = dir.resolve("bitflip.avro");
        writeAvro(avro, pages, PageCodec.LZ4);
        byte[] avroBytes = Files.readAllBytes(avro);
        int avroBodyOffset = indexOf(avroBytes, body);
        assertThat(avroBodyOffset).isPositive();
        avroBytes[avroBodyOffset + mutation.offset()] ^= mutation.mask();
        Files.write(avro, avroBytes);

        List<ListEntry> corrupted;
        try (AvroPageRunContainer.Reader reader = AvroPageRunContainer.openReader(avro)) {
            corrupted = drainPage(reader.nextPage());
            assertThat(reader.nextPage()).isNull();
        }
        assertThat(corrupted).isEqualTo(mutation.decoded()).isNotEqualTo(original);
    }

    private static List<ListEntry> drainAvro(Path path) throws IOException {
        List<ListEntry> entries = new ArrayList<>();
        try (AvroPageRunContainer.Reader reader = AvroPageRunContainer.openReader(path)) {
            PageBlock page;
            while ((page = reader.nextPage()) != null) {
                PageBlockCursor cursor = page.cursor();
                while (cursor.hasNext()) {
                    entries.add(cursor.next());
                }
            }
        }
        return entries;
    }

    private static AvroPageRunContainer.WriteResult writeAvro(
            Path path, List<PageBlock> pages, PageCodec codec) throws IOException {
        try (AvroPageRunContainer.Writer writer = AvroPageRunContainer.openWriter(path, codec)) {
            for (PageBlock page : pages) {
                writer.append(page);
            }
            return writer.seal();
        }
    }

    private static void writeCustom(Path path, List<PageBlock> pages) throws IOException {
        try (PageRunSegmentEncoder writer = PageRunSegmentEncoder.open(
                path, SortMetrics.NO_OP, null, SortMode.OBJECTS)) {
            for (PageBlock page : pages) {
                writer.append(page);
            }
            writer.finish(SegmentKind.CASCADE_INTERMEDIATE);
        }
    }

    private static List<PageBlock> pages(int pageCount, int rowsPerPage, PageCodec codec) {
        List<PageBlock> pages = new ArrayList<>();
        for (int page = 0; page < pageCount; page++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int row = 0; row < rowsPerPage; row++) {
                int ordinal = page * rowsPerPage + row;
                rows.add(object(ordinal));
            }
            pages.add(PageBlock.pack(rows, CMP, codec));
        }
        return pages;
    }

    private static ObjectEntry object(int ordinal) {
        return new ObjectEntry(KeyBytes.ofUtf8(key(ordinal)), 4096L + ordinal,
                1_788_131_200_000_000L + ordinal, String.format("%032x", ordinal),
                "STANDARD", null, false, "owner-0001", null, "SHA256", "FULL_OBJECT");
    }

    private static String key(int ordinal) {
        return String.format("tenant=0042/region=us-east-1/year=2026/month=08/day=31/"
                + "hour=12/partition=0017/object-%012d.dat", ordinal);
    }

    private static Path writePrefix(Path target, byte[] source, long length) throws IOException {
        Files.write(target, Arrays.copyOf(source, Math.toIntExact(length)));
        return target;
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static List<ListEntry> drainPage(PageBlock page) {
        List<ListEntry> entries = new ArrayList<>();
        PageBlockCursor cursor = page.cursor();
        while (cursor.hasNext()) {
            entries.add(cursor.next());
        }
        return entries;
    }

    private static SilentMutation findSilentMutation(
            byte[] original, PageBlockCodec.Header header) {
        List<ListEntry> expected = drainPage(PageBlock.deserialize(original.clone()));
        int end = header.payloadOffset() + header.payloadLength();
        for (int offset = header.payloadOffset(); offset < end; offset++) {
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                byte mask = (byte) (1 << bit);
                byte[] candidate = original.clone();
                candidate[offset] ^= mask;
                try {
                    List<ListEntry> decoded = drainPage(PageBlock.deserialize(candidate));
                    if (!decoded.equals(expected)) {
                        return new SilentMutation(offset, mask, decoded);
                    }
                } catch (RuntimeException rejected) {
                    // This mutation was structurally noticed; keep searching for a silent one.
                }
            }
        }
        throw new AssertionError("no silently accepted LZ4 payload mutation found");
    }

    private record SilentMutation(int offset, byte mask, List<ListEntry> decoded) {
    }
}
