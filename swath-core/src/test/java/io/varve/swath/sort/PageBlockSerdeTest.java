/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * {@link PageBlock#serialize()} / {@link PageBlock#deserialize(byte[])} round-trip — a deserialized
 * block must expose an
 * identical {@link PageBlock#firstKey()}, {@link PageBlock#lastKey()}, {@link PageBlock#count()},
 * {@link PageBlock#orderedUnderFullComparator()}, and decode the same entry sequence via {@link
 * PageBlock#cursor()} as the original — over adversarial key shapes, dict-overflow, all five dict
 * columns, and both ordered and out-of-order pages, for EVERY {@link PageCodec} (NONE/LZ4/ZSTD1: the
 * payload compression is transparent to every one of these invariants).
 */
class PageBlockSerdeTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    private static void assertRoundTrips(List<ListEntry> in) {
        assertRoundTrips(in, PageCodec.NONE);
    }

    private static void assertRoundTrips(List<ListEntry> in, PageCodec codec) {
        PageBlock original = PageBlock.pack(in, CMP, codec);
        byte[] record = original.serialize();
        PageBlock restored = PageBlock.deserialize(record);

        assertThat(restored.firstKey()).containsExactly(original.firstKey());
        assertThat(restored.lastKey()).containsExactly(original.lastKey());
        assertThat(restored.count()).isEqualTo(original.count());
        assertThat(restored.orderedUnderFullComparator()).isEqualTo(original.orderedUnderFullComparator());
        assertThat(decodeAll(restored)).containsExactlyElementsOf(decodeAll(original));
        assertThat(decodeAll(restored)).containsExactlyElementsOf(in);

        // firstEntry()/lastEntry() lazily re-derived from the payload must match full fidelity too.
        assertThat(restored.firstEntry()).isEqualTo(original.firstEntry());
        assertThat(restored.lastEntry()).isEqualTo(original.lastEntry());
    }

    private static List<ListEntry> decodeAll(PageBlock block) {
        List<ListEntry> out = new ArrayList<>();
        PageBlockCursor c = block.cursor();
        while (c.hasNext()) {
            out.add(c.next());
        }
        return out;
    }

    @Test
    void roundTripsASingleRowPage() {
        assertRoundTrips(List.of(object("only")));
    }

    @Test
    void persistedCursorOverreadRemainsAnIteratorProtocolError() {
        PageBlock original = PageBlock.pack(List.of(object("only")), CMP);
        PageBlockCursor cursor = PageBlock.deserialize(original.serialize(), Path.of("only.pageseg"))
                .cursor();

        assertThat(cursor.next()).isEqualTo(object("only"));
        assertThatThrownBy(cursor::next).isInstanceOf(java.util.NoSuchElementException.class);
    }

    @Test
    void roundTripsKeysWithNulAndFfBytes() {
        List<ListEntry> in = List.of(
                new CommonPrefixEntry(KeyBytes.of(new byte[] {0x00, 0x00, (byte) 0xFF})),
                new CommonPrefixEntry(KeyBytes.of(new byte[] {(byte) 0xFF, 0x00, (byte) 0xFF, 0x00})),
                object("plain"));
        assertRoundTrips(in);
    }

    @Test
    void roundTripsOneKiloByteKeys() {
        byte[] bigKey = new byte[1024];
        for (int i = 0; i < bigKey.length; i++) {
            bigKey[i] = (byte) (i & 0xFF);
        }
        byte[] bigKey2 = new byte[1024];
        for (int i = 0; i < bigKey2.length; i++) {
            bigKey2[i] = (byte) ((i + 7) & 0xFF);
        }
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.of(bigKey), 9L, 5L, "etag", "STANDARD", null, false, null, null, null, null),
                new ObjectEntry(KeyBytes.of(bigKey2), 9L, 5L, null, null, null, false, null, null, null, null));
        assertRoundTrips(in);
    }

    @Test
    void roundTripsAnOrderedPage() {
        assertRoundTrips(List.of(object("aaa"), object("mmm"), object("zzz")));
    }

    @Test
    void roundTripsAnOutOfOrderPage() {
        List<ListEntry> in = List.of(object("zzz/last"), object("aaa/first"), object("mmm/middle"));
        PageBlock block = PageBlock.pack(in, CMP);
        assertThat(block.orderedUnderFullComparator()).isFalse();
        assertRoundTrips(in);
    }

    @Test
    void roundTripsAllFiveDictColumnsAtLowCardinality() {
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a"), 1L, 0L, null, "STANDARD", null, false,
                        "owner-1", "Owner One", "CRC32", "FULL_OBJECT"),
                new ObjectEntry(KeyBytes.ofUtf8("b"), 1L, 0L, null, "GLACIER", null, false,
                        "owner-2", "Owner Two", "SHA256", "COMPOSITE"),
                new ObjectEntry(KeyBytes.ofUtf8("c"), 1L, 0L, null, "STANDARD", null, false,
                        "owner-1", "Owner One", "CRC32", "FULL_OBJECT"),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("d"), "v1", true, 0L, "owner-1"));
        assertRoundTrips(in);
    }

    @Test
    void roundTripsWhenADictColumnOverflowsPastTheCapAndFallsBackToRaw() {
        List<ListEntry> in = new ArrayList<>();
        for (int i = 0; i < PageBlock.DICT_CAP + 1; i++) {
            in.add(new ObjectEntry(KeyBytes.ofUtf8("k" + i), 1L, 0L, null,
                    i % 2 == 0 ? "STANDARD" : "GLACIER", null, false,
                    "owner-" + i, "Owner " + (i % 3),
                    i % 2 == 0 ? "CRC32" : "SHA256", i % 3 == 0 ? "FULL_OBJECT" : "COMPOSITE"));
        }
        // owner_id (>64 distinct) falls back to raw; the other dict columns stay dict-encoded.
        assertRoundTrips(in);
    }

    @Test
    void roundTripsMixedEntryTypesWithNullFields() {
        List<ListEntry> in = List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a/plain"), 123L, 1_700_000_000_000_000L,
                        "d41d8cd98f00b204e9800998ecf8427e", "STANDARD", null, false, null, null, null, null),
                new ObjectEntry(KeyBytes.ofUtf8("a/versioned"), 0L, 0L, null, "GLACIER",
                        "vid-123", true, "owner-1", "Owner One", "SHA256", "COMPOSITE"),
                new CommonPrefixEntry(KeyBytes.ofUtf8("a/folder/")),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("a/gone"), "delver", true, 42L, "owner-2"),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("a/gone2"), null, false, 0L, null));
        assertRoundTrips(in);
    }

    @Test
    void serializedRecordIsSelfContainedAndDeterministicByteExact() {
        List<ListEntry> in = List.of(object("aaa"), object("mmm"), object("zzz"));
        PageBlock block = PageBlock.pack(in, CMP);
        byte[] r1 = block.serialize();
        byte[] r2 = block.serialize();
        assertThat(r1).containsExactly(r2);
    }

    @ParameterizedTest
    @EnumSource(PageCodec.class)
    void persistedBlockRetainsOneBodyAndPayloadSliceWithoutCopy(PageCodec codec) {
        List<ListEntry> rows = List.of(object("aaa"), object("mmm"), object("zzz"));
        byte[] body = PageBlock.pack(rows, CMP, codec).serialize();
        PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
        PageBlock persisted = PageBlockCodec.deserialize(body, header, Path.of("owned.pageseg"));

        assertThat(persisted.payloadOwnerUnsafe())
                .as("the CRC-validated record body is the persisted block's payload owner")
                .isSameAs(body);
        assertThat(persisted.parsedHeaderUnsafe())
                .as("decode reuses the frontier's one header parse")
                .isSameAs(header);
        assertThat(persisted.payloadOffset()).isEqualTo(header.payloadOffset()).isPositive();
        assertThat(persisted.payloadLength()).isEqualTo(header.payloadLength())
                .isEqualTo(body.length - header.payloadOffset());
        assertThat(persisted.serialize()).containsExactly(body);
        assertThat(decodeAll(persisted)).containsExactlyElementsOf(rows);
    }

    @Test
    void decodedPageOwnsBodyAcrossFrontierAdvanceAndClose(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(configWithCodec(PageCodec.NONE), CMP);
        buffer.admit(1L, List.of(object("alpha"), object("bravo")));
        buffer.admit(1L, List.of(object("charlie"), object("delta")));
        Path path = dir.resolve("owned-lifetime.pageseg");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);

        PageBlock first;
        PageBlock second;
        byte[] firstOwner;
        PageFrontierReader reader = new PageFrontierReader(path, SortMetrics.NO_OP);
        try {
            firstOwner = reader.currentBodyOwner();
            PageBlockCodec.Header firstHeader = reader.currentHeader();
            first = reader.decodeCurrentPage();
            assertThat(first.payloadOwnerUnsafe()).isSameAs(firstOwner);
            assertThat(first.parsedHeaderUnsafe()).isSameAs(firstHeader);

            reader.advance();
            assertThat(reader.currentBodyOwner())
                    .as("each frame owns a distinct immutable body")
                    .isNotSameAs(firstOwner);
            second = reader.decodeCurrentPage();
            assertThat(second.payloadOwnerUnsafe()).isSameAs(reader.currentBodyOwner());
        } finally {
            reader.close();
        }

        assertThat(reader.currentBodyOwner()).isNull();
        assertThat(reader.currentHeader()).isNull();
        assertThat(decodeAll(first)).containsExactly(object("alpha"), object("bravo"));
        assertThat(decodeAll(second)).containsExactly(object("charlie"), object("delta"));
    }

    @Test
    void malformedStoredPayloadLengthIsRejectedBeforeABlockCanOwnTheBody() {
        byte[] body = PageBlock.pack(List.of(object("only")), CMP, PageCodec.NONE).serialize();
        int payloadLengthOffset = rawPayloadLenOffset(body) + Integer.BYTES;
        ByteBuffer.wrap(body).putInt(payloadLengthOffset,
                ByteBuffer.wrap(body).getInt(payloadLengthOffset) - 1);

        assertThatThrownBy(() -> PageBlockCodec.parseHeader(body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not equal remaining body bytes");
    }

    // ------------------------------------------------------------------
    // Compress-at-pack: every adversarial shape above round-trips identically under
    // EVERY codec, a corrupt/mismatched declared length fails fast, and PageFrontierReader keeps
    // reading min/max/count WITHOUT decompressing over a page-run file of LZ4-compressed pages.
    // ------------------------------------------------------------------

    /** The same adversarial/edge shapes exercised by the individual (implicitly NONE-codec) tests above. */
    private static List<List<ListEntry>> adversarialShapes() {
        List<List<ListEntry>> shapes = new ArrayList<>();
        shapes.add(List.of(object("only")));
        shapes.add(List.of(
                new CommonPrefixEntry(KeyBytes.of(new byte[] {0x00, 0x00, (byte) 0xFF})),
                new CommonPrefixEntry(KeyBytes.of(new byte[] {(byte) 0xFF, 0x00, (byte) 0xFF, 0x00})),
                object("plain")));
        byte[] bigKey = new byte[1024];
        byte[] bigKey2 = new byte[1024];
        for (int i = 0; i < bigKey.length; i++) {
            bigKey[i] = (byte) (i & 0xFF);
            bigKey2[i] = (byte) ((i + 7) & 0xFF);
        }
        shapes.add(List.of(
                new ObjectEntry(KeyBytes.of(bigKey), 9L, 5L, "etag", "STANDARD", null, false, null, null, null, null),
                new ObjectEntry(KeyBytes.of(bigKey2), 9L, 5L, null, null, null, false, null, null, null, null)));
        shapes.add(List.of(object("aaa"), object("mmm"), object("zzz")));
        shapes.add(List.of(object("zzz/last"), object("aaa/first"), object("mmm/middle")));
        shapes.add(List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a"), 1L, 0L, null, "STANDARD", null, false,
                        "owner-1", "Owner One", "CRC32", "FULL_OBJECT"),
                new ObjectEntry(KeyBytes.ofUtf8("b"), 1L, 0L, null, "GLACIER", null, false,
                        "owner-2", "Owner Two", "SHA256", "COMPOSITE"),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("d"), "v1", true, 0L, "owner-1")));
        List<ListEntry> dictOverflow = new ArrayList<>();
        for (int i = 0; i < PageBlock.DICT_CAP + 1; i++) {
            dictOverflow.add(new ObjectEntry(KeyBytes.ofUtf8("k" + i), 1L, 0L, null,
                    i % 2 == 0 ? "STANDARD" : "GLACIER", null, false,
                    "owner-" + i, "Owner " + (i % 3),
                    i % 2 == 0 ? "CRC32" : "SHA256", i % 3 == 0 ? "FULL_OBJECT" : "COMPOSITE"));
        }
        shapes.add(dictOverflow);
        return shapes;
    }

    @ParameterizedTest
    @EnumSource(PageCodec.class)
    void roundTripsAdversarialShapesUnderEveryCodec(PageCodec codec) {
        for (List<ListEntry> shape : adversarialShapes()) {
            assertRoundTrips(shape, codec);
        }
    }

    @ParameterizedTest
    @EnumSource(PageCodec.class)
    void corruptRawPayloadLengthFailsFastOnDecodeForEveryCodec(PageCodec codec) {
        List<ListEntry> in = List.of(object("aaa"), object("mmm"), object("zzz"));
        PageBlock block = PageBlock.pack(in, CMP, codec);
        byte[] record = block.serialize();

        int offset = rawPayloadLenOffset(record);
        ByteBuffer view = ByteBuffer.wrap(record);
        int declared = view.getInt(offset);
        view.putInt(offset, declared + 10_000);   // corrupt: no longer matches the real decompressed size

        PageBlock restored = PageBlock.deserialize(record);
        assertThatThrownBy(restored::cursor).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void pageFrontierReaderReadsMinMaxOfLz4CompressedPagesWithoutDecompressing(@TempDir Path dir) throws IOException {
        SortConfig config = configWithCodec(PageCodec.LZ4);
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("alpha"), object("bravo")));
        buffer.admit(1L, List.of(object("charlie"), object("delta")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);

        Path path = dir.resolve("lz4.pgr");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE).flush(sealed, path);

        try (PageFrontierReader reader = new PageFrontierReader(path, SortMetrics.NO_OP)) {
            // Header fields (min/max/count) are readable immediately after open()/advance() — before
            // decodeCurrentPage() is ever called, i.e. before any row is decompressed.
            assertThat(reader.hasPage()).isTrue();
            assertThat(new String(reader.minKey(), StandardCharsets.UTF_8)).isEqualTo("alpha");
            assertThat(new String(reader.maxKey(), StandardCharsets.UTF_8)).isEqualTo("bravo");
            assertThat(reader.count()).isEqualTo(2);

            reader.advance();
            assertThat(reader.hasPage()).isTrue();
            assertThat(new String(reader.minKey(), StandardCharsets.UTF_8)).isEqualTo("charlie");
            assertThat(new String(reader.maxKey(), StandardCharsets.UTF_8)).isEqualTo("delta");

            // Only NOW does decoding (and therefore decompression) happen — and it must still be correct.
            PageBlock page = reader.decodeCurrentPage();
            assertThat(decodeAll(page)).containsExactly(object("charlie"), object("delta"));

            reader.advance();
            assertThat(reader.hasPage()).isFalse();
        }
    }

    /**
     * Locate the {@code rawPayloadLen u32} field within a {@link PageBlock#serialize()} record by
     * replaying the exact same header parse {@link PageBlock#deserialize} performs, so the offset is
     * correct regardless of key lengths / dict table contents.
     */
    private static int rawPayloadLenOffset(byte[] record) {
        ByteBuffer buf = ByteBuffer.wrap(record);
        skipLenBytes(buf);   // minKey
        skipLenBytes(buf);   // maxKey
        buf.getInt();        // count
        buf.get();           // ordered-bit
        for (int i = 0; i < 5; i++) {
            int n = buf.getShort() & 0xFFFF;
            for (int j = 0; j < n; j++) {
                skipLenBytes(buf);
            }
        }
        buf.get();   // packed useDict byte
        buf.get();   // codec byte
        return buf.position();
    }

    private static void skipLenBytes(ByteBuffer buf) {
        int len = buf.getShort() & 0xFFFF;
        buf.position(buf.position() + len);
    }

    private static SortConfig configWithCodec(PageCodec codec) {
        return SortConfigs.base().withSegmentCodec(codec);
    }

    private static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }
}
