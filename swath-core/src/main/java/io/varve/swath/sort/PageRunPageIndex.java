/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/** Current and legacy sparse page-offset indexes stored in the optional page-run trailer extension. */
final class PageRunPageIndex {

    static final short TYPE = PageRunFormat.PAGE_INDEX_EXTENSION;
    static final short LEGACY_TYPE = PageRunFormat.LEGACY_PAGE_INDEX_EXTENSION;
    static final short VERSION = 1;
    private static final int DECODED_MAX_BYTES = Integer.BYTES;
    private static final int ENTRY_FIXED_BYTES = 4 * Long.BYTES;
    private static final int MAX_INDEX_KEY_BYTES = ByteMidpoint.MAX_KEY_LEN;

    enum Status {
        EMBEDDED,
        EMBEDDED_MINIMA_ONLY,
        SKIPPED,
        ABSENT,
        UNKNOWN,
        INVALID_LENGTH,
        INVALID_COUNT,
        INVALID_CRC,
        INVALID_ORDER,
        INVALID_BOUNDS,
        INVALID_OFFSET,
        INVALID_CUMULATIVE
    }

    /** Location-only metadata retained by a segment descriptor; it owns no sampled key arrays. */
    record Locator(long payloadStart, long entriesEnd, long payloadLength, int entryCount) {
    }

    /** Validated extension result plus the legacy-compatible boundary-sample view. */
    record ReadResult(Status status, short extensionType, int entryCount, long totalRecords,
                      long bytesRead, long firstOffset, long lastOffset,
                      int maxRawPayloadLength, Locator locator,
                      PageRunBoundarySample.ReadResult boundarySample) {
        boolean valid() {
            return status == Status.EMBEDDED;
        }

        boolean hasDecodedPageMaximum() {
            return valid() && extensionType == TYPE;
        }
    }

    /** O(1) extension-header probe used when sparse entries are not needed. */
    record Probe(Status status, short extensionType, short version, long payloadLength,
                 long declaredCount, long extensionStart, long extensionBytes,
                 long bytesRead, byte[] header) {

        boolean supportedPhysicalType() {
            if (status == Status.ABSENT) {
                return true;
            }
            if (status != null) {
                return false;
            }
            return (extensionType == PageRunBoundarySample.TYPE
                    && version == PageRunBoundarySample.VERSION)
                    || ((extensionType == TYPE || extensionType == LEGACY_TYPE)
                    && version == VERSION);
        }
    }

    /** One decoded page-index entry. Arrays are owned by this value and treated as read-only. */
    record IndexEntry(long pageOrdinal, long fileOffset, long cumulativeEntries,
                      long cumulativeFramedBytes, byte[] minKey, byte[] prefixMax) {
    }

    /** One cursor entry plus its absolute position inside the extension payload. */
    record LocatedEntry(long payloadOffset, IndexEntry entry) {
    }

    /** One exact positional entry read and its metadata-byte cost. */
    record EntryRead(LocatedEntry located, int bytesRead) {
    }

    /** Bounded in-memory index assembled while listing pages are framed. */
    record Snapshot(List<IndexEntry> entries, byte[] finalPrefixMax, int maxRawPayloadLength) {
    }

    private PageRunPageIndex() {
    }

    static PageRunPageIndexBuilder exactBuilder(int totalPages) {
        return new PageRunPageIndexBuilder(totalPages);
    }

    static ReadResult skipped(long totalRecords, Probe probe) {
        if (probe.status() != null) {
            return result(probe.status(), probe.extensionType(), 0, totalRecords,
                    probe.bytesRead(), -1, -1, null);
        }
        return new ReadResult(Status.SKIPPED, probe.extensionType(), 0, totalRecords,
                probe.bytesRead(), -1, -1, -1, null,
                new PageRunBoundarySample.ReadResult(PageRunBoundarySample.Status.SKIPPED,
                        0, totalRecords, probe.bytesRead()));
    }

    /** Read only the fixed optional-extension header, never its sparse payload. */
    static Probe probe(PageRunSegmentIo io, PageRunTrailer.Trailer trailer) throws IOException {
        long fixedTailStart = io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long extensionStart = trailer.extensionStart();
        long extensionBytes = fixedTailStart - extensionStart;
        if (extensionBytes == 0) {
            return new Probe(Status.ABSENT, (short) 0, (short) 0, 0, 0,
                    extensionStart, 0, 0, null);
        }
        if (extensionBytes < PageRunBoundarySample.HEADER_BYTES
                + PageRunBoundarySample.CRC_BYTES) {
            return new Probe(Status.INVALID_LENGTH, (short) 0, (short) 0, 0, 0,
                    extensionStart, extensionBytes, 0, null);
        }
        byte[] header = io.readAt(extensionStart, PageRunBoundarySample.HEADER_BYTES).array();
        ByteBuffer fields = ByteBuffer.wrap(header);
        int magic = fields.getInt();
        short type = fields.getShort();
        short version = fields.getShort();
        long payloadLength = fields.getInt() & 0xFFFFFFFFL;
        long declaredCount = fields.getInt() & 0xFFFFFFFFL;
        Status status = magic == PageRunBoundarySample.MAGIC ? null : Status.UNKNOWN;
        return new Probe(status, type, version, payloadLength, declaredCount,
                extensionStart, extensionBytes, header.length, header);
    }

    /**
     * Dispatch the optional extension. Type 1 retains minima-only behavior; valid legacy type 2 and
     * current type 3 retain a positional locator, while type 3 also declares decoded-page residency.
     */
    static ReadResult read(PageRunSegmentIo io, PageRunTrailer.Trailer trailer,
                           Consumer<byte[]> validMinKeySink) throws IOException {
        return read(io, trailer, validMinKeySink, probe(io, trailer));
    }

    /** Fully validate and parse a sparse extension after an O(1) header probe. */
    static ReadResult read(PageRunSegmentIo io, PageRunTrailer.Trailer trailer,
            Consumer<byte[]> validMinKeySink, Probe probe) throws IOException {
        if (probe.status() == Status.ABSENT) {
            return result(Status.ABSENT, (short) 0, 0, trailer.totalRecords(), 0, -1, -1,
                    null);
        }
        if (probe.status() == Status.INVALID_LENGTH) {
            return result(Status.INVALID_LENGTH, (short) 0, 0, trailer.totalRecords(), 0, -1, -1,
                    null);
        }
        if (probe.status() == Status.UNKNOWN) {
            return result(Status.UNKNOWN, probe.extensionType(), 0, trailer.totalRecords(),
                    probe.bytesRead(), -1, -1,
                    null);
        }
        short type = probe.extensionType();
        short version = probe.version();
        byte[] header = probe.header();
        if (type == PageRunBoundarySample.TYPE && version == PageRunBoundarySample.VERSION) {
            PageRunBoundarySample.ReadResult legacy = PageRunBoundarySample.read(
                    io, trailer, validMinKeySink, header);
            Status status = legacy.valid() ? Status.EMBEDDED_MINIMA_ONLY : fromLegacy(legacy.status());
            return new ReadResult(status, type, legacy.entryCount(), legacy.totalRecords(),
                    legacy.bytesRead(), -1, -1, -1, null, legacy);
        }
        if ((type != TYPE && type != LEGACY_TYPE) || version != VERSION) {
            return result(Status.UNKNOWN, type, 0, trailer.totalRecords(), header.length, -1, -1,
                    null);
        }
        return readType2(io, trailer, validMinKeySink,
                probe.extensionStart(), probe.extensionBytes(), header,
                probe.payloadLength(), probe.declaredCount(), type);
    }

    /** Open a bounded, non-retaining cursor over an already validated page-index block. */
    static Cursor cursor(PageRunSegmentIo io, ReadResult result) {
        if (!result.valid() || result.locator() == null) {
            throw new IllegalArgumentException("page-run page index is not valid");
        }
        return new Cursor(io, result.locator());
    }

    /** Read exactly one planned entry without a 64 KiB cursor buffer or adjacent-entry prefetch. */
    static EntryRead readEntryAt(PageRunSegmentIo io, ReadResult result, long payloadOffset)
            throws IOException {
        if (!result.valid() || result.locator() == null) {
            throw new IllegalArgumentException("page-run page index is not valid");
        }
        Locator locator = result.locator();
        if (payloadOffset < locator.payloadStart()
                || payloadOffset > locator.entriesEnd() - ENTRY_FIXED_BYTES - 4L) {
            throw new IOException("page-run page index target entry is out of bounds");
        }
        ByteBuffer fixed = io.readAt(payloadOffset, ENTRY_FIXED_BYTES);
        long ordinal = fixed.getLong();
        long offset = fixed.getLong();
        long cumulativeEntries = fixed.getLong();
        long cumulativeFramedBytes = fixed.getLong();
        long position = payloadOffset + ENTRY_FIXED_BYTES;
        PositionalKey min = readKeyAt(io, position, locator.entriesEnd());
        position += min.encodedBytes();
        PositionalKey prefix = readKeyAt(io, position, locator.entriesEnd());
        position += prefix.encodedBytes();
        int bytesRead = Math.toIntExact(position - payloadOffset);
        IndexEntry entry = new IndexEntry(ordinal, offset, cumulativeEntries,
                cumulativeFramedBytes, min.key(), prefix.key());
        return new EntryRead(new LocatedEntry(payloadOffset, entry), bytesRead);
    }

    static void write(WritableByteChannel channel, Snapshot snapshot) throws IOException {
        long payloadLength = 2L + snapshot.finalPrefixMax().length + DECODED_MAX_BYTES;
        for (IndexEntry entry : snapshot.entries()) {
            requireKeyLength(entry.minKey());
            requireKeyLength(entry.prefixMax());
            payloadLength += ENTRY_FIXED_BYTES + 2L + entry.minKey().length
                    + 2L + entry.prefixMax().length;
        }
        requireKeyLength(snapshot.finalPrefixMax());
        if (snapshot.entries().isEmpty() != (snapshot.maxRawPayloadLength() == 0)
                || snapshot.maxRawPayloadLength() < 0
                || snapshot.maxRawPayloadLength() > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
            throw new IOException("invalid page-run decoded-page maximum: "
                    + snapshot.maxRawPayloadLength());
        }
        if (snapshot.entries().size() > PageRunBoundarySample.MAX_ENTRIES) {
            throw new IOException("page-run page index exceeds entry cap: " + snapshot.entries().size());
        }
        if (payloadLength > 0xFFFFFFFFL) {
            throw new IOException("page-run page index payload exceeds u32 length: " + payloadLength);
        }

        byte[] header = ByteBuffer.allocate(PageRunBoundarySample.HEADER_BYTES)
                .putInt(PageRunBoundarySample.MAGIC)
                .putShort(TYPE)
                .putShort(VERSION)
                .putInt((int) payloadLength)
                .putInt(snapshot.entries().size())
                .array();
        CRC32C crc = new CRC32C();
        PageRunBoundarySample.ChunkedWriter out = new PageRunBoundarySample.ChunkedWriter(channel);
        out.write(header);
        crc.update(header, 0, header.length);
        for (IndexEntry entry : snapshot.entries()) {
            writeLong(out, crc, entry.pageOrdinal());
            writeLong(out, crc, entry.fileOffset());
            writeLong(out, crc, entry.cumulativeEntries());
            writeLong(out, crc, entry.cumulativeFramedBytes());
            writeKey(out, crc, entry.minKey());
            writeKey(out, crc, entry.prefixMax());
        }
        writeKey(out, crc, snapshot.finalPrefixMax());
        writeInt(out, crc, snapshot.maxRawPayloadLength());
        out.writeInt((int) crc.getValue());
        out.flush();
    }

    private static ReadResult readType2(PageRunSegmentIo io, PageRunTrailer.Trailer trailer,
            Consumer<byte[]> validMinKeySink, long extensionStart, long extensionBytes,
            byte[] header, long payloadLength, long declaredCount, short type) throws IOException {
        long bytesRead = header.length;
        if (payloadLength != extensionBytes - PageRunBoundarySample.HEADER_BYTES
                - PageRunBoundarySample.CRC_BYTES) {
            return result(Status.INVALID_LENGTH, type, 0, trailer.totalRecords(), bytesRead, -1, -1,
                    null);
        }
        int expectedCount = PageRunBoundarySample.expectedCount(trailer.totalRecords());
        if (declaredCount != expectedCount || declaredCount > PageRunBoundarySample.MAX_ENTRIES) {
            return result(Status.INVALID_COUNT, type, 0, trailer.totalRecords(), bytesRead, -1, -1,
                    null);
        }
        if (payloadLength > maximumPayloadLength(expectedCount, type)) {
            return result(Status.INVALID_LENGTH, type, 0, trailer.totalRecords(), bytesRead, -1, -1,
                    null);
        }
        // Validate the complete block before allocating or retaining a single key. A torn extension
        // must take the bounded fallback path, never consume hundreds of MiB in provisional arrays
        // only to discover its bad CRC at the end.
        if (!crcValid(io, extensionStart, payloadLength, header)) {
            return result(Status.INVALID_CRC, type, 0, trailer.totalRecords(), extensionBytes,
                    -1, -1, null);
        }
        bytesRead = extensionBytes;

        long payloadStart = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        PageRunBoundarySample.ChunkedReader in = new PageRunBoundarySample.ChunkedReader(
                io, payloadStart, payloadLength);
        CRC32C ignoredCrc = new CRC32C();
        List<byte[]> minima = new ArrayList<>((int) declaredCount);
        long stride = PageRunBoundarySample.stride(trailer.totalRecords());
        long payloadRemaining = payloadLength;
        long previousOffset = -1;
        long previousEntries = -1;
        long previousFramedBytes = -1;
        byte[] previousMin = null;
        byte[] previousPrefixMax = null;
        long firstOffset = -1;
        long lastOffset = -1;
        for (int i = 0; i < declaredCount; i++) {
            if (payloadRemaining < ENTRY_FIXED_BYTES + 4L) {
                return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
            }
            long ordinal = in.readLong(ignoredCrc);
            long offset = in.readLong(ignoredCrc);
            long cumulativeEntries = in.readLong(ignoredCrc);
            long cumulativeFramedBytes = in.readLong(ignoredCrc);
            payloadRemaining -= ENTRY_FIXED_BYTES;
            KeyRead min = readKey(in, ignoredCrc, payloadRemaining);
            if (min == null) {
                return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
            }
            payloadRemaining -= min.encodedBytes();
            KeyRead prefix = readKey(in, ignoredCrc, payloadRemaining);
            if (prefix == null) {
                return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
            }
            payloadRemaining -= prefix.encodedBytes();

            long expectedOrdinal = (long) i * stride;
            if (ordinal != expectedOrdinal || ordinal >= trailer.totalRecords()) {
                return invalid(Status.INVALID_COUNT, type, trailer, bytesRead + in.bytesRead());
            }
            if (offset < io.headerBytes || offset >= io.trailerStart
                    || (i == 0 && offset != io.headerBytes)
                    || (previousOffset >= 0 && offset <= previousOffset)) {
                return invalid(Status.INVALID_OFFSET, type, trailer, bytesRead + in.bytesRead());
            }
            if (cumulativeEntries < 0 || cumulativeEntries > trailer.totalEntries()
                    || (i == 0 && cumulativeEntries != 0)
                    || cumulativeEntries < ordinal
                    || (previousEntries >= 0 && cumulativeEntries <= previousEntries)
                    || cumulativeFramedBytes != offset - io.headerBytes
                    || (i == 0 && cumulativeFramedBytes != 0)
                    || (previousFramedBytes >= 0 && cumulativeFramedBytes <= previousFramedBytes)) {
                return invalid(Status.INVALID_CUMULATIVE, type, trailer, bytesRead + in.bytesRead());
            }
            if ((previousMin != null && Arrays.compareUnsigned(min.key(), previousMin) < 0)
                    || (previousPrefixMax != null
                    && Arrays.compareUnsigned(prefix.key(), previousPrefixMax) < 0)) {
                return invalid(Status.INVALID_ORDER, type, trailer, bytesRead + in.bytesRead());
            }
            if (Arrays.compareUnsigned(min.key(), prefix.key()) > 0) {
                return invalid(Status.INVALID_BOUNDS, type, trailer, bytesRead + in.bytesRead());
            }

            minima.add(min.key());
            previousOffset = offset;
            previousEntries = cumulativeEntries;
            previousFramedBytes = cumulativeFramedBytes;
            previousMin = min.key();
            previousPrefixMax = prefix.key();
            if (i == 0) {
                firstOffset = offset;
            }
            lastOffset = offset;
        }
        long entriesEnd = payloadStart + in.consumed();
        long metadataBytes = type == TYPE ? DECODED_MAX_BYTES : 0;
        KeyRead finalPrefix = readKey(in, ignoredCrc, payloadRemaining - metadataBytes);
        if (finalPrefix == null) {
            return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
        }
        payloadRemaining -= finalPrefix.encodedBytes();
        int maxRawPayloadLength = -1;
        if (type == TYPE) {
            if (payloadRemaining != DECODED_MAX_BYTES) {
                return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
            }
            maxRawPayloadLength = in.readInt();
            payloadRemaining -= DECODED_MAX_BYTES;
            if ((declaredCount == 0) != (maxRawPayloadLength == 0)
                    || maxRawPayloadLength < 0
                    || maxRawPayloadLength > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
                return invalid(Status.INVALID_BOUNDS, type, trailer, bytesRead + in.bytesRead());
            }
        }
        if (payloadRemaining != 0) {
            return invalid(Status.INVALID_LENGTH, type, trailer, bytesRead + in.bytesRead());
        }
        bytesRead += in.bytesRead();
        if (minima.isEmpty()) {
            if (trailer.segMinKey().length != 0 || trailer.segMaxKey().length != 0
                    || finalPrefix.key().length != 0) {
                return invalid(Status.INVALID_BOUNDS, type, trailer, bytesRead);
            }
        } else if (!Arrays.equals(minima.getFirst(), trailer.segMinKey())
                || !Arrays.equals(finalPrefix.key(), trailer.segMaxKey())
                || Arrays.compareUnsigned(previousPrefixMax, finalPrefix.key()) > 0) {
            return invalid(Status.INVALID_BOUNDS, type, trailer, bytesRead);
        }
        minima.forEach(validMinKeySink);
        Locator locator = new Locator(payloadStart, entriesEnd, payloadLength, minima.size());
        PageRunBoundarySample.ReadResult sample = new PageRunBoundarySample.ReadResult(
                PageRunBoundarySample.Status.EMBEDDED, minima.size(), trailer.totalRecords(), bytesRead);
        return new ReadResult(Status.EMBEDDED, type, minima.size(), trailer.totalRecords(), bytesRead,
                firstOffset, lastOffset, maxRawPayloadLength, locator, sample);
    }

    private static KeyRead readKey(PageRunBoundarySample.ChunkedReader in, CRC32C crc,
                                   long payloadRemaining) throws IOException {
        if (payloadRemaining < Short.BYTES) {
            return null;
        }
        int length = in.readUnsignedShort(crc);
        if (length > MAX_INDEX_KEY_BYTES || length > payloadRemaining - Short.BYTES) {
            return null;
        }
        byte[] key = new byte[length];
        in.read(key, crc);
        return new KeyRead(key, Short.BYTES + (long) length);
    }

    private static PositionalKey readKeyAt(PageRunSegmentIo io, long position, long limit)
            throws IOException {
        if (position > limit - Short.BYTES) {
            throw new IOException("page-run page index target key prefix exceeds entry bounds");
        }
        int length = io.readAt(position, Short.BYTES).getShort() & 0xffff;
        if (length > MAX_INDEX_KEY_BYTES || length > limit - position - Short.BYTES) {
            throw new IOException("page-run page index target key exceeds entry bounds");
        }
        return new PositionalKey(io.readAt(position + Short.BYTES, length).array(),
                Short.BYTES + length);
    }

    private static ReadResult invalid(Status status, PageRunTrailer.Trailer trailer, long bytesRead) {
        return invalid(status, TYPE, trailer, bytesRead);
    }

    private static ReadResult invalid(Status status, short type,
            PageRunTrailer.Trailer trailer, long bytesRead) {
        return result(status, type, 0, trailer.totalRecords(), bytesRead, -1, -1, null);
    }

    private static ReadResult result(Status status, short type, int count, long totalRecords,
            long bytesRead, long firstOffset, long lastOffset, Locator locator) {
        PageRunBoundarySample.Status sampleStatus = switch (status) {
            case EMBEDDED -> PageRunBoundarySample.Status.EMBEDDED;
            case SKIPPED -> PageRunBoundarySample.Status.SKIPPED;
            case ABSENT -> PageRunBoundarySample.Status.ABSENT;
            case UNKNOWN -> PageRunBoundarySample.Status.UNKNOWN;
            case INVALID_LENGTH -> PageRunBoundarySample.Status.INVALID_LENGTH;
            case INVALID_COUNT, INVALID_CUMULATIVE -> PageRunBoundarySample.Status.INVALID_COUNT;
            case INVALID_CRC -> PageRunBoundarySample.Status.INVALID_CRC;
            case INVALID_ORDER -> PageRunBoundarySample.Status.INVALID_ORDER;
            case INVALID_BOUNDS, INVALID_OFFSET -> PageRunBoundarySample.Status.INVALID_BOUNDS;
            case EMBEDDED_MINIMA_ONLY -> throw new IllegalArgumentException("legacy result required");
        };
        PageRunBoundarySample.ReadResult sample = new PageRunBoundarySample.ReadResult(
                sampleStatus, count, totalRecords, bytesRead);
        return new ReadResult(status, type, count, totalRecords, bytesRead, firstOffset,
                lastOffset, -1, locator, sample);
    }

    private static Status fromLegacy(PageRunBoundarySample.Status status) {
        return switch (status) {
            case EMBEDDED -> Status.EMBEDDED_MINIMA_ONLY;
            case SKIPPED -> Status.SKIPPED;
            case ABSENT -> Status.ABSENT;
            case UNKNOWN -> Status.UNKNOWN;
            case INVALID_LENGTH -> Status.INVALID_LENGTH;
            case INVALID_COUNT -> Status.INVALID_COUNT;
            case INVALID_CRC -> Status.INVALID_CRC;
            case INVALID_ORDER -> Status.INVALID_ORDER;
            case INVALID_BOUNDS -> Status.INVALID_BOUNDS;
        };
    }

    private static void requireKeyLength(byte[] key) throws IOException {
        if (key.length > MAX_INDEX_KEY_BYTES) {
            throw new IOException("page-run page index key exceeds the S3 key limit: " + key.length);
        }
    }

    private static long maximumPayloadLength(int entryCount, short type) {
        long keyBytes = Short.BYTES + (long) MAX_INDEX_KEY_BYTES;
        long metadataBytes = type == TYPE ? DECODED_MAX_BYTES : 0;
        return (long) entryCount * (ENTRY_FIXED_BYTES + 2L * keyBytes) + keyBytes + metadataBytes;
    }

    /** CRC-first streaming validation with one fixed scratch buffer and no key allocation. */
    private static boolean crcValid(PageRunSegmentIo io, long extensionStart, long payloadLength,
                                    byte[] header) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
        ByteBuffer scratch = ByteBuffer.allocate(PageRunBoundarySample.IO_BUFFER_BYTES);
        long position = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        long remaining = payloadLength;
        while (remaining > 0) {
            int length = (int) Math.min(scratch.capacity(), remaining);
            scratch.clear().limit(length);
            io.readAt(position, scratch);
            crc.update(scratch.array(), 0, length);
            position += length;
            remaining -= length;
        }
        int expected = io.readAt(position, PageRunBoundarySample.CRC_BYTES).getInt();
        return (int) crc.getValue() == expected;
    }

    private static void writeLong(PageRunBoundarySample.ChunkedWriter out, CRC32C crc, long value)
            throws IOException {
        out.writeLong(value);
        for (int shift = 56; shift >= 0; shift -= 8) {
            crc.update((int) (value >>> shift) & 0xFF);
        }
    }

    private static void writeInt(PageRunBoundarySample.ChunkedWriter out, CRC32C crc, int value)
            throws IOException {
        out.writeInt(value);
        for (int shift = 24; shift >= 0; shift -= 8) {
            crc.update(value >>> shift & 0xFF);
        }
    }

    private static void writeKey(PageRunBoundarySample.ChunkedWriter out, CRC32C crc, byte[] key)
            throws IOException {
        out.writeShort(key.length);
        crc.update(key.length >>> 8);
        crc.update(key.length);
        out.write(key);
        crc.update(key, 0, key.length);
    }

    private record KeyRead(byte[] key, long encodedBytes) {
    }

    private record PositionalKey(byte[] key, int encodedBytes) {
    }

    /** Streaming entry cursor used by later range planning without descriptor-level sample retention. */
    static final class Cursor {
        private final PageRunSegmentIo io;
        private final PageRunBoundarySample.ChunkedReader in;
        private final CRC32C ignoredCrc = new CRC32C();
        private final long payloadStart;
        private int remaining;

        private Cursor(PageRunSegmentIo io, Locator locator) {
            this.io = io;
            this.in = new PageRunBoundarySample.ChunkedReader(io, locator.payloadStart(),
                    locator.entriesEnd() - locator.payloadStart());
            this.payloadStart = locator.payloadStart();
            this.remaining = locator.entryCount();
        }

        boolean hasNext() {
            return remaining > 0;
        }

        LocatedEntry next() throws IOException {
            if (remaining == 0) {
                throw new IllegalStateException("page-run page index cursor exhausted");
            }
            long payloadOffset = payloadStart + in.consumed();
            long ordinal = in.readLong(ignoredCrc);
            long offset = in.readLong(ignoredCrc);
            long cumulativeEntries = in.readLong(ignoredCrc);
            long cumulativeFramedBytes = in.readLong(ignoredCrc);
            KeyRead min = readKey(in, ignoredCrc, Long.MAX_VALUE);
            KeyRead prefix = min == null ? null : readKey(in, ignoredCrc, Long.MAX_VALUE);
            if (min == null || prefix == null) {
                throw io.indexMismatch("page-index cursor entry key exceeds the key limit", null);
            }
            remaining--;
            IndexEntry entry = new IndexEntry(ordinal, offset, cumulativeEntries,
                    cumulativeFramedBytes, min.key(), prefix.key());
            return new LocatedEntry(payloadOffset, entry);
        }

        long bytesRead() {
            return in.bytesRead();
        }
    }
}
