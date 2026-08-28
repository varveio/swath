/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/** Current sparse page-offset index stored in the optional page-run trailer extension. */
final class PageRunPageIndex {

    static final short TYPE = 2;
    static final short VERSION = 1;
    private static final int ENTRY_FIXED_BYTES = 4 * Long.BYTES;

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
                      long bytesRead, long firstOffset, long lastOffset, Locator locator,
                      PageRunBoundarySample.ReadResult boundarySample) {
        boolean valid() {
            return status == Status.EMBEDDED;
        }
    }

    /** One decoded type-2 entry. Arrays are owned by this value and treated as read-only. */
    record IndexEntry(long pageOrdinal, long fileOffset, long cumulativeEntries,
                      long cumulativeFramedBytes, byte[] minKey, byte[] prefixMax) {
    }

    /** One cursor entry plus its absolute position inside the extension payload. */
    record LocatedEntry(long payloadOffset, IndexEntry entry) {
    }

    /** Bounded in-memory index assembled while listing pages are framed. */
    record Snapshot(List<IndexEntry> entries, byte[] finalPrefixMax) {
    }

    private PageRunPageIndex() {
    }

    static PageRunPageIndexBuilder exactBuilder(int totalPages) {
        return new PageRunPageIndexBuilder(totalPages);
    }

    static ReadResult skipped(long totalRecords) {
        return new ReadResult(Status.SKIPPED, (short) 0, 0, totalRecords, 0, -1, -1,
                null, PageRunBoundarySample.skipped(totalRecords));
    }

    /**
     * Dispatch the optional extension. Type 1 retains its legacy minima-only behavior; a valid type 2
     * additionally retains only a positional locator for the later planning cursor.
     */
    static ReadResult read(PageRunSegmentIo io, PageRunTrailer.Trailer trailer,
                           Consumer<byte[]> validMinKeySink) throws IOException {
        long fixedTailStart = io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long extensionStart = trailer.extensionStart();
        long extensionBytes = fixedTailStart - extensionStart;
        if (extensionBytes == 0) {
            return result(Status.ABSENT, (short) 0, 0, trailer.totalRecords(), 0, -1, -1,
                    null);
        }
        if (extensionBytes < PageRunBoundarySample.HEADER_BYTES + PageRunBoundarySample.CRC_BYTES) {
            return result(Status.INVALID_LENGTH, (short) 0, 0, trailer.totalRecords(), 0, -1, -1,
                    null);
        }

        byte[] header = io.readAt(extensionStart, PageRunBoundarySample.HEADER_BYTES).array();
        ByteBuffer fields = ByteBuffer.wrap(header);
        int magic = fields.getInt();
        short type = fields.getShort();
        short version = fields.getShort();
        long payloadLength = fields.getInt() & 0xFFFFFFFFL;
        long declaredCount = fields.getInt() & 0xFFFFFFFFL;
        if (magic != PageRunBoundarySample.MAGIC) {
            return result(Status.UNKNOWN, type, 0, trailer.totalRecords(), header.length, -1, -1,
                    null);
        }
        if (type == PageRunBoundarySample.TYPE && version == PageRunBoundarySample.VERSION) {
            PageRunBoundarySample.ReadResult legacy = PageRunBoundarySample.read(
                    io, trailer, validMinKeySink, header);
            Status status = legacy.valid() ? Status.EMBEDDED_MINIMA_ONLY : fromLegacy(legacy.status());
            return new ReadResult(status, type, legacy.entryCount(), legacy.totalRecords(),
                    legacy.bytesRead(), -1, -1, null, legacy);
        }
        if (type != TYPE || version != VERSION) {
            return result(Status.UNKNOWN, type, 0, trailer.totalRecords(), header.length, -1, -1,
                    null);
        }
        return readType2(io, trailer, validMinKeySink, extensionStart, extensionBytes,
                header, payloadLength, declaredCount);
    }

    /** Open a bounded, non-retaining cursor over entries of an already validated type-2 block. */
    static Cursor cursor(PageRunSegmentIo io, ReadResult result) {
        if (!result.valid() || result.locator() == null) {
            throw new IllegalArgumentException("page-run page index is not valid type 2");
        }
        return new Cursor(io, result.locator());
    }

    static void write(WritableByteChannel channel, Snapshot snapshot) throws IOException {
        long payloadLength = 2L + snapshot.finalPrefixMax().length;
        for (IndexEntry entry : snapshot.entries()) {
            requireKeyLength(entry.minKey());
            requireKeyLength(entry.prefixMax());
            payloadLength += ENTRY_FIXED_BYTES + 2L + entry.minKey().length
                    + 2L + entry.prefixMax().length;
        }
        requireKeyLength(snapshot.finalPrefixMax());
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
        out.writeInt((int) crc.getValue());
        out.flush();
    }

    private static ReadResult readType2(PageRunSegmentIo io, PageRunTrailer.Trailer trailer,
            Consumer<byte[]> validMinKeySink, long extensionStart, long extensionBytes,
            byte[] header, long payloadLength, long declaredCount) throws IOException {
        long bytesRead = header.length;
        if (payloadLength != extensionBytes - PageRunBoundarySample.HEADER_BYTES
                - PageRunBoundarySample.CRC_BYTES) {
            return result(Status.INVALID_LENGTH, TYPE, 0, trailer.totalRecords(), bytesRead, -1, -1,
                    null);
        }
        int expectedCount = PageRunBoundarySample.expectedCount(trailer.totalRecords());
        if (declaredCount != expectedCount || declaredCount > PageRunBoundarySample.MAX_ENTRIES) {
            return result(Status.INVALID_COUNT, TYPE, 0, trailer.totalRecords(), bytesRead, -1, -1,
                    null);
        }

        long payloadStart = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        PageRunBoundarySample.ChunkedReader in = new PageRunBoundarySample.ChunkedReader(
                io, payloadStart, extensionBytes - PageRunBoundarySample.HEADER_BYTES);
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
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
                return invalid(Status.INVALID_LENGTH, trailer, bytesRead + in.bytesRead());
            }
            long ordinal = in.readLong(crc);
            long offset = in.readLong(crc);
            long cumulativeEntries = in.readLong(crc);
            long cumulativeFramedBytes = in.readLong(crc);
            payloadRemaining -= ENTRY_FIXED_BYTES;
            KeyRead min = readKey(in, crc, payloadRemaining);
            if (min == null) {
                return invalid(Status.INVALID_LENGTH, trailer, bytesRead + in.bytesRead());
            }
            payloadRemaining -= min.encodedBytes();
            KeyRead prefix = readKey(in, crc, payloadRemaining);
            if (prefix == null) {
                return invalid(Status.INVALID_LENGTH, trailer, bytesRead + in.bytesRead());
            }
            payloadRemaining -= prefix.encodedBytes();

            long expectedOrdinal = (long) i * stride;
            if (ordinal != expectedOrdinal || ordinal >= trailer.totalRecords()) {
                return invalid(Status.INVALID_COUNT, trailer, bytesRead + in.bytesRead());
            }
            if (offset < PageRunSegmentWriter.HEADER_BYTES || offset >= io.trailerStart
                    || (i == 0 && offset != PageRunSegmentWriter.HEADER_BYTES)
                    || (previousOffset >= 0 && offset <= previousOffset)) {
                return invalid(Status.INVALID_OFFSET, trailer, bytesRead + in.bytesRead());
            }
            if (cumulativeEntries < 0 || cumulativeEntries > trailer.totalEntries()
                    || (i == 0 && cumulativeEntries != 0)
                    || (previousEntries >= 0 && cumulativeEntries < previousEntries)
                    || cumulativeFramedBytes != offset - PageRunSegmentWriter.HEADER_BYTES
                    || (i == 0 && cumulativeFramedBytes != 0)
                    || (previousFramedBytes >= 0 && cumulativeFramedBytes <= previousFramedBytes)) {
                return invalid(Status.INVALID_CUMULATIVE, trailer, bytesRead + in.bytesRead());
            }
            if ((previousMin != null && Arrays.compareUnsigned(min.key(), previousMin) < 0)
                    || (previousPrefixMax != null
                    && Arrays.compareUnsigned(prefix.key(), previousPrefixMax) < 0)) {
                return invalid(Status.INVALID_ORDER, trailer, bytesRead + in.bytesRead());
            }
            if (Arrays.compareUnsigned(min.key(), prefix.key()) > 0) {
                return invalid(Status.INVALID_BOUNDS, trailer, bytesRead + in.bytesRead());
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
        KeyRead finalPrefix = readKey(in, crc, payloadRemaining);
        if (finalPrefix == null) {
            return invalid(Status.INVALID_LENGTH, trailer, bytesRead + in.bytesRead());
        }
        payloadRemaining -= finalPrefix.encodedBytes();
        if (payloadRemaining != 0) {
            return invalid(Status.INVALID_LENGTH, trailer, bytesRead + in.bytesRead());
        }
        int expectedCrc = in.readInt();
        bytesRead += in.bytesRead();
        if ((int) crc.getValue() != expectedCrc) {
            return invalid(Status.INVALID_CRC, trailer, bytesRead);
        }
        if (minima.isEmpty()) {
            if (trailer.segMinKey().length != 0 || trailer.segMaxKey().length != 0
                    || finalPrefix.key().length != 0) {
                return invalid(Status.INVALID_BOUNDS, trailer, bytesRead);
            }
        } else if (!Arrays.equals(minima.getFirst(), trailer.segMinKey())
                || !Arrays.equals(finalPrefix.key(), trailer.segMaxKey())
                || Arrays.compareUnsigned(previousPrefixMax, finalPrefix.key()) > 0) {
            return invalid(Status.INVALID_BOUNDS, trailer, bytesRead);
        }
        minima.forEach(validMinKeySink);
        Locator locator = new Locator(payloadStart, entriesEnd, payloadLength, minima.size());
        PageRunBoundarySample.ReadResult sample = new PageRunBoundarySample.ReadResult(
                PageRunBoundarySample.Status.EMBEDDED, minima.size(), trailer.totalRecords(), bytesRead);
        return new ReadResult(Status.EMBEDDED, TYPE, minima.size(), trailer.totalRecords(), bytesRead,
                firstOffset, lastOffset, locator, sample);
    }

    private static KeyRead readKey(PageRunBoundarySample.ChunkedReader in, CRC32C crc,
                                   long payloadRemaining) throws IOException {
        if (payloadRemaining < Short.BYTES) {
            return null;
        }
        int length = in.readUnsignedShort(crc);
        if (length > payloadRemaining - Short.BYTES) {
            return null;
        }
        byte[] key = new byte[length];
        in.read(key, crc);
        return new KeyRead(key, Short.BYTES + (long) length);
    }

    private static ReadResult invalid(Status status, PageRunTrailer.Trailer trailer, long bytesRead) {
        return result(status, TYPE, 0, trailer.totalRecords(), bytesRead, -1, -1, null);
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
                lastOffset, locator, sample);
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
        if (key.length > 0xFFFF) {
            throw new IOException("page-run page index key exceeds u16 length: " + key.length);
        }
    }

    private static void writeLong(PageRunBoundarySample.ChunkedWriter out, CRC32C crc, long value)
            throws IOException {
        out.writeLong(value);
        for (int shift = 56; shift >= 0; shift -= 8) {
            crc.update((int) (value >>> shift) & 0xFF);
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

    /** Streaming entry cursor used by later range planning without descriptor-level sample retention. */
    static final class Cursor {
        private final PageRunBoundarySample.ChunkedReader in;
        private final CRC32C ignoredCrc = new CRC32C();
        private final long payloadStart;
        private int remaining;

        private Cursor(PageRunSegmentIo io, Locator locator) {
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
            KeyRead prefix = readKey(in, ignoredCrc, Long.MAX_VALUE);
            remaining--;
            IndexEntry entry = new IndexEntry(ordinal, offset, cumulativeEntries,
                    cumulativeFramedBytes, min.key(), prefix.key());
            return new LocatedEntry(payloadOffset, entry);
        }
    }
}
