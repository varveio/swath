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
import java.util.zip.CRC32C;

/** Optional, backward-compatible page-minimum sample embedded before a page-run's fixed EOF tail. */
final class PageRunBoundarySample {

    /**
     * The sample only resolves {@code R-1} split points, so thousands of candidates per segment is
     * ample while keeping sampler heap independent of the listing's row count (I11).
     */
    static final int MAX_ENTRIES = 4_096;
    static final int IO_BUFFER_BYTES = 64 * 1_024;

    private static final int MAGIC = 0x53504758; // "SPGX"
    private static final short TYPE = 1;
    private static final short VERSION = 1;
    private static final int HEADER_BYTES = 4 + 2 + 2 + 4 + 4;
    private static final int CRC_BYTES = 4;

    enum Status {
        EMBEDDED,
        ABSENT,
        UNKNOWN,
        INVALID_LENGTH,
        INVALID_COUNT,
        INVALID_CRC,
        INVALID_ORDER,
        INVALID_BOUNDS
    }

    record ReadResult(Status status, List<byte[]> keys, long totalRecords, long bytesRead) {
        boolean valid() {
            return status == Status.EMBEDDED;
        }
    }

    private PageRunBoundarySample() {
    }

    static long stride(long totalPages) {
        return totalPages <= MAX_ENTRIES ? 1 : (totalPages + MAX_ENTRIES - 1) / MAX_ENTRIES;
    }

    static int expectedCount(long totalPages) {
        if (totalPages == 0) {
            return 0;
        }
        long stride = stride(totalPages);
        return Math.toIntExact((totalPages - 1) / stride + 1);
    }

    static List<byte[]> select(List<PageBlock> pages) {
        long stride = stride(pages.size());
        List<byte[]> sample = new ArrayList<>(expectedCount(pages.size()));
        for (long i = 0; i < pages.size(); i += stride) {
            sample.add(pages.get(Math.toIntExact(i)).firstKey());
        }
        return sample;
    }

    static void write(WritableByteChannel channel, List<byte[]> keys) throws IOException {
        if (keys.size() > MAX_ENTRIES) {
            throw new IOException("page-run boundary sample exceeds entry cap: " + keys.size());
        }
        long payloadLength = 0;
        for (byte[] key : keys) {
            if (key.length > 0xFFFF) {
                throw new IOException("page-run boundary sample key exceeds u16 length: " + key.length);
            }
            payloadLength += 2L + key.length;
        }
        if (payloadLength > 0xFFFFFFFFL) {
            throw new IOException("page-run boundary sample payload exceeds u32 length: " + payloadLength);
        }

        byte[] header = ByteBuffer.allocate(HEADER_BYTES)
                .putInt(MAGIC)
                .putShort(TYPE)
                .putShort(VERSION)
                .putInt((int) payloadLength)
                .putInt(keys.size())
                .array();
        CRC32C crc = new CRC32C();
        ChunkedWriter out = new ChunkedWriter(channel);
        out.write(header);
        crc.update(header, 0, header.length);
        for (byte[] key : keys) {
            out.writeShort(key.length);
            crc.update(key.length >>> 8);
            crc.update(key.length);
            out.write(key);
            crc.update(key, 0, key.length);
        }
        out.writeInt((int) crc.getValue());
        out.flush();
    }

    static ReadResult read(PageRunSegmentIo io) throws IOException {
        long fixedTailStart = io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        Bounds bounds = readBounds(io, fixedTailStart);
        if (bounds == null) {
            return invalid(Status.INVALID_LENGTH, io.totalRecords, 0);
        }
        long extensionStart = bounds.extensionStart;
        long extensionBytes = fixedTailStart - extensionStart;
        long bytesRead = 0;
        if (extensionBytes == 0) {
            return new ReadResult(Status.ABSENT, List.of(), io.totalRecords, bytesRead);
        }
        if (extensionBytes < HEADER_BYTES + CRC_BYTES) {
            return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead);
        }

        byte[] header = io.readAt(extensionStart, HEADER_BYTES).array();
        bytesRead += HEADER_BYTES;
        ByteBuffer fields = ByteBuffer.wrap(header);
        int magic = fields.getInt();
        short type = fields.getShort();
        short version = fields.getShort();
        long payloadLength = fields.getInt() & 0xFFFFFFFFL;
        long entryCount = fields.getInt() & 0xFFFFFFFFL;
        if (magic != MAGIC || type != TYPE || version != VERSION) {
            return new ReadResult(Status.UNKNOWN, List.of(), io.totalRecords, bytesRead);
        }
        int expectedCount = expectedCount(io.totalRecords);
        long maxExtensionBytes = HEADER_BYTES + CRC_BYTES + (long) expectedCount * (2 + 0xFFFF);
        if (extensionBytes > maxExtensionBytes) {
            return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead);
        }
        if (payloadLength != extensionBytes - HEADER_BYTES - CRC_BYTES) {
            return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead);
        }
        if (entryCount > Math.min(io.totalRecords, MAX_ENTRIES) || entryCount != expectedCount) {
            return invalid(Status.INVALID_COUNT, io.totalRecords, bytesRead);
        }

        ChunkedReader in = new ChunkedReader(io, extensionStart + HEADER_BYTES,
                extensionBytes - HEADER_BYTES);
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
        List<byte[]> keys = new ArrayList<>((int) entryCount);
        byte[] previous = null;
        long payloadRemaining = payloadLength;
        for (int i = 0; i < entryCount; i++) {
            if (payloadRemaining < 2) {
                return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead + in.bytesRead());
            }
            int keyLength = in.readUnsignedShort(crc);
            payloadRemaining -= 2;
            if (keyLength > payloadRemaining) {
                return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead + in.bytesRead());
            }
            byte[] key = new byte[keyLength];
            in.read(key, crc);
            payloadRemaining -= keyLength;
            if (previous != null && Arrays.compareUnsigned(key, previous) < 0) {
                return invalid(Status.INVALID_ORDER, io.totalRecords, bytesRead + in.bytesRead());
            }
            keys.add(key);
            previous = key;
        }
        if (payloadRemaining != 0) {
            return invalid(Status.INVALID_LENGTH, io.totalRecords, bytesRead + in.bytesRead());
        }
        int expectedCrc = in.readInt();
        bytesRead += in.bytesRead();
        if ((int) crc.getValue() != expectedCrc) {
            return invalid(Status.INVALID_CRC, io.totalRecords, bytesRead);
        }
        if (keys.isEmpty()) {
            if (bounds.min.length != 0 || bounds.max.length != 0) {
                return invalid(Status.INVALID_BOUNDS, io.totalRecords, bytesRead);
            }
        } else if (!Arrays.equals(keys.getFirst(), bounds.min)
                || Arrays.compareUnsigned(keys.getLast(), bounds.max) > 0) {
            return invalid(Status.INVALID_BOUNDS, io.totalRecords, bytesRead);
        }
        return new ReadResult(Status.EMBEDDED, List.copyOf(keys), io.totalRecords, bytesRead);
    }

    private static Bounds readBounds(PageRunSegmentIo io, long fixedTailStart) throws IOException {
        long pos = io.trailerStart;
        if (pos < PageRunSegmentWriter.HEADER_BYTES || fixedTailStart - pos < 4) {
            return null;
        }
        int minLength = io.readAt(pos, 2).getShort() & 0xFFFF;
        pos += 2;
        if (minLength > fixedTailStart - pos) {
            return null;
        }
        byte[] min = io.readAt(pos, minLength).array();
        pos += minLength;
        if (pos > fixedTailStart - 2) {
            return null;
        }
        int maxLength = io.readAt(pos, 2).getShort() & 0xFFFF;
        pos += 2;
        if (maxLength > fixedTailStart - pos) {
            return null;
        }
        byte[] max = io.readAt(pos, maxLength).array();
        pos += maxLength;
        return new Bounds(min, max, pos);
    }

    private static ReadResult invalid(Status status, long totalRecords, long bytesRead) {
        return new ReadResult(status, List.of(), totalRecords, bytesRead);
    }

    private record Bounds(byte[] min, byte[] max, long extensionStart) {
    }

    private static final class ChunkedWriter {
        private final WritableByteChannel channel;
        private final ByteBuffer buffer = ByteBuffer.allocate(IO_BUFFER_BYTES);

        private ChunkedWriter(WritableByteChannel channel) {
            this.channel = channel;
        }

        private void write(byte[] bytes) throws IOException {
            int offset = 0;
            while (offset < bytes.length) {
                if (!buffer.hasRemaining()) {
                    flush();
                }
                int length = Math.min(buffer.remaining(), bytes.length - offset);
                buffer.put(bytes, offset, length);
                offset += length;
            }
        }

        private void writeShort(int value) throws IOException {
            if (buffer.remaining() < Short.BYTES) {
                flush();
            }
            buffer.putShort((short) value);
        }

        private void writeInt(int value) throws IOException {
            if (buffer.remaining() < Integer.BYTES) {
                flush();
            }
            buffer.putInt(value);
        }

        private void flush() throws IOException {
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            buffer.clear();
        }
    }

    private static final class ChunkedReader {
        private final PageRunSegmentIo io;
        private final ByteBuffer buffer = ByteBuffer.allocate(IO_BUFFER_BYTES);
        private long position;
        private long remaining;
        private long bytesRead;

        private ChunkedReader(PageRunSegmentIo io, long position, long length) {
            this.io = io;
            this.position = position;
            this.remaining = length;
            buffer.limit(0);
        }

        private int readUnsignedShort(CRC32C crc) throws IOException {
            int high = readByte();
            int low = readByte();
            crc.update(high);
            crc.update(low);
            return high << 8 | low;
        }

        private int readInt() throws IOException {
            return readByte() << 24 | readByte() << 16 | readByte() << 8 | readByte();
        }

        private void read(byte[] destination, CRC32C crc) throws IOException {
            int offset = 0;
            while (offset < destination.length) {
                refillIfEmpty();
                int length = Math.min(buffer.remaining(), destination.length - offset);
                buffer.get(destination, offset, length);
                crc.update(destination, offset, length);
                offset += length;
            }
        }

        private int readByte() throws IOException {
            refillIfEmpty();
            return buffer.get() & 0xff;
        }

        private void refillIfEmpty() throws IOException {
            if (buffer.hasRemaining()) {
                return;
            }
            if (remaining == 0) {
                throw new IOException("unexpected end of page-run boundary extension");
            }
            int length = (int) Math.min(IO_BUFFER_BYTES, remaining);
            buffer.clear().limit(length);
            io.readAt(position, buffer);
            buffer.flip();
            position += length;
            remaining -= length;
            bytesRead += length;
        }

        private long bytesRead() {
            return bytesRead;
        }
    }
}
