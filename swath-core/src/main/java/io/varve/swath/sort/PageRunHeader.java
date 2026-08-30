/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Encoder/parser for the versioned, extensible page-run header envelope. */
final class PageRunHeader {

    static final short HEADER_VERSION = 1;
    static final int PREFIX_BYTES = Integer.BYTES + Short.BYTES + Short.BYTES + Integer.BYTES;
    static final int CRC_BYTES = Integer.BYTES;
    static final int CURRENT_BYTES = PREFIX_BYTES + Short.BYTES + Short.BYTES + 1 + CRC_BYTES;

    private static final int MAX_METADATA_BYTES = 1024 * 1024;
    private static final short ORDERING_MODE_FIELD = 1;
    private static final short OBJECTS_VALUE = 1;
    private static final short VERSIONS_VALUE = 2;

    private PageRunHeader() {
    }

    record Header(short formatVersion, SortMode orderingMode, int encodedBytes) {
    }

    static int write(FileChannel channel, SortMode orderingMode) throws IOException {
        Objects.requireNonNull(orderingMode, "orderingMode");
        int metadataLength = Short.BYTES + Short.BYTES + 1;
        ByteBuffer header = ByteBuffer.allocate(PREFIX_BYTES + metadataLength + CRC_BYTES);
        header.putInt(PageRunSegmentWriter.MAGIC);
        header.putShort(PageRunSegmentWriter.FORMAT_VERSION);
        header.putShort(HEADER_VERSION);
        header.putInt(metadataLength);
        header.putShort(ORDERING_MODE_FIELD);
        header.putShort((short) 1);
        header.put((byte) switch (orderingMode) {
            case OBJECTS -> OBJECTS_VALUE;
            case VERSIONS -> VERSIONS_VALUE;
        });
        CRC32C crc = new CRC32C();
        crc.update(header.array(), 0, header.position());
        header.putInt((int) crc.getValue());
        writeFully(channel, header.flip());
        return header.capacity();
    }

    static Header read(FileChannel channel, Path path, long fileSize, SortMetrics metrics)
            throws IOException {
        if (fileSize < PREFIX_BYTES + CRC_BYTES) {
            throw headerCorruption(path, metrics, "truncated page-run header");
        }
        ByteBuffer prefix = readAt(channel, path, 0, PREFIX_BYTES, metrics);
        int magic = prefix.getInt();
        short formatVersion = prefix.getShort();
        short headerVersion = prefix.getShort();
        int metadataLength = prefix.getInt();
        if (magic != PageRunSegmentWriter.MAGIC) {
            throw headerCorruption(path, metrics,
                    "bad page-run magic 0x" + Integer.toHexString(magic));
        }
        if (formatVersion != PageRunSegmentWriter.FORMAT_VERSION) {
            metrics.recordStealReason("SORT", "page_run_format_mismatch");
            throw new SegmentCorruptionException(path,
                    SegmentCorruptionException.PAGE_RUN_FORMAT_MISMATCH,
                    "unsupported page-run format version " + formatVersion);
        }
        if (headerVersion != HEADER_VERSION) {
            throw headerCorruption(path, metrics,
                    "unsupported page-run header envelope version " + headerVersion);
        }
        if (metadataLength < 0 || metadataLength > MAX_METADATA_BYTES
                || PREFIX_BYTES + (long) metadataLength + CRC_BYTES > fileSize) {
            throw headerCorruption(path, metrics,
                    "invalid page-run header metadata length " + metadataLength);
        }

        int encodedBytes = Math.addExact(PREFIX_BYTES, Math.addExact(metadataLength, CRC_BYTES));
        byte[] encoded = readAt(channel, path, 0, encodedBytes, metrics).array();
        int expectedCrc = ByteBuffer.wrap(encoded).getInt(encodedBytes - CRC_BYTES);
        CRC32C crc = new CRC32C();
        crc.update(encoded, 0, encodedBytes - CRC_BYTES);
        if ((int) crc.getValue() != expectedCrc) {
            throw headerCorruption(path, metrics, "page-run header CRC32C mismatch");
        }

        ByteBuffer metadata = ByteBuffer.wrap(encoded, PREFIX_BYTES, metadataLength).slice();
        SortMode orderingMode = null;
        while (metadata.hasRemaining()) {
            if (metadata.remaining() < Short.BYTES * 2) {
                throw headerCorruption(path, metrics, "truncated page-run header TLV");
            }
            int fieldId = Short.toUnsignedInt(metadata.getShort());
            int fieldLength = Short.toUnsignedInt(metadata.getShort());
            if (fieldLength > metadata.remaining()) {
                throw headerCorruption(path, metrics,
                        "page-run header TLV length exceeds metadata envelope");
            }
            if (fieldId == ORDERING_MODE_FIELD) {
                if (orderingMode != null || fieldLength != 1) {
                    throw headerCorruption(path, metrics,
                            "invalid or duplicate page-run ordering-mode field");
                }
                int value = Byte.toUnsignedInt(metadata.get());
                orderingMode = switch (value) {
                    case OBJECTS_VALUE -> SortMode.OBJECTS;
                    case VERSIONS_VALUE -> SortMode.VERSIONS;
                    default -> throw headerCorruption(path, metrics,
                            "unknown page-run ordering mode " + value);
                };
            } else {
                metadata.position(metadata.position() + fieldLength);
            }
        }
        if (orderingMode == null) {
            throw headerCorruption(path, metrics, "page-run header has no ordering mode");
        }
        return new Header(formatVersion, orderingMode, encodedBytes);
    }

    private static SegmentCorruptionException headerCorruption(
            Path path, SortMetrics metrics, String message) {
        recordHeaderRejection(metrics);
        return new SegmentCorruptionException(path,
                SegmentCorruptionException.PAGE_RUN_HEADER_CORRUPTION, message);
    }

    private static void recordHeaderRejection(SortMetrics metrics) {
        metrics.recordStealReason("SORT", "page_run_header_corruption");
    }

    private static ByteBuffer readAt(FileChannel channel, Path path, long position, int bytes,
            SortMetrics metrics) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        long offset = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset);
            if (read < 0) {
                throw headerCorruption(path, metrics, "unexpected EOF reading page-run header");
            }
            offset += read;
        }
        return buffer.flip();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
