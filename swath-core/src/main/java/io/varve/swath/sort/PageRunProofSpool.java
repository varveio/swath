/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Fixed-slot temporary storage for exact variable-length physical-zone proof keys. */
final class PageRunProofSpool {

    private static final int OPEN = 1;
    private static final int FINISHED = 2;
    private static final int FIXED_BYTES = 56;
    private static final int KEY_SLOT_BYTES = Short.BYTES + ByteMidpoint.MAX_KEY_LEN;
    private static final int SLOT_BYTES = FIXED_BYTES + KeyField.values().length * KEY_SLOT_BYTES;

    enum KeyField {
        FIRST_MIN,
        LAST_MIN,
        ZONE_MAX,
        FIRST_SAMPLE_PREFIX,
        FIRST_SAMPLE_PAGE_MAX,
        ROLLING_SAMPLE_PREFIX
    }

    record Summary(long pages, long entries, long framedBytes, long firstFrameOffset,
                   long endFrameOffset, int verifiedSamples, boolean sampleMismatch,
                   byte[] firstMin, byte[] lastMin, byte[] zoneMax,
                   byte[] firstSamplePrefix, byte[] firstSamplePageMax) {
    }

    private PageRunProofSpool() {
    }

    static long logicalBytes(int segments) {
        return Math.multiplyExact((long) segments, SLOT_BYTES);
    }

    static final class Writer implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;

        Writer(Path path) throws IOException {
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        void markOpen(int segment) throws IOException {
            writeInt(slot(segment), OPEN);
        }

        void writeKey(int segment, KeyField field, byte[] key) throws IOException {
            if (key.length > ByteMidpoint.MAX_KEY_LEN) {
                throw new IOException("page-run proof key exceeds the S3 key limit: " + key.length);
            }
            ByteBuffer value = ByteBuffer.allocate(Short.BYTES + key.length)
                    .putShort((short) key.length).put(key).flip();
            writeFully(channel, value, keyOffset(segment, field));
        }

        byte[] readKey(int segment, KeyField field) throws IOException {
            return PageRunProofSpool.readKey(channel, path, keyOffset(segment, field));
        }

        void finish(int segment, long pages, long entries, long framedBytes,
                    long firstFrameOffset, long endFrameOffset, int verifiedSamples,
                    boolean sampleMismatch) throws IOException {
            ByteBuffer fields = ByteBuffer.allocate(FIXED_BYTES - Integer.BYTES);
            fields.putInt(sampleMismatch ? 1 : 0);
            fields.putLong(pages).putLong(entries).putLong(framedBytes);
            fields.putLong(firstFrameOffset).putLong(endFrameOffset);
            fields.putInt(verifiedSamples);
            while (fields.hasRemaining()) {
                fields.put((byte) 0);
            }
            writeFully(channel, fields.flip(), slot(segment) + Integer.BYTES);
            writeInt(slot(segment), FINISHED);
        }

        private void writeInt(long position, int value) throws IOException {
            writeFully(channel, ByteBuffer.allocate(Integer.BYTES).putInt(value).flip(), position);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    static final class Reader implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;

        Reader(Path path) throws IOException {
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
        }

        Summary read(int segment, boolean hasPages, boolean hasSamples) throws IOException {
            ByteBuffer fields = readAt(channel, path, slot(segment), FIXED_BYTES);
            int state = fields.getInt();
            if (state != FINISHED) {
                throw new IOException("page-run proof spool has incomplete segment summary "
                        + segment + " in " + path);
            }
            boolean mismatch = fields.getInt() != 0;
            long pages = fields.getLong();
            long entries = fields.getLong();
            long framedBytes = fields.getLong();
            long firstFrameOffset = fields.getLong();
            long endFrameOffset = fields.getLong();
            int verifiedSamples = fields.getInt();
            byte[] firstMin = hasPages ? readKey(segment, KeyField.FIRST_MIN) : null;
            byte[] lastMin = hasPages ? readKey(segment, KeyField.LAST_MIN) : null;
            byte[] zoneMax = hasPages ? readKey(segment, KeyField.ZONE_MAX) : null;
            byte[] firstSamplePrefix = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PREFIX) : null;
            byte[] firstSamplePageMax = hasSamples
                    ? readKey(segment, KeyField.FIRST_SAMPLE_PAGE_MAX) : null;
            return new Summary(pages, entries, framedBytes, firstFrameOffset, endFrameOffset,
                    verifiedSamples, mismatch, firstMin, lastMin, zoneMax,
                    firstSamplePrefix, firstSamplePageMax);
        }

        private byte[] readKey(int segment, KeyField field) throws IOException {
            return PageRunProofSpool.readKey(channel, path, keyOffset(segment, field));
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    static void delete(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    private static long slot(int segment) {
        return Math.multiplyExact((long) segment, SLOT_BYTES);
    }

    private static long keyOffset(int segment, KeyField field) {
        return slot(segment) + FIXED_BYTES + (long) field.ordinal() * KEY_SLOT_BYTES;
    }

    private static byte[] readKey(FileChannel channel, Path path, long position)
            throws IOException {
        ByteBuffer length = readAt(channel, path, position, Short.BYTES);
        int size = length.getShort() & 0xffff;
        if (size > ByteMidpoint.MAX_KEY_LEN) {
            throw new IOException("page-run proof spool key length out of bounds in " + path);
        }
        return readAt(channel, path, position + Short.BYTES, size).array();
    }

    private static ByteBuffer readAt(FileChannel channel, Path path, long position, int bytes)
            throws IOException {
        ByteBuffer target = ByteBuffer.allocate(bytes);
        long offset = position;
        while (target.hasRemaining()) {
            int read = channel.read(target, offset);
            if (read < 0) {
                throw new EOFException("unexpected EOF reading page-run proof spool " + path);
            }
            offset += read;
        }
        return target.flip();
    }

    private static void writeFully(FileChannel channel, ByteBuffer source, long position)
            throws IOException {
        long offset = position;
        while (source.hasRemaining()) {
            offset += channel.write(source, offset);
        }
    }
}
