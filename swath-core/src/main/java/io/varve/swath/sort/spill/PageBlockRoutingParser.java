/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Decode-free parser for the bounded metadata used by reference routing. */
final class PageBlockRoutingParser {

    private PageBlockRoutingParser() {
    }

    static PageBlockCodec.RoutingHeader parse(
            int recordLength, PageBlockCodec.RoutingInput input) throws IOException {
        Cursor cursor = new Cursor(recordLength, input);
        byte[] minKey = cursor.readKey("minKey");
        byte[] maxKey = cursor.readKey("maxKey");
        int count = cursor.readInt("count");
        if (count <= 0) {
            throw PageBlockCodec.malformed("count must be positive, got " + count);
        }
        int ordered = cursor.readByte("ordered flag") & 0xFF;
        if (ordered > 1) {
            throw PageBlockCodec.malformed("ordered flag must be 0 or 1, got " + ordered);
        }
        for (int dictionary = 0; dictionary < PageBlockCodec.DICT_COLUMN_COUNT; dictionary++) {
            int values = cursor.readUnsignedShort("dictionary count");
            if (values > PageBlock.DICT_CAP) {
                throw PageBlockCodec.malformed("dictionary " + dictionary + " count " + values
                        + " exceeds " + PageBlock.DICT_CAP);
            }
            for (int value = 0; value < values; value++) {
                cursor.skip(cursor.readUnsignedShort("dictionary value length"),
                        "dictionary value");
            }
        }
        int packedUseDict = cursor.readByte("useDict") & 0xFF;
        int validUseDictBits = (1 << PageBlockCodec.DICT_COLUMN_COUNT) - 1;
        if ((packedUseDict & ~validUseDictBits) != 0) {
            throw PageBlockCodec.malformed("useDict contains unknown bits: 0x"
                    + Integer.toHexString(packedUseDict));
        }
        int codecCode = cursor.readByte("codec") & 0xFF;
        PageCodec codec;
        try {
            codec = PageCodec.fromCode((byte) codecCode);
        } catch (IllegalStateException e) {
            throw PageBlockCodec.malformed("unsupported codec " + codecCode, e);
        }
        int rawPayloadLength = cursor.readInt("raw payload length");
        int storedPayloadLength = cursor.readInt("stored payload length");
        if (rawPayloadLength <= 0 || rawPayloadLength > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
            throw PageBlockCodec.malformed("raw payload length " + rawPayloadLength
                    + " is outside 1.." + PageBlock.MAX_RAW_PAYLOAD_BYTES);
        }
        if (storedPayloadLength <= 0 || storedPayloadLength != cursor.remaining()) {
            throw PageBlockCodec.malformed("stored payload length " + storedPayloadLength
                    + " does not equal remaining body bytes " + cursor.remaining());
        }
        if (codec == PageCodec.NONE && storedPayloadLength != rawPayloadLength) {
            throw PageBlockCodec.malformed("NONE payload lengths differ: raw=" + rawPayloadLength
                    + " stored=" + storedPayloadLength);
        }
        return new PageBlockCodec.RoutingHeader(minKey, maxKey, count, rawPayloadLength);
    }

    private static final class Cursor {
        private static final int READ_AHEAD_BYTES = 8 << 10;

        private final int length;
        private final PageBlockCodec.RoutingInput input;
        private ByteBuffer cache = ByteBuffer.allocate(0);
        private int cacheStart;
        private int position;

        Cursor(int length, PageBlockCodec.RoutingInput input) {
            if (length <= 0) {
                throw PageBlockCodec.malformed("record body must be non-empty");
            }
            this.length = length;
            this.input = input;
        }

        byte[] readKey(String field) throws IOException {
            int keyLength = readUnsignedShort(field + " length");
            if (keyLength > ByteMidpoint.MAX_KEY_LEN) {
                throw PageBlockCodec.malformed(field + " exceeds the S3 key limit of "
                        + ByteMidpoint.MAX_KEY_LEN + " bytes");
            }
            return readBytes(keyLength, field);
        }

        int readUnsignedShort(String field) throws IOException {
            ensure(Short.BYTES, field);
            int value = cache.getShort(position - cacheStart) & 0xFFFF;
            position += Short.BYTES;
            return value;
        }

        int readInt(String field) throws IOException {
            ensure(Integer.BYTES, field);
            int value = cache.getInt(position - cacheStart);
            position += Integer.BYTES;
            return value;
        }

        byte readByte(String field) throws IOException {
            ensure(1, field);
            return cache.get(position++ - cacheStart);
        }

        byte[] readBytes(int bytes, String field) throws IOException {
            ensure(bytes, field);
            byte[] result = new byte[bytes];
            ByteBuffer view = cache.duplicate();
            view.position(position - cacheStart).limit(position - cacheStart + bytes);
            view.get(result);
            position += bytes;
            return result;
        }

        void skip(int bytes, String field) {
            if (bytes < 0 || position > length - bytes) {
                throw PageBlockCodec.malformed(field + " exceeds record body");
            }
            position += bytes;
        }

        int remaining() {
            return length - position;
        }

        private void ensure(int bytes, String field) throws IOException {
            if (bytes < 0 || position > length - bytes) {
                throw PageBlockCodec.malformed(field + " exceeds record body");
            }
            int cacheEnd = cacheStart + cache.limit();
            if (position >= cacheStart && position + bytes <= cacheEnd) {
                return;
            }
            int read = Math.min(length - position, Math.max(bytes, READ_AHEAD_BYTES));
            cache = input.read(position, read);
            cacheStart = position;
            if (cache.remaining() != read) {
                throw PageBlockCodec.malformed("short metadata read");
            }
        }
    }
}
