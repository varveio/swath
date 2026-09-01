/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Self-contained persisted page layout and one-pass structural header parser. Parsed headers retain
 * offsets into their immutable owning record body; they never copy the stored payload.
 */
final class PageBlockCodec {

    static final ListEntryComparator ENTRY_COMPARATOR = new ListEntryComparator();

    static final byte TAG_OBJECT = 0;
    static final byte TAG_COMMON_PREFIX = 1;
    static final byte TAG_DELETE_MARKER = 2;

    static final byte ETAG_NULL = 0;
    static final byte ETAG_PACKED_MD5 = 1;
    static final byte ETAG_RAW = 2;

    enum DictColumn {
        STORAGE_CLASS,
        CHECKSUM_ALGORITHM,
        CHECKSUM_TYPE,
        OWNER_ID,
        OWNER_DISPLAY_NAME
    }

    static final int DICT_COLUMN_COUNT = DictColumn.values().length;
    /** Raw contents of the two fixed five-int arrays in a persisted dictionary header. */
    static final int PERSISTED_DICTIONARY_COORDINATE_DATA_BYTES =
            2 * DICT_COLUMN_COUNT * Integer.BYTES;
    /** Conservative heap reservation including both array headers and the coordinate owner. */
    static final int PERSISTED_DICTIONARY_COORDINATE_BYTES =
            PERSISTED_DICTIONARY_COORDINATE_DATA_BYTES + 88;
    /**
     * Maximum fixed heap added when every persisted dictionary slot materializes. The variable
     * UTF-16 character storage is covered by two times the encoded dictionary bytes in the record;
     * this term covers cache arrays, String/backing-array headers, and dictionary coordinates.
     */
    static final long MAX_PERSISTED_DICTIONARY_OVERHEAD_BYTES =
            16L + (long) DICT_COLUMN_COUNT * Long.BYTES
                    + (long) DICT_COLUMN_COUNT
                    * (16L + (long) PageBlock.DICT_CAP * Long.BYTES)
                    + (long) DICT_COLUMN_COUNT * PageBlock.DICT_CAP * 64L
                    + PERSISTED_DICTIONARY_COORDINATE_BYTES;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private PageBlockCodec() {
    }

    /**
     * Serialize the record body in its stable big-endian layout. The payload is already compressed;
     * min/max keys, dictionary tables, mode bits, counts, and lengths remain plain.
     */
    static byte[] serialize(PageBlock block) {
        PageBlockDictionaries dictionaries = block.dictionariesUnsafe();
        byte[][][] packedDictionaryBytes = dictionaries.byteBacked()
                ? null : new byte[DICT_COLUMN_COUNT][][];
        int dictTablesSize = 0;
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            int size = 2;
            if (packedDictionaryBytes != null) {
                packedDictionaryBytes[i] = new byte[dictionaries.size(i)][];
            }
            for (int j = 0; j < dictionaries.size(i); j++) {
                byte[] packedBytes = packedDictionaryBytes == null ? null
                        : dictionaries.packedValue(i, j).getBytes(StandardCharsets.UTF_8);
                if (packedDictionaryBytes != null) {
                    packedDictionaryBytes[i][j] = packedBytes;
                }
                int length = packedBytes == null
                        ? dictionaries.encodedLength(i, j) : packedBytes.length;
                if (length > 0xFFFF) {
                    throw new IllegalArgumentException(
                            "dictionary value exceeds the persisted u16 length limit");
                }
                size += 2 + length;
            }
            dictTablesSize += size;
        }

        byte[] firstKey = block.firstKeyUnsafe();
        byte[] lastKey = block.lastKeyUnsafe();
        requireWriterKey(firstKey, "first key");
        requireWriterKey(lastKey, "last key");
        byte[] payloadOwner = block.payloadOwnerUnsafe();
        int payloadOffset = block.payloadOffset();
        int payloadLength = block.payloadLength();
        int total = 2 + firstKey.length + 2 + lastKey.length
                + 4 + 1 + dictTablesSize + 1 + 1 + 4 + 4 + payloadLength;
        ByteBuffer buffer = ByteBuffer.allocate(total);
        putLenBytes(buffer, firstKey);
        putLenBytes(buffer, lastKey);
        buffer.putInt(block.count());
        buffer.put((byte) (block.orderedUnderFullComparator() ? 1 : 0));
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            buffer.putShort((short) dictionaries.size(i));
            for (int j = 0; j < dictionaries.size(i); j++) {
                if (packedDictionaryBytes == null) {
                    dictionaries.writeEncoded(buffer, i, j);
                } else {
                    putLenBytes(buffer, packedDictionaryBytes[i][j]);
                }
            }
        }
        int packedUseDict = 0;
        boolean[] useDict = block.useDictUnsafe();
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            if (useDict[i]) {
                packedUseDict |= 1 << i;
            }
        }
        buffer.put((byte) packedUseDict);
        buffer.put(block.codec().code());
        buffer.putInt(block.rawPayloadLength());
        buffer.putInt(payloadLength);
        buffer.put(payloadOwner, payloadOffset, payloadLength);
        return buffer.array();
    }

    static PageBlock deserialize(byte[] record, Path sourcePath) {
        return deserialize(record, parseHeader(record), sourcePath);
    }

    /** Build a persisted block from the already-parsed header and its owning record body. */
    static PageBlock deserialize(byte[] record, Header header, Path sourcePath) {
        return new PageBlock(header, record, null, null, sourcePath);
    }

    /**
     * Structurally validated page metadata plus a slice into the owning serialized record body.
     * The header owns only decoded key bounds and fixed-size dictionary table coordinates. Persisted
     * dictionary strings stay as validated byte slices in the immutable record body and are decoded
     * only if a row references them. {@code payloadOffset}/{@code payloadLength} remain valid for as
     * long as the caller retains the body passed to {@link #parseHeader(byte[])}.
     */
    record Header(byte[] minKey, byte[] maxKey, int count, boolean ordered,
                  PageBlockDictionaries dictionaries, boolean[] useDict, PageCodec codec,
                  int rawPayloadLength, int payloadOffset, int payloadLength) {
    }

    /** Routing fields available without reading the stored page payload. */
    record RoutingHeader(byte[] minKey, byte[] maxKey, int count, int rawPayloadLength) {
    }

    @FunctionalInterface
    interface RoutingInput {
        ByteBuffer read(int position, int bytes) throws IOException;
    }

    /**
     * Parse only metadata needed by reference routing. Dictionary values and the stored payload are
     * skipped by validated length, so the header pass does not materialize or CRC-read page bodies.
     */
    static RoutingHeader parseRoutingHeader(int recordLength, RoutingInput input)
            throws IOException {
        return PageBlockRoutingParser.parse(recordLength, input);
    }

    /** Validate every header length, count, and mode and return a zero-copy payload slice. */
    static Header parseHeader(byte[] record) {
        ByteBuffer buffer = ByteBuffer.wrap(record);
        byte[] minKey = getBoundedKey(buffer, "minKey");
        byte[] maxKey = getBoundedKey(buffer, "maxKey");
        requireRemaining(buffer, 5, "count and ordered flag");
        int count = buffer.getInt();
        if (count <= 0) {
            throw malformed("count must be positive, got " + count);
        }
        byte orderedByte = buffer.get();
        if (orderedByte != 0 && orderedByte != 1) {
            throw malformed("ordered flag must be 0 or 1, got " + (orderedByte & 0xFF));
        }

        int[] dictionaryStarts = new int[DICT_COLUMN_COUNT];
        int[] dictionaryCounts = new int[DICT_COLUMN_COUNT];
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            requireRemaining(buffer, 2, "dictionary count");
            int countValues = buffer.getShort() & 0xFFFF;
            if (countValues > PageBlock.DICT_CAP) {
                throw malformed("dictionary " + i + " count " + countValues + " exceeds "
                        + PageBlock.DICT_CAP);
            }
            dictionaryStarts[i] = buffer.position();
            dictionaryCounts[i] = countValues;
            for (int j = 0; j < countValues; j++) {
                requireRemaining(buffer, Short.BYTES, "dictionary value length");
                int length = buffer.getShort() & 0xFFFF;
                requireRemaining(buffer, length, "dictionary value");
                validateUtf8(record, buffer.position(), length, "dictionary value");
                buffer.position(buffer.position() + length);
            }
        }
        PageBlockDictionaries dictionaries = PageBlockDictionaries.persisted(
                record, dictionaryStarts, dictionaryCounts);

        requireRemaining(buffer, 10, "page modes and payload lengths");
        int packedUseDict = buffer.get() & 0xFF;
        int validUseDictBits = (1 << DICT_COLUMN_COUNT) - 1;
        if ((packedUseDict & ~validUseDictBits) != 0) {
            throw malformed("useDict contains unknown bits: 0x"
                    + Integer.toHexString(packedUseDict));
        }
        boolean[] useDict = new boolean[DICT_COLUMN_COUNT];
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            useDict[i] = (packedUseDict & (1 << i)) != 0;
        }

        PageCodec codec;
        byte codecCode = buffer.get();
        try {
            codec = PageCodec.fromCode(codecCode);
        } catch (IllegalStateException e) {
            throw malformed("unsupported codec " + (codecCode & 0xFF), e);
        }
        int rawPayloadLength = buffer.getInt();
        int storedPayloadLength = buffer.getInt();
        if (rawPayloadLength <= 0 || rawPayloadLength > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
            throw malformed("raw payload length " + rawPayloadLength + " is outside 1.."
                    + PageBlock.MAX_RAW_PAYLOAD_BYTES);
        }
        if (storedPayloadLength <= 0 || storedPayloadLength != buffer.remaining()) {
            throw malformed("stored payload length " + storedPayloadLength
                    + " does not equal remaining body bytes " + buffer.remaining());
        }
        return new Header(minKey, maxKey, count, orderedByte == 1, dictionaries, useDict,
                codec, rawPayloadLength, buffer.position(), storedPayloadLength);
    }

    static IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("malformed PageBlock: " + message);
    }

    static String unpackMd5(byte[] bytes, int offset) {
        char[] hex = new char[32];
        for (int i = 0; i < 16; i++) {
            int value = bytes[offset + i] & 0xFF;
            hex[i * 2] = HEX[value >>> 4];
            hex[i * 2 + 1] = HEX[value & 0x0F];
        }
        return new String(hex);
    }

    /** Decode a persisted string only after strict, allocation-free UTF-8 validation. */
    static String decodeUtf8Strict(byte[] bytes, int offset, int length, String field) {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw malformed(field + " exceeds its owning byte array");
        }
        validateUtf8(bytes, offset, length, field);
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    /** Reject overlong forms, isolated continuations, surrogate encodings, and code points > U+10FFFF. */
    private static void validateUtf8(byte[] bytes, int offset, int length, String field) {
        int end = offset + length;
        for (int p = offset; p < end; ) {
            int first = bytes[p] & 0xFF;
            int width;
            int secondMin = 0x80;
            int secondMax = 0xBF;
            if (first <= 0x7F) {
                p++;
                continue;
            } else if (first >= 0xC2 && first <= 0xDF) {
                width = 2;
            } else if (first >= 0xE0 && first <= 0xEF) {
                width = 3;
                if (first == 0xE0) {
                    secondMin = 0xA0;
                } else if (first == 0xED) {
                    secondMax = 0x9F;
                }
            } else if (first >= 0xF0 && first <= 0xF4) {
                width = 4;
                if (first == 0xF0) {
                    secondMin = 0x90;
                } else if (first == 0xF4) {
                    secondMax = 0x8F;
                }
            } else {
                throw malformedUtf8(field, p - offset);
            }
            if (p > end - width) {
                throw malformedUtf8(field, p - offset);
            }
            int second = bytes[p + 1] & 0xFF;
            if (second < secondMin || second > secondMax) {
                throw malformedUtf8(field, p - offset);
            }
            for (int i = 2; i < width; i++) {
                int continuation = bytes[p + i] & 0xFF;
                if (continuation < 0x80 || continuation > 0xBF) {
                    throw malformedUtf8(field, p - offset);
                }
            }
            p += width;
        }
    }

    private static IllegalArgumentException malformedUtf8(String field, int relativeOffset) {
        return malformed(field + " contains malformed UTF-8 at byte " + relativeOffset);
    }

    private static void putLenBytes(ByteBuffer buffer, byte[] bytes) {
        if (bytes.length > 0xFFFF) {
            throw new IllegalArgumentException("value exceeds the persisted u16 length limit");
        }
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static byte[] getBoundedLenBytes(ByteBuffer buffer, String field) {
        requireRemaining(buffer, 2, field + " length");
        int length = buffer.getShort() & 0xFFFF;
        requireRemaining(buffer, length, field);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private static byte[] getBoundedKey(ByteBuffer buffer, String field) {
        byte[] key = getBoundedLenBytes(buffer, field);
        if (key.length > ByteMidpoint.MAX_KEY_LEN) {
            throw malformed(field + " length " + key.length + " exceeds the S3 key limit of "
                    + ByteMidpoint.MAX_KEY_LEN + " bytes");
        }
        return key;
    }

    static void requireWriterKey(byte[] key, String field) {
        if (key.length > ByteMidpoint.MAX_KEY_LEN) {
            throw new IllegalArgumentException(field + " length " + key.length
                    + " exceeds the S3 key limit of " + ByteMidpoint.MAX_KEY_LEN + " bytes");
        }
    }

    private static void requireRemaining(ByteBuffer buffer, int needed, String field) {
        if (needed < 0 || buffer.remaining() < needed) {
            throw malformed(field + " exceeds record body (needed " + needed
                    + ", remaining " + buffer.remaining() + ")");
        }
    }

    static IllegalArgumentException malformed(String message, Throwable cause) {
        return new IllegalArgumentException("malformed PageBlock: " + message, cause);
    }

}
