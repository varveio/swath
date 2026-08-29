/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Self-contained persisted page layout, one-pass structural header parser, and front-coded payload
 * writer. Parsed headers retain offsets into their immutable owning record body; they never copy
 * the stored payload.
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
    private static final byte[] EMPTY_KEY = new byte[0];
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private PageBlockCodec() {
    }

    /**
     * Serialize the record body in its stable big-endian layout. The payload is already compressed;
     * min/max keys, dictionary tables, mode bits, counts, and lengths remain plain.
     */
    static byte[] serialize(PageBlock block) {
        Dictionaries dictionaries = block.dictionariesUnsafe();
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
        return new PageBlock(header, record, null, null, header.payloadLength(), sourcePath);
    }

    /**
     * Structurally validated page metadata plus a slice into the owning serialized record body.
     * The header owns only decoded key bounds and fixed-size dictionary table coordinates. Persisted
     * dictionary strings stay as validated byte slices in the immutable record body and are decoded
     * only if a row references them. {@code payloadOffset}/{@code payloadLength} remain valid for as
     * long as the caller retains the body passed to {@link #parseHeader(byte[])}.
     */
    record Header(byte[] minKey, byte[] maxKey, int count, boolean ordered,
                  Dictionaries dictionaries, boolean[] useDict, PageCodec codec,
                  int rawPayloadLength, int payloadOffset, int payloadLength) {
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
        Dictionaries dictionaries = Dictionaries.persisted(
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

    /** Packed Java dictionaries or byte-backed persisted dictionary-table coordinates. */
    static final class Dictionaries {
        private final String[][] packed;
        private final byte[] owner;
        private final int[] starts;
        private final int[] counts;

        private Dictionaries(String[][] packed, byte[] owner, int[] starts, int[] counts) {
            this.packed = packed;
            this.owner = owner;
            this.starts = starts;
            this.counts = counts;
        }

        static Dictionaries packed(String[][] values) {
            return new Dictionaries(values, null, null, null);
        }

        static Dictionaries persisted(byte[] owner, int[] starts, int[] counts) {
            return new Dictionaries(null, owner, starts, counts);
        }

        int size(int column) {
            return packed != null ? packed[column].length : counts[column];
        }

        boolean byteBacked() {
            return owner != null;
        }

        String packedValue(int column, int index) {
            if (packed == null) {
                throw new IllegalStateException("persisted dictionary has no packed String value");
            }
            return packed[column][index];
        }

        int coordinateBytes() {
            return owner == null ? 0 : PERSISTED_DICTIONARY_COORDINATE_BYTES;
        }

        /** Conservative retained heap for lazily decoded persisted dictionary strings and caches. */
        long decodedCacheBudgetBytes() {
            if (owner == null) {
                return 0;
            }
            int totalValues = 0;
            for (int count : counts) {
                totalValues += count;
            }
            if (totalValues == 0) {
                return 0;
            }
            long bytes = 16L + (long) DICT_COLUMN_COUNT * Long.BYTES;
            for (int column = 0; column < DICT_COLUMN_COUNT; column++) {
                bytes += 16L + (long) size(column) * Long.BYTES;
                for (int index = 0; index < size(column); index++) {
                    // UTF-16 needs at most two bytes per validated UTF-8 input byte. The 64-byte
                    // allowance covers the String and backing-array headers/alignment.
                    bytes += 2L * encodedLength(column, index) + 64L;
                }
            }
            return bytes;
        }

        String value(int column, int index) {
            if (index < 0 || index >= size(column)) {
                throw new IndexOutOfBoundsException(index);
            }
            if (packed != null) {
                return packed[column][index];
            }
            Slice slice = locate(column, index);
            return decodeUtf8Strict(owner, slice.offset(), slice.length(),
                    "dictionary " + column + " value " + index);
        }

        int encodedLength(int column, int index) {
            return packed != null
                    ? packed[column][index].getBytes(StandardCharsets.UTF_8).length
                    : locate(column, index).length();
        }

        void writeEncoded(ByteBuffer target, int column, int index) {
            if (packed != null) {
                putLenBytes(target, packed[column][index].getBytes(StandardCharsets.UTF_8));
                return;
            }
            Slice slice = locate(column, index);
            target.putShort((short) slice.length());
            target.put(owner, slice.offset(), slice.length());
        }

        private Slice locate(int column, int target) {
            int position = starts[column];
            for (int index = 0; index <= target; index++) {
                int length = ((owner[position] & 0xFF) << 8) | (owner[position + 1] & 0xFF);
                position += Short.BYTES;
                if (index == target) {
                    return new Slice(position, length);
                }
                position += length;
            }
            throw new AssertionError("dictionary target was bounds-checked");
        }

        private record Slice(int offset, int length) {
        }
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

    private static void requireWriterKey(byte[] key, String field) {
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

    private static IllegalArgumentException malformed(String message, Throwable cause) {
        return new IllegalArgumentException("malformed PageBlock: " + message, cause);
    }

    /** Front-coded row payload writer with stable first-seen dictionary ordering. */
    static final class Writer {

        private byte[] buffer = new byte[256];
        private int length;
        private final Dict[] dicts;
        private final boolean[] useDict;
        private final int maxRawPayloadBytes;
        private byte[] previousKey = EMPTY_KEY;

        Writer(int hint, boolean[] useDict) {
            this(hint, useDict, PageBlock.MAX_RAW_PAYLOAD_BYTES);
        }

        Writer(int hint, boolean[] useDict, int maxRawPayloadBytes) {
            if (hint > 8) {
                buffer = new byte[(int) Math.min(
                        Math.min((long) hint * 48, 1 << 20), maxRawPayloadBytes)];
            }
            this.useDict = useDict;
            this.maxRawPayloadBytes = maxRawPayloadBytes;
            this.dicts = new Dict[DICT_COLUMN_COUNT];
            for (int i = 0; i < dicts.length; i++) {
                dicts[i] = new Dict();
            }
        }

        void write(ListEntry entry) {
            switch (entry) {
                case ObjectEntry object -> writeObject(object);
                case CommonPrefixEntry prefix -> {
                    tag(TAG_COMMON_PREFIX);
                    key(prefix.key());
                }
                case DeleteMarkerEntry marker -> writeDeleteMarker(marker);
            }
        }

        private void writeObject(ObjectEntry object) {
            tag(TAG_OBJECT);
            key(object.key());
            fixedLong(object.size());
            fixedLong(object.lastModifiedEpochMicros());
            etag(object.etag());
            dictOrRaw(DictColumn.STORAGE_CLASS, object.storageClass());
            nullableString(object.versionId());
            bool(object.isLatest());
            dictOrRaw(DictColumn.OWNER_ID, object.ownerId());
            dictOrRaw(DictColumn.OWNER_DISPLAY_NAME, object.ownerDisplayName());
            dictOrRaw(DictColumn.CHECKSUM_ALGORITHM, object.checksumAlgorithm());
            dictOrRaw(DictColumn.CHECKSUM_TYPE, object.checksumType());
        }

        private void writeDeleteMarker(DeleteMarkerEntry marker) {
            tag(TAG_DELETE_MARKER);
            key(marker.key());
            nullableString(marker.versionId());
            bool(marker.isLatest());
            fixedLong(marker.lastModifiedEpochMicros());
            dictOrRaw(DictColumn.OWNER_ID, marker.ownerId());
        }

        private void tag(byte tag) {
            put(tag);
        }

        private void key(KeyBytes key) {
            byte[] raw = key.rawUnsafe();
            requireWriterKey(raw, "row key");
            int shared = commonPrefixLength(previousKey, raw);
            int suffixLength = raw.length - shared;
            varint(shared);
            varint(suffixLength);
            write(raw, shared, suffixLength);
            previousKey = raw;
        }

        private void fixedLong(long value) {
            ensure(8);
            buffer[length++] = (byte) (value >>> 56);
            buffer[length++] = (byte) (value >>> 48);
            buffer[length++] = (byte) (value >>> 40);
            buffer[length++] = (byte) (value >>> 32);
            buffer[length++] = (byte) (value >>> 24);
            buffer[length++] = (byte) (value >>> 16);
            buffer[length++] = (byte) (value >>> 8);
            buffer[length++] = (byte) value;
        }

        private void bool(boolean value) {
            put((byte) (value ? 1 : 0));
        }

        private void nullableString(String value) {
            if (value == null) {
                varint(0);
                return;
            }
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            varint(bytes.length + 1);
            write(bytes);
        }

        private void etag(String etag) {
            if (etag == null) {
                put(ETAG_NULL);
                return;
            }
            byte[] packed = tryPackMd5(etag);
            if (packed != null) {
                put(ETAG_PACKED_MD5);
                write(packed);
            } else {
                put(ETAG_RAW);
                byte[] bytes = etag.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length);
                write(bytes);
            }
        }

        private void dictOrRaw(DictColumn column, String value) {
            if (value == null) {
                varint(0);
                return;
            }
            if (useDict[column.ordinal()]) {
                varint(dicts[column.ordinal()].indexOf(value) + 1);
            } else {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length + 1);
                write(bytes);
            }
        }

        String[][] dictArrays() {
            String[][] result = new String[dicts.length][];
            for (int i = 0; i < dicts.length; i++) {
                result[i] = dicts[i].toArray();
            }
            return result;
        }

        byte[] toBytes() {
            byte[] result = new byte[length];
            System.arraycopy(buffer, 0, result, 0, length);
            return result;
        }

        private void varint(int value) {
            int remaining = value;
            while ((remaining & ~0x7F) != 0) {
                put((byte) ((remaining & 0x7F) | 0x80));
                remaining >>>= 7;
            }
            put((byte) remaining);
        }

        private void put(byte value) {
            ensure(1);
            buffer[length++] = value;
        }

        private void write(byte[] bytes) {
            write(bytes, 0, bytes.length);
        }

        private void write(byte[] bytes, int offset, int writeLength) {
            ensure(writeLength);
            System.arraycopy(bytes, offset, buffer, length, writeLength);
            length += writeLength;
        }

        private void ensure(int extra) {
            if (extra < 0 || extra > maxRawPayloadBytes - length) {
                throw new PageBlock.RawPayloadLimitException(maxRawPayloadBytes);
            }
            int required = length + extra;
            if (required <= buffer.length) {
                return;
            }
            int capacity = buffer.length;
            while (capacity < required) {
                capacity = Math.min(maxRawPayloadBytes,
                        Math.max(capacity + 1, capacity << 1));
            }
            byte[] grown = new byte[capacity];
            System.arraycopy(buffer, 0, grown, 0, length);
            buffer = grown;
        }
    }

    private static int commonPrefixLength(byte[] first, byte[] second) {
        int length = Math.min(first.length, second.length);
        int index = 0;
        while (index < length && first[index] == second[index]) {
            index++;
        }
        return index;
    }

    private static byte[] tryPackMd5(String etag) {
        if (etag.length() != 32) {
            return null;
        }
        byte[] result = new byte[16];
        for (int i = 0; i < 16; i++) {
            int high = hexValue(etag.charAt(i * 2));
            int low = hexValue(etag.charAt(i * 2 + 1));
            if (high < 0 || low < 0) {
                return null;
            }
            result[i] = (byte) ((high << 4) | low);
        }
        return result;
    }

    private static int hexValue(char value) {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return value - 'a' + 10;
        }
        return -1;
    }

    private static final class Dict {
        private final List<String> values = new ArrayList<>();
        private final Map<String, Integer> index = new HashMap<>();

        int indexOf(String value) {
            Integer existing = index.get(value);
            if (existing == null) {
                if (value.getBytes(StandardCharsets.UTF_8).length > 0xFFFF) {
                    throw new IllegalArgumentException(
                            "dictionary value exceeds the persisted u16 length limit");
                }
                existing = values.size();
                values.add(value);
                index.put(value, existing);
            }
            return existing;
        }

        String[] toArray() {
            return values.toArray(new String[0]);
        }
    }
}
