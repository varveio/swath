/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.util.Arrays;
import java.util.NoSuchElementException;

/** Forward-only decoder holding the independent front-coding state for one page traversal. */
public final class PageBlockCursor {

    private static final byte[] EMPTY_KEY = new byte[0];

    private final PageBlock block;
    private final byte[] payload;
    private final int payloadEnd;
    private int position;
    private int emitted;
    private byte[] previousKey = EMPTY_KEY;
    private boolean havePreviousKey;
    private int currentRawOrder;
    private byte[] decodedFirstKey;
    private ListEntry previousEntry;
    private String[][] dictionaryCache;

    PageBlockCursor(PageBlock block, byte[] payload, int offset, int length) {
        this.block = block;
        this.payload = payload;
        this.position = offset;
        this.payloadEnd = Math.addExact(offset, length);
        if (offset < 0 || length < 0 || payloadEnd > payload.length) {
            throw PageBlockCodec.malformed("payload slice exceeds its owning array");
        }
    }

    public boolean hasNext() {
        return emitted < block.count();
    }

    public ListEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            requireRemaining(1, "row tag");
            byte tag = payload[position++];
            ListEntry entry = switch (tag) {
                case PageBlockCodec.TAG_OBJECT -> object();
                case PageBlockCodec.TAG_COMMON_PREFIX -> new CommonPrefixEntry(key());
                case PageBlockCodec.TAG_DELETE_MARKER -> deleteMarker();
                default -> throw new IllegalStateException("bad PageBlock tag: " + tag);
            };
            if (block.validatesPersistedOrder() && previousEntry != null) {
                if (currentRawOrder > 0 || (currentRawOrder == 0
                        && PageBlockCodec.ENTRY_COMPARATOR.compare(previousEntry, entry) > 0)) {
                    throw PageBlockCodec.malformed(
                            "decoded row order regressed inside persisted page");
                }
            }
            previousEntry = entry;
            emitted++;
            if (emitted == 1) {
                decodedFirstKey = entry.key().rawUnsafe();
            }
            if (emitted == block.count()) {
                validateEnd(entry);
            }
            return entry;
        } catch (RuntimeException e) {
            throw block.decodedCorruption(e);
        }
    }

    /** Decode the unconsumed tail so row count, payload exhaustion, and persisted bounds are checked. */
    public void drainAndValidate() {
        while (hasNext()) {
            next();
        }
    }

    private ObjectEntry object() {
        KeyBytes key = key();
        long size = fixedLong();
        long lastModified = fixedLong();
        String etag = etag();
        String storageClass = dictOrRaw(PageBlockCodec.DictColumn.STORAGE_CLASS);
        String versionId = nullableString();
        boolean isLatest = bool();
        String ownerId = dictOrRaw(PageBlockCodec.DictColumn.OWNER_ID);
        String ownerDisplayName = dictOrRaw(PageBlockCodec.DictColumn.OWNER_DISPLAY_NAME);
        String checksumAlgorithm = dictOrRaw(PageBlockCodec.DictColumn.CHECKSUM_ALGORITHM);
        String checksumType = dictOrRaw(PageBlockCodec.DictColumn.CHECKSUM_TYPE);
        return new ObjectEntry(key, size, lastModified, etag, storageClass, versionId, isLatest,
                ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
    }

    private DeleteMarkerEntry deleteMarker() {
        KeyBytes key = key();
        String versionId = nullableString();
        boolean isLatest = bool();
        long lastModified = fixedLong();
        String ownerId = dictOrRaw(PageBlockCodec.DictColumn.OWNER_ID);
        return new DeleteMarkerEntry(key, versionId, isLatest, lastModified, ownerId);
    }

    private void validateEnd(ListEntry lastEntry) {
        if (position != payloadEnd) {
            throw PageBlockCodec.malformed("decoded " + block.count() + " rows with "
                    + (payloadEnd - position) + " trailing payload bytes");
        }
        if (!Arrays.equals(decodedFirstKey, block.firstKeyUnsafe())
                || !Arrays.equals(lastEntry.key().rawUnsafe(), block.lastKeyUnsafe())) {
            throw PageBlockCodec.malformed(
                    "decoded first/last raw keys do not match persisted page bounds");
        }
    }

    private KeyBytes key() {
        int shared = varint("key shared-prefix length");
        int suffixLength = varint("key suffix length");
        if (shared > previousKey.length) {
            throw PageBlockCodec.malformed("key shared-prefix length " + shared
                    + " exceeds previous key length " + previousKey.length);
        }
        requireRemaining(suffixLength, "key suffix");
        int fullLength;
        try {
            fullLength = Math.addExact(shared, suffixLength);
        } catch (ArithmeticException e) {
            throw PageBlockCodec.malformed("reconstructed key length overflows int32");
        }
        if (fullLength > ByteMidpoint.MAX_KEY_LEN) {
            throw PageBlockCodec.malformed("reconstructed key length " + fullLength
                    + " exceeds the S3 key limit of " + ByteMidpoint.MAX_KEY_LEN + " bytes");
        }
        byte[] prior = previousKey;
        byte[] full = new byte[fullLength];
        System.arraycopy(prior, 0, full, 0, shared);
        System.arraycopy(payload, position, full, shared, suffixLength);
        currentRawOrder = havePreviousKey
                ? compareFrontCoded(prior, full, shared, suffixLength) : 0;
        position += suffixLength;
        previousKey = full;
        havePreviousKey = true;
        return KeyBytes.of(full);
    }

    /** Compare previous to current from the front-coding seam; ordinary canonical rows need one byte. */
    private static int compareFrontCoded(byte[] previous, byte[] current, int shared,
                                         int suffixLength) {
        if (shared == previous.length) {
            return suffixLength == 0 ? 0 : -1;
        }
        if (suffixLength == 0) {
            return 1;
        }
        int firstDifference = Integer.compare(previous[shared] & 0xFF, current[shared] & 0xFF);
        return firstDifference != 0
                ? firstDifference : Arrays.compareUnsigned(previous, current);
    }

    private long fixedLong() {
        requireRemaining(Long.BYTES, "fixed-width long");
        long value = ((long) (payload[position] & 0xFF) << 56)
                | ((long) (payload[position + 1] & 0xFF) << 48)
                | ((long) (payload[position + 2] & 0xFF) << 40)
                | ((long) (payload[position + 3] & 0xFF) << 32)
                | ((long) (payload[position + 4] & 0xFF) << 24)
                | ((long) (payload[position + 5] & 0xFF) << 16)
                | ((long) (payload[position + 6] & 0xFF) << 8)
                | (payload[position + 7] & 0xFF);
        position += 8;
        return value;
    }

    private boolean bool() {
        requireRemaining(1, "boolean");
        byte value = payload[position++];
        if (value != 0 && value != 1) {
            throw PageBlockCodec.malformed("boolean must be 0 or 1, got " + (value & 0xFF));
        }
        return value == 1;
    }

    private String nullableString() {
        int encodedLength = varint("nullable string length");
        if (encodedLength == 0) {
            return null;
        }
        int length = encodedLength - 1;
        requireRemaining(length, "nullable string");
        String value = PageBlockCodec.decodeUtf8Strict(
                payload, position, length, "nullable string");
        position += length;
        return value;
    }

    private String etag() {
        requireRemaining(1, "etag marker");
        byte marker = payload[position++];
        return switch (marker) {
            case PageBlockCodec.ETAG_NULL -> null;
            case PageBlockCodec.ETAG_PACKED_MD5 -> {
                requireRemaining(16, "packed etag");
                String value = PageBlockCodec.unpackMd5(payload, position);
                position += 16;
                yield value;
            }
            case PageBlockCodec.ETAG_RAW -> {
                int length = varint("raw etag length");
                requireRemaining(length, "raw etag");
                String value = PageBlockCodec.decodeUtf8Strict(
                        payload, position, length, "raw etag");
                position += length;
                yield value;
            }
            default -> throw new IllegalStateException("bad etag marker: " + marker);
        };
    }

    private String dictOrRaw(PageBlockCodec.DictColumn column) {
        int encoded = varint(column + " value");
        if (encoded == 0) {
            return null;
        }
        if (block.useDictUnsafe()[column.ordinal()]) {
            PageBlockDictionaries dictionaries = block.dictionariesUnsafe();
            int index = encoded - 1;
            if (index >= dictionaries.size(column.ordinal())) {
                throw PageBlockCodec.malformed(column + " dictionary index " + index
                        + " exceeds dictionary size " + dictionaries.size(column.ordinal()));
            }
            return dictionaryValue(dictionaries, column.ordinal(), index);
        }
        int length = encoded - 1;
        requireRemaining(length, column + " raw value");
        String value = PageBlockCodec.decodeUtf8Strict(
                payload, position, length, column + " raw value");
        position += length;
        return value;
    }

    private String dictionaryValue(
            PageBlockDictionaries dictionaries, int column, int index) {
        if (!dictionaries.byteBacked()) {
            return dictionaries.value(column, index);
        }
        if (dictionaryCache == null) {
            dictionaryCache = new String[PageBlockCodec.DICT_COLUMN_COUNT][];
        }
        String[] values = dictionaryCache[column];
        if (values == null) {
            values = new String[dictionaries.size(column)];
            dictionaryCache[column] = values;
        }
        String value = values[index];
        if (value == null) {
            value = dictionaries.value(column, index);
            values[index] = value;
        }
        return value;
    }

    private int varint(String field) {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            requireRemaining(1, field + " varint");
            int value = payload[position++] & 0xFF;
            if (shift == 28 && (value & 0xF8) != 0) {
                throw PageBlockCodec.malformed(field + " varint overflows int32");
            }
            result |= (value & 0x7F) << shift;
            if ((value & 0x80) == 0) {
                return result;
            }
        }
        throw PageBlockCodec.malformed(field + " varint is too long");
    }

    private void requireRemaining(int needed, String field) {
        if (needed < 0 || needed > payloadEnd - position) {
            throw PageBlockCodec.malformed(field + " exceeds decoded payload (needed " + needed
                    + ", remaining " + (payloadEnd - position) + ")");
        }
    }
}
