/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;

/** Forward-only decoder holding the independent front-coding state for one page traversal. */
final class PageBlockCursor {

    private static final byte[] EMPTY_KEY = new byte[0];

    private final PageBlock block;
    private final byte[] payload;
    private final int payloadEnd;
    private int position;
    private int emitted;
    private byte[] previousKey = EMPTY_KEY;
    private byte[] decodedFirstKey;

    PageBlockCursor(PageBlock block, byte[] payload, int offset, int length) {
        this.block = block;
        this.payload = payload;
        this.position = offset;
        this.payloadEnd = Math.addExact(offset, length);
    }

    boolean hasNext() {
        return emitted < block.count();
    }

    ListEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            emitted++;
            byte tag = payload[position++];
            ListEntry entry = switch (tag) {
                case PageBlockCodec.TAG_OBJECT -> object();
                case PageBlockCodec.TAG_COMMON_PREFIX -> new CommonPrefixEntry(key());
                case PageBlockCodec.TAG_DELETE_MARKER -> deleteMarker();
                default -> throw new IllegalStateException("bad PageBlock tag: " + tag);
            };
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
        int shared = varint();
        int suffixLength = varint();
        byte[] full = new byte[shared + suffixLength];
        System.arraycopy(previousKey, 0, full, 0, shared);
        System.arraycopy(payload, position, full, shared, suffixLength);
        position += suffixLength;
        previousKey = full;
        return KeyBytes.of(full);
    }

    private long fixedLong() {
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
        return payload[position++] != 0;
    }

    private String nullableString() {
        int encodedLength = varint();
        if (encodedLength == 0) {
            return null;
        }
        int length = encodedLength - 1;
        String value = new String(payload, position, length, StandardCharsets.UTF_8);
        position += length;
        return value;
    }

    private String etag() {
        byte marker = payload[position++];
        return switch (marker) {
            case PageBlockCodec.ETAG_NULL -> null;
            case PageBlockCodec.ETAG_PACKED_MD5 -> {
                String value = PageBlockCodec.unpackMd5(payload, position);
                position += 16;
                yield value;
            }
            case PageBlockCodec.ETAG_RAW -> {
                int length = varint();
                String value = new String(payload, position, length, StandardCharsets.UTF_8);
                position += length;
                yield value;
            }
            default -> throw new IllegalStateException("bad etag marker: " + marker);
        };
    }

    private String dictOrRaw(PageBlockCodec.DictColumn column) {
        int encoded = varint();
        if (encoded == 0) {
            return null;
        }
        if (block.useDictUnsafe()[column.ordinal()]) {
            return block.dictsUnsafe()[column.ordinal()][encoded - 1];
        }
        int length = encoded - 1;
        String value = new String(payload, position, length, StandardCharsets.UTF_8);
        position += length;
        return value;
    }

    private int varint() {
        int result = 0;
        int shift = 0;
        while (true) {
            byte value = payload[position++];
            result |= (value & 0x7F) << shift;
            if ((value & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }
}
