/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Packed strings or validated byte coordinates for one persisted page's dictionaries. */
final class PageBlockDictionaries {
    private final String[][] packed;
    private final byte[] owner;
    private final int[] starts;
    private final int[] counts;

    private PageBlockDictionaries(String[][] packed, byte[] owner, int[] starts, int[] counts) {
        this.packed = packed;
        this.owner = owner;
        this.starts = starts;
        this.counts = counts;
    }

    static PageBlockDictionaries packed(String[][] values) {
        return new PageBlockDictionaries(values, null, null, null);
    }

    static PageBlockDictionaries persisted(byte[] owner, int[] starts, int[] counts) {
        return new PageBlockDictionaries(null, owner, starts, counts);
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
        return owner == null ? 0 : PageBlockCodec.PERSISTED_DICTIONARY_COORDINATE_BYTES;
    }

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
        long bytes = 16L + (long) PageBlockCodec.DICT_COLUMN_COUNT * Long.BYTES;
        for (int column = 0; column < PageBlockCodec.DICT_COLUMN_COUNT; column++) {
            bytes += 16L + (long) size(column) * Long.BYTES;
            for (int index = 0; index < size(column); index++) {
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
        return PageBlockCodec.decodeUtf8Strict(owner, slice.offset(), slice.length(),
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

    private static void putLenBytes(ByteBuffer buffer, byte[] bytes) {
        if (bytes.length > 0xFFFF) {
            throw new IllegalArgumentException(
                    "dictionary value exceeds the persisted u16 length limit");
        }
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private record Slice(int offset, int length) {
    }
}
