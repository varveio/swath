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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Admission-time dictionary and payload-encoding policy for one {@link PageBlock}. */
final class PageBlockPacker {

    private static final byte[] EMPTY_KEY = new byte[0];

    private PageBlockPacker() {
    }

    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator,
            PageCodec codec, int maxRawPayloadBytes) {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("cannot pack an empty page");
        }
        if (maxRawPayloadBytes <= 0 || maxRawPayloadBytes > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("raw page limit is outside the format bound: "
                    + maxRawPayloadBytes);
        }

        boolean ordered = true;
        ListEntry previous = null;
        DictProbe[] probes = new DictProbe[PageBlockCodec.DICT_COLUMN_COUNT];
        for (int i = 0; i < probes.length; i++) {
            probes[i] = new DictProbe();
        }
        for (ListEntry entry : entries) {
            if (previous != null && comparator.compare(previous, entry) > 0) {
                ordered = false;
            }
            previous = entry;
            switch (entry) {
                case ObjectEntry object -> {
                    probes[PageBlockCodec.DictColumn.STORAGE_CLASS.ordinal()]
                            .offer(object.storageClass());
                    probes[PageBlockCodec.DictColumn.CHECKSUM_ALGORITHM.ordinal()]
                            .offer(object.checksumAlgorithm());
                    probes[PageBlockCodec.DictColumn.CHECKSUM_TYPE.ordinal()]
                            .offer(object.checksumType());
                    probes[PageBlockCodec.DictColumn.OWNER_ID.ordinal()].offer(object.ownerId());
                    probes[PageBlockCodec.DictColumn.OWNER_DISPLAY_NAME.ordinal()]
                            .offer(object.ownerDisplayName());
                }
                case DeleteMarkerEntry marker ->
                        probes[PageBlockCodec.DictColumn.OWNER_ID.ordinal()].offer(marker.ownerId());
                case CommonPrefixEntry ignored -> { }
            }
        }
        boolean[] useDict = new boolean[PageBlockCodec.DICT_COLUMN_COUNT];
        for (int i = 0; i < probes.length; i++) {
            useDict[i] = probes[i].useDict();
        }

        PayloadWriter writer = new PayloadWriter(entries.size(), useDict, maxRawPayloadBytes);
        long estimate = 0;
        for (ListEntry entry : entries) {
            estimate += PageBlock.estimatedBytes(entry);
            writer.write(entry);
        }
        ListEntry first = entries.getFirst();
        ListEntry last = entries.getLast();
        byte[] raw = writer.toBytes();
        byte[] stored = codec.compress(raw);
        return new PageBlock(stored, raw.length, codec, writer.dictArrays(), useDict,
                entries.size(), first.key().rawUnsafe(), last.key().rawUnsafe(), first, last,
                estimate, ordered, null);
    }

    /** Front-coded row payload writer with stable first-seen dictionary ordering. */
    private static final class PayloadWriter {
        private byte[] buffer = new byte[256];
        private int length;
        private final Dict[] dicts;
        private final boolean[] useDict;
        private final int maxRawPayloadBytes;
        private byte[] previousKey = EMPTY_KEY;

        PayloadWriter(int hint, boolean[] useDict, int maxRawPayloadBytes) {
            if (hint > 8) {
                buffer = new byte[(int) Math.min(
                        Math.min((long) hint * 48, 1 << 20), maxRawPayloadBytes)];
            }
            this.useDict = useDict;
            this.maxRawPayloadBytes = maxRawPayloadBytes;
            this.dicts = new Dict[PageBlockCodec.DICT_COLUMN_COUNT];
            for (int i = 0; i < dicts.length; i++) {
                dicts[i] = new Dict();
            }
        }

        void write(ListEntry entry) {
            switch (entry) {
                case ObjectEntry object -> writeObject(object);
                case CommonPrefixEntry prefix -> {
                    put(PageBlockCodec.TAG_COMMON_PREFIX);
                    key(prefix.key());
                }
                case DeleteMarkerEntry marker -> writeDeleteMarker(marker);
            }
        }

        private void writeObject(ObjectEntry object) {
            put(PageBlockCodec.TAG_OBJECT);
            key(object.key());
            fixedLong(object.size());
            fixedLong(object.lastModifiedEpochMicros());
            etag(object.etag());
            dictOrRaw(PageBlockCodec.DictColumn.STORAGE_CLASS, object.storageClass());
            nullableString(object.versionId());
            bool(object.isLatest());
            dictOrRaw(PageBlockCodec.DictColumn.OWNER_ID, object.ownerId());
            dictOrRaw(PageBlockCodec.DictColumn.OWNER_DISPLAY_NAME, object.ownerDisplayName());
            dictOrRaw(PageBlockCodec.DictColumn.CHECKSUM_ALGORITHM, object.checksumAlgorithm());
            dictOrRaw(PageBlockCodec.DictColumn.CHECKSUM_TYPE, object.checksumType());
        }

        private void writeDeleteMarker(DeleteMarkerEntry marker) {
            put(PageBlockCodec.TAG_DELETE_MARKER);
            key(marker.key());
            nullableString(marker.versionId());
            bool(marker.isLatest());
            fixedLong(marker.lastModifiedEpochMicros());
            dictOrRaw(PageBlockCodec.DictColumn.OWNER_ID, marker.ownerId());
        }

        private void key(KeyBytes key) {
            byte[] raw = key.rawUnsafe();
            PageBlockCodec.requireWriterKey(raw, "row key");
            int shared = commonPrefixLength(previousKey, raw);
            int suffixLength = raw.length - shared;
            varint(shared);
            varint(suffixLength);
            write(raw, shared, suffixLength);
            previousKey = raw;
        }

        private void fixedLong(long value) {
            ensure(8);
            for (int shift = 56; shift >= 0; shift -= 8) {
                buffer[length++] = (byte) (value >>> shift);
            }
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
            write(bytes, 0, bytes.length);
        }

        private void etag(String etag) {
            if (etag == null) {
                put(PageBlockCodec.ETAG_NULL);
                return;
            }
            byte[] packed = tryPackMd5(etag);
            if (packed != null) {
                put(PageBlockCodec.ETAG_PACKED_MD5);
                write(packed, 0, packed.length);
            } else {
                put(PageBlockCodec.ETAG_RAW);
                byte[] bytes = etag.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length);
                write(bytes, 0, bytes.length);
            }
        }

        private void dictOrRaw(PageBlockCodec.DictColumn column, String value) {
            if (value == null) {
                varint(0);
            } else if (useDict[column.ordinal()]) {
                varint(dicts[column.ordinal()].indexOf(value) + 1);
            } else {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length + 1);
                write(bytes, 0, bytes.length);
            }
        }

        String[][] dictArrays() {
            String[][] result = new String[dicts.length][];
            for (int i = 0; i < dicts.length; i++) {
                result[i] = dicts[i].values.toArray(new String[0]);
            }
            return result;
        }

        byte[] toBytes() {
            return java.util.Arrays.copyOf(buffer, length);
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
                capacity = Math.min(maxRawPayloadBytes, Math.max(capacity + 1, capacity << 1));
            }
            buffer = java.util.Arrays.copyOf(buffer, capacity);
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

    private static final class DictProbe {
        private Set<String> seen = new HashSet<>();
        private boolean capped;

        void offer(String value) {
            if (value == null || capped) {
                return;
            }
            seen.add(value);
            if (seen.size() > PageBlock.DICT_CAP) {
                capped = true;
                seen = null;
            }
        }

        boolean useDict() {
            return !capped;
        }
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
    }
}
