/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.output.parquet.sorted.RowGroupOrderException;
import io.varve.swath.replay.protocol.ByteKeys;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * One row group's immutable, off-heap keys: contiguous bytes plus {@code long} offsets, searched
 * copy-free in unsigned order. It is {@link StreamingListingStore}'s promptly closable residency
 * unit, unlike {@link KeyArena}'s on-heap whole-fixture lifetime.
 */
final class KeyBlock implements AutoCloseable {

    /** Initial staging capacity per key; staging is discarded after the exact-size copy. */
    private static final long STAGING_BYTES_PER_KEY = 128;

    private final Arena arena;
    private final MemorySegment keys;
    private final MemorySegment offsets;
    private final int count;

    private KeyBlock(Arena arena, MemorySegment keys, MemorySegment offsets, int count) {
        this.arena = arena;
        this.keys = keys;
        this.offsets = offsets;
        this.count = count;
    }

    /**
     * A builder for a row group of exactly {@code rowCount} keys, refusing a key that would push the
     * block past {@code maxBytes}.
     */
    static Builder builder(long rowCount, long maxBytes) {
        return new Builder(rowCount, maxBytes);
    }

    /** The number of keys held. */
    int size() {
        return count;
    }

    /** Exact resident footprint: key bytes plus the {@code count + 1} offset table. */
    long residentBytes() {
        return keys.byteSize() + offsets.byteSize();
    }

    /** A fresh copy of key {@code index}. */
    byte[] keyAt(int index) {
        long from = start(index);
        int length = (int) (start(index + 1) - from);
        byte[] key = new byte[length];
        MemorySegment.copy(keys, ValueLayout.JAVA_BYTE, from, key, 0, length);
        return key;
    }

    /** The first index whose key is {@code >= key} (== {@link #size()} when none is). */
    int lowerBound(byte[] key) {
        return search(key, 0);
    }

    /** The first index whose key is {@code > key} (== {@link #size()} when none is). */
    int upperBound(byte[] key) {
        return search(key, 1);
    }

    @Override
    public void close() {
        arena.close();
    }

    /** Shared lower/upper-bound search; wraps the target once and makes no key copies. */
    private int search(byte[] key, int bias) {
        MemorySegment target = MemorySegment.ofArray(key);
        int low = 0;
        int high = count;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compareKeyAt(mid, target) < bias) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** Copy-free unsigned comparison; a prefix sorts by length. */
    private int compareKeyAt(int index, MemorySegment target) {
        long from = start(index);
        long to = start(index + 1);
        long length = to - from;
        long targetLength = target.byteSize();
        long mismatch = MemorySegment.mismatch(keys, from, to, target, 0, targetLength);
        if (mismatch < 0) {
            return 0;
        }
        if (mismatch == Math.min(length, targetLength)) {
            return Long.compare(length, targetLength);
        }
        return Byte.compareUnsigned(keys.get(ValueLayout.JAVA_BYTE, from + mismatch),
                target.get(ValueLayout.JAVA_BYTE, mismatch));
    }

    private long start(int index) {
        return offsets.getAtIndex(ValueLayout.JAVA_LONG, index);
    }

    /**
     * Builds one declared-size row group in ascending order. The fixed offset table and trimmed
     * final key buffer make {@link #residentBytes()} an exact residency charge.
     */
    static final class Builder {

        private final long rowCount;
        private final long maxBytes;
        private final long offsetTableBytes;
        private final Arena arena = Arena.ofShared();
        private final MemorySegment offsets;
        private Arena staging = Arena.ofConfined();
        private MemorySegment stagedKeys;
        private byte[] previousKey;
        private int count;
        private long used;

        private Builder(long rowCount, long maxBytes) {
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("row group row count out of range: " + rowCount);
            }
            if (maxBytes < 1) {
                throw new IllegalArgumentException("key block max-bytes must be at least 1, got " + maxBytes);
            }
            this.rowCount = rowCount;
            this.maxBytes = maxBytes;
            this.offsetTableBytes = (rowCount + 1) * Long.BYTES;
            this.offsets = arena.allocate(ValueLayout.JAVA_LONG, rowCount + 1);
            this.stagedKeys = staging.allocate(Math.max(rowCount * STAGING_BYTES_PER_KEY, Long.BYTES));
        }

        /**
         * Appends {@code key}, or returns {@code false} without changing the block when it exceeds
         * {@code maxBytes}; callers must abandon that row group rather than create a hole.
         *
         * @throws RowGroupOrderException when {@code key} is not strictly above its predecessor
         * @throws IllegalStateException when more keys arrive than declared
         */
        boolean append(byte[] key) {
            if (count == rowCount) {
                throw new IllegalStateException("row group declared " + rowCount
                        + " rows but a further key arrived (" + ByteKeys.percentEncode(key) + ")");
            }
            requireAscending(key);
            long end = used + key.length;
            if (end + offsetTableBytes > maxBytes) {
                return false;
            }
            ensureStagingCapacity(end);
            MemorySegment.copy(key, 0, stagedKeys, ValueLayout.JAVA_BYTE, used, key.length);
            offsets.setAtIndex(ValueLayout.JAVA_LONG, count, used);
            previousKey = key;
            used = end;
            count++;
            return true;
        }

        /** @throws IllegalStateException when fewer keys arrived than declared */
        KeyBlock build() {
            if (count != rowCount) {
                throw new IllegalStateException("row group declared " + rowCount + " rows but only "
                        + count + " keys were decoded");
            }
            offsets.setAtIndex(ValueLayout.JAVA_LONG, count, used);
            MemorySegment keys = arena.allocate(used);
            MemorySegment.copy(stagedKeys, 0, keys, 0, used);
            staging.close();
            staging = null;
            stagedKeys = null;
            previousKey = null;
            return new KeyBlock(arena, keys, offsets, count);
        }

        /** Releases allocations for an over-budget or failed decode. */
        void discard() {
            if (staging != null) {
                staging.close();
                staging = null;
                stagedKeys = null;
            }
            arena.close();
        }

        /**
         * Binary search requires strict within-group order. The block reports its row ordinal; the
         * store adds fixture and row-group context to the typed failure.
         */
        private void requireAscending(byte[] key) {
            // Retain the decoder's fresh row array to avoid reading staging memory for this check.
            if (previousKey != null && Arrays.compareUnsigned(previousKey, key) >= 0) {
                throw RowGroupOrderException.atRow(count,
                        "row group keys must arrive in strictly ascending unsigned order; key " + count
                                + " (" + ByteKeys.percentEncode(key) + ") is at or below its predecessor");
            }
        }

        private void ensureStagingCapacity(long required) {
            if (required <= stagedKeys.byteSize()) {
                return;
            }
            long capacity = stagedKeys.byteSize();
            while (capacity < required) {
                capacity *= 2;
            }
            MemorySegment grown = staging.allocate(capacity);
            MemorySegment.copy(stagedKeys, 0, grown, 0, used);
            stagedKeys = grown;
        }
    }
}
