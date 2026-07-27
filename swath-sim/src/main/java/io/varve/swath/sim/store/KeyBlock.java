/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.protocol.ByteKeys;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * One row group's keys, decoded once into <b>off-heap</b> memory: a contiguous run of key bytes plus
 * an {@code int64} offset per key, searchable by unsigned byte order without materialising a key.
 * {@link StreamingListingStore}'s unit of residency.
 *
 * <h2>Why two buffers, and why off-heap</h2>
 * The layout is deliberately the plainest one that can be written to a file and read back by
 * pointing at it: {@code offsets[i]} is where key {@code i} starts and {@code offsets[i + 1]} where
 * it ends, over one byte buffer holding every key end to end. That is exactly Arrow's
 * {@code LargeBinary} shape, so the door stays open to decoding a fixture <em>once ever</em> into a
 * sidecar file and memory-mapping it on later runs ({@link java.nio.channels.FileChannel#map} yields
 * a {@link MemorySegment} over the same two buffers) instead of re-decoding Parquet per run. Nothing
 * here holds an in-process pointer, an object reference, or any other state that would not survive a
 * round trip through a file — a constraint on the layout, not a promise that the sidecar exists.
 *
 * <p>Off-heap, via {@link Arena}, for two reasons beyond that. A {@link java.lang.foreign.MemorySegment}
 * is {@code long}-indexed, so there is no 2 GiB block-splitting to do and a key can never straddle a
 * block boundary. And a tier whose whole claim is a bounded, promptly-released working set wants
 * memory it frees when it evicts, not when a collector next runs — {@link #close()} releases a
 * block's bytes at the moment the residency budget says they are gone.
 *
 * <p><b>Distinct from {@link KeyArena} on purpose.</b> That is the arena tier's single, on-heap,
 * whole-fixture key column, built once at load and never evicted; this is a per-row-group block, one
 * of many, faulted and dropped as cursors move. They share a shape (sorted keys plus an offset table,
 * searched by binary search) but not a lifecycle, an allocation strategy, or an addressing scheme,
 * and the arena tier is not rebuilt on this one.
 */
final class KeyBlock implements AutoCloseable {

    /**
     * Bytes per key the builder's staging buffer starts at. Comfortably above real key lengths (a
     * measured giant fixture averages ~102), so a decode normally never grows and pays one copy into
     * its exactly-sized final buffer; the staging buffer is transient either way.
     */
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

    /** This block's off-heap footprint: the key bytes plus the {@code count + 1} entry offset table. */
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

    /**
     * Binary search for the first index whose key compares {@code >= bias} against {@code key}:
     * {@code bias == 0} keeps an exact match (lower bound), {@code bias == 1} steps past it (upper
     * bound). The target is wrapped as a heap segment once per search, not once per comparison, so
     * the whole search costs one wrapper and no key copies.
     */
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

    /**
     * Unsigned byte-order comparison of key {@code index} against {@code target}, copy-free.
     * {@link MemorySegment#mismatch} does the scan (an intrinsic) and reports the smaller length when
     * one key is a prefix of the other, which is exactly the case unsigned lexicographic order
     * resolves by length.
     */
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
     * Appends a row group's keys in ascending order. Single-threaded by construction: a block is
     * built inside one decode and is immutable afterwards.
     *
     * <p>The offset table is allocated once, exactly, from the row count the routing index already
     * carries. Only the key bytes are unknown up front, so they are staged in a temporary
     * {@linkplain Arena#ofConfined() confined arena} — grown by doubling, released the moment
     * {@link #build()} has copied them into an exactly-sized buffer. Trimming rather than keeping the
     * staging buffer costs one copy per decode and buys a block whose footprint is what it says it
     * is, which is what makes the residency budget a real bound rather than an approximate one.
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
         * Appends {@code key}, or returns {@code false} without appending when the block would exceed
         * {@code maxBytes}. Once {@code false} is returned the block is over budget for good — the
         * caller abandons it rather than serving a row group with keys missing from the middle.
         *
         * @throws IllegalStateException when {@code key} is not strictly above its predecessor, or
         *                               when more keys arrive than the row group declared
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

        /**
         * @throws IllegalStateException when fewer keys arrived than the row group declared — the
         *                               offset table would then be part-written and every search over
         *                               the block's tail would read a zero offset as a real one
         */
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

        /** Releases everything allocated so far, for a build abandoned part-way (an over-budget row
         *  group, or a decode that threw). */
        void discard() {
            if (staging != null) {
                staging.close();
                staging = null;
                stagedKeys = null;
            }
            arena.close();
        }

        /**
         * Every lookup a block answers is a binary search, so a row group that hands back a duplicate
         * or an out-of-order key does not merely degrade it — it corrupts every later search, and the
         * store then reports confident wrong answers. Sorted-serving eligibility proved the ascent of
         * row-group <em>first</em> keys only; this is what proves it within a group.
         */
        private void requireAscending(byte[] key) {
            // The decoder hands over a fresh array per row and never reuses it, so retaining the
            // previous one is a plain heap comparison — no read back out of the staging buffer, and
            // no per-key segment wrapper on the hottest loop in the tier.
            if (previousKey != null && Arrays.compareUnsigned(previousKey, key) >= 0) {
                throw new IllegalStateException("row group keys must arrive in strictly ascending unsigned "
                        + "order; key " + count + " (" + ByteKeys.percentEncode(key)
                        + ") is at or below its predecessor");
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
