/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An immutable, ascending, keys-only column: the whole key set of a fixture packed into
 * fixed-size byte <b>segments</b> plus a {@code long} offset table, searchable by unsigned byte
 * order without materialising a single key.
 *
 * <h2>Why segments</h2>
 * A Java {@code byte[]} cannot exceed 2 GiB, and a bucket's key bytes routinely can, so the keys
 * cannot live in one array. They are written end-to-end into {@link #SEGMENT_BYTES}-sized
 * segments and addressed by a global {@code long} offset; {@code offsets[i]} is where key
 * {@code i} starts and {@code offsets[i + 1]} where it ends, so the table is {@code n + 1} long
 * and the length of a key is a subtraction, never a second array. A key may straddle a segment
 * boundary — no padding, no per-segment key alignment; {@link #keyAt} and {@link #compareKeyAt}
 * walk the boundary.
 *
 * <h2>Why the capacity budget is in bytes</h2>
 * Keys are legal up to {@value #MAX_KEY_BYTES} bytes each, so key <em>count</em> says almost
 * nothing about footprint — two fixtures with the same count can differ by three orders of
 * magnitude. The builder is therefore bounded by {@linkplain #encodedBytes() encoded bytes} (key
 * bytes plus the offset table) and declines the append that would cross the budget, so a caller
 * learns the fixture does not fit without having to guess in advance.
 *
 * <p>Keys are stored raw. Front-coding a sorted key set commonly buys 3–5x, but that is an
 * optimisation to make against a measured fixture, not one to assume here.
 */
final class KeyArena {

    /** The largest key S3 accepts, and the largest this arena will store (algorithms.md §11.13). */
    static final int MAX_KEY_BYTES = 1024;

    /**
     * Segment size. Large enough that the per-segment bookkeeping is noise at bucket scale, small
     * enough that a modest fixture does not pay a huge floor; well under the 2 GiB array ceiling.
     */
    static final int SEGMENT_BYTES = 1 << 24;

    private final byte[][] segments;
    private final int segmentBytes;
    private final long[] offsets;
    private final int count;

    private KeyArena(byte[][] segments, int segmentBytes, long[] offsets, int count) {
        this.segments = segments;
        this.segmentBytes = segmentBytes;
        this.offsets = offsets;
        this.count = count;
    }

    static Builder builder(long maxEncodedBytes) {
        return new Builder(maxEncodedBytes, SEGMENT_BYTES);
    }

    /**
     * The encoded footprint of {@code count} keys totalling {@code keyBytes}: the key bytes plus
     * the {@code count + 1} entry offset table. Segment slack is excluded — it is bounded by one
     * segment, not by the fixture.
     */
    static long encodedBytes(long keyBytes, long count) {
        return keyBytes + (count + 1) * Long.BYTES;
    }

    int size() {
        return count;
    }

    /** This arena's actual encoded footprint, the quantity the capacity budget is expressed in. */
    long encodedBytes() {
        return encodedBytes(offsets[count], count);
    }

    /** A fresh copy of key {@code index}. */
    byte[] keyAt(int index) {
        long from = offsets[index];
        int length = (int) (offsets[index + 1] - from);
        byte[] key = new byte[length];
        copyOut(from, key);
        return key;
    }

    /** Unsigned byte-order comparison of key {@code index} against {@code other}, copy-free. */
    int compareKeyAt(int index, byte[] other) {
        long from = offsets[index];
        int remaining = (int) (offsets[index + 1] - from);
        int otherPos = 0;
        while (remaining > 0 && otherPos < other.length) {
            int segment = (int) (from / segmentBytes);
            int position = (int) (from % segmentBytes);
            int run = Math.min(remaining, segmentBytes - position);
            run = Math.min(run, other.length - otherPos);
            int cmp = Arrays.compareUnsigned(segments[segment], position, position + run,
                    other, otherPos, otherPos + run);
            if (cmp != 0) {
                return cmp;
            }
            from += run;
            remaining -= run;
            otherPos += run;
        }
        return Integer.compare(remaining, other.length - otherPos);
    }

    /** The first index whose key is {@code >= key} (== {@link #size()} when none is). */
    int lowerBound(byte[] key) {
        return search(key, 0);
    }

    /** The first index whose key is {@code > key} (== {@link #size()} when none is). */
    int upperBound(byte[] key) {
        return search(key, 1);
    }

    /**
     * Binary search for the first index whose key compares {@code >= bias} against {@code key}:
     * {@code bias == 0} keeps an exact match (lower bound), {@code bias == 1} steps past it
     * (upper bound). One implementation, so the two bounds cannot drift apart.
     */
    private int search(byte[] key, int bias) {
        int low = 0;
        int high = count;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compareKeyAt(mid, key) < bias) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private void copyOut(long from, byte[] destination) {
        int written = 0;
        while (written < destination.length) {
            int segment = (int) (from / segmentBytes);
            int position = (int) (from % segmentBytes);
            int run = Math.min(destination.length - written, segmentBytes - position);
            System.arraycopy(segments[segment], position, destination, written, run);
            from += run;
            written += run;
        }
    }

    /**
     * Appends keys in ascending order, refusing the first one that would push the arena past its
     * encoded-byte budget. Single-threaded by construction: an arena is built once, at load, and
     * is immutable afterwards.
     */
    static final class Builder {

        private static final int INITIAL_OFFSET_CAPACITY = 1024;

        private final long maxEncodedBytes;
        private final int segmentBytes;
        private final List<byte[]> segments = new ArrayList<>();
        private long[] offsets = new long[INITIAL_OFFSET_CAPACITY];
        private int count;
        private long used;

        // segmentBytes is a parameter so a test can force multi-segment (and boundary-straddling)
        // layouts without allocating production-sized segments.
        Builder(long maxEncodedBytes, int segmentBytes) {
            if (maxEncodedBytes < 1) {
                throw new IllegalArgumentException("arena max-encoded-bytes must be at least 1, got "
                        + maxEncodedBytes);
            }
            if (segmentBytes < MAX_KEY_BYTES) {
                throw new IllegalArgumentException("arena segment bytes must be at least " + MAX_KEY_BYTES
                        + ", got " + segmentBytes);
            }
            this.maxEncodedBytes = maxEncodedBytes;
            this.segmentBytes = segmentBytes;
        }

        /**
         * Appends {@code key}, or returns {@code false} without appending when it would exceed the
         * budget. Once {@code false} is returned the arena is over budget for good — the caller
         * abandons it rather than skipping a key and silently serving an incomplete fixture.
         */
        boolean append(byte[] key) {
            if (key.length > MAX_KEY_BYTES) {
                throw new IllegalArgumentException("key of " + key.length
                        + " bytes exceeds the " + MAX_KEY_BYTES + "-byte maximum");
            }
            long end = used + key.length;
            if (encodedBytes(end, count + 1) > maxEncodedBytes) {
                return false;
            }
            ensureOffsetCapacity(count + 1);
            offsets[count] = used;
            growSegmentsTo(end);
            copyIn(key);
            used = end;
            count++;
            return true;
        }

        KeyArena build() {
            ensureOffsetCapacity(count + 1);
            offsets[count] = used;
            return new KeyArena(segments.toArray(new byte[0][]), segmentBytes,
                    Arrays.copyOf(offsets, count + 1), count);
        }

        private void ensureOffsetCapacity(int required) {
            if (offsets.length < required) {
                offsets = Arrays.copyOf(offsets, Math.max(required, offsets.length * 2));
            }
        }

        private void growSegmentsTo(long end) {
            while ((long) segments.size() * segmentBytes < end) {
                segments.add(new byte[segmentBytes]);
            }
        }

        private void copyIn(byte[] key) {
            long at = used;
            int read = 0;
            while (read < key.length) {
                int segment = (int) (at / segmentBytes);
                int position = (int) (at % segmentBytes);
                int run = Math.min(key.length - read, segmentBytes - position);
                System.arraycopy(key, read, segments.get(segment), position, run);
                at += run;
                read += run;
            }
        }
    }
}
