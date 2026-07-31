/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.varve.swath.replay.protocol.ByteKeys;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Immutable whole-fixture key column: ascending on-heap key bytes in fixed segments plus
 * {@code long} offsets. Keys may cross segment boundaries and are searched in unsigned order.
 */
final class KeyArena {

    /** The largest key S3 accepts and this arena stores. */
    static final int MAX_KEY_BYTES = 1024;

    /** Production segment size, below the Java array limit. */
    static final int SEGMENT_BYTES = 1 << 24;

    private final List<byte[]> segments;
    private final int segmentBytes;
    private final long[] offsets;
    private final int count;

    private KeyArena(List<byte[]> segments, int segmentBytes, long[] offsets, int count) {
        this.segments = segments;
        this.segmentBytes = segmentBytes;
        this.offsets = offsets;
        this.count = count;
    }

    static Builder builder(long maxEncodedBytes, int segmentBytes) {
        return new Builder(maxEncodedBytes, segmentBytes);
    }

    /** Key bytes plus the {@code count + 1} offset table; segment slack is excluded and bounded. */
    static long encodedBytes(long keyBytes, long count) {
        return keyBytes + (count + 1) * Long.BYTES;
    }

    int size() {
        return count;
    }

    /** Actual encoded footprint, the capacity-budget unit. */
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
        return compareStored(segments, segmentBytes, from, (int) (offsets[index + 1] - from), other);
    }

    /** The first index whose key is {@code >= key} (== {@link #size()} when none is). */
    int lowerBound(byte[] key) {
        return search(key, 0);
    }

    /** The first index whose key is {@code > key} (== {@link #size()} when none is). */
    int upperBound(byte[] key) {
        return search(key, 1);
    }

    /** Shared lower/upper-bound search so their ordering cannot diverge. */
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
            System.arraycopy(segments.get(segment), position, destination, written, run);
            from += run;
            written += run;
        }
    }

    /** Unsigned comparison across segment boundaries, shared by lookup and the builder. */
    private static int compareStored(List<byte[]> segments, int segmentBytes, long from, int length,
                                     byte[] other) {
        int remaining = length;
        int otherPos = 0;
        while (remaining > 0 && otherPos < other.length) {
            int segment = (int) (from / segmentBytes);
            int position = (int) (from % segmentBytes);
            int run = Math.min(remaining, segmentBytes - position);
            run = Math.min(run, other.length - otherPos);
            int cmp = Arrays.compareUnsigned(segments.get(segment), position, position + run,
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

    /** Builds an immutable arena from one strictly ascending source. */
    static final class Builder {

        private static final int INITIAL_OFFSET_CAPACITY = 1024;

        private final long maxEncodedBytes;
        private final int segmentBytes;
        private final List<byte[]> segments = new ArrayList<>();
        private long[] offsets = new long[INITIAL_OFFSET_CAPACITY];
        private int count;
        private long used;

        // Test seam for boundary-spanning layouts; production always uses SEGMENT_BYTES.
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
         * Appends {@code key}, or returns {@code false} before changing the arena when it exceeds
         * the byte budget; callers must abandon the incomplete fixture.
         *
         * @throws IllegalArgumentException when {@code key} is over-long or not strictly ascending
         */
        boolean append(byte[] key) {
            if (key.length > MAX_KEY_BYTES) {
                throw new IllegalArgumentException("key of " + key.length
                        + " bytes exceeds the " + MAX_KEY_BYTES + "-byte maximum");
            }
            requireAscending(key);
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
            return new KeyArena(List.copyOf(segments), segmentBytes,
                    Arrays.copyOf(offsets, count + 1), count);
        }

        /** Strict order is required by binary search and exclusive batch resumption. */
        private void requireAscending(byte[] key) {
            if (count == 0) {
                return;
            }
            long previous = offsets[count - 1];
            int cmp = compareStored(segments, segmentBytes, previous, (int) (used - previous), key);
            if (cmp >= 0) {
                throw new IllegalArgumentException("arena keys must arrive in strictly ascending unsigned "
                        + "order; key " + count + " (" + ByteKeys.percentEncode(key) + ") is "
                        + (cmp == 0 ? "a duplicate of" : "below") + " its predecessor");
            }
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
