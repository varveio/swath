/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.UpperBound;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Azure Blob's listing order over UTF-8 key bytes, and its translation into byte ranges of the
 * byte-sorted fixture.
 *
 * <p>Azure orders names by UTF-16 code units. For valid UTF-8 that is unsigned byte order with one
 * exception at the first differing byte: lead bytes {@code F0..F4} (U+10000 and above, surrogate
 * pairs in UTF-16) sort before lead bytes {@code EE..EF} (U+E000 to U+FFFF). Continuation bytes
 * never meet a lead byte at the first difference of two valid names, so the rule is a fixed byte
 * permutation applied position by position: {@code 00..ED, F0..F4, EE..EF, F5..FF}. The same
 * permutation is the order of names that are not valid UTF-8; the Azure route refuses to render such
 * a name, so their order only has to be total and stable.
 *
 * <p>A prefix window is contiguous in both orders, and the two orders disagree only between an
 * {@code EE/EF} branch and an {@code F0..F4} branch under the same parent. So provider order over a
 * byte range is the byte order of that range with each such pair of sibling branches swapped,
 * recursively, which {@link AzureOrderedCursor} does as it reads.
 */
final class AzureOrder {
    private AzureOrder() {
    }

    /** Position of {@code value} in the provider byte order. */
    static int rank(int value) {
        if (value >= 0xf0 && value <= 0xf4) return value - 2;
        if (value == 0xee || value == 0xef) return value + 5;
        return value;
    }

    private static int unrank(int rank) {
        if (rank >= 0xee && rank <= 0xf2) return rank + 2;
        if (rank == 0xf3 || rank == 0xf4) return rank - 5;
        return rank;
    }

    /** Provider-order comparison of two names. */
    static int compare(byte[] a, byte[] b) {
        int mismatch = Arrays.mismatch(a, b);
        if (mismatch < 0) return 0;
        if (mismatch == a.length) return -1;
        if (mismatch == b.length) return 1;
        return Integer.compare(rank(a[mismatch] & 0xff), rank(b[mismatch] & 0xff));
    }

    /**
     * The least name after every name that starts with {@code prefix} in provider order, or
     * {@code null} when none exists. This is a resume point for a skipped BlobPrefix subtree.
     */
    static byte[] upperOf(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            int rank = rank(prefix[i] & 0xff);
            if (rank != 0xff) {
                byte[] upper = Arrays.copyOf(prefix, i + 1);
                upper[i] = (byte) unrank(rank + 1);
                return upper;
            }
        }
        return null;
    }

    /**
     * The first position of {@code name} holding an {@code EE/EF} byte whose {@code F0..F4} siblings
     * fall below {@code upper}, or -1. Names in that sibling branch rank before {@code name} although
     * they follow it in byte order; no other name read after {@code name} below {@code upper} can.
     */
    static int firstSwap(byte[] name, byte[] upper) {
        for (int at = 0; at < name.length; at++) {
            int value = name[at] & 0xff;
            if ((value == 0xee || value == 0xef)
                    && (upper == null || ByteKeys.compareUnsigned(child(name, at, 0xf0), upper) < 0)) {
                return at;
            }
        }
        return -1;
    }

    /**
     * The byte ranges, in provider order, holding every name in the prefix window at or after
     * {@code lower} (after it when {@code inclusive} is false) in provider order. {@code lower} must
     * start with {@code prefix}. Each range is a set of whole subtrees whose provider order still has
     * to be read with {@link AzureOrderedCursor}; consecutive ranges never interleave.
     *
     * <p>The set after a name L is, from the deepest position outward, the subtree below L and then, at
     * each position i, the siblings of {@code L[i]} under {@code L[0..i)} that rank after it. Those
     * siblings are one byte range unless {@code L[i]} is a lead byte {@code EE..F4}: after an
     * {@code F0..F4} byte come {@code (L[i], F4]}, then {@code EE..EF}, then {@code F5..FF}; after an
     * {@code EE/EF} byte come {@code (L[i], EF]} and {@code F5..FF}, leaving out the F0 branch that
     * ranks before it. Ranges that touch in byte order are merged, so a name without such lead bytes
     * yields one range and each lead byte adds at most two.
     */
    static List<Range> rangesFrom(byte[] lower, boolean inclusive, byte[] prefix) {
        List<Range> ranges = new ArrayList<>();
        byte[] from = lower;
        boolean fromInclusive = inclusive;
        for (int i = lower.length - 1; i >= prefix.length; i--) {
            int value = lower[i] & 0xff;
            if (value >= 0xee && value <= 0xf4) {
                boolean supplementary = value >= 0xf0;
                ranges.add(new Range(from, fromInclusive, child(lower, i, supplementary ? 0xf5 : 0xf0)));
                if (supplementary) ranges.add(new Range(child(lower, i, 0xee), true, child(lower, i, 0xf0)));
                from = child(lower, i, 0xf5);
                fromInclusive = true;
            }
        }
        byte[] upper = switch (ByteKeys.prefixUpper(prefix)) {
            case UpperBound.Bounded(ByteKey key) -> key.toByteArray();
            case UpperBound.Open() -> null;
        };
        ranges.add(new Range(from, fromInclusive, upper));
        return ranges;
    }

    /** {@code name[0..at)} followed by {@code value}. */
    static byte[] child(byte[] name, int at, int value) {
        byte[] child = Arrays.copyOf(name, at + 1);
        child[at] = (byte) value;
        return child;
    }

    /** One byte range {@code [from, upper)} of the byte-sorted fixture; {@code upper == null} is open. */
    record Range(byte[] from, boolean fromInclusive, byte[] upper) {
    }
}
