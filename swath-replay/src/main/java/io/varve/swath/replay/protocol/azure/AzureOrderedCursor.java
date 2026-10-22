/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Reads listing entries in Azure provider order from byte ranges of the byte-sorted fixture.
 *
 * <p>Each range is read in byte order, a batch at a time. An entry is next in provider order unless
 * {@link AzureOrder#firstSwap} finds an {@code EE/EF} byte at position j whose {@code F0..F4}
 * siblings under the same j-byte parent Q are still inside the range. The rest of the range then
 * splits in three: {@code [Q+F0, Q+F5)} is read first, then the {@code EE/EF} branch from this
 * entry up to {@code Q+F0}, then {@code [Q+F5, upper)}. Entries already read are handed to the part
 * they belong to, so a split costs a store read only when a part has not been read yet. A range with
 * no such pair of branches is read exactly as in byte order.
 *
 * <p>A bound or a name with many lead bytes yields hundreds of nested parts, and most are usually
 * empty. Parts are queued in provider order, which is not byte order: the {@code EE/EF} parts of a
 * lower bound, and the parts split off a name, descend in byte order as the queue advances. So when
 * a read comes back empty, one {@link Probe} for the first key at or after the lowest pending store
 * position settles every part below that key, wherever it is queued, and parts split off later
 * inherit what it found. A read that finds rows costs what it did before; each empty one adds at
 * most one probe.
 */
final class AzureOrderedCursor {
    /** Reads up to {@code limit} entries of {@code [from, upper)} in byte order. */
    interface Source {
        List<Item> read(byte[] from, boolean inclusive, byte[] upper, int limit);
    }

    /** The first fixture key at or after {@code from} in byte order, or {@code null} when none is. */
    interface Probe {
        byte[] first(byte[] from, boolean inclusive);
    }

    /** A Blob ({@code object != null}) or a rolled BlobPrefix spelled {@code name}. */
    record Item(byte[] name, ListedObject object) {
        static Item blob(ListedObject object) {
            return new Item(object.key(), object);
        }

        static Item prefix(byte[] name) {
            return new Item(name, null);
        }

        boolean isPrefix() {
            return object == null;
        }
    }

    private static final int MAX_BATCH = 4096;
    private final Source source;
    private final Probe probe;
    private final Deque<Part> parts = new ArrayDeque<>();
    private int splits;

    AzureOrderedCursor(Source source, Probe probe, List<AzureOrder.Range> ranges) {
        this.source = source;
        this.probe = probe;
        for (int i = ranges.size() - 1; i >= 0; i--) {
            AzureOrder.Range range = ranges.get(i);
            parts.push(new Part(range.from(), range.fromInclusive(), range.upper()));
        }
    }

    /** Branch swaps performed so far. */
    int splits() {
        return splits;
    }

    /**
     * The next entry in provider order, or {@code null} at the end. {@code wanted} sizes a store read
     * when one is needed: the number of entries the caller still expects to take.
     */
    Item next(int wanted) {
        while (!parts.isEmpty()) {
            Part part = parts.peek();
            if (part.buffered.isEmpty()) {
                if (part.exhausted) {
                    parts.pop();
                } else {
                    part.fill(source, Math.clamp(wanted, 2, MAX_BATCH));
                    if (part.buffered.isEmpty()) probeLowest();
                }
                continue;
            }
            Item item = part.buffered.peekFirst();
            int at = AzureOrder.firstSwap(item.name(), part.upper);
            if (at < 0) {
                return part.buffered.pollFirst();
            }
            split(parts.pop(), item.name(), at);
        }
        return null;
    }

    private void split(Part part, byte[] name, int at) {
        splits++;
        byte[] f0 = AzureOrder.child(name, at, 0xf0);
        byte[] f5 = AzureOrder.child(name, at, 0xf5);
        boolean tail = part.upper == null || ByteKeys.compareUnsigned(f5, part.upper) < 0;
        Part branch = new Part(part.from, part.fromInclusive, f0);
        Part supplementary = new Part(f0, true, tail ? f5 : part.upper);
        Part rest = tail ? new Part(f5, true, part.upper) : null;
        byte[] last = part.buffered.peekLast().name();
        for (Item item : part.buffered) {
            (ByteKeys.compareUnsigned(item.name(), f0) < 0 ? branch
                    : rest == null || ByteKeys.compareUnsigned(item.name(), f5) < 0 ? supplementary : rest)
                    .buffered.add(item);
        }
        for (Part child : rest == null ? List.of(branch, supplementary) : List.of(branch, supplementary, rest)) {
            if (part.exhausted || child.upper != null && ByteKeys.compareUnsigned(last, child.upper) >= 0) {
                child.exhausted = true;
            } else if (!child.buffered.isEmpty()) {
                child.from = part.from;
                child.fromInclusive = part.fromInclusive;
            }
        }
        if (part.probed) {
            for (Part child : rest == null ? List.of(branch, supplementary) : List.of(branch, supplementary, rest)) {
                child.learn(part.from, part.fromInclusive, part.next);
            }
        }
        if (rest != null) parts.push(rest);
        parts.push(branch);
        parts.push(supplementary);
    }

    /**
     * Probes once from the lowest store position still to be read and not yet probed, then applies
     * the key found to every part: below it a part is exhausted, holding it a part needs no probe.
     */
    private void probeLowest() {
        Part lowest = null;
        for (Part part : parts) {
            if (!part.exhausted && !part.probed && (lowest == null || part.startsBelow(lowest))) lowest = part;
        }
        if (lowest == null) return;
        byte[] from = lowest.from;
        boolean inclusive = lowest.fromInclusive;
        byte[] key = probe.first(from, inclusive);
        for (Part part : parts) part.learn(from, inclusive, key);
    }

    /**
     * A byte range still to be read in provider order: {@code buffered} holds entries read in byte
     * order and not yet returned, {@code from} is where the next store read starts.
     */
    private static final class Part {
        private final Deque<Item> buffered = new ArrayDeque<>();
        private final byte[] upper;
        private byte[] from;
        private boolean fromInclusive;
        private boolean exhausted;
        /** {@code next} is the first fixture key at or after {@code from}, or {@code null} for none. */
        private boolean probed;
        private byte[] next;

        Part(byte[] from, boolean fromInclusive, byte[] upper) {
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.upper = upper;
        }

        /** Whether this part's store position is below {@code other}'s, an inclusive one first on a tie. */
        boolean startsBelow(Part other) {
            int cmp = ByteKeys.compareUnsigned(from, other.from);
            return cmp < 0 || cmp == 0 && fromInclusive && !other.fromInclusive;
        }

        /**
         * Applies a probe from {@code at}: {@code key} is the first fixture key at or after it (after
         * it when {@code inclusive} is false), {@code null} for none. A part whose store position is
         * not below {@code at} is then exhausted when its range ends at or before {@code key}.
         */
        void learn(byte[] at, boolean inclusive, byte[] key) {
            int cmp = ByteKeys.compareUnsigned(from, at);
            if (exhausted || cmp < 0 || cmp == 0 && fromInclusive && !inclusive) return;
            if (key == null || upper != null && ByteKeys.compareUnsigned(upper, key) <= 0) {
                exhausted = true;
                return;
            }
            int position = ByteKeys.compareUnsigned(key, from);
            if (position > 0 || position == 0 && fromInclusive) {
                probed = true;
                next = key;
            }
        }

        void fill(Source source, int limit) {
            probed = false;
            List<Item> read = source.read(from, fromInclusive, upper, limit);
            buffered.addAll(read);
            if (read.size() < limit) {
                exhausted = true;
                return;
            }
            Item last = read.get(read.size() - 1);
            if (!last.isPrefix()) {
                from = last.name();
                fromInclusive = false;
                return;
            }
            byte[] after = AzureListPager.byteSuccessor(last.name());
            if (after == null) {
                exhausted = true;
            } else {
                from = after;
                fromInclusive = true;
            }
        }
    }
}
