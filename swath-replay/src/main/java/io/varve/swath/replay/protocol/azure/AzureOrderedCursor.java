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
 */
final class AzureOrderedCursor {
    /** Reads up to {@code limit} entries of {@code [from, upper)} in byte order. */
    interface Source {
        List<Item> read(byte[] from, boolean inclusive, byte[] upper, int limit);
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
    private final Deque<Part> parts = new ArrayDeque<>();
    private int splits;

    AzureOrderedCursor(Source source, List<AzureOrder.Range> ranges) {
        this.source = source;
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
        if (rest != null) parts.push(rest);
        parts.push(branch);
        parts.push(supplementary);
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

        Part(byte[] from, boolean fromInclusive, byte[] upper) {
            this.from = from;
            this.fromInclusive = fromInclusive;
            this.upper = upper;
        }

        void fill(Source source, int limit) {
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
