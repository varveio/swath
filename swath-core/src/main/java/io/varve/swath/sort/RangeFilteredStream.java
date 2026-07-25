/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;

/**
 * An off-by-default {@link EntryStream} view of an underlying sorted stream restricted
 * to one contiguous key range {@code [lo, hi)} — {@code lo} inclusive, {@code hi} exclusive, either
 * bound {@code null} meaning unbounded (the first/last range). Range membership is decided on the
 * <b>key bytes only</b> ({@link KeyBytes#compareUnsigned}, the primary component of
 * {@link ListEntryComparator}), so all rows sharing a key — a key's versions, and any cross-row-type
 * rows at the same key — land in exactly one range and stay grouped (the boundary is a key value; a
 * row whose key equals a boundary belongs to the range that has that boundary as its <em>lo</em>).
 *
 * <p>Because the underlying stream is already sorted, this is a two-phase scan: skip the prefix of
 * keys {@code < lo}, then stream through until the first key {@code >= hi} (at which point this view
 * is exhausted). This is the CORRECTNESS layer — every row is assigned to its range by an exact
 * per-row key compare, so it stays correct regardless of what the reader below it decodes. It sits on
 * top of {@link ParallelRangeMerge}'s row-group skip (via {@link SortedFileIndex#rowGroupSpans}):
 * {@link SegmentReader}'s scoped constructor decodes only the selected physical row groups, so each
 * range decodes ~1/R of the data instead of
 * every segment; a boundary row group the skip still reads whole is trimmed per-row here, and
 * correctness never depends on skip precision — only on the skip never pruning a group that could
 * hold an in-range key.
 *
 * <p>Closing this view closes the wrapped stream (it owns exactly one underlying reader).
 */
final class RangeFilteredStream implements EntryStream {

    private final EntryStream inner;
    private final byte[] lo;   // inclusive, or null for -inf
    private final byte[] hi;   // exclusive, or null for +inf
    private ListEntry head;

    RangeFilteredStream(EntryStream inner, byte[] lo, byte[] hi) throws IOException {
        this.inner = inner;
        this.lo = lo;
        this.hi = hi;
        this.head = loadFirst();
    }

    private ListEntry loadFirst() throws IOException {
        while (inner.hasNext()) {
            ListEntry e = inner.peek();
            if (belowLo(e)) {
                inner.next();   // skip the < lo prefix
                continue;
            }
            return atOrAboveHi(e) ? null : inner.next();
        }
        return null;
    }

    private ListEntry advance() throws IOException {
        if (!inner.hasNext()) {
            return null;
        }
        return atOrAboveHi(inner.peek()) ? null : inner.next();
    }

    private boolean belowLo(ListEntry e) {
        return lo != null && KeyBytes.compareUnsigned(e.key().rawUnsafe(), lo) < 0;
    }

    private boolean atOrAboveHi(ListEntry e) {
        return hi != null && KeyBytes.compareUnsigned(e.key().rawUnsafe(), hi) >= 0;
    }

    @Override
    public boolean hasNext() {
        return head != null;
    }

    @Override
    public ListEntry peek() {
        return head;
    }

    @Override
    public ListEntry next() throws IOException {
        ListEntry current = head;
        head = advance();
        return current;
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
