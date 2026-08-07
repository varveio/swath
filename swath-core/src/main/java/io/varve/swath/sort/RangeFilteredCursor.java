/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.util.NoSuchElementException;

/**
 * {@link RangeFilteredStream}'s twin one level up: the same {@code [lo, hi)} key-only trim applied to
 * a merged {@link SortedCursor} instead of to each input {@link EntryStream}.
 *
 * <p><b>Why the trim moves up on the page-run path.</b> {@link ParallelRangeMerge} restricts a range's
 * Parquet inputs by wrapping each opened stream, because the columnar path merges entry-typed streams
 * anyway. The page-run path must not do that: {@link KWayMerge} takes {@link PageAwareMerger}'s
 * decode-free page-whole fast path only when every input reports {@link
 * KWayMerge.SegmentIo#supportsPageFrontier}, and a frontier is a stream of PAGES, not of entries — so
 * an entry-level wrapper around each input would silently force the whole merge back onto the
 * entry-typed {@link StreamingMerger} and lose the very fast path the page-run format exists for.
 * Filtering the merged output instead keeps the fast path and produces exactly the same rows.
 *
 * <p><b>What still needs trimming after the page skip.</b> {@link RangeScopedPageFrontier} drops
 * pages that cannot overlap the range, but a page that STRADDLES a boundary is kept whole and carries
 * rows on the far side of it. Those are the only out-of-range rows that reach here, and this cursor
 * removes them by the same exact per-row key compare {@link RangeFilteredStream} uses — {@code lo}
 * inclusive, {@code hi} exclusive, either bound {@code null} meaning unbounded — so all rows sharing a
 * key (versions, cross row_types) still land in exactly one range.
 *
 * <p>Because the underlying cursor is sorted, this is a two-phase scan: skip the {@code < lo} prefix,
 * then stream until the first key {@code >= hi}, after which the view is exhausted (the underlying
 * cursor may still hold rows; closing this closes it).
 */
final class RangeFilteredCursor implements SortedCursor {

    private final SortedCursor inner;
    private final byte[] lo;   // inclusive, or null for -inf
    private final byte[] hi;   // exclusive, or null for +inf
    private ListEntry head;

    RangeFilteredCursor(SortedCursor inner, byte[] lo, byte[] hi) {
        this.inner = inner;
        this.lo = lo;
        this.hi = hi;
        this.head = loadFirst();
    }

    private ListEntry loadFirst() {
        while (inner.hasNext()) {
            ListEntry e = inner.next();
            if (belowLo(e)) {
                continue;   // skip the < lo prefix
            }
            return atOrAboveHi(e) ? null : e;
        }
        return null;
    }

    private ListEntry advance() {
        if (!inner.hasNext()) {
            return null;
        }
        ListEntry e = inner.next();
        return atOrAboveHi(e) ? null : e;
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
    public ListEntry next() {
        if (head == null) {
            throw new NoSuchElementException();
        }
        ListEntry current = head;
        head = advance();
        return current;
    }

    @Override
    public void close() {
        inner.close();
    }
}
