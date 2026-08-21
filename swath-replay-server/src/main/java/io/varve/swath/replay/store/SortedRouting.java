/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import java.util.List;

/**
 * The pure routing math behind {@link SortedParquetStore}: which row group a key lives in, from the
 * derived {@code (file, rowGroup, firstKey, rowCount)} index alone — never Parquet footer stats
 * (§9.1), which may be truncated for long keys and silently misroute. No I/O — extracted so the
 * search and its boundary cases are unit-testable in isolation.
 *
 * <p><b>This class used to plan bounded queries.</b> It computed an exclusive upper bound (by
 * accumulating whole row groups' {@code rowCount} until they could supply {@code limit}) and the set
 * of files that window spanned, because the store answered a range read with a SQL query that would
 * otherwise scan to the end of the file. The store now reads through Parquet's page index and stops
 * as soon as it holds {@code limit} rows, so there is nothing left to bound: a reader that stops does
 * not need stopping. The planner, its speculative variant, and the {@code rowCount}-must-equal-the-
 * OBJECT-row-count assumption they all rested on went with it — the last was a real hazard, since an
 * undercounting index could silently truncate a listing, which the page-index reader is immune to.
 *
 * <p>{@link #startRowGroup} survives because two in-tree stores key off the same derived index — this
 * one, and {@code swath-sim}'s decode-once streaming tier seeking to the group a fresh cursor lands
 * in — and re-deriving that search there would make two places responsible for agreeing on what
 * "contains" means at a group boundary.
 */
public final class SortedRouting {

    private SortedRouting() {
    }

    /** The row group that contains {@code from}: the last one whose first key is {@code <= from}. */
    public static int startRowGroup(List<IndexEntry> index, ByteKey from) {
        if (from == null) {
            return 0;
        }
        return Math.max(0, countFirstKeys(index, from) - 1);
    }

    /**
     * Count of index entries whose first key is {@code <= key}. The index is globally ascending, so a
     * binary search suffices.
     */
    private static int countFirstKeys(List<IndexEntry> index, ByteKey key) {
        int lo = 0;
        int hi = index.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (index.get(mid).firstKey().compareTo(key) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
