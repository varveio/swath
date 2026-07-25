/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The pure routing math behind {@link SortedParquetStore}: turn a range read into the exclusive upper
 * bound and the set of files a single bounded {@code read_parquet} must touch, using only the derived
 * {@code (file, rowGroup, firstKey, rowCount)} index — never Parquet footer stats (§9.1), which may
 * be truncated for long keys and silently misroute. No I/O, no DuckDB — extracted so the upper-bound
 * invariant and its edge cases are unit-testable in isolation.
 *
 * <p><b>Upper bound by invariant.</b> {@code from} may sit anywhere inside its row group, so the math
 * conservatively counts <b>zero</b> rows from that group and accumulates whole following groups'
 * {@code rowCount}s until the total reaches {@code limit} ({@code = maxKeys + 1}), then bounds at the
 * first key of the next group after that (with 50–100k-row groups this is typically {@code
 * minKey(RG_{i+2})}). It clamps to an <b>open</b> bound when the tail cannot supply {@code limit}
 * rows, and intersects with the caller's {@code toExclusive} (the prefix upper bound) by taking the
 * tighter of the two — guaranteeing {@code [from, bound)} holds at least
 * {@code limit} rows whenever that many exist within the caller's window
 * {@code [from, toExclusive)}.
 *
 * <p><b>{@code rowCount} is assumed to equal the row group's OBJECT-row count</b> (delete markers
 * can't reach a sorted file — they carry a {@code version_id} and fail the sort-fixture versioned
 * fail-fast, §0.6 — but a legacy delimiter'd capture's {@code COMMON_PREFIX} rows can). That
 * assumption is sound only because sorted-serving eligibility ({@code SortedFixtures#loadIndex})
 * refuses to build an index over any row group that isn't provably pure {@code
 * row_type='OBJECT'} — this class never re-checks that itself; it trusts the index it was handed.
 */
final class SortedRouting {

    private SortedRouting() {
    }

    /** The files a read touches, in key order, and its exclusive upper bound ({@code null} = open). */
    record QueryPlan(List<Path> files, ByteKey upperBound) {

        boolean isEmpty() {
            return files.isEmpty();
        }
    }

    static QueryPlan plan(List<IndexEntry> index, ByteKey from, ByteKey toExclusive, int limit) {
        if (index.isEmpty() || limit <= 0) {
            return new QueryPlan(List.of(), null);
        }
        int startRg = startRowGroup(index, from);
        ByteKey invariantBound = invariantUpperBound(index, startRg, limit);
        ByteKey upper = tighter(invariantBound, toExclusive);
        return new QueryPlan(touchedFiles(index, startRg, upper), upper);
    }

    /** The row group that contains {@code from}: the last one whose first key is {@code <= from}. */
    static int startRowGroup(List<IndexEntry> index, ByteKey from) {
        if (from == null) {
            return 0;
        }
        return Math.max(0, countFirstKeys(index, from, true) - 1);
    }

    /**
     * The invariant exclusive upper bound: accumulate whole row groups <b>after</b> {@code startRg}
     * until their combined {@code rowCount} reaches {@code limit}, then bound at the first key of the
     * next group; {@code null} (open) when the tail cannot supply {@code limit} rows.
     */
    static ByteKey invariantUpperBound(List<IndexEntry> index, int startRg, int limit) {
        long accumulated = 0;
        for (int k = startRg + 1; k < index.size(); k++) {
            accumulated += index.get(k).rowCount();
            if (accumulated >= limit) {
                return (k + 1 < index.size()) ? index.get(k + 1).firstKey() : null;
            }
        }
        return null;
    }

    /** The distinct files of the row groups the window {@code [startRg, upper)} spans, in key order. */
    static List<Path> touchedFiles(List<IndexEntry> index, int startRg, ByteKey upper) {
        int last = (upper == null) ? index.size() - 1 : countFirstKeys(index, upper, false) - 1;
        if (last < startRg) {
            return List.of();
        }
        Set<Path> files = new LinkedHashSet<>();
        for (int k = startRg; k <= last; k++) {
            files.add(index.get(k).file());
        }
        return new ArrayList<>(files);
    }

    static ByteKey tighter(ByteKey a, ByteKey b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Count of index entries whose first key is {@code <= key} (when {@code orEqual}) or {@code < key}.
     * The index is globally ascending, so a binary search suffices.
     */
    private static int countFirstKeys(List<IndexEntry> index, ByteKey key, boolean orEqual) {
        int lo = 0;
        int hi = index.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = index.get(mid).firstKey().compareTo(key);
            boolean before = orEqual ? cmp <= 0 : cmp < 0;
            if (before) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
