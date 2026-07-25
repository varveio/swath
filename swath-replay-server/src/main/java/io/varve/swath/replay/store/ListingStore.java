/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import java.util.List;

/**
 * An ordered, byte-range reader over a listing fixture. A store speaks <b>ranges</b>, never S3
 * pagination semantics: it returns the first {@code limit} object rows whose key falls in
 * {@code [from, toExclusive)} (with {@code from} inclusive or exclusive per {@code fromInclusive}),
 * in ascending unsigned key order. All of max-keys accounting, truncation, delimiter rollup,
 * common-prefix resume, and continuation tokens live in {@code ListObjectsV2Pager}, above this seam,
 * so every store — DuckDB and sorted Parquet alike — plugs in behind the same pager and any
 * differential disagreement is attributable to a store by construction.
 *
 * <p><b>One optional, opt-in exception: {@link #delimitedRollup}.</b> The pager's range-based rollup
 * is O(directories) <em>store round-trips</em> — one seek per rolled-up common prefix. On a backing
 * store with a high fixed per-call cost (a DuckDB {@code read_parquet} query is ~31 ms regardless of
 * rows returned) that is the dominant cost of a wide {@code delimiter=/} structure probe: a
 * 121-child directory is ~121 queries ≈ 3.7 s, enough to blow a client's probe timeout. A store MAY
 * override {@code delimitedRollup} to answer the whole rollup as a native skip-scan instead — hopping
 * the cursor from prefix to successor prefix against its own routing index, touching no more than one
 * row group per hop and none at all for a row group provably inside one common prefix ({@link
 * SortedParquetStore}'s implementation is the current example: O(entries emitted), never
 * O(subtree objects), and it respects {@code max-keys} rather than rolling up the whole tree
 * regardless of it). This deliberately lifts delimiter semantics below the seam for that one shape; it
 * is gated to the {@code /} delimiter and MUST return byte-identical results to the pager's range
 * walk, which the sorted-vs-DuckDB differential suite enforces (the DuckDB oracle does not override
 * it, so it stays the range-based reference). Default: decline, and the pager walks ranges as before.
 *
 * @implSpec {@link #rows} must not apply prefix, delimiter, or start-after semantics; it honors only
 *           the range bounds, the limit, and the projection. Bounds are {@link ByteKey}s
 *           (defensively copied) and may be {@code null} to mean an open lower/upper bound.
 */
public interface ListingStore extends AutoCloseable {

    /**
     * Returns up to {@code limit} rows with key in {@code [from, toExclusive)} in ascending unsigned
     * key order.
     *
     * @param from          inclusive/exclusive lower bound (per {@code fromInclusive}); {@code null} = open
     * @param fromInclusive whether {@code from} itself is included (ignored when {@code from} is null)
     * @param toExclusive   exclusive upper bound; {@code null} = open
     * @param limit         maximum rows to return
     * @param projection    which columns to materialise
     */
    List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit, Projection projection);

    /**
     * One entry of a {@code delimiter=/} rollup produced by {@link #delimitedRollup}: either a
     * rolled-up common prefix (bytes ending in the delimiter, {@code object == null}) or a bare
     * object directly under the scan prefix ({@code commonPrefix == null}).
     */
    record DelimitedEntry(byte[] commonPrefix, ListedObject object) {
        public boolean isCommonPrefix() {
            return commonPrefix != null;
        }
    }

    /**
     * Optional fast path for a {@code delimiter=/} listing over {@code [from, toExclusive)} scoped to
     * {@code prefix}: the rolled-up common prefixes and bare objects, interleaved in ascending key
     * order, up to {@code limit + 1} entries so the caller can detect truncation from the extra one.
     * Returns {@code null} to decline — the caller then walks ranges via {@link #rows} (the default,
     * and the behaviour of every store that does not override this).
     *
     * @param delimiter   the raw delimiter bytes; an implementation MAY restrict its fast path (e.g.
     *                    to a single {@code /}) and decline otherwise
     * @param prefix      the scan prefix every returned key/common-prefix starts with
     * @return up to {@code limit + 1} entries, or {@code null} to decline
     */
    default List<DelimitedEntry> delimitedRollup(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                                 byte[] prefix, byte[] delimiter, int limit, Projection projection) {
        return null;
    }

    @Override
    void close();
}
