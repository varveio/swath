/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.Successor;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.sort.RowGroupOrderException;
import io.varve.swath.sort.SortedRangeReader;
import io.varve.swath.sort.SortedRowGroupReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link ListingStore} backed by bounded {@code read_parquet} queries over a stamped, globally
 * sorted swath Parquet fixture. It never materialises a temp database and never builds an
 * index — the in-memory row-group routing index ({@link IndexEntry}, derived once at startup) turns
 * each range read into <b>one</b> tight {@code key >|>= ? AND key < :bound ORDER BY key LIMIT ?}
 * query limited to the file(s) the range spans, avoiding a whole-fixture top-N query.
 *
 * <h2>Upper bound by invariant, never a constant</h2>
 * The exclusive {@code :bound} — the query's {@code key < ?} parameter — is computed by
 * {@link SortedRouting}, which guarantees {@code [from, bound)} holds at least {@code limit} rows
 * if that many exist within {@code [from, toExclusive)} ({@code limit} is the pager's
 * {@code maxKeys + 1}), routing off the derived index only, never Parquet footer stats.
 *
 * <p><b>The invariant assumes {@code rowCount == OBJECT-row-count} per row group.</b> That is only
 * sound because sorted-serving <em>eligibility</em> ({@code SortedFixtures#loadIndex}) refuses any
 * file with a row group that isn't provably pure {@code row_type='OBJECT'} (a mixed-row-type file —
 * see {@code docs/internals/metrics-internals.md} §8, {@code mixed_row_types} — is never handed to this
 * store; it falls back to the materialized DuckDB store instead). The {@code WHERE row_type =
 * 'OBJECT'} clause in every
 * query built here is therefore belt-and-braces — eligibility already guarantees purity — not what
 * makes the invariant correct.
 *
 * <p>Edge cases are first-class: an <b>empty fixture</b> (no row groups ⇒ empty index) returns no
 * rows; a {@code from} <b>before the first key</b> starts at row group 0; a {@code from}
 * <b>in or past the last row group</b> yields an open upper bound (read to end of file); and a range
 * that lies entirely below the first key or above {@code toExclusive} touches no files at all.
 *
 * <p>DuckDB's Parquet metadata cache is enabled per connection so repeated pages over the
 * same file reuse the decoded footer. Owner columns are decoded only when {@link Projection#owner()}
 * — the whole point of the projection, unlike the DuckDB store where they are behavior-cheap.
 *
 * <p>Unlike {@link DuckDbListingStore}, this store assumes the <b>canonical</b> current schema
 * unconditionally (no {@code owner_display_name}/{@code checksum_type} legacy-column backfill): every
 * sorted file is produced by the in-tree writer ({@code SortedParquetWriter} / {@code
 * SegmentParquetSink}), which always writes the full canonical schema, so there is no legacy-schema
 * sorted file for this store to tolerate — unlike {@code DuckDbListingStore}, which must also serve
 * arbitrary pre-existing unsorted captures written by older schema versions.
 *
 * <h2>Performance characteristics</h2>
 * Each bare-store {@link #rows} read executes one bounded DuckDB query. The per-connection object
 * cache retains Parquet metadata, but this class does not cache decoded rows or query results.
 * Production wiring normally wraps it in {@link WindowedListingStore}, which amortizes delegate reads
 * across sequential pages while keeping memory bounded. Query shape, pool size, and cache changes must
 * preserve the routing invariant above and should be evaluated with the replay server's {@code bench}
 * command on representative fixtures; no fixed page latency is implied by this implementation.
 *
 * <h2>The {@code delimiter=/} skip-scan</h2>
 * {@link #delimitedRollup} does not go through DuckDB at all: it answers a rollup as a series of
 * index hops directly against the row-group routing index, each hop random-accessing exactly the one
 * row group ({@link SortedRowGroupReader}, in {@code swath-core}) a scan cursor currently lands in —
 * O(entries emitted), never O(subtree). See its own javadoc for the algorithm.
 */
public final class SortedParquetStore implements ListingStore {

    /**
     * Whether a range read is answered by Parquet's own page index ({@link SortedRangeReader}) rather
     * than by a bounded DuckDB {@code read_parquet} query. On by default; set
     * {@code -Dswath.replay.sorted.range-reads=duckdb} to serve them the old way.
     *
     * <p>The switch exists because the two paths must be able to answer the same request in the same
     * process for the differential suites to mean anything — and because a serving path this new
     * should have a way back that does not require a release.
     */
    private static final boolean PARQUET_RANGE_READS =
            !"duckdb".equalsIgnoreCase(System.getProperty("swath.replay.sorted.range-reads", "parquet"));

    private final List<IndexEntry> index;
    private final DuckDbConnectionPool pool;
    private final ReplayMetrics metrics;
    private final boolean recordPageReadLatency;
    private final Map<Path, SortedRangeReader> rangeReaders;

    public SortedParquetStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics) {
        this(files, index, metrics, defaultConnectionCount());
    }

    /**
     * @param files          every sorted file backing the fixture, in key order (used only to validate
     *                       the index against the fixture — routing is index-driven)
     * @param index          the derived {@code (file, rowGroup, firstKey, rowCount)} routing index,
     *                       globally ascending by first key (its sanity was checked at load)
     * @param metrics        records {@code swath.replay.page.read.latency} per read
     * @param connectionCount pooled in-memory DuckDB connections (== the natural request-concurrency
     *                        bound), each with the Parquet metadata cache enabled
     */
    public SortedParquetStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics,
                              int connectionCount) {
        this(files, index, metrics, connectionCount, true);
    }

    /**
     * @param recordPageReadLatency whether this store records {@code swath.replay.page.read.latency}
     *                              around each read. {@code true} for the bare (unwrapped) sorted path;
     *                              {@code false} when a {@link WindowedListingStore} decorates this
     *                              store — the wrapper then owns the outer per-page {@code
     *                              page.read.latency} so the corridor metric measures the amortized
     *                              per-page cost (hit or miss), not the ~50k-row window fill, and the
     *                              fill is instead measured by {@code prefetch.window.fill}.
     */
    public SortedParquetStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics,
                              int connectionCount, boolean recordPageReadLatency) {
        this.index = List.copyOf(index);
        validateIndexWithinFiles(this.index, files);
        this.metrics = metrics;
        this.recordPageReadLatency = recordPageReadLatency;
        this.rangeReaders = PARQUET_RANGE_READS ? openRangeReaders(files) : Map.of();
        List<Connection> pooled = openPool(connectionCount);
        this.pool = new DuckDbConnectionPool(pooled,
                "interrupted waiting for a sorted Parquet connection",
                "sorted Parquet connection pool rejected a returned connection",
                "failed to close sorted Parquet store");
    }

    /** The CPU-bounded default connection count, mirroring {@link DuckDbListingStore}. */
    public static int defaultConnectionCount() {
        return DuckDbListingStore.defaultConnectionCount();
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        SortedRouting.QueryPlan plan = SortedRouting.plan(index, from, toExclusive, limit);
        if (plan.isEmpty()) {
            return List.of();
        }
        if (!rangeReaders.isEmpty()) {
            if (!recordPageReadLatency) {
                return rangeRead(from, fromInclusive, toExclusive, limit, projection);
            }
            var readerSample = metrics.startPageReadTimer();
            try {
                return rangeRead(from, fromInclusive, toExclusive, limit, projection);
            } finally {
                metrics.recordPageRead(readerSample);
            }
        }
        SortedRouting.QueryPlan speculative = SortedRouting.speculativePlan(index, from, toExclusive, limit);
        if (speculative != null && Objects.equals(speculative.upperBound(), plan.upperBound())) {
            // The caller's own window already bounds at or inside the group end; nothing to gain.
            speculative = null;
        }
        Connection connection = pool.borrow();
        try {
            if (!recordPageReadLatency) {
                return readSpeculatively(connection, speculative, plan, from, fromInclusive, limit, projection);
            }
            var sample = metrics.startPageReadTimer();
            try {
                return readSpeculatively(connection, speculative, plan, from, fromInclusive, limit, projection);
            } finally {
                metrics.recordPageRead(sample);
            }
        } finally {
            pool.release(connection);
        }
    }

    /**
     * Reads the speculative window first and widens to the invariant one only if it came up short.
     *
     * <p>The gamble is sound because a tighter upper bound returns a prefix of what a looser one
     * would: a full-count result is byte-identical to the invariant read, and a short one is the
     * only signal that needs acting on. Correctness therefore never depends on the guess, only cost
     * does — a widened read costs one wasted query, a hit saves a whole row group's decode.
     */
    private List<ListedObject> readSpeculatively(Connection connection, SortedRouting.QueryPlan speculative,
                                                 SortedRouting.QueryPlan plan, ByteKey from,
                                                 boolean fromInclusive, int limit, Projection projection) {
        if (speculative == null) {
            metrics.recordSpeculativeBound(ReplayMetrics.SPECULATION_DECLINED);
            return query(connection, plan.files(), from, fromInclusive, speculativeUpper(speculative, plan),
                    limit, projection);
        }
        List<ListedObject> rows = query(connection, speculative.files(), from, fromInclusive,
                speculative.upperBound(), limit, projection);
        if (rows.size() >= limit) {
            metrics.recordSpeculativeBound(ReplayMetrics.SPECULATION_HIT);
            return rows;
        }
        // Short: either the group's tail really did run out before `limit`, or the listing ends
        // here. The invariant window is the only one that can tell those apart, so re-read it.
        metrics.recordSpeculativeBound(ReplayMetrics.SPECULATION_WIDENED);
        return query(connection, plan.files(), from, fromInclusive, plan.upperBound(), limit, projection);
    }

    private static ByteKey speculativeUpper(SortedRouting.QueryPlan speculative, SortedRouting.QueryPlan plan) {
        return speculative == null ? plan.upperBound() : speculative.upperBound();
    }

    /**
     * A range read answered by the Parquet page index, file by file in key order.
     *
     * <p>No upper-bound routing is needed here, and that is the point: the invariant bound exists to
     * stop DuckDB scanning to the end of the file, whereas this reader stops as soon as it has
     * {@code limit} rows and skips pages the index rules out. Only the caller's own {@code
     * toExclusive} is passed down, because that one is part of the answer rather than a performance
     * device.
     *
     * <p>A rolled multi-file fixture is range-partitioned in file order, so reading the files in
     * index order and concatenating preserves global key order; only the first file starts at {@code
     * from}, every later one starts at its own beginning.
     */
    private List<ListedObject> rangeRead(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                         int limit, Projection projection) {
        byte[] lower = from == null ? null : from.toByteArray();
        byte[] upper = toExclusive == null ? null : toExclusive.toByteArray();
        boolean inclusive = fromInclusive;
        List<ListedObject> out = new ArrayList<>(Math.min(limit, 1024));
        Path previous = null;
        int rows = 0;
        boolean success = false;
        var sample = metrics.startParquetQueryTimer();
        try {
            for (int rg = SortedRouting.startRowGroup(index, from); rg < index.size() && out.size() < limit; rg++) {
                Path file = index.get(rg).file();
                if (file.equals(previous)) {
                    continue;   // the index is per row group; the reader works a whole file at a time
                }
                previous = file;
                for (SortedRowGroupReader.ObjectRow row :
                        rangeReaders.get(file).range(lower, inclusive, upper, limit - out.size(),
                                projection.owner())) {
                    out.add(new ListedObject(row.key(), row.size(), row.lastModifiedEpochMicros(),
                            row.etag(), row.storageClass(), row.ownerId(), row.ownerDisplayName(),
                            row.checksumAlgorithm(), row.checksumType()));
                }
                lower = null;
                inclusive = true;
            }
            rows = out.size();
            success = true;
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("failed to range-read the sorted Parquet fixture", e);
        } finally {
            metrics.recordParquetQuery(sample, rows, success);
        }
    }

    private static Map<Path, SortedRangeReader> openRangeReaders(List<Path> files) {
        Map<Path, SortedRangeReader> readers = new LinkedHashMap<>();
        try {
            for (Path file : files) {
                readers.put(file, new SortedRangeReader(file));
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to open a sorted Parquet range reader", e);
        }
        return Map.copyOf(readers);
    }

    /**
     * The native {@code delimiter=/} skip-scan (the {@link ListingStore#delimitedRollup} fast path).
     * Only the single {@code /} (0x2F) delimiter is handled; every other shape (multi-byte delimiter)
     * declines and the pager walks ranges instead. {@code toExclusive} and {@code prefix} may both be
     * {@code null} — an open upper bound (no prefix, or the all-{@code 0xFF} prefix whose 0xFF-carry
     * has no finite bound) is scanned to the end of the fixture, same as {@link #rows}.
     */
    @Override
    public List<DelimitedEntry> delimitedRollup(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                                byte[] prefix, byte[] delimiter, int limit, Projection projection) {
        if (delimiter == null || delimiter.length != 1 || delimiter[0] != (byte) '/') {
            return null;
        }
        var sample = metrics.startParquetQueryTimer();
        int entries = 0;
        boolean success = false;
        try {
            List<DelimitedEntry> out = skipScan(from, fromInclusive, toExclusive, prefix, limit, projection);
            entries = out.size();
            success = true;
            return out;
        } catch (RowGroupOrderException e) {
            // Count BEFORE rethrowing: the request fails and no other path can take over, so the
            // exclusion has to survive into the metrics a sweep classifies from (the same discipline
            // io.varve.swath.sort.PageRunSegmentIo's own pre-throw count keeps).
            metrics.recordServingRefused(e.reason());
            throw e;
        } catch (IOException e) {
            throw new IllegalStateException("failed to run sorted Parquet delimiter skip-scan", e);
        } finally {
            metrics.recordParquetQuery(sample, entries, success);
        }
    }

    @Override
    public void close() {
        RuntimeException failure = pool.close();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * The skip-scan proper: walk a cursor forward through {@code [from, toExclusive)} one hop at a
     * time, exactly as S3's own {@code CommonPrefixes} algorithm does — never a scan of every key
     * under the prefix. At each hop, the row-group index ({@link SortedRouting#startRowGroup}, the
     * same binary search {@link #rows} uses) locates the one row group the cursor currently lands in,
     * and a {@link SortedRowGroupReader.KeyCursor} — opened once per group and kept across every
     * further hop that lands in the same one — steps forward to the cursor's position. The key cursor
     * is <em>resumable</em>, not a bulk decode: a row group here can be far larger than the directory a
     * single hop is chasing, so it decodes only the rows strictly between the previous hop's position
     * and this one's, however many that is, rather than the whole group up front. Before any of that, a
     * hop first tries a zero-I/O shortcut off the routing index alone (see the loop's own comment): a
     * row group whose first key and successor group's first key already share a common prefix is, by
     * sortedness, entirely that one common prefix — skip it whole, no Parquet read at all. On a bucket
     * whose directories run many whole row groups deep this is the dominant cost saving; the per-row
     * cursor below is what a hop falls back to at a prefix boundary.
     *
     * <ul>
     *   <li>The first key at/after the cursor has a {@code /} after the scan prefix → it belongs to a
     *       common prefix {@code P}; emit {@code P} (unless it is {@code <= from} — a resume that lands
     *       inside a directory does not re-emit that directory; only the very first hop can trigger
     *       this, since every later cursor is already past the previous entry) and jump the cursor to
     *       {@link ByteKeys#successor}{@code (P)} inclusive, past {@code P}'s whole subtree in one hop.
     *   <li>No {@code /} after the prefix → a bare object directly under it. This is the only hop that
     *       pays for the full row (all nine {@code ListedObject} columns, via {@link
     *       SortedRowGroupReader#rows}) rather than just the key column — bare objects directly under a
     *       scanned prefix are rare in a real bucket, so bulk-decoding the row group's other eight
     *       columns here, rather than threading a single-row-index deeper into the Parquet reader, is
     *       the simpler trade. Advance the cursor past this exact key (exclusive) and continue.
     *   <li>The cursor lands past every key in its row group (a gap — {@code successor(P)} is rarely an
     *       actual key) → jump straight to the next group's first key; the last group exhausting the
     *       fixture ends the scan.
     * </ul>
     * Stops once {@code limit + 1} entries have been collected (the extra one lets the pager detect
     * truncation) or the range is exhausted, whichever comes first — the early termination that makes
     * this O(entries emitted) rather than O(subtree).
     */
    private List<DelimitedEntry> skipScan(ByteKey from, boolean fromInclusive, ByteKey toExclusive,
                                          byte[] prefix, int limit, Projection projection) throws IOException {
        if (index.isEmpty() || limit <= 0) {
            return List.of();
        }
        byte[] fromBytes = from == null ? null : from.toByteArray();
        byte[] upper = toExclusive == null ? null : toExclusive.toByteArray();
        List<DelimitedEntry> out = new ArrayList<>();

        byte[] cursor = fromBytes;
        boolean inclusive = fromInclusive;
        Path openFile = null;
        SortedRowGroupReader reader = null;
        int cachedBlockIndex = -1;
        SortedRowGroupReader.KeyCursor keyCursor = null;
        List<SortedRowGroupReader.ObjectRow> cachedRows = null;
        try {
            hop:
            while (out.size() < limit + 1) {
                int rg = SortedRouting.startRowGroup(index, cursor == null ? null : ByteKey.copyOf(cursor));
                IndexEntry entry = index.get(rg);
                byte[] groupFirstKey = entry.firstKey().toByteArray();
                if (upper != null && ByteKeys.compareUnsigned(groupFirstKey, upper) >= 0) {
                    break;   // this group starts at/past the upper bound: nothing more in range
                }

                // Zero-I/O shortcut: if this group's first key and the next group's first key already
                // share a common prefix P, then — because the fixture is globally sorted — every key in
                // THIS group (wherever the cursor lands inside it) shares P too: a key between two keys
                // that both start with P must itself start with P (the successor(P) carry construction
                // makes [P, successor(P)) a contiguous interval of the total order, and this group's
                // whole key range sits inside that interval). So the whole group is provably one common
                // prefix without decoding a single row of it — the routing index (free; already resident)
                // is all this needs. Falls through to the normal per-row hop below whenever it doesn't
                // apply (the group straddles a prefix boundary, or is the fixture's last group).
                if (rg + 1 < index.size()) {
                    byte[] wholeGroupPrefix = commonPrefixAfter(groupFirstKey, prefix);
                    if (wholeGroupPrefix != null && Arrays.equals(wholeGroupPrefix,
                            commonPrefixAfter(index.get(rg + 1).firstKey().toByteArray(), prefix))) {
                        if (fromBytes == null || ByteKeys.compareUnsigned(wholeGroupPrefix, fromBytes) > 0) {
                            out.add(new DelimitedEntry(wholeGroupPrefix, null));
                        }
                        switch (ByteKeys.successor(wholeGroupPrefix)) {
                            case Successor.EndOfKeyspace() -> {
                                break hop;
                            }
                            case Successor.Key(ByteKey successorKey) -> {
                                cursor = successorKey.toByteArray();
                                inclusive = true;
                            }
                        }
                        continue;
                    }
                }

                if (!entry.file().equals(openFile)) {
                    if (keyCursor != null) {
                        keyCursor.close();   // releases the group's page buffers; file close alone does not
                        keyCursor = null;
                    }
                    if (reader != null) {
                        reader.close();
                    }
                    reader = new SortedRowGroupReader(entry.file());
                    openFile = entry.file();
                    cachedBlockIndex = -1;
                }
                if (cachedBlockIndex != entry.rowGroup()) {
                    if (keyCursor != null) {
                        keyCursor.close();
                    }
                    keyCursor = reader.openKeyCursor(entry.rowGroup());
                    cachedBlockIndex = entry.rowGroup();
                    cachedRows = null;   // decoded lazily below, only if a bare object actually needs it
                    // A row-group open the zero-I/O shortcut above didn't catch (the group straddles a
                    // prefix boundary, or is the fixture's last group). Counted so a test can pin the
                    // skip-scan's cost to O(prefixes emitted) rather than O(keys under the prefix) —
                    // the very regression this rollup exists to avoid.
                    metrics.recordDelimiterSkipScanRowGroupOpen();
                }
                keyCursor.advanceTo(cursor, inclusive);
                if (!keyCursor.hasCurrent()) {
                    // Nothing at/after the cursor in this group: its whole key range is behind us, so
                    // hop straight to the next group's first key — provably past everything just checked.
                    if (rg + 1 >= index.size()) {
                        break;   // last group exhausted: end of keyspace
                    }
                    cursor = index.get(rg + 1).firstKey().toByteArray();
                    inclusive = true;
                    continue;
                }
                byte[] key = keyCursor.currentKey();
                if (upper != null && ByteKeys.compareUnsigned(key, upper) >= 0) {
                    break;   // past the scan's upper bound: nothing more in range
                }
                byte[] commonPrefix = commonPrefixAfter(key, prefix);
                if (commonPrefix != null) {
                    if (fromBytes == null || ByteKeys.compareUnsigned(commonPrefix, fromBytes) > 0) {
                        out.add(new DelimitedEntry(commonPrefix, null));
                    }
                    switch (ByteKeys.successor(commonPrefix)) {
                        case Successor.EndOfKeyspace() -> {
                            break hop;   // nothing sorts after an all-0xFF prefix: end of listing
                        }
                        case Successor.Key(ByteKey successorKey) -> {
                            cursor = successorKey.toByteArray();
                            inclusive = true;
                        }
                    }
                } else {
                    if (cachedRows == null) {
                        cachedRows = reader.rows(entry.rowGroup(), projection.owner());
                    }
                    out.add(new DelimitedEntry(null, toListedObject(cachedRows.get((int) keyCursor.position()))));
                    cursor = key;
                    inclusive = false;
                }
            }
        } finally {
            if (keyCursor != null) {
                keyCursor.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
        return out;
    }

    /**
     * {@code key}'s rolled-up common prefix under {@code prefix} for the single {@code '/'} delimiter —
     * {@code prefix} plus everything up to and including the first {@code '/'} found strictly after it
     * — or {@code null} when {@code key} is a bare object directly under {@code prefix} (no {@code '/'}
     * appears past the scan prefix). {@code prefix == null} (a root, no-prefix rollup) scans from the
     * start of the key, same as the pager's own {@code commonPrefix} treats an absent prefix.
     */
    private static byte[] commonPrefixAfter(byte[] key, byte[] prefix) {
        int start = prefix == null ? 0 : prefix.length;
        for (int i = start; i < key.length; i++) {
            if (key[i] == '/') {
                return Arrays.copyOf(key, i + 1);
            }
        }
        return null;
    }

    private static ListedObject toListedObject(SortedRowGroupReader.ObjectRow row) {
        return new ListedObject(row.key(), row.size(), rowMapperCompatibleMicros(row.lastModifiedEpochMicros()),
                row.etag(), row.storageClass(), row.ownerId(), row.ownerDisplayName(),
                row.checksumAlgorithm(), row.checksumType());
    }

    /**
     * Reproduces, bit for bit, what {@link RowMapper#read} derives for {@code last_modified} — not
     * the true UTC instant the row actually stores. DuckDB's JDBC driver, for a Parquet-sourced
     * {@code TIMESTAMPTZ} column, does not honor the {@link java.util.Calendar} passed to {@code
     * ResultSet.getTimestamp(column, calendar)}: it takes the value's own UTC wall-clock digits and
     * re-interprets them as wall-clock digits in the <b>JVM's default zone</b> instead — a shift by
     * that zone's offset at the instant in question, a no-op only when the JVM default zone is UTC.
     * {@link RowMapper} unavoidably inherits this because it explicitly requests a UTC calendar (to
     * defend against a different, legitimate shift on a plain un-zoned {@code TIMESTAMP} column, which
     * {@link DuckDbListingStore}'s materialized table can produce for an older capture). Both existing
     * stores go through {@link RowMapper}, so they already agree with each other on the shifted value;
     * this store's native decode must reproduce the very same shift, not the true instant, to stay
     * byte-identical to the DuckDB-backed range walk the differential suite compares it against.
     */
    private static long rowMapperCompatibleMicros(long trueEpochMicros) {
        if (trueEpochMicros == 0L) {
            return 0L;   // RowMapper's own sentinel for "no timestamp" (a null column)
        }
        Instant trueInstant = Instant.ofEpochSecond(
                Math.floorDiv(trueEpochMicros, 1_000_000L), Math.floorMod(trueEpochMicros, 1_000_000L) * 1_000L);
        LocalDateTime utcWallClock = LocalDateTime.ofInstant(trueInstant, ZoneOffset.UTC);
        Instant shifted = utcWallClock.atZone(ZoneId.systemDefault()).toInstant();
        return shifted.getEpochSecond() * 1_000_000L + shifted.getNano() / 1_000L;
    }

    private List<ListedObject> query(Connection connection, List<Path> files, ByteKey from,
                                     boolean fromInclusive, ByteKey upper, int limit, Projection projection) {
        String sql = buildSql(files, from, fromInclusive, upper, projection);
        var sample = metrics.startParquetQueryTimer();
        int rows = 0;
        boolean success = false;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int i = 1;
            if (from != null) {
                ps.setBytes(i++, from.toByteArray());
            }
            if (upper != null) {
                ps.setBytes(i++, upper.toByteArray());
            }
            ps.setInt(i, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<ListedObject> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(RowMapper.read(rs));
                }
                rows = out.size();
                success = true;
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to query sorted Parquet store", e);
        } finally {
            metrics.recordParquetQuery(sample, rows, success);
        }
    }

    private static String buildSql(List<Path> files, ByteKey from, boolean fromInclusive, ByteKey upper,
                                   Projection projection) {
        String ownerId = projection.owner() ? "owner_id" : "NULL::VARCHAR AS owner_id";
        String ownerDisplayName = projection.owner()
                ? "owner_display_name" : "NULL::VARCHAR AS owner_display_name";
        StringBuilder sql = new StringBuilder()
                .append("SELECT hex(key) AS key_hex, size, last_modified, etag, storage_class, ")
                .append(ownerId).append(", ").append(ownerDisplayName)
                .append(", checksum_algorithm, checksum_type FROM read_parquet(")
                .append(fileList(files))
                .append(") WHERE row_type = 'OBJECT'");
        if (from != null) {
            sql.append(fromInclusive ? " AND key >= ?" : " AND key > ?");
        }
        if (upper != null) {
            sql.append(" AND key < ?");
        }
        sql.append(" ORDER BY key LIMIT ?");
        return sql.toString();
    }

    private static String fileList(List<Path> files) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < files.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            out.append(RowMapper.sqlString(files.get(i).toAbsolutePath().toString()));
        }
        return out.append(']').toString();
    }

    private static void validateIndexWithinFiles(List<IndexEntry> index, List<Path> files) {
        Set<Path> known = new LinkedHashSet<>();
        for (Path f : files) {
            known.add(f.toAbsolutePath());
        }
        for (IndexEntry entry : index) {
            if (!known.contains(entry.file().toAbsolutePath())) {
                throw new IllegalArgumentException(
                        "index references a file outside the fixture: " + entry.file());
            }
        }
    }

    private List<Connection> openPool(int connectionCount) {
        if (connectionCount < 1) {
            throw new IllegalArgumentException("connection count must be at least 1, got " + connectionCount);
        }
        List<Connection> out = new ArrayList<>(connectionCount);
        try {
            int threadsPerConnection = threadsPerConnection(connectionCount);
            for (int i = 0; i < connectionCount; i++) {
                out.add(openConnection(threadsPerConnection));
            }
            return out;
        } catch (SQLException | RuntimeException e) {
            for (Connection connection : out) {
                try {
                    connection.close();
                } catch (SQLException closeError) {
                    e.addSuppressed(closeError);
                }
            }
            throw new IllegalStateException("failed to open sorted Parquet store", e);
        }
    }

    /**
     * DuckDB threads to give each pooled connection. Every connection is an independent in-memory
     * DuckDB instance and defaults its thread pool to the machine's core count — so an N-connection
     * pool defaults to N x cores threads on cores cores. That oversubscription is invisible on a
     * single-client walk and catastrophic under a real work-stealing client: concurrent range reads
     * stop sharing the machine and start fighting for it, and per-query latency degrades
     * super-linearly in the number of in-flight requests. Divide the machine across the pool instead,
     * with a floor of one thread so a large pool still works.
     */
    private static int threadsPerConnection(int connectionCount) {
        return Math.max(1, Runtime.getRuntime().availableProcessors() / connectionCount);
    }

    private static Connection openConnection(int threadsPerConnection) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        try (var st = connection.createStatement()) {
            st.execute("SET threads=" + threadsPerConnection);
            // Keep decoded Parquet footers across pages on this connection, so a bounded read never
            // re-reads the row-group index. DuckDB renamed this from the older `enable_object_cache`,
            // which still parses but is a documented no-op ("[PLACEHOLDER] Legacy setting - does
            // nothing") — setting the old name silently bought nothing.
            st.execute("SET parquet_metadata_cache=true");
        }
        return connection;
    }
}
