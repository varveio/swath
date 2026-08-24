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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ListingStore} over a stamped, globally sorted swath Parquet fixture, answered by
 * <b>Parquet's own page index</b> rather than by a query engine. It never materialises a temp
 * database and never builds an index: the in-memory row-group routing index ({@link IndexEntry},
 * derived once at startup) says which row group a range starts in, and {@link SortedRangeReader}
 * reads it there.
 *
 * <h2>Why not a query engine</h2>
 * This store used to answer a range read with a bounded {@code read_parquet} query, and DuckDB does
 * not consume the Parquet ColumnIndex/OffsetIndex for a {@code WHERE + ORDER BY + LIMIT} shape — an
 * {@code EXPLAIN ANALYZE} gate measured rows scanned equal to the full spanned row groups, with zero
 * slack. A row group holds hundreds of thousands of rows and a listing page asks for about a
 * thousand, so a cold page decoded most of a row group to return a fraction of a percent of it.
 * {@link DuckDbListingStore} survives as the role-1 store for fixtures that cannot be served sorted
 * at all.
 *
 * <h2>What a cold page costs</h2>
 * A bounded read decodes at least one whole <b>page</b> per column, because the page index prunes
 * pages and a page's encodings decode strictly forward. Three things follow, all load-bearing: the
 * read is bounded to the rows the answer can need ({@link SortedRangeReader#range}, since a listing
 * page states no upper key bound); the fixture is written with pages a listing page wide ({@code
 * SortConfig#DEFAULT_FINAL_PAGE_ROWS}); and it is written with small dictionaries ({@code
 * ListEntryParquetWriters#SERVED_DICTIONARY_BYTES}), a dictionary being decoded in full before its
 * column yields a value. Together they make a page's cost scale with the answer rather than being
 * flat in it. An older fixture still serves correct answers — the geometry is a Parquet-internal
 * choice, not a format change — and picks up the write-side share when it is next sorted.
 *
 * <p>Edge cases are first-class: an <b>empty fixture</b> (no row groups ⇒ empty index) returns no
 * rows; a {@code from} <b>before the first key</b> starts at row group 0; and a range that lies
 * entirely below the first key or above {@code toExclusive} touches no rows at all.
 *
 * <p>Owner columns are decoded only when {@link Projection#owner()} — the whole point of the
 * projection, unlike the DuckDB store where they are behavior-cheap.
 *
 * <p>Unlike {@link DuckDbListingStore}, this store assumes the <b>canonical</b> current schema
 * unconditionally (no {@code owner_display_name}/{@code checksum_type} legacy-column backfill): every
 * sorted file is produced by the in-tree writer ({@code SortedParquetWriter} / {@code
 * SegmentParquetSink}), which always writes the full canonical schema, so there is no legacy-schema
 * sorted file for this store to tolerate — unlike {@code DuckDbListingStore}, which must also serve
 * arbitrary pre-existing unsorted captures written by older schema versions.
 *
 * <p>Row groups are addressed by the routing index and never scanned for, so a read touches no group
 * before the one it starts in. Readers are pooled because a Parquet reader carries mutable per-read
 * state; the pool width is the store's request-concurrency bound. This class caches no decoded rows;
 * {@link WindowedListingStore} may still wrap it to amortize sequential pages, though that buys much
 * less than it did when a cold page cost two orders of magnitude more.
 *
 * <h2>The {@code delimiter=/} skip-scan</h2>
 * {@link #delimitedRollup} answers a rollup as a series of index hops against the row-group routing
 * index, each hop random-accessing exactly the one row group ({@link SortedRowGroupReader}, in
 * {@code swath-core}) a scan cursor currently lands in — O(entries emitted), never O(subtree). See
 * its own javadoc for the algorithm.
 */
public final class SortedParquetStore implements ListingStore {

    private final List<IndexEntry> index;
    private final ReplayMetrics metrics;
    private final Map<Path, SortedRangeReader> rangeReaders;
    private final Map<Path, LazyGroupReaderPool> groupReaders;

    public SortedParquetStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics) {
        this(files, index, metrics, defaultConnectionCount());
    }

    /**
     * @param files          every sorted file backing the fixture, in key order (used only to validate
     *                       the index against the fixture — routing is index-driven)
     * @param index          the derived {@code (file, rowGroup, firstKey, rowCount)} routing index,
     *                       globally ascending by first key (its sanity was checked at load)
     * @param metrics        records {@code swath.replay.page.read.latency} per read
     * @param connectionCount pooled Parquet readers per file (== the natural request-concurrency
     *                        bound); see {@link #defaultConnectionCount()}
     */
    public SortedParquetStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics,
                              int connectionCount) {
        this.index = List.copyOf(index);
        validateIndexWithinFiles(this.index, files);
        this.metrics = metrics;
        if (connectionCount < 1) {
            throw new IllegalArgumentException("reader count must be at least 1, got " + connectionCount);
        }
        Map<Path, SortedRangeReader> openedRangeReaders = openRangeReaders(files, connectionCount,
                metrics::parquetReaderAcquired, elapsedNanos -> {
                    try {
                        metrics.recordPageRead(elapsedNanos);
                    } finally {
                        metrics.parquetReaderReleased();
                    }
                });
        this.groupReaders = groupReaderPools(files, connectionCount, metrics);
        this.rangeReaders = openedRangeReaders;
    }

    /**
     * Default pooled readers per file — and so the store's request-concurrency bound.
     *
     * <p>This used to mirror {@link DuckDbListingStore#defaultConnectionCount()}, a CPU-bounded
     * {@code min(4, cores)}. That was right when a pooled slot was a DuckDB connection, which owns a
     * thread pool; it is wrong now that a slot is a Parquet file handle plus its decoded footer.
     * Decoding is still CPU work, so many multiples of the core count buy nothing — but a request
     * stalled on an uncached page should not hold the only slot on a small machine. One slot per
     * visible core saturated the decode path in local large-fixture sweeps; doubling it increased
     * page latency and GC while reducing throughput. Keep the small-machine floor for I/O headroom,
     * and cap large machines so a multi-file fixture does not hold hundreds of open footers per file
     * for no throughput.
     */
    public static int defaultConnectionCount() {
        return Math.max(8, Math.min(32, Runtime.getRuntime().availableProcessors()));
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        return rangeRead(from, fromInclusive, toExclusive, limit, projection);
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
                // The routing index already knows which physical row group the range starts in, so
                // the reader is told rather than left to look; later files start at their own first.
                int startBlock = lower == null ? 0 : index.get(rg).rowGroup();
                for (SortedRowGroupReader.ObjectRow row :
                        rangeReader(file).range(startBlock, lower, inclusive, upper,
                                limit - out.size(), projection.owner())) {
                    out.add(toListedObject(row));
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

    /**
     * One pool of {@link SortedRowGroupReader}s per file, for the delimiter skip-scan.
     *
     * <p>The skip-scan used to construct a reader per request, which re-parsed the footer — the same
     * per-request footer parse the range path was pooled to avoid, and a cost that <em>rises</em> with
     * the small pages a served fixture is written with, because the offset and column indexes it
     * carries grow with the page count. A {@code SortedRowGroupReader} holds mutable per-read state
     * (its requested schema, its open page store), so it is pooled rather than shared, exactly as the
     * range readers are.
     */
    private static Map<Path, LazyGroupReaderPool> groupReaderPools(List<Path> files, int poolSize,
                                                                   ReplayMetrics metrics) {
        Map<Path, LazyGroupReaderPool> readers = new LinkedHashMap<>();
        for (Path file : files) {
            readers.put(file.toAbsolutePath(), new LazyGroupReaderPool(file, poolSize, metrics));
        }
        return Map.copyOf(readers);
    }

    /**
     * The readers for {@code file}, looked up the same way {@link #validateIndexWithinFiles} admits an
     * index entry — by absolute path. Keying on the raw {@link Path} would let an index that named its
     * files relatively pass validation and then miss the map, which is a {@code NullPointerException}
     * at read time rather than the clear rejection the validation exists to give.
     */
    private SortedRangeReader rangeReader(Path file) {
        SortedRangeReader reader = rangeReaders.get(file.toAbsolutePath());
        if (reader == null) {
            throw new IllegalStateException("no sorted Parquet reader for fixture file " + file);
        }
        return reader;
    }

    private LazyGroupReaderPool pool(Path file) {
        LazyGroupReaderPool readers = groupReaders.get(file.toAbsolutePath());
        if (readers == null) {
            throw new IllegalStateException("no sorted Parquet row-group reader for fixture file " + file);
        }
        return readers;
    }

    private SortedRowGroupReader borrowGroupReader(Path file) {
        return pool(file).borrow();
    }

    private static Map<Path, SortedRangeReader> openRangeReaders(
            List<Path> files, int poolSize, Runnable readerAcquired,
            java.util.function.LongConsumer readerReleased) {
        Map<Path, SortedRangeReader> readers = new LinkedHashMap<>();
        try {
            for (Path file : files) {
                // Same width as the DuckDB pool it replaces: one reader per concurrent request the
                // store is sized for, since a Parquet reader carries per-read state.
                readers.put(file.toAbsolutePath(),
                        new SortedRangeReader(file, poolSize, readerAcquired, readerReleased));
            }
        } catch (IOException | RuntimeException e) {
            readers.values().forEach(SortedParquetStore::closeQuietly);
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

    /**
     * Closes every reader this store opened — the row-group readers and the range readers alike.
     *
     * <p>Both halves are load-bearing. A row-group reader still held by a request is closed when that
     * request returns it; an idle one is closed here. The range readers were once missed entirely:
     * they are the sole ordinary-page serving path, and each owns one open {@code ParquetFileReader}
     * per pooled slot per file.
     */
    @Override
    public void close() {
        for (LazyGroupReaderPool pool : groupReaders.values()) {
            pool.close();
        }
        for (SortedRangeReader reader : rangeReaders.values()) {
            closeQuietly(reader);
        }
    }

    private static void closeQuietly(AutoCloseable reader) {
        try {
            reader.close();
        } catch (Exception e) {
            // Best effort: a fixture reader holds no state a failed close could corrupt.
        }
    }

    /**
     * Delimiter readers are a second footer-bearing reader fleet per fixture file. Most replay
     * traffic is ordinary range pages, and a worker-only run must not pay that fleet's startup and
     * resident-memory cost merely because the endpoint also supports delimiter probes. Initialize a
     * file's pool on its first delimiter request; publication occurs only after the whole pool opens,
     * so concurrent first probes either see the complete pool or wait for it.
     */
    static final class LazyGroupReaderPool {

        private final Path file;
        private final int size;
        private final ReplayMetrics metrics;
        private ArrayDeque<SortedRowGroupReader> available;
        private boolean closed;

        LazyGroupReaderPool(Path file, int size, ReplayMetrics metrics) {
            this.file = file;
            this.size = Math.max(1, size);
            this.metrics = metrics;
        }

        synchronized SortedRowGroupReader borrow() {
            initialize();
            while (available.isEmpty() && !closed) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted waiting for a sorted Parquet row-group reader", e);
                }
            }
            if (closed) {
                throw new IllegalStateException("sorted Parquet row-group reader pool is closed for " + file);
            }
            return available.removeFirst();
        }

        synchronized void release(SortedRowGroupReader reader) {
            if (closed) {
                closeQuietly(reader);
            } else {
                available.addLast(reader);
                notifyAll();
            }
        }

        private synchronized void initialize() {
            if (closed) {
                throw new IllegalStateException("sorted Parquet row-group reader pool is closed for " + file);
            }
            if (available != null) {
                return;
            }
            ArrayDeque<SortedRowGroupReader> creating = new ArrayDeque<>(size);
            var sample = metrics.startTimer();
            try {
                for (int i = 0; i < size; i++) {
                    creating.addLast(new SortedRowGroupReader(file));
                }
            } catch (IOException | RuntimeException e) {
                creating.forEach(SortedParquetStore::closeQuietly);
                throw new IllegalStateException(
                        "failed to open a sorted Parquet row-group reader for " + file, e);
            }
            available = creating;
            metrics.recordDelimiterReaderPoolOpen(sample);
        }

        synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (available != null) {
                available.forEach(SortedParquetStore::closeQuietly);
                available.clear();
            }
            notifyAll();
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
     * and this one's, rather than the whole group up front. It is opened <em>at the page that can hold
     * the hop's target</em> rather than at the group's first row, so a hop landing mid-group does not
     * decode the half that lies behind. Before any of that, a
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
     *   <li>No {@code /} after the prefix → a bare object directly under it. The only hop that pays
     *       for the full row rather than just the key column, read through the same pooled
     *       page-index reader the range path uses, a run of rows at a time ({@link #objectAt}).
     *       Advance the cursor past this exact key (exclusive) and continue.
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
        Deque<SortedRowGroupReader.ObjectRow> lookahead = new ArrayDeque<>();
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
                        metrics.recordDelimiterSkipScanWholeGroupShortcut();
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
                        // Cleared BEFORE the return, so the `finally` cannot return it a second time if
                        // the borrow below throws. A double return is not a leak but the opposite and
                        // worse: with a contended pool the add succeeds and one reader is in the pool
                        // twice, so two requests borrow the same non-thread-safe instance and race on
                        // its requested schema and its open page store.
                        SortedRowGroupReader done = reader;
                        Path doneFile = openFile;
                        reader = null;
                        openFile = null;
                        returnGroupReader(doneFile, done);
                    }
                    reader = borrowGroupReader(entry.file());
                    metrics.parquetReaderAcquired();
                    openFile = entry.file();
                    cachedBlockIndex = -1;
                }
                if (cachedBlockIndex != entry.rowGroup()) {
                    if (keyCursor != null) {
                        keyCursor.close();
                    }
                    // Opened at the page that can hold this hop's target, not at the group's first row,
                    // and carrying no page that can only hold keys at/above the scan's upper bound.
                    keyCursor = reader.openKeyCursor(entry.rowGroup(), cursor, inclusive, upper);
                    cachedBlockIndex = entry.rowGroup();
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
                    out.add(new DelimitedEntry(null, toListedObject(objectAt(
                            lookahead, entry, key, upper, limit + 1 - out.size(), projection.owner()))));
                    cursor = key;
                    inclusive = false;
                }
            }
        } finally {
            if (keyCursor != null) {
                keyCursor.close();
            }
            if (reader != null) {
                returnGroupReader(openFile, reader);
            }
        }
        return out;
    }

    /** Returns one delimiter reader without letting diagnostics strand the pooled resource. */
    private void returnGroupReader(Path file, SortedRowGroupReader reader) {
        try {
            metrics.parquetReaderReleased();
        } finally {
            pool(file).release(reader);
        }
    }

    /**
     * The full row at {@code key} — every listing column — read through the pooled page-index reader
     * the range path uses, <b>a run of rows at a time</b>.
     *
     * <p>A bare object is one hop, and hops are what the scan is O(). Reading one row per hop is
     * correct and, on a directory that is mostly bare objects, a thousand separate page-index seeks
     * landing in the same handful of pages. Bare objects under one prefix are consecutive in key
     * order, so {@code want} of them cost one read.
     *
     * <p>{@code lookahead} is validated by exact key, never by position: the scan may jump to a
     * successor at any hop, and a row buffered before the jump must not be served after it. A miss
     * refills, so the buffer is a saving and never a source of truth.
     */
    private SortedRowGroupReader.ObjectRow objectAt(Deque<SortedRowGroupReader.ObjectRow> lookahead,
                                                    IndexEntry entry, byte[] key, byte[] upper, int want,
                                                    boolean includeOwner) throws IOException {
        if (lookahead.isEmpty() || !Arrays.equals(lookahead.peek().keyUnsafe(), key)) {
            lookahead.clear();
            lookahead.addAll(rangeReader(entry.file())
                    .range(entry.rowGroup(), key, true, upper, Math.max(1, want), includeOwner));
        }
        SortedRowGroupReader.ObjectRow row = lookahead.poll();
        if (row != null && Arrays.equals(row.keyUnsafe(), key)) {
            return row;
        }
        // Only reachable if the key vanished between the cursor reading it and this read, which the
        // skip-scan cannot recover from silently.
        throw new IllegalStateException("sorted fixture has no OBJECT row at a key its own key cursor "
                + "just returned, in " + entry.file() + " row group " + entry.rowGroup());
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
        return new ListedObject(row.keyUnsafe(), row.size(), row.lastModifiedEpochMicros(),
                row.etag(), row.storageClass(), row.ownerId(), row.ownerDisplayName(),
                row.checksumAlgorithm(), row.checksumType());
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
}
