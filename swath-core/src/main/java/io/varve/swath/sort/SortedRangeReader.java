/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.sort.SortedRowGroupReader.ObjectRow;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.LongConsumer;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * A bounded key-range read over one sorted Parquet file, answered by Parquet's own
 * <b>ColumnIndex/OffsetIndex</b> page filtering rather than by a SQL engine.
 *
 * <p><b>Why this exists.</b> The replay server's sorted store answered range reads with a bounded
 * {@code read_parquet} query, and DuckDB does not consume the Parquet page index for a
 * {@code WHERE + ORDER BY + LIMIT} shape — an {@code EXPLAIN ANALYZE} gate measured rows-scanned
 * equal to the full spanned row groups, with zero slack. Since a row group holds hundreds of
 * thousands of rows and a listing page asks for about a thousand, a cold page decoded most of a row
 * group to return a fraction of a percent of it, at a cost that measured flat from one key to five
 * thousand — fixed per-query machinery rather than anything to do with the answer's size.
 *
 * <p><b>The file handle is opened once, not per read.</b> Parquet fixes a reader's filter at open
 * time, so the obvious shape — reopen per request with that request's predicate — re-parses the
 * footer every time. Instead the row ranges are computed here, from the same {@link
 * ColumnIndexFilter} the reader would have used, and handed to {@link
 * ParquetFileReader#readFilteredRowGroup(int, RowRanges)}. One long-lived reader per pooled slot
 * then serves every request, and the footer is read once at construction. (This is also the way
 * around a JDK-25 dead end: parquet-java's footer-reuse constructors all take a Hadoop {@code Path},
 * whose {@code UserGroupInformation} call throws under JEP 486.)
 *
 * <p><b>Row groups are addressed, not scanned for.</b> The caller passes the physical row-group
 * index its routing index says the range starts in, so no work is spent locating it and no earlier
 * group is touched at all.
 *
 * <p><b>The page filter is a pruning hint, not the answer.</b> It removes pages that cannot contain
 * the range; rows inside a surviving page may still fall outside it. Every row is therefore
 * re-checked against the range with {@link KeyBytes#compareUnsigned} — which is also the ordering
 * the whole sorted format is defined in, and the one Parquet's own binary filtering was measured to
 * use (a 0xFF-keyed probe, since a signed comparison would silently drop every key with a high byte
 * set).
 *
 * <p>Schema and column-IO are derived once per file: they are pure functions of (file, projection),
 * and rebuilding them per request measured ~13 ms of avoidable cost. Readers are pooled because a
 * {@link ParquetFileReader} carries mutable per-read state (the requested schema) and cannot be
 * shared across concurrent requests.
 *
 * <p>Lives in {@code swath-core} for the same reason {@link SortedRowGroupReader} does: it traffics
 * only in {@code byte[]}/{@code long}/{@code int}/collections, so the replay server drives it
 * without an {@code org.apache.parquet} type reaching that module's compile classpath.
 */
public final class SortedRangeReader implements AutoCloseable {

    private static final String KEY_FIELD = "key";
    private static final Set<ColumnPath> FILTERED_COLUMNS = Set.of(ColumnPath.get(KEY_FIELD));

    private final BlockingQueue<ParquetFileReader> readers;
    private final List<ParquetFileReader> owned;
    private final List<BlockMetaData> blocks;
    private final MessageType schemaWithOwner;
    private final MessageType schemaWithoutOwner;
    private final MessageColumnIO columnIoWithOwner;
    private final MessageColumnIO columnIoWithoutOwner;
    private final Runnable readerAcquired;
    private final LongConsumer readerReleased;

    public SortedRangeReader(Path file, int poolSize) throws IOException {
        this(file, poolSize, () -> { }, ignored -> { });
    }

    public SortedRangeReader(
            Path file, int poolSize, Runnable readerAcquired, LongConsumer readerReleased) throws IOException {
        int size = Math.max(1, poolSize);
        this.readerAcquired = Objects.requireNonNull(readerAcquired, "readerAcquired");
        this.readerReleased = Objects.requireNonNull(readerReleased, "readerReleased");
        this.owned = new ArrayList<>(size);
        this.readers = new ArrayBlockingQueue<>(size);
        try {
            for (int i = 0; i < size; i++) {
                ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file));
                owned.add(reader);
                readers.add(reader);
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly();
            throw e;
        }
        ParquetFileReader first = owned.getFirst();
        MessageType fileSchema = first.getFooter().getFileMetaData().getSchema();
        this.blocks = List.copyOf(first.getFooter().getBlocks());
        ColumnIOFactory factory = new ColumnIOFactory();
        this.schemaWithOwner = SortedRowGroupReader.objectProjection(fileSchema, true, true);
        this.schemaWithoutOwner = SortedRowGroupReader.objectProjection(fileSchema, false, true);
        this.columnIoWithOwner = factory.getColumnIO(schemaWithOwner, fileSchema);
        this.columnIoWithoutOwner = factory.getColumnIO(schemaWithoutOwner, fileSchema);
    }

    /** How many physical row groups this file holds. */
    public int rowGroupCount() {
        return blocks.size();
    }

    /**
     * The first {@code limit} {@code OBJECT} rows at or after {@code from} and strictly before
     * {@code toExclusive}, in ascending key order, starting the search at row group
     * {@code startRowGroup}.
     *
     * @param startRowGroup physical row-group index the range starts in, per the caller's routing index
     * @param from          range start; {@code null} starts at the beginning of {@code startRowGroup}
     * @param fromInclusive whether {@code from} itself is in range
     * @param toExclusive   exclusive range end; {@code null} reads to the end of the file
     * @param limit         maximum rows to return; the read stops as soon as it has them
     * @param includeOwner  whether to decode the owner columns at all
     */
    public List<ObjectRow> range(int startRowGroup, byte[] from, boolean fromInclusive, byte[] toExclusive,
                                 int limit, boolean includeOwner) throws IOException {
        if (limit <= 0 || startRowGroup >= blocks.size()) {
            return List.of();
        }
        MessageType schema = includeOwner ? schemaWithOwner : schemaWithoutOwner;
        MessageColumnIO columnIo = includeOwner ? columnIoWithOwner : columnIoWithoutOwner;
        FilterCompat.Filter filter = FilterCompat.get(predicate(from, fromInclusive, toExclusive));
        List<ObjectRow> out = new ArrayList<>(Math.min(limit, 1024));
        ParquetFileReader reader = borrow();
        boolean acquisitionRecorded = false;
        long readStartedNanos = 0L;
        try {
            readerAcquired.run();
            acquisitionRecorded = true;
            readStartedNanos = System.nanoTime();
            reader.setRequestedSchema(schema);
            for (int block = Math.max(0, startRowGroup); block < blocks.size() && out.size() < limit; block++) {
                ColumnIndexStore indexStore = reader.getColumnIndexStore(block);
                RowRanges ranges = ColumnIndexFilter.calculateRowRanges(
                        filter, indexStore, FILTERED_COLUMNS, blocks.get(block).getRowCount());
                if (ranges.rowCount() == 0) {
                    continue;   // no page in this group can hold the range
                }
                RowRanges wanted = firstRowsOf(ranges, indexStore, limit - out.size());
                int before = out.size();
                readInto(out, reader, columnIo, schema, block, wanted, from, fromInclusive, toExclusive,
                        limit, includeOwner);
                if (out.size() - before < limit - before && wanted.rowCount() < ranges.rowCount()) {
                    // The window is sized to hold `limit` qualifying rows whenever every row in it is
                    // an OBJECT, which is exactly what sorted-serving eligibility promises. Coming up
                    // short means the promise did not hold, so re-read the whole surviving range —
                    // correctness never rides on the window, only cost does.
                    out.subList(before, out.size()).clear();
                    readInto(out, reader, columnIo, schema, block, ranges, from, fromInclusive,
                            toExclusive, limit, includeOwner);
                }
            }
        } finally {
            try {
                if (acquisitionRecorded) {
                    readerReleased.accept(System.nanoTime() - readStartedNanos);
                }
            } finally {
                readers.add(reader);
            }
        }
        return out;
    }

    /** Decodes {@code ranges} of one row group into {@code out}, stopping at {@code limit} rows. */
    private void readInto(List<ObjectRow> out, ParquetFileReader reader, MessageColumnIO columnIo,
                          MessageType schema, int block, RowRanges ranges, byte[] from,
                          boolean fromInclusive, byte[] toExclusive, int limit, boolean includeOwner)
            throws IOException {
        try (PageReadStore pages = reader.readFilteredRowGroup(block, ranges)) {
            long rowCount = pages.getRowCount();
            RecordReader<Group> rowReader =
                    columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
            for (long i = 0; i < rowCount && out.size() < limit; i++) {
                Group g = rowReader.read();
                if (!isObject(g)) {
                    // Eligibility should have excluded a fixture carrying anything else, but a
                    // reader that assumes it would serve a rolled-up prefix as an object.
                    continue;
                }
                byte[] key = g.getBinary(KEY_FIELD, 0).getBytes();
                if (!inRange(key, from, fromInclusive, toExclusive)) {
                    continue;   // the index prunes pages, never rows
                }
                out.add(SortedRowGroupReader.toObjectRow(g, key, includeOwner));
            }
        }
    }

    /**
     * The head of {@code ranges} that can still be needed once {@code want} more rows would satisfy
     * the request — the difference between reading a page and reading a row group's whole tail.
     *
     * <p>A listing page asks for {@code n} rows <em>at or after</em> a key, with no upper key bound
     * to state, so the predicate is {@code key >= from} and every page from the seek to the end of the
     * row group satisfies it. The decode loop stops at {@code limit}; {@link
     * ParquetFileReader#readFilteredRowGroup} has by then already read every one of those pages,
     * across every projected column.
     *
     * <p>The window is {@code want} rows plus one page of slack, because only the <em>first</em>
     * surviving page can hold rows below {@code from} — pages are in key order, so every later one
     * starts at or after it. With that slack the window holds {@code want} qualifying rows whenever
     * the group's remaining rows do, which is what makes the caller's widen path a backstop rather
     * than an expected cost.
     */
    private static RowRanges firstRowsOf(RowRanges ranges, ColumnIndexStore indexStore, int want) {
        long first = ranges.iterator().nextLong();
        long slack = pageRowsAt(indexStore, first);
        long end = first + slack + want;
        if (end <= 0 || end >= first + ranges.rowCount()) {
            return ranges;   // overflow, or the window already spans everything that survived
        }
        return RowRanges.intersection(ranges, RowRanges.createSingle(end));
    }

    /**
     * Rows in the key-column page that starts at row {@code firstRow} — the window's slack.
     *
     * <p>Every "don't know" answer here returns a slack large enough to decline to bound at all
     * ({@link #firstRowsOf} then reads the full surviving ranges), because under-stating the slack is
     * the one error that could shorten a page. A fixture with no page index cannot be read by this
     * class in the first place, but that must fail on the read, not on a silently narrowed window.
     */
    private static long pageRowsAt(ColumnIndexStore indexStore, long firstRow) {
        OffsetIndex offsets;
        try {
            offsets = indexStore.getOffsetIndex(ColumnPath.get(KEY_FIELD));
        } catch (ColumnIndexStore.MissingOffsetIndexException e) {
            return DO_NOT_BOUND;
        }
        if (offsets == null) {
            return DO_NOT_BOUND;
        }
        int pages = offsets.getPageCount();
        for (int p = 0; p < pages; p++) {
            if (offsets.getFirstRowIndex(p) == firstRow) {
                return p + 1 < pages ? offsets.getFirstRowIndex(p + 1) - firstRow : DO_NOT_BOUND;
            }
        }
        return DO_NOT_BOUND;   // not page-aligned: decline to bound rather than under-read
    }

    /**
     * A slack big enough that {@link #firstRowsOf}'s window always spans everything that survived the
     * page filter — i.e. "decline to bound". Well below {@link Long#MAX_VALUE} so adding a caller's
     * {@code limit} to it cannot overflow.
     */
    private static final long DO_NOT_BOUND = Long.MAX_VALUE / 4;

    private ParquetFileReader borrow() {
        try {
            return readers.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for a sorted Parquet range reader", e);
        }
    }

    private static boolean isObject(Group g) {
        return g.getFieldRepetitionCount(SortedRowGroupReader.ROW_TYPE_FIELD) > 0
                && SortedRowGroupReader.OBJECT_ROW_TYPE.equals(
                        g.getString(SortedRowGroupReader.ROW_TYPE_FIELD, 0));
    }

    private static boolean inRange(byte[] key, byte[] from, boolean fromInclusive, byte[] toExclusive) {
        if (from != null) {
            int c = KeyBytes.compareUnsigned(key, from);
            if (c < 0 || (c == 0 && !fromInclusive)) {
                return false;
            }
        }
        return toExclusive == null || KeyBytes.compareUnsigned(key, toExclusive) < 0;
    }

    private static FilterPredicate predicate(byte[] from, boolean fromInclusive, byte[] toExclusive) {
        Operators.BinaryColumn key = FilterApi.binaryColumn(KEY_FIELD);
        FilterPredicate lower = null;
        if (from != null) {
            Binary bound = Binary.fromConstantByteArray(Arrays.copyOf(from, from.length));
            lower = fromInclusive ? FilterApi.gtEq(key, bound) : FilterApi.gt(key, bound);
        }
        FilterPredicate upper = toExclusive == null ? null
                : FilterApi.lt(key, Binary.fromConstantByteArray(Arrays.copyOf(toExclusive, toExclusive.length)));
        if (lower == null && upper == null) {
            // Every row qualifies; a tautology the filter API cannot express directly.
            return FilterApi.gtEq(key, Binary.fromConstantByteArray(new byte[0]));
        }
        if (lower == null) {
            return upper;
        }
        return upper == null ? lower : FilterApi.and(lower, upper);
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        for (ParquetFileReader reader : owned) {
            try {
                reader.close();
            } catch (IOException e) {
                // Best effort: a fixture reader holds no state a failed close could corrupt.
            }
        }
    }

    /** Kept so a caller can size a pool without reaching for the class's internals. */
    public static Set<ColumnPath> filteredColumns() {
        return new HashSet<>(FILTERED_COLUMNS);
    }
}
