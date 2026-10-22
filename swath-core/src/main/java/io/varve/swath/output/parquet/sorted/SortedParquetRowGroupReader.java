/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Per-row-group decode of a sorted Parquet file's {@code OBJECT} rows, addressed by the file's
 * <b>physical</b> row-group block index (what {@code
 * io.varve.swath.replay.fixture.SortedFixtures.IndexEntry#rowGroup} carries once index-derive has
 * run, and what {@link ParquetFileReader#readRowGroup(int)} takes directly). Built for the replay
 * server's {@code delimiter=/} skip-scan: a rollup answered as a series of index hops rather than a
 * whole-subtree scan, where each hop touches exactly one row group and only the columns and rows that
 * hop needs.
 *
 * <p>{@link #openKeyCursor} is the cheap tier a common-prefix hop uses to find where a scan cursor
 * lands: it is a <b>forward-only, resumable</b> cursor over the row group's key column, not a bulk
 * decode. A row group here can be far larger than the directory a single hop is chasing (tens of
 * thousands of rows is typical), so materialising every key up front — only to binary-search for one
 * position — would pay for rows the hop never needed, and would repay nothing on the next hop that
 * lands in the same group. A forward cursor instead decodes only the rows between the last position
 * and the next, whichever hop asks, one key page at a time. {@link #objectsAt} is the value tier used
 * when the cursor lands on bare objects: it reads the value columns of a run of rows by the row
 * position the cursor already reports, reusing this reader's per-row-group page index after priming it
 * under the maximal object projection. {@link #rows} remains the explicit whole-row-group tier for
 * callers that genuinely need every object row.
 *
 * <p>{@link #forEachKey} is the third shape: <b>every</b> key of one row group, in order, handed to a
 * visitor. A caller that is going to consume the whole group anyway (the simulator's decode-once
 * streaming tier packs each faulted group into an in-memory key block) wants neither the cursor's
 * resumability nor its per-step comparison, and reads the key column through parquet's column API
 * directly rather than through record assembly — measured at ~10.5M keys/s against ~6.5M for the same
 * group drained a step at a time through {@link KeyCursor}. <b>Both figures were taken before the
 * cursor's ascent check existed</b>, so the cursor's is now an over-statement by one unsigned compare
 * per row; the gap the two numbers are quoted for is only wider, and neither has been re-measured
 * since. The two tiers never disagree on what they read, which {@code SortedParquetRowGroupReaderTest} pins
 * directly rather than leaving to inspection.
 *
 * <p>Every method here traffics only in {@code byte[]}/{@code long}/{@code String}/collections. That
 * is the whole point of putting this class in {@code swath-core}: {@code io.varve.swath.replay}'s
 * sorted-serving store drives it without an {@code org.apache.parquet}/{@code org.apache.hadoop} type
 * ever reaching that module's compile classpath (enforced there by
 * {@code verifyNoParquetOrHadoopOnCompileClasspath}), the same seam {@link SortedParquetIndex} already
 * keeps for the routing-index derive.
 *
 * <p>Column projection is set on the shared {@link ParquetFileReader} immediately before each read
 * (never once at construction), so {@link #openKeyCursor}, {@link #objectsAt}, and {@link #rows}
 * can freely interleave against the same open file handle, each paying for only its own columns. Not
 * thread-safe — a caller serving concurrent requests must not share one instance across threads
 * (mirrors {@link ParquetEntryReader}).
 */
public final class SortedParquetRowGroupReader implements AutoCloseable {

    private static final String KEY_FIELD = "key";
    private static final String[] OBJECT_FIELDS_WITH_OWNER = {
            "key", "size", "last_modified", "etag", "storage_class",
            "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type",
    };
    private static final String[] OBJECT_FIELDS_WITHOUT_OWNER = {
            "key", "size", "last_modified", "etag", "storage_class",
            "checksum_algorithm", "checksum_type",
    };

    /**
     * One decoded {@code OBJECT} row — the plain-typed twin of the replay server's own row shape,
     * kept independent so this module never depends on {@code io.varve.swath.replay}.
     * {@code ownerId}/{@code ownerDisplayName} are {@code null} when the row group was decoded without
     * owner columns (see {@link #objectRange} and {@link #rows}), matching how a projection-pruning
     * store reports an unrequested column elsewhere in the replay server.
     */
    public record ObjectRow(byte[] key, long size, long lastModifiedEpochMicros, String etag,
                            String storageClass, String ownerId, String ownerDisplayName,
                            String checksumAlgorithm, String checksumType) {

        public ObjectRow {
            key = key.clone();   // defensive copy crossing the public seam
        }

        /** Defensive copy — callers may mutate the returned array without corrupting this row. */
        @Override
        public byte[] key() {
            return key.clone();
        }

        /**
         * Internal zero-copy view for the replay serving pipeline. Callers must never mutate it;
         * public {@link #key()} remains the defensive API for general consumers.
         */
        public byte[] keyUnsafe() {
            return key;
        }
    }

    private static final ColumnPath KEY_COLUMN_PATH = ColumnPath.get(KEY_FIELD);
    private static final ColumnPath SIZE_COLUMN_PATH = ColumnPath.get("size");

    private final Path file;
    private final ParquetFileReader reader;
    private final List<org.apache.parquet.hadoop.metadata.BlockMetaData> blocks;
    private final ColumnIOFactory columnIoFactory = new ColumnIOFactory();
    private final String createdBy;
    private final MessageType keySchema;
    private final ColumnDescriptor keyColumn;
    private final MessageType objectSchemaWithOwner;
    private final MessageColumnIO objectColumnIoWithOwner;
    private final MessageType objectSchemaWithoutOwner;
    private final MessageColumnIO objectColumnIoWithoutOwner;
    private final MessageType indexSchema;
    private final MessageType valueSchemaWithOwner;
    private final MessageType valueSchemaWithoutOwner;
    private final Map<Integer, byte[][]> keyPageMaxima = new HashMap<>();

    public SortedParquetRowGroupReader(Path file) throws IOException {
        this.file = file;
        this.reader = ParquetFileReader.open(new BufferedLocalInputFile(file));
        this.createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
        this.blocks = List.copyOf(reader.getFooter().getBlocks());
        MessageType full = reader.getFooter().getFileMetaData().getSchema();
        this.keySchema = project(full, KEY_FIELD);
        this.keyColumn = keySchema.getColumns().getFirst();
        this.objectSchemaWithOwner = objectProjection(full, true);
        this.objectColumnIoWithOwner = columnIoFactory.getColumnIO(objectSchemaWithOwner);
        this.objectSchemaWithoutOwner = objectProjection(full, false);
        this.objectColumnIoWithoutOwner = columnIoFactory.getColumnIO(objectSchemaWithoutOwner);
        this.indexSchema = objectProjection(full, true, true);
        this.valueSchemaWithOwner = valueProjection(full, true);
        this.valueSchemaWithoutOwner = valueProjection(full, false);
    }

    /**
     * Opens a forward-only key-column cursor over the physical row group {@code blockIndex}, positioned
     * at its first row (row 0) — the cheap tier. See the class javadoc for why this is a cursor and not
     * a bulk list.
     */
    public KeyCursor openKeyCursor(int blockIndex) throws IOException {
        return openKeyCursor(blockIndex, null);
    }

    /**
     * As {@link #openKeyCursor(int)}, but positioned at the first row of the first <b>page</b> that
     * can hold {@code from}; every page before it is neither read nor decoded.
     *
     * <p>{@link #openKeyCursor(int)} starts at row 0 and {@link KeyCursor#advanceTo} walks forward,
     * decoding every key it steps over; a hop landing in the middle of a row group therefore paid for
     * half of it to answer a question about one row, and paid again in the next group. The page index
     * answers "which page can hold this key": the first page whose maximum reaches {@code from}, found
     * by binary search over the group's page maxima, decoded once per reader and row group. A
     * truncated maximum is only ever rounded up, so that page is never past the first one holding a
     * qualifying key.
     *
     * <p>Every property the skip-scan relies on survives: still forward-only and still resumable, so
     * a later hop in the same page run costs only the rows between the two positions, and {@link
     * KeyCursor#position()} still reports a row index within the row group. The landing page normally
     * holds rows below {@code from}; {@code advanceTo} steps past them as it always did. The cursor
     * reads no further than the caller steps it, one page at a time, so a caller that stops at an
     * upper bound reads at most the page holding the first key at or beyond it.
     *
     * @param from lower bound to position at; {@code null} opens at the group's first row
     */
    public KeyCursor openKeyCursor(int blockIndex, byte[] from) throws IOException {
        ColumnIndexStore indexStore = columnIndexStore(blockIndex, keySchema);
        requirePagesAscend(indexStore, file, blockIndex);
        OffsetIndex offsets = indexStore.getOffsetIndex(KEY_COLUMN_PATH);
        byte[][] maxima = keyPageMaxima(blockIndex, indexStore);

        int firstPage = from == null || maxima == null ? 0 : firstPageReaching(maxima, from);
        if (firstPage >= offsets.getPageCount()) {
            return KeyCursor.exhausted(file, blockIndex);
        }
        return new KeyCursor(this, file, blockIndex, offsets, maxima,
                blocks.get(blockIndex).getRowCount(), firstPage);
    }

    /**
     * The value columns of up to {@code count} consecutive rows of one physical row group, starting at
     * row {@code firstRow}: the companion to {@link KeyCursor#position()} for a delimiter skip-scan
     * that lands on bare objects. Fewer rows are returned only when the row group ends first.
     *
     * <p>The caller already holds this reader for its key cursor, so the row group's
     * {@link ColumnIndexStore} is shared, primed under the maximal object projection. The rows are
     * addressed by position rather than re-found by key: the read covers the whole pages of the
     * {@code size} column that hold the requested rows (Parquet addresses pages, not rows), and the
     * column readers skip to {@code firstRow}. Every value column is read; owner columns only when
     * {@code includeOwner}.
     *
     * @throws IllegalStateException if a requested row is not an {@code OBJECT} row, which sorted
     *                               eligibility rules out for every row group it admits
     */
    public List<ObjectValues> objectsAt(int blockIndex, long firstRow, int count, boolean includeOwner)
            throws IOException {
        long rowCount = blocks.get(blockIndex).getRowCount();
        int wanted = (int) Math.min(count, rowCount - firstRow);
        if (wanted <= 0) {
            return List.of();
        }
        MessageType schema = includeOwner ? valueSchemaWithOwner : valueSchemaWithoutOwner;
        ColumnIndexStore indexStore = columnIndexStore(blockIndex, schema);

        OffsetIndex sizeOffsets = indexStore.getOffsetIndex(SIZE_COLUMN_PATH);
        int firstPage = pageHolding(sizeOffsets, firstRow);
        int lastPage = pageHolding(sizeOffsets, firstRow + wanted - 1);
        RowRanges pages = RowRanges.create(rowCount,
                IntStream.rangeClosed(firstPage, lastPage).iterator(), sizeOffsets);
        long skip = firstRow - sizeOffsets.getFirstRowIndex(firstPage);

        try (PageReadStore store = reader.readFilteredRowGroup(blockIndex, pages)) {
            ColumnReadStoreImpl columns = new ColumnReadStoreImpl(store,
                    new GroupRecordConverter(schema).getRootConverter(), schema, createdBy);
            ValueColumns values = new ValueColumns(columns, schema, skip);
            List<ObjectValues> out = new ArrayList<>(wanted);
            for (int i = 0; i < wanted; i++) {
                if (!OBJECT_ROW_TYPE.equals(values.string(ROW_TYPE_FIELD))) {
                    throw new IllegalStateException("sorted fixture row " + (firstRow + i) + " of " + file
                            + " row group " + blockIndex + " is not an OBJECT row");
                }
                out.add(new ObjectValues(values.number("size"), values.number("last_modified"),
                        values.string("etag"), values.string("storage_class"),
                        includeOwner ? values.string("owner_id") : null,
                        includeOwner ? values.string("owner_display_name") : null,
                        values.string("checksum_algorithm"), values.string("checksum_type")));
                values.next();
            }
            return out;
        }
    }

    /**
     * One object row's values without its key, which the caller's key cursor already holds. Missing
     * values read as {@link #toObjectRow} reports them: 0 for a number, {@code null} for a string.
     */
    public record ObjectValues(long size, long lastModifiedEpochMicros, String etag, String storageClass,
                               String ownerId, String ownerDisplayName, String checksumAlgorithm,
                               String checksumType) {
    }

    /** Lockstep column readers over one read of a value projection, positioned on the same row. */
    private static final class ValueColumns {

        private final Map<String, ColumnReader> byField = new HashMap<>();

        ValueColumns(ColumnReadStoreImpl columns, MessageType schema, long skip) {
            for (ColumnDescriptor column : schema.getColumns()) {
                ColumnReader reader = columns.getColumnReader(column);
                for (long i = 0; i < skip; i++) {
                    advance(reader);
                }
                byField.put(column.getPath()[0], reader);
            }
        }

        long number(String field) {
            ColumnReader column = byField.get(field);
            return defined(column) ? column.getLong() : 0L;
        }

        String string(String field) {
            ColumnReader column = byField.get(field);
            return defined(column) ? column.getBinary().toStringUsingUTF8() : null;
        }

        void next() {
            byField.values().forEach(ValueColumns::advance);
        }

        /**
         * Moves past the current row. {@code consume()} advances only the levels: a present value that
         * was never read must be skipped first, or every later value in the column lags by one.
         */
        private static void advance(ColumnReader column) {
            if (defined(column)) {
                column.skip();   // a no-op once the value has been read
            }
            column.consume();
        }

        private static boolean defined(ColumnReader column) {
            return column.getCurrentDefinitionLevel() == column.getDescriptor().getMaxDefinitionLevel();
        }
    }

    /**
     * Returns the row group's cached page indexes, selecting {@code requestedSchema} for the data read.
     *
     * <p>{@link ParquetFileReader} builds that cache only for the requested paths active on first
     * access and never invalidates it when the projection changes. A key cursor that created the cache
     * under {@link #keySchema} would therefore leave later object reads without offset indexes for
     * their value columns. Always prime under the maximal projection before narrowing the real read;
     * this reads footer indexes only, never data pages.
     */
    private ColumnIndexStore columnIndexStore(int blockIndex, MessageType requestedSchema) {
        reader.setRequestedSchema(indexSchema);
        ColumnIndexStore indexStore = reader.getColumnIndexStore(blockIndex);
        reader.setRequestedSchema(requestedSchema);
        return indexStore;
    }

    /**
     * Loads key page {@code page} of a row group, or {@code null} past its last page.
     *
     * <p>One page at a time, because a page is what Parquet can address and what a hop consumes: a
     * served fixture writes key pages of about a listing page's rows, and a scan that consumes more
     * just loads the next page. Keys are read through the column reader directly; record assembly
     * would allocate a record per key.
     */
    private Window loadWindow(int blockIndex, OffsetIndex offsets, long rowCount, int page) throws IOException {
        if (page >= offsets.getPageCount()) {
            return null;
        }
        reader.setRequestedSchema(keySchema);
        RowRanges window = RowRanges.create(rowCount, IntStream.of(page).iterator(), offsets);
        PageReadStore store = reader.readFilteredRowGroup(blockIndex, window);
        try {
            ColumnReader keys = new ColumnReadStoreImpl(store,
                    new GroupRecordConverter(keySchema).getRootConverter(), keySchema, createdBy)
                    .getColumnReader(keyColumn);
            return new Window(store, keys, offsets.getFirstRowIndex(page), window.rowCount(), page + 1);
        } catch (RuntimeException | Error e) {
            store.close();   // the page buffers are ours until a Window owns them
            throw e;
        }
    }

    /**
     * One loaded key page of a row group: the page store behind it, the key column reader over it, the
     * row index it starts at, how many rows it carries, and the page to resume from.
     */
    private record Window(PageReadStore pages, ColumnReader keys, long firstRow, long rows, int nextPage) {
    }

    /**
     * The key column's page maxima for one row group, decoded once per reader, or {@code null} when the
     * group has no usable column index (the cursor then opens at the group's first page).
     */
    private byte[][] keyPageMaxima(int blockIndex, ColumnIndexStore indexStore) {
        byte[][] cached = keyPageMaxima.get(blockIndex);
        if (cached != null) {
            return cached;
        }
        ColumnIndex keyIndex = indexStore.getColumnIndex(KEY_COLUMN_PATH);
        if (keyIndex == null || keyIndex.getNullPages().contains(Boolean.TRUE)) {
            return null;
        }
        List<ByteBuffer> values = keyIndex.getMaxValues();
        byte[][] maxima = new byte[values.size()][];
        for (int page = 0; page < maxima.length; page++) {
            ByteBuffer value = values.get(page).duplicate();
            maxima[page] = new byte[value.remaining()];
            value.get(maxima[page]);
        }
        keyPageMaxima.put(blockIndex, maxima);
        return maxima;
    }

    /** The first page whose maximum is at or above {@code target}, or the page count if none is. */
    private static int firstPageReaching(byte[][] maxima, byte[] target) {
        int low = 0;
        int high = maxima.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (KeyBytes.compareUnsigned(maxima[mid], target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** The page whose first-row interval contains {@code row}. */
    private static int pageHolding(OffsetIndex offsets, long row) {
        int low = 0;
        int high = offsets.getPageCount();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (offsets.getFirstRowIndex(mid) <= row) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low - 1;
    }

    /**
     * Refuses a row group whose key column's <b>pages</b> are not in ascending order, before a single
     * row of it is read.
     *
     * <p>The per-row ascent check proves what it steps over, which was everything while a cursor read
     * the whole row group. A cursor that prunes pages never reads the pages it prunes — and on a
     * disordered group the page index is what misleads it: a page whose keys sort below the target
     * has a {@code max} below the target too, so it is pruned, and its rows leave the listing with
     * nothing having read them and nothing to check.
     *
     * <p>Parquet already computes this as {@code BoundaryOrder} over the column index's per-page
     * min/max: a footer read, cached per row group, no I/O per request. It is <em>complementary</em>
     * to the per-row check — a single page is trivially ascending whatever its rows do, so disorder
     * inside a page is still caught by the rows being read. An absent column index is not disorder
     * and is not reported as one; the read then fails on the offset index it also needs.
     */
    private static void requirePagesAscend(ColumnIndexStore indexStore, Path file, int blockIndex) {
        ColumnIndex keyIndex = indexStore.getColumnIndex(KEY_COLUMN_PATH);
        if (keyIndex == null || keyIndex.getBoundaryOrder() == BoundaryOrder.ASCENDING) {
            return;
        }
        // row -1: the disorder is a property of the group's page boundaries, not of any one row.
        throw RowGroupOrderException.at(file, blockIndex, -1,
                "its keys must be in strictly ascending unsigned order, but the column index reports "
                        + "its pages " + keyIndex.getBoundaryOrder());
    }

    /**
     * A forward-only, resumable position within one row group's key column: {@link #advanceTo} steps
     * past rows strictly before the target, decoding only the ones it steps past, and leaves the
     * cursor positioned at the first row at/after the target (or exhausted, {@link #hasCurrent()}
     * {@code false}, if the group's last key is still before it). Never moves backward — the skip-scan
     * driving it never asks it to, since its own scan cursor only ever advances.
     *
     * <p>Holds its row group's {@link PageReadStore}, whose page buffers are released only by
     * {@code close()} — closing the enclosing file reader does not release them — so the caller must
     * {@link #close()} a cursor it replaces or abandons, or repeated scans retain every visited
     * group's buffers until GC.
     *
     * <p><b>The ascent of the rows it decodes is checked as it steps</b> ({@link #step()}): a
     * skip-scan hop trusts this cursor's position to stand for "the first key at/after the target",
     * and a row group whose rows are not in ascending order makes that reading silently false — the
     * hop then emits a common prefix it has already passed, or skips a subtree it never reached. The
     * sortedness a fixture was admitted on ({@code SortedParquetIndex}/the replay server's index derive)
     * proves the ascent of row-group <em>first</em> keys only, so a group's own rows are proved here,
     * where they are decoded anyway. Page-index pruning and the caller's whole-group shortcut may
     * deliberately leave other rows unread; this is a request-time guard against a bad position,
     * not an exhaustive sorted-fixture validation pass. Pruning remains answer-safe because the
     * Parquet page index can discard a page only when its conservative maximum is below the target;
     * a page containing a qualifying key is retained and decoded. The comparison is the same one
     * {@link #advanceTo} already makes per stepped row, so it costs a compare and no I/O. The failure
     * is a {@link RowGroupOrderException}, carrying the machine-readable
     * {@link RowGroupOrderException#ROW_GROUP_DISORDER} reason its callers count and classify by.
     */
    public static final class KeyCursor implements AutoCloseable {

        private final SortedParquetRowGroupReader owner;
        private final Path file;
        private final int blockIndex;
        private final OffsetIndex offsets;
        private final byte[][] keyPageMaxima;
        private final long groupRowCount;

        private Window window;
        private long windowEnd;      // exclusive row index of the loaded window
        private int nextPage;
        private long position;
        private byte[] currentKey;
        private long decodedRows;

        private KeyCursor(SortedParquetRowGroupReader owner, Path file, int blockIndex, OffsetIndex offsets,
                          byte[][] keyPageMaxima, long groupRowCount, int firstPage) throws IOException {
            this.owner = owner;
            this.file = file;
            this.blockIndex = blockIndex;
            this.offsets = offsets;
            this.keyPageMaxima = keyPageMaxima;
            this.groupRowCount = groupRowCount;
            this.nextPage = firstPage;
            if (!loadNextWindow()) {
                this.position = 0;
                return;
            }
            this.position = window.firstRow() - 1;
            try {
                step();
            } catch (RuntimeException | Error e) {
                // The caller never receives this cursor, so nothing else can close its page buffers.
                close();
                throw e;
            }
        }

        /** A cursor over a row group no page of which can hold the requested range. */
        private static KeyCursor exhausted(Path file, int blockIndex) {
            return new KeyCursor(file, blockIndex);
        }

        private KeyCursor(Path file, int blockIndex) {
            this.owner = null;
            this.file = file;
            this.blockIndex = blockIndex;
            this.offsets = null;
            this.keyPageMaxima = null;
            this.groupRowCount = 0;
            this.window = null;
            this.windowEnd = 0;
            this.nextPage = 0;
            this.position = 0;
            this.currentKey = null;
        }

        private boolean loadNextWindow() throws IOException {
            if (window != null) {
                window.pages().close();
                window = null;
            }
            Window next = owner.loadWindow(blockIndex, offsets, groupRowCount, nextPage);
            if (next == null) {
                return false;
            }
            window = next;
            windowEnd = next.firstRow() + next.rows();
            nextPage = next.nextPage();
            return true;
        }

        @Override
        public void close() {
            if (window != null) {
                window.pages().close();
                window = null;
            }
        }

        private void step() {
            byte[] previousKey = currentKey;
            position++;
            currentKey = readAt(position);
            if (currentKey != null) {
                decodedRows++;
            }
            if (currentKey != null && previousKey != null
                    && KeyBytes.compareUnsigned(previousKey, currentKey) >= 0) {
                throw RowGroupOrderException.at(file, blockIndex, position,
                        "its keys must be in strictly ascending unsigned order, but row " + position
                                + " (" + HexFormat.of().formatHex(currentKey)
                                + ") is at or below its predecessor");
            }
        }

        /** The key at row {@code row}, loading the next window if the current one ends before it. */
        private byte[] readAt(long row) {
            if (window == null) {
                return null;
            }
            if (row >= windowEnd) {
                try {
                    if (!loadNextWindow()) {
                        return null;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "failed to read the next key window of " + file + " row group " + blockIndex, e);
                }
                // A window boundary is a page boundary, so the next window resumes exactly here.
                position = window.firstRow();
            }
            ColumnReader keys = window.keys();
            byte[] key = keys.getBinary().getBytes();
            keys.consume();
            return key;
        }

        /** Whether a current row is available — {@code false} once the group is exhausted. */
        public boolean hasCurrent() {
            return currentKey != null;
        }

        /** The current row's key. Only valid while {@link #hasCurrent()}. */
        public byte[] currentKey() {
            return currentKey;
        }

        /** The current row's 0-based position within this row group. Only valid while {@link #hasCurrent()}. */
        public long position() {
            return position;
        }

        /** Number of key rows this cursor decoded, including its current row. */
        public long decodedRows() {
            return decodedRows;
        }

        /**
         * Whether {@code target} can still occur in the physical data page holding the current row.
         * A conservative page maximum may retain a page unnecessarily, but can never place a real
         * value below its reported maximum. Therefore {@code false} proves that advancing this
         * forward cursor would leave the current page and lets the caller profitably reopen through
         * the page index; {@code true} avoids closing and re-decoding one page for dense, tiny hops.
         */
        public boolean targetMayBeInCurrentPage(byte[] target) {
            if (currentKey == null || target == null || offsets == null || keyPageMaxima == null) {
                return true;   // no proof of a page jump: retain the safe forward cursor
            }
            int page = pageHolding(offsets, position);
            if (page < 0 || page >= keyPageMaxima.length) {
                return true;
            }
            return KeyBytes.compareUnsigned(keyPageMaxima[page], target) >= 0;
        }


        /**
         * Advances until the current row is at/after {@code target} ({@code inclusive}) or strictly
         * after it (not {@code inclusive}), or the group is exhausted. {@code target == null} means no
         * lower bound — a no-op, since the cursor's current position already qualifies.
         */
        public void advanceTo(byte[] target, boolean inclusive) {
            if (target == null) {
                return;
            }
            while (currentKey != null) {
                int cmp = KeyBytes.compareUnsigned(currentKey, target);
                boolean before = inclusive ? cmp < 0 : cmp <= 0;
                if (!before) {
                    return;
                }
                step();
            }
        }
    }

    /** Receives each key of a row group, in ascending on-disk order; see {@link #forEachKey}. */
    @FunctionalInterface
    public interface KeyVisitor {

        /**
         * Called once per row. {@code key} is decoded fresh for this call and is not retained or
         * reused by the reader, so a visitor may keep it without copying.
         */
        void key(byte[] key);
    }

    /**
     * Hands every key of the physical row group {@code blockIndex} to {@code visitor}, in ascending
     * on-disk order — the bulk key tier (see the class javadoc for why it exists alongside {@link
     * #openKeyCursor}). Reads the key column through parquet's column API rather than assembling a
     * record per row, which is what makes it the faster of the two for a caller that consumes the
     * whole group; the {@code key} column is {@code required} in swath's canonical schema, so every
     * row yields exactly one value and there is no definition level to test.
     *
     * <p>Unlike {@link KeyCursor}, this tier does <b>not</b> check the group's ascent as it visits: a
     * caller draining a whole group builds something out of it that has to prove the same property for
     * its own sake (the simulator's key block rejects a non-ascending key on the way in), so checking
     * here too would be the same comparison twice on the fastest key path in the tree.
     *
     * @return the number of keys visited, i.e. the row group's row count
     */
    public long forEachKey(int blockIndex, KeyVisitor visitor) throws IOException {
        reader.setRequestedSchema(keySchema);
        try (PageReadStore pages = reader.readRowGroup(blockIndex)) {
            ColumnReadStoreImpl columns = new ColumnReadStoreImpl(pages,
                    new GroupRecordConverter(keySchema).getRootConverter(), keySchema, createdBy);
            ColumnReader column = columns.getColumnReader(keyColumn);
            long rowCount = pages.getRowCount();
            for (long i = 0; i < rowCount; i++) {
                visitor.key(column.getBinary().getBytes());
                column.consume();
            }
            return rowCount;
        }
    }

    /**
     * The physical row group {@code blockIndex}'s full {@code OBJECT} rows, in on-disk (ascending) row
     * order — the explicit whole-group tier. Callers needing only a bounded range should use {@link
     * #objectRange}. When {@code includeOwner} is {@code false}, owner columns are never decoded and
     * every row's owner fields are {@code null}.
     */
    public List<ObjectRow> rows(int blockIndex, boolean includeOwner) throws IOException {
        MessageType schema = includeOwner ? objectSchemaWithOwner : objectSchemaWithoutOwner;
        MessageColumnIO columnIo = includeOwner ? objectColumnIoWithOwner : objectColumnIoWithoutOwner;
        reader.setRequestedSchema(schema);
        // The rows are fully materialized before returning, so the page store (and its buffers —
        // released by close(), not by closing the file reader) is done the moment this method is.
        try (PageReadStore pages = reader.readRowGroup(blockIndex)) {
            RecordReader<Group> rowReader = columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
            long rowCount = pages.getRowCount();
            List<ObjectRow> out = new ArrayList<>((int) rowCount);
            for (long i = 0; i < rowCount; i++) {
                Group g = rowReader.read();
                out.add(toObjectRow(g, g.getBinary(KEY_FIELD, 0).getBytes(), includeOwner));
            }
            return out;
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * The listing projection over {@code full}, with or without the owner columns — shared with
     * {@link SortedParquetRangeReader} so the two readers cannot drift on which columns a served row has.
     */
    static MessageType objectProjection(MessageType full, boolean includeOwner) {
        return objectProjection(full, includeOwner, false);
    }

    /**
     * As {@link #objectProjection(MessageType, boolean)}, optionally carrying {@code row_type}.
     *
     * <p>{@link SortedParquetRangeReader} needs it: sorted-serving eligibility is supposed to guarantee
     * every row group is pure {@code OBJECT}, but a reader that trusts that guarantee absolutely
     * would serve a rolled-up common prefix as though it were an object on any fixture where the
     * guarantee slipped. Decoding one more column is the cost of not doing that.
     */
    static MessageType objectProjection(MessageType full, boolean includeOwner, boolean includeRowType) {
        String[] fields = includeOwner ? OBJECT_FIELDS_WITH_OWNER : OBJECT_FIELDS_WITHOUT_OWNER;
        if (includeRowType) {
            String[] withRowType = Arrays.copyOf(fields, fields.length + 1);
            withRowType[fields.length] = ROW_TYPE_FIELD;
            fields = withRowType;
        }
        return project(full, fields);
    }

    /**
     * The listing projection without the key, plus {@code row_type}: the columns {@link #objectsAt}
     * reads by row position for objects whose keys a cursor has already decoded.
     */
    private static MessageType valueProjection(MessageType full, boolean includeOwner) {
        String[] fields = includeOwner ? OBJECT_FIELDS_WITH_OWNER : OBJECT_FIELDS_WITHOUT_OWNER;
        String[] values = Arrays.copyOf(Arrays.copyOfRange(fields, 1, fields.length), fields.length);
        values[fields.length - 1] = ROW_TYPE_FIELD;
        return project(full, values);
    }

    /** The value {@code row_type} carries for a listed object, as opposed to a rolled-up prefix. */
    static final String OBJECT_ROW_TYPE = "OBJECT";

    static final String ROW_TYPE_FIELD = "row_type";

    /**
     * Maps one decoded record to an {@link ObjectRow}. Shared with {@link SortedParquetRangeReader} for the
     * same reason as {@link #objectProjection}: two readers feeding the same serving path must agree
     * on every field, including which ones an owner-less projection nulls out.
     */
    static ObjectRow toObjectRow(Group g, byte[] key, boolean includeOwner) {
        return new ObjectRow(
                key,
                optLong(g, "size"),
                optLong(g, "last_modified"),
                optString(g, "etag"),
                optString(g, "storage_class"),
                includeOwner ? optString(g, "owner_id") : null,
                includeOwner ? optString(g, "owner_display_name") : null,
                optString(g, "checksum_algorithm"),
                optString(g, "checksum_type"));
    }

    private static MessageType project(MessageType full, String... fields) {
        Type[] types = new Type[fields.length];
        for (int i = 0; i < fields.length; i++) {
            types[i] = full.getType(fields[i]);
        }
        return new MessageType(full.getName(), types);
    }

    private static String optString(Group g, String field) {
        return g.getFieldRepetitionCount(field) == 0 ? null : g.getString(field, 0);
    }

    private static long optLong(Group g, String field) {
        return g.getFieldRepetitionCount(field) == 0 ? 0L : g.getLong(field, 0);
    }
}
