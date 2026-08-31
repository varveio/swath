/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.output.parquet.fixture.SegmentReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
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
 * and the next, whichever hop asks; parquet's page-level decompression is itself lazy per read, so a
 * cursor that never revisits an earlier row also never re-pays for a page already stepped past.
 * {@link #objectRange} is the bounded full-row tier used when the cursor lands on bare objects; it
 * reuses this reader's per-row-group page index after priming it under the maximal object projection,
 * and decodes only the answer's pages. {@link #rows} remains the explicit whole-row-group tier for
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
 * since. The two tiers never disagree on what they read, which {@code SortedRowGroupReaderTest} pins
 * directly rather than leaving to inspection.
 *
 * <p>Every method here traffics only in {@code byte[]}/{@code long}/{@code String}/collections. That
 * is the whole point of putting this class in {@code swath-core}: {@code io.varve.swath.replay}'s
 * sorted-serving store drives it without an {@code org.apache.parquet}/{@code org.apache.hadoop} type
 * ever reaching that module's compile classpath (enforced there by
 * {@code verifyNoParquetOrHadoopOnCompileClasspath}), the same seam {@link SortedFileIndex} already
 * keeps for the routing-index derive.
 *
 * <p>Column projection is set on the shared {@link ParquetFileReader} immediately before each read
 * (never once at construction), so {@link #openKeyCursor}, {@link #objectRange}, and {@link #rows}
 * can freely interleave against the same open file handle, each paying for only its own columns. Not
 * thread-safe — a caller serving concurrent requests must not share one instance across threads
 * (mirrors {@link SegmentReader}).
 */
public final class SortedRowGroupReader implements AutoCloseable {

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
    private static final Set<ColumnPath> KEY_COLUMN = Set.of(KEY_COLUMN_PATH);

    private final Path file;
    private final ParquetFileReader reader;
    private final List<org.apache.parquet.hadoop.metadata.BlockMetaData> blocks;
    private final ColumnIOFactory columnIoFactory = new ColumnIOFactory();
    private final String createdBy;
    private final MessageType keySchema;
    private final MessageColumnIO keyColumnIo;
    private final ColumnDescriptor keyColumn;
    private final MessageType objectSchemaWithOwner;
    private final MessageColumnIO objectColumnIoWithOwner;
    private final MessageType objectSchemaWithoutOwner;
    private final MessageColumnIO objectColumnIoWithoutOwner;
    private final MessageType objectRangeSchemaWithOwner;
    private final MessageColumnIO objectRangeColumnIoWithOwner;
    private final MessageType objectRangeSchemaWithoutOwner;
    private final MessageColumnIO objectRangeColumnIoWithoutOwner;

    public SortedRowGroupReader(Path file) throws IOException {
        this.file = file;
        this.reader = ParquetFileReader.open(new LocalInputFile(file));
        this.createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
        this.blocks = List.copyOf(reader.getFooter().getBlocks());
        MessageType full = reader.getFooter().getFileMetaData().getSchema();
        this.keySchema = project(full, KEY_FIELD);
        this.keyColumnIo = columnIoFactory.getColumnIO(keySchema);
        this.keyColumn = keySchema.getColumns().getFirst();
        this.objectSchemaWithOwner = objectProjection(full, true);
        this.objectColumnIoWithOwner = columnIoFactory.getColumnIO(objectSchemaWithOwner);
        this.objectSchemaWithoutOwner = objectProjection(full, false);
        this.objectColumnIoWithoutOwner = columnIoFactory.getColumnIO(objectSchemaWithoutOwner);
        this.objectRangeSchemaWithOwner = objectProjection(full, true, true);
        this.objectRangeColumnIoWithOwner = columnIoFactory.getColumnIO(objectRangeSchemaWithOwner);
        this.objectRangeSchemaWithoutOwner = objectProjection(full, false, true);
        this.objectRangeColumnIoWithoutOwner = columnIoFactory.getColumnIO(objectRangeSchemaWithoutOwner);
    }

    /**
     * Opens a forward-only key-column cursor over the physical row group {@code blockIndex}, positioned
     * at its first row (row 0) — the cheap tier. See the class javadoc for why this is a cursor and not
     * a bulk list.
     */
    public KeyCursor openKeyCursor(int blockIndex) throws IOException {
        return openKeyCursor(blockIndex, null, true, null);
    }

    /**
     * As {@link #openKeyCursor(int)}, but positioned at the first row of the first <b>page</b> that
     * can hold {@code from} — every page before it is neither read nor decoded — and carrying only
     * the pages that can hold a key below {@code toExclusive}.
     *
     * <p>{@link #openKeyCursor(int)} starts at row 0 and {@link KeyCursor#advanceTo} walks forward,
     * decoding every key it steps over; a hop landing in the middle of a row group therefore paid for
     * half of it to answer a question about one row, and paid again in the next group. The page index
     * answers "which page can hold this key" from the footer.
     *
     * <p>Every property the skip-scan relies on survives: still forward-only and still resumable, so
     * a later hop in the same page run costs only the rows between the two positions, and {@link
     * KeyCursor#position()} still reports a row index within the row group. The page filter prunes
     * pages, never rows, so the first surviving page normally holds rows below {@code from} —
     * {@code advanceTo} steps past them as it always did.
     *
     * @param from        lower bound to position at; {@code null} opens at the group's first row
     * @param inclusive   whether {@code from} itself qualifies
     * @param toExclusive optional upper bound; pages that can only hold keys at or above it are not read
     */
    public KeyCursor openKeyCursor(int blockIndex, byte[] from, boolean inclusive, byte[] toExclusive)
            throws IOException {
        long rowCount = blocks.get(blockIndex).getRowCount();
        ColumnIndexStore indexStore = columnIndexStore(blockIndex, keySchema);
        requirePagesAscend(indexStore, file, blockIndex);
        RowRanges eligible = from == null && toExclusive == null
                ? RowRanges.createSingle(rowCount)
                : ColumnIndexFilter.calculateRowRanges(
                        FilterCompat.get(keyBetween(from, inclusive, toExclusive)),
                        indexStore, KEY_COLUMN, rowCount);
        if (eligible.rowCount() == 0) {
            return KeyCursor.exhausted(file, blockIndex);
        }
        OffsetIndex offsets = indexStore.getOffsetIndex(KEY_COLUMN_PATH);
        return new KeyCursor(this, file, blockIndex, eligible, offsets, rowCount);
    }

    /**
     * The first {@code limit} object rows in one physical row group at/after {@code from}. This is the
     * full-row companion to {@link #openKeyCursor(int, byte[], boolean, byte[])} for a delimiter
     * skip-scan that lands on bare objects.
     *
     * <p>The important part is ownership: the caller already holds this reader for its key cursor, and
     * the row group's {@link ColumnIndexStore} was primed under the maximal object projection before
     * that cursor narrowed the reader to its key column. Borrowing a separate range reader rebuilt the
     * same column/offset indexes once per pooled slot before the first bare-object batch could be
     * returned. Reusing this reader leaves concurrency bounded by the delimiter-reader lease and
     * retains the exact same page-bounded read as {@link SortedRangeReader}.
     */
    public List<ObjectRow> objectRange(int blockIndex, byte[] from, boolean fromInclusive,
                                       byte[] toExclusive, int limit, boolean includeOwner) throws IOException {
        if (limit <= 0 || blockIndex < 0 || blockIndex >= blocks.size()) {
            return List.of();
        }
        MessageType schema = includeOwner ? objectRangeSchemaWithOwner : objectRangeSchemaWithoutOwner;
        MessageColumnIO columnIo = includeOwner
                ? objectRangeColumnIoWithOwner : objectRangeColumnIoWithoutOwner;
        ColumnIndexStore indexStore = columnIndexStore(blockIndex, schema);
        requirePagesAscend(indexStore, file, blockIndex);
        RowRanges ranges = ColumnIndexFilter.calculateRowRanges(
                FilterCompat.get(SortedRangeReader.predicate(from, fromInclusive, toExclusive)),
                indexStore, KEY_COLUMN, blocks.get(blockIndex).getRowCount());
        if (ranges.rowCount() == 0) {
            return List.of();
        }
        RowRanges wanted = SortedRangeReader.firstRowsOf(ranges, indexStore, limit);
        List<ObjectRow> out = new ArrayList<>(Math.min(limit, 1024));
        readObjectsInto(out, blockIndex, wanted, from, fromInclusive, toExclusive,
                limit, schema, columnIo, includeOwner);
        if (out.size() < limit && wanted.rowCount() < ranges.rowCount()) {
            // Eligibility promises pure OBJECT groups, so this is only a correctness backstop if
            // that promise ever slips: widen to every page the key predicate retained.
            out.clear();
            readObjectsInto(out, blockIndex, ranges, from, fromInclusive, toExclusive,
                    limit, schema, columnIo, includeOwner);
        }
        return out;
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
        reader.setRequestedSchema(objectRangeSchemaWithOwner);
        ColumnIndexStore indexStore = reader.getColumnIndexStore(blockIndex);
        reader.setRequestedSchema(requestedSchema);
        return indexStore;
    }

    private void readObjectsInto(List<ObjectRow> out, int blockIndex, RowRanges ranges, byte[] from,
                                 boolean fromInclusive, byte[] toExclusive, int limit,
                                 MessageType schema, MessageColumnIO columnIo, boolean includeOwner)
            throws IOException {
        try (PageReadStore pages = reader.readFilteredRowGroup(blockIndex, ranges)) {
            RecordReader<ObjectRow> rowReader = columnIo.getRecordReader(
                    pages, new SortedRangeReader.ObjectRowMaterializer(schema, includeOwner));
            long rowCount = pages.getRowCount();
            for (long i = 0; i < rowCount && out.size() < limit; i++) {
                ObjectRow row = rowReader.read();
                if (row != null && SortedRangeReader.inRange(
                        row.keyUnsafe(), from, fromInclusive, toExclusive)) {
                    out.add(row);
                }
            }
        }
    }

    /**
     * Loads the next window of at most {@link KeyCursor#WINDOW_ROWS} rows of {@code eligible} starting
     * at page {@code fromPage}, or {@code null} once no eligible page is left.
     *
     * <p>Whole pages, because a page is what Parquet can address; the row budget only makes "a few
     * pages" mean the same thing whatever the pages hold. A window is always a <b>contiguous</b> run
     * of pages, so that {@code firstRow + rows} is a row index and {@link KeyCursor#position()} keeps
     * meaning what it says — a pruned page mid-window would skew it by the gap, silently. Ending the
     * window at a gap costs nothing, since the next window re-seats the position at its own first row
     * anyway.
     */
    private Window loadWindow(int blockIndex, RowRanges eligible, OffsetIndex offsets, long rowCount,
                              int fromPage) throws IOException {
        int pageCount = offsets.getPageCount();
        List<Integer> pages = new ArrayList<>();
        long rows = 0;
        int page = fromPage;
        for (; page < pageCount && rows < KeyCursor.WINDOW_ROWS; page++) {
            long first = offsets.getFirstRowIndex(page);
            long last = offsets.getLastRowIndex(page, rowCount);
            if (!eligible.isOverlapping(first, last)) {
                if (pages.isEmpty()) {
                    continue;   // still skipping ahead to the first page that can hold the range
                }
                break;   // a gap ENDS the window; see below
            }
            pages.add(page);
            rows += last - first + 1;
        }
        if (pages.isEmpty()) {
            return null;
        }
        reader.setRequestedSchema(keySchema);
        RowRanges window = RowRanges.create(rowCount,
                pages.stream().mapToInt(Integer::intValue).iterator(), offsets);
        PageReadStore store = reader.readFilteredRowGroup(blockIndex, window);
        try {
            RecordReader<Group> rowReader =
                    keyColumnIo.getRecordReader(store, new GroupRecordConverter(keySchema));
            return new Window(store, rowReader, offsets.getFirstRowIndex(pages.getFirst()),
                    window.rowCount(), page);
        } catch (RuntimeException | Error e) {
            store.close();   // the page buffers are ours until a Window owns them
            throw e;
        }
    }

    /**
     * One loaded stretch of a row group's key column: the pages behind it, the reader over them, the
     * row index it starts at, how many rows it carries, and the page to resume from.
     */
    private record Window(PageReadStore pages, RecordReader<Group> rowReader, long firstRow, long rows,
                          int nextPage) {
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

    /** The page-index predicate for {@code [from, toExclusive)}; at least one bound is non-null. */
    private static FilterPredicate keyBetween(byte[] from, boolean inclusive, byte[] toExclusive) {
        Operators.BinaryColumn key = FilterApi.binaryColumn(KEY_FIELD);
        FilterPredicate lower = from == null ? null
                : inclusive ? FilterApi.gtEq(key, Binary.fromConstantByteArray(from.clone()))
                            : FilterApi.gt(key, Binary.fromConstantByteArray(from.clone()));
        FilterPredicate upper = toExclusive == null ? null
                : FilterApi.lt(key, Binary.fromConstantByteArray(toExclusive.clone()));
        if (lower == null) {
            return upper;
        }
        return upper == null ? lower : FilterApi.and(lower, upper);
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
     * <p><b>The ascent of the rows it steps over is checked as it steps</b> ({@link #step()}): a
     * skip-scan hop trusts this cursor's position to stand for "the first key at/after the target",
     * and a row group whose rows are not in ascending order makes that reading silently false — the
     * hop then emits a common prefix it has already passed, or skips a subtree it never reached. The
     * sortedness a fixture was admitted on ({@code SortedFileIndex}/the replay server's index derive)
     * proves the ascent of row-group <em>first</em> keys only, so a group's own rows are proved here,
     * where they are decoded anyway, and nowhere else. The comparison is the same one
     * {@link #advanceTo} already makes per stepped row, so it costs a compare and no I/O. The failure
     * is a {@link RowGroupOrderException}, carrying the machine-readable
     * {@link RowGroupOrderException#ROW_GROUP_DISORDER} reason its callers count and classify by.
     */
    public static final class KeyCursor implements AutoCloseable {

        /**
         * Rows a single load pulls in.
         *
         * <p>The cursor used to load its whole row group, which was invisible while a group was a
         * dozen pages and is not once a served fixture writes pages a listing page wide. A few pages
         * is what a hop consumes; a scan that consumes more just loads the next window.
         */
        static final long WINDOW_ROWS = 8192;

        private final SortedRowGroupReader owner;
        private final Path file;
        private final int blockIndex;
        private final RowRanges eligible;
        private final OffsetIndex offsets;
        private final long groupRowCount;

        private Window window;
        private long windowEnd;      // exclusive row index of the loaded window
        private int nextPage;
        private long position;
        private byte[] currentKey;

        private KeyCursor(SortedRowGroupReader owner, Path file, int blockIndex, RowRanges eligible,
                          OffsetIndex offsets, long groupRowCount) throws IOException {
            this.owner = owner;
            this.file = file;
            this.blockIndex = blockIndex;
            this.eligible = eligible;
            this.offsets = offsets;
            this.groupRowCount = groupRowCount;
            this.nextPage = 0;
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
            this.eligible = RowRanges.EMPTY;
            this.offsets = null;
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
            Window next = owner.loadWindow(blockIndex, eligible, offsets, groupRowCount, nextPage);
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
            return window.rowReader().read().getBinary(KEY_FIELD, 0).getBytes();
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
     * {@link SortedRangeReader} so the two readers cannot drift on which columns a served row has.
     */
    static MessageType objectProjection(MessageType full, boolean includeOwner) {
        return objectProjection(full, includeOwner, false);
    }

    /**
     * As {@link #objectProjection(MessageType, boolean)}, optionally carrying {@code row_type}.
     *
     * <p>{@link SortedRangeReader} needs it: sorted-serving eligibility is supposed to guarantee
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

    /** The value {@code row_type} carries for a listed object, as opposed to a rolled-up prefix. */
    static final String OBJECT_ROW_TYPE = "OBJECT";

    static final String ROW_TYPE_FIELD = "row_type";

    /**
     * Maps one decoded record to an {@link ObjectRow}. Shared with {@link SortedRangeReader} for the
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
