/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
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
 * and the next, whichever hop asks; parquet's page-level decompression is itself lazy per read, so a
 * cursor that never revisits an earlier row also never re-pays for a page already stepped past. {@link
 * #rows} — the expensive tier, full-row decode of every listing column — stays a bulk per-row-group
 * read: it is paid only for a bare object directly under the scan prefix, rare in a real bucket, so
 * there is no equivalent hot path to spare it from.
 *
 * <p>{@link #forEachKey} is the third shape: <b>every</b> key of one row group, in order, handed to a
 * visitor. A caller that is going to consume the whole group anyway (the simulator's decode-once
 * streaming tier packs each faulted group into an in-memory key block) wants neither the cursor's
 * resumability nor its per-step comparison, and reads the key column through parquet's column API
 * directly rather than through record assembly — measured at ~10.5M keys/s against ~6.5M for the same
 * group drained a step at a time through {@link KeyCursor}. The two never disagree, which
 * {@code SortedRowGroupReaderTest} pins directly rather than leaving to inspection.
 *
 * <p>Every method here traffics only in {@code byte[]}/{@code long}/{@code String}/collections. That
 * is the whole point of putting this class in {@code swath-core}: {@code io.varve.swath.replay}'s
 * sorted-serving store drives it without an {@code org.apache.parquet}/{@code org.apache.hadoop} type
 * ever reaching that module's compile classpath (enforced there by
 * {@code verifyNoParquetOrHadoopOnCompileClasspath}), the same seam {@link SortedFileIndex} already
 * keeps for the routing-index derive.
 *
 * <p>Column projection is set on the shared {@link ParquetFileReader} immediately before each read
 * (never once at construction), so {@link #openKeyCursor} and {@link #rows} can freely interleave
 * against the same open file handle, each paying for only its own columns. Not thread-safe — a caller
 * serving concurrent requests must not share one instance across threads (mirrors {@link
 * SegmentReader}).
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
     * owner columns (see {@link #rows}), matching how a projection-pruning store reports an
     * unrequested column elsewhere in the replay server.
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
    }

    private final ParquetFileReader reader;
    private final ColumnIOFactory columnIoFactory = new ColumnIOFactory();
    private final String createdBy;
    private final MessageType keySchema;
    private final MessageColumnIO keyColumnIo;
    private final ColumnDescriptor keyColumn;
    private final MessageType objectSchemaWithOwner;
    private final MessageColumnIO objectColumnIoWithOwner;
    private final MessageType objectSchemaWithoutOwner;
    private final MessageColumnIO objectColumnIoWithoutOwner;

    public SortedRowGroupReader(Path file) throws IOException {
        this.reader = ParquetFileReader.open(new LocalInputFile(file));
        this.createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
        MessageType full = reader.getFooter().getFileMetaData().getSchema();
        this.keySchema = project(full, KEY_FIELD);
        this.keyColumnIo = columnIoFactory.getColumnIO(keySchema);
        this.keyColumn = keySchema.getColumns().getFirst();
        this.objectSchemaWithOwner = project(full, OBJECT_FIELDS_WITH_OWNER);
        this.objectColumnIoWithOwner = columnIoFactory.getColumnIO(objectSchemaWithOwner);
        this.objectSchemaWithoutOwner = project(full, OBJECT_FIELDS_WITHOUT_OWNER);
        this.objectColumnIoWithoutOwner = columnIoFactory.getColumnIO(objectSchemaWithoutOwner);
    }

    /**
     * Opens a forward-only key-column cursor over the physical row group {@code blockIndex}, positioned
     * at its first row (row 0) — the cheap tier. See the class javadoc for why this is a cursor and not
     * a bulk list.
     */
    public KeyCursor openKeyCursor(int blockIndex) throws IOException {
        reader.setRequestedSchema(keySchema);
        PageReadStore pages = reader.readRowGroup(blockIndex);
        RecordReader<Group> rowReader = keyColumnIo.getRecordReader(pages, new GroupRecordConverter(keySchema));
        return new KeyCursor(pages, rowReader, pages.getRowCount());
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
     */
    public static final class KeyCursor implements AutoCloseable {

        private final PageReadStore pages;
        private final RecordReader<Group> rowReader;
        private final long rowCount;
        private long position = -1;
        private byte[] currentKey;

        private KeyCursor(PageReadStore pages, RecordReader<Group> rowReader, long rowCount) {
            this.pages = pages;
            this.rowReader = rowReader;
            this.rowCount = rowCount;
            step();
        }

        @Override
        public void close() {
            pages.close();
        }

        private void step() {
            position++;
            currentKey = position < rowCount ? rowReader.read().getBinary(KEY_FIELD, 0).getBytes() : null;
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
     * order — the expensive tier, paid only for a bare object directly under the scan prefix (rare in
     * a real bucket). {@code includeOwner} mirrors the replay server's {@code Projection#owner()}: when
     * {@code false} the owner columns are never decoded and every row's owner fields are {@code null}.
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
                out.add(new ObjectRow(
                        g.getBinary(KEY_FIELD, 0).getBytes(),
                        optLong(g, "size"),
                        optLong(g, "last_modified"),
                        optString(g, "etag"),
                        optString(g, "storage_class"),
                        includeOwner ? optString(g, "owner_id") : null,
                        includeOwner ? optString(g, "owner_display_name") : null,
                        optString(g, "checksum_algorithm"),
                        optString(g, "checksum_type")));
            }
            return out;
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
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
