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
import java.util.List;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
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
 * <p>parquet-java consumes the same index the engine ignored: {@link
 * ParquetFileReader#readNextFilteredRowGroup()} skips whole pages whose ColumnIndex bounds exclude
 * the range before decoding any of them. Measured against a real 788,903-row capture, a cold
 * thousand-row page fell from ~205 ms to ~62 ms with the full projection.
 *
 * <p><b>The page filter is a pruning hint, not the answer.</b> It removes pages that cannot contain
 * the range; rows inside a surviving page may still fall outside it. Every row is therefore
 * re-checked here against the range with {@link KeyBytes#compareUnsigned} — which is also the
 * ordering the whole sorted format is defined in, and the one Parquet's own binary filtering was
 * measured to use (a 0xFF-keyed probe, since a signed comparison would silently drop every key with
 * a high byte set).
 *
 * <p>Schema and column-IO are derived once per file and reused across reads: they are pure functions
 * of (file, projection), and rebuilding them per request measured ~13 ms of avoidable cost. The
 * {@link ParquetFileReader} itself cannot be shared, because a filter is fixed at open time — so one
 * is opened per read (~2 ms on a 21 MB fixture). Reading the footer once here is what keeps that
 * open cheap.
 *
 * <p>Lives in {@code swath-core} for the same reason {@link SortedRowGroupReader} does: it traffics
 * only in {@code byte[]}/{@code long}/{@code int}/collections, so the replay server drives it
 * without an {@code org.apache.parquet} type reaching that module's compile classpath. Not
 * thread-safe for the cached derivations' sake alone; concurrent callers should hold one instance
 * each, or pool them.
 */
public final class SortedRangeReader implements AutoCloseable {

    private static final String KEY_FIELD = "key";

    private final Path file;
    private final MessageType fileSchema;
    private final MessageType schemaWithOwner;
    private final MessageType schemaWithoutOwner;
    private final MessageColumnIO columnIoWithOwner;
    private final MessageColumnIO columnIoWithoutOwner;

    public SortedRangeReader(Path file) throws IOException {
        this.file = file;
        try (ParquetFileReader footer = ParquetFileReader.open(new LocalInputFile(file))) {
            this.fileSchema = footer.getFooter().getFileMetaData().getSchema();
        }
        ColumnIOFactory factory = new ColumnIOFactory();
        this.schemaWithOwner = SortedRowGroupReader.objectProjection(fileSchema, true, true);
        this.schemaWithoutOwner = SortedRowGroupReader.objectProjection(fileSchema, false, true);
        this.columnIoWithOwner = factory.getColumnIO(schemaWithOwner, fileSchema);
        this.columnIoWithoutOwner = factory.getColumnIO(schemaWithoutOwner, fileSchema);
    }

    /**
     * The first {@code limit} {@code OBJECT} rows at or after {@code from} and strictly before
     * {@code toExclusive}, in ascending key order.
     *
     * @param from         range start; {@code null} starts at the beginning of the file
     * @param fromInclusive whether {@code from} itself is in range
     * @param toExclusive  exclusive range end; {@code null} reads to the end of the file
     * @param limit        maximum rows to return; the read stops as soon as it has them
     * @param includeOwner whether to decode the owner columns at all
     */
    public List<ObjectRow> range(byte[] from, boolean fromInclusive, byte[] toExclusive, int limit,
                                 boolean includeOwner) throws IOException {
        if (limit <= 0) {
            return List.of();
        }
        MessageType schema = includeOwner ? schemaWithOwner : schemaWithoutOwner;
        MessageColumnIO columnIo = includeOwner ? columnIoWithOwner : columnIoWithoutOwner;
        ParquetReadOptions.Builder options = ParquetReadOptions.builder().useColumnIndexFilter(true);
        FilterPredicate predicate = predicate(from, fromInclusive, toExclusive);
        if (predicate != null) {
            options.withRecordFilter(FilterCompat.get(predicate));
        }
        List<ObjectRow> out = new ArrayList<>(Math.min(limit, 1024));
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file), options.build())) {
            reader.setRequestedSchema(schema);
            PageReadStore pages;
            while (out.size() < limit && (pages = reader.readNextFilteredRowGroup()) != null) {
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
                        // A surviving page can still hold rows outside the range: the index prunes
                        // pages, never rows.
                        continue;
                    }
                    out.add(SortedRowGroupReader.toObjectRow(g, key, includeOwner));
                }
            }
        }
        return out;
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
        if (lower == null) {
            return upper;
        }
        return upper == null ? lower : FilterApi.and(lower, upper);
    }

    @Override
    public void close() {
        // Nothing held open: each read owns its reader, and the footer read in the constructor is
        // closed there. Present so callers can treat this like every other reader in the package.
    }
}
