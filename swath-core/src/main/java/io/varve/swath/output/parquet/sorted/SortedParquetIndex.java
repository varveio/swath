/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import io.varve.swath.output.parquet.ParquetFiles;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/**
 * The index-derive API (§0.7): for each row group of a sorted Parquet
 * file, the file's <b>actual first row's</b> key bytes plus that row group's row count —
 * <b>never</b> the Parquet footer's min/max statistics. Statistics can be silently truncated for
 * long keys (S3 keys reach 1&nbsp;KB) and the Parquet spec only guarantees they bound the true
 * values for pruning, not that they equal them; a truncated min sorts below the true first key,
 * which would misroute a bounded-range query even though it's safe for engine-side pruning.
 * Consumed by the replay server to build its in-memory routing index once at startup. Each row
 * group's {@code row_type} footer statistics are also consulted — but only for an <b>eligibility</b>
 * proof, never for routing; see {@link #isPureObjectRowType} for why that is a sound, narrow
 * exception to the never-trust-stats rule rather than a violation of it.
 *
 * <p><b>Efficient by construction, never a whole-file scan.</b> {@link ParquetFileReader} is
 * pointed at a {@link ParquetFileReader#setRequestedSchema projected} schema containing only the
 * {@code key} column, so each row group costs exactly one short read of that row group's key
 * column chunk — the same low-level, one-row-group-at-a-time pattern {@link ParquetEntryReader} uses,
 * minus the other columns. Only the first decoded record of each row group is consulted; the rest
 * of that row group's key-column data is left unread once the next {@code readNextRowGroup()} call
 * advances past it.
 */
public final class SortedParquetIndex {

    private static final String KEY_FIELD = "key";
    private static final String ROW_TYPE_FIELD = "row_type";
    private static final ColumnPath ROW_TYPE_PATH = ColumnPath.get(ROW_TYPE_FIELD);
    private static final byte[] OBJECT_ROW_TYPE_BYTES = "OBJECT".getBytes(StandardCharsets.UTF_8);
    private SortedParquetIndex() {
    }

    /**
     * {@code file}'s total row count as its footer declares it — the one reading here that costs no
     * column read at all, so a caller can size a file before deciding whether to pay for its index.
     * A footer row count is not a statistic in the sense the class javadoc refuses: it is the count
     * the writer recorded of rows actually written, which the reader relies on to decode at all.
     */
    public static long rowCount(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFiles.open(file)) {
            return reader.getRecordCount();
        }
    }

    /**
     * One row group's actual first key, row count, and whether every row in the group is
     * <b>provably</b> {@code row_type='OBJECT'} (see {@link #firstKeysPerRowGroup} — {@code firstKey}
     * and {@code rowCount} never come from footer stats, but {@link #pureObjectRowType} deliberately
     * does; see its own javadoc for why that is sound).
     */
    public record RowGroupKey(byte[] firstKey, long rowCount, boolean pureObjectRowType) {

        public RowGroupKey {
            firstKey = firstKey.clone();   // defensive copy crossing the public seam
        }

        /** Defensive copy — callers may mutate the returned array without corrupting this entry. */
        @Override
        public byte[] firstKey() {
            return firstKey.clone();
        }

        /**
         * Content equality — two entries with byte-identical {@code firstKey} arrays (and equal
         * {@code rowCount}/{@code pureObjectRowType}) are equal, regardless of whether they share the
         * same underlying array (records generate reference-identity equals/hashCode for array
         * components by default, which is wrong here; matches {@code ContinuationToken}'s fix).
         */
        @Override
        public boolean equals(Object o) {
            return o instanceof RowGroupKey other
                    && rowCount == other.rowCount
                    && pureObjectRowType == other.pureObjectRowType
                    && Arrays.equals(firstKey, other.firstKey);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(firstKey);
            result = 31 * result + Long.hashCode(rowCount);
            result = 31 * result + Boolean.hashCode(pureObjectRowType);
            return result;
        }
    }

    /**
     * Derive {@code (firstKey, rowCount, pureObjectRowType)} for every row group of {@code file}, in
     * row-group order. Returns an empty list for a file with no row groups (e.g. an empty sorted
     * output).
     */
    public static List<RowGroupKey> firstKeysPerRowGroup(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFiles.open(file)) {
            MessageType full = reader.getFooter().getFileMetaData().getSchema();
            MessageType keyOnly = new MessageType(full.getName(), full.getType(KEY_FIELD));
            reader.setRequestedSchema(keyOnly);
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(keyOnly);
            List<BlockMetaData> blocks = reader.getRowGroups();

            List<RowGroupKey> out = new ArrayList<>();
            PageReadStore pages;
            int blockIndex = 0;
            while ((pages = reader.readNextRowGroup()) != null) {
                BlockMetaData block = blocks.get(blockIndex++);
                long rowCount = pages.getRowCount();
                if (rowCount == 0) {
                    // Defensive: an empty row group carries no first key, so skip it rather than
                    // decode nothing. Safe to just omit — the returned list is consumed as
                    // key -> boundary values for the bounded-range computation (a routing index),
                    // never as (file, row-group-ordinal) -> content, so a skipped empty group shifts
                    // no downstream row-group index and drops no routing information.
                    continue;
                }
                RecordReader<Group> rowReader = columnIo.getRecordReader(pages, new GroupRecordConverter(keyOnly));
                byte[] firstKey = rowReader.read().getBinary(KEY_FIELD, 0).getBytes();
                out.add(new RowGroupKey(firstKey, rowCount, isPureObjectRowType(block)));
            }
            return out;
        }
    }

    /**
     * One non-empty row group's actual first key, its <b>physical</b> block index, and row count.
     * Unlike {@link RowGroupKey}, {@link #blockIndex} is carried so an indexed Parquet reader can
     * address the physical group directly via {@link
     * org.apache.parquet.hadoop.ParquetFileReader#readRowGroup(int)}.
     */
    public record RowGroupSpan(byte[] firstKey, int blockIndex, long rowCount) {

        public RowGroupSpan {
            firstKey = firstKey.clone();   // defensive copy crossing the public seam
        }

        /** Defensive copy — callers may mutate the returned array without corrupting this entry. */
        @Override
        public byte[] firstKey() {
            return firstKey.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RowGroupSpan other
                    && blockIndex == other.blockIndex
                    && rowCount == other.rowCount
                    && Arrays.equals(firstKey, other.firstKey);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(firstKey);
            result = 31 * result + Integer.hashCode(blockIndex);
            result = 31 * result + Long.hashCode(rowCount);
            return result;
        }
    }

    /**
     * Like {@link #firstKeysPerRowGroup} (actual decoded first keys, never footer stats §9.1), but
     * carries each non-empty row group's <b>physical</b> block index — the ordinal into {@link
     * ParquetFileReader#getRowGroups()}, usable directly with {@link
     * ParquetFileReader#readRowGroup(int)}. Empty row groups are omitted (they carry no first key and
     * no rows), so a {@link RowGroupSpan}'s position in the returned list is <b>not</b> its physical
     * block index — that is exactly why {@link RowGroupSpan#blockIndex} is carried explicitly
     * (an indexed reader must call {@code readRowGroup(physicalIndex)}, never {@code
     * readRowGroup(listPosition)}). Returns an empty list for a file with no rows.
     */
    public static List<RowGroupSpan> rowGroupSpans(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFiles.open(file)) {
            MessageType full = reader.getFooter().getFileMetaData().getSchema();
            MessageType keyOnly = new MessageType(full.getName(), full.getType(KEY_FIELD));
            reader.setRequestedSchema(keyOnly);
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(keyOnly);

            List<RowGroupSpan> out = new ArrayList<>();
            PageReadStore pages;
            int blockIndex = 0;
            while ((pages = reader.readNextRowGroup()) != null) {
                int physicalIndex = blockIndex++;   // physical ordinal, incremented for EVERY group
                long rowCount = pages.getRowCount();
                if (rowCount == 0) {
                    continue;   // empty group: no first key, no rows — omit (its physical index is skipped)
                }
                RecordReader<Group> rowReader = columnIo.getRecordReader(pages, new GroupRecordConverter(keyOnly));
                byte[] firstKey = rowReader.read().getBinary(KEY_FIELD, 0).getBytes();
                out.add(new RowGroupSpan(firstKey, physicalIndex, rowCount));
            }
            return out;
        }
    }

    /**
     * A file's true first key, true last key, and total row count (feeds the manifest's
     * {@code minKey}/{@code maxKey}/{@code rowCount}) — computed in ONE row-group pass over
     * just the {@code key} column, the same low-level projected-schema pattern {@link
     * #firstKeysPerRowGroup} uses. Never the Parquet footer's min/max statistics, for the same
     * truncation reason documented on the class (§9.1). {@code firstKey}/{@code lastKey} are
     * {@code null} for a file with zero rows (e.g. an empty sorted output); {@code rowCount} is
     * always accurate.
     */
    public record Bounds(byte[] firstKey, byte[] lastKey, long rowCount) {

        public Bounds {
            firstKey = firstKey == null ? null : firstKey.clone();
            lastKey = lastKey == null ? null : lastKey.clone();
        }

        @Override
        public byte[] firstKey() {
            return firstKey == null ? null : firstKey.clone();
        }

        @Override
        public byte[] lastKey() {
            return lastKey == null ? null : lastKey.clone();
        }
    }

    /** Exact bounds scan with a cooperative check for merge/publication callers. */
    public static Bounds bounds(Path file, Runnable cancellationCheck) throws IOException {
        Objects.requireNonNull(cancellationCheck, "cancellationCheck");
        try (ParquetFileReader reader = ParquetFiles.open(file)) {
            MessageType full = reader.getFooter().getFileMetaData().getSchema();
            MessageType keyOnly = new MessageType(full.getName(), full.getType(KEY_FIELD));
            reader.setRequestedSchema(keyOnly);
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(keyOnly);

            byte[] first = null;
            byte[] last = null;
            long total = 0;
            PageReadStore pages;
            while (true) {
                cancellationCheck.run();
                pages = reader.readNextRowGroup();
                if (pages == null) {
                    break;
                }
                long rowCount = pages.getRowCount();
                if (rowCount == 0) {
                    continue;
                }
                RecordReader<Group> rowReader = columnIo.getRecordReader(pages, new GroupRecordConverter(keyOnly));
                for (long i = 0; i < rowCount; i++) {
                    if ((i & 0xfffL) == 0) {
                        cancellationCheck.run();
                    }
                    byte[] key = rowReader.read().getBinary(KEY_FIELD, 0).getBytes();
                    if (first == null) {
                        first = key;
                    }
                    last = key;
                }
                total += rowCount;
            }
            return new Bounds(first, last, total);
        }
    }

    /**
     * Whether {@code block}'s {@code row_type} column footer statistics <b>prove</b> every row in
     * the row group is {@code 'OBJECT'} — used only for the sorted-serving <b>eligibility</b> check
     * (a sorted file may legitimately mix row types, e.g. a legacy delimiter'd capture re-sorted by
     * {@code sort-fixture}, which still stamps {@code mode=objects}; role-1 serving is correct for
     * such a file because it filters {@code row_type} at materialization, but role-2's routing
     * invariant assumes {@code rowCount == OBJECT-row-count} per group, so a file that isn't provably
     * pure must fall back rather than silently under-count and truncate early).
     *
     * <p><b>This is a deliberate, narrow exception to the "never trust footer stats" rule (§9.1),
     * not a violation of it.</b> §9.1 forbids stats for <em>routing</em> (choosing a boundary key)
     * because a truncated bound can sort strictly inside the true value range and misroute — silent
     * data loss. This method never routes anything; its only two outcomes are "proven pure" (serve
     * sorted) or "not proven" (fall back to role-1, still correct). The proof is sound even under
     * truncation: a Parquet writer may only report a <em>widened</em> bound — {@code reportedMin <=
     * trueMin <= trueMax <= reportedMax} always holds, truncated or not. So if {@code reportedMin ==
     * reportedMax}, every true value is squeezed to that single point (the two inequalities collapse
     * {@code trueMin} and {@code trueMax} onto the reported bound), and truncation only ever makes
     * {@code reportedMin == reportedMax} <em>less</em> likely to spuriously hold, never more — a false
     * "pure" verdict is not possible. Missing/empty/unreadable statistics, or {@code min != max}, are
     * conservatively treated as "not proven" (fall back), never as "proven impure": this method only
     * ever asserts purity, never the reverse.
     */
    private static boolean isPureObjectRowType(BlockMetaData block) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            if (!column.getPath().equals(ROW_TYPE_PATH)) {
                continue;
            }
            Statistics<?> stats = column.getStatistics();
            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
                return false;
            }
            byte[] min = stats.getMinBytes();
            byte[] max = stats.getMaxBytes();
            return Arrays.equals(min, max) && Arrays.equals(min, OBJECT_ROW_TYPE_BYTES);
        }
        return false;   // row_type column missing from the footer — conservatively "not proven"
    }
}
