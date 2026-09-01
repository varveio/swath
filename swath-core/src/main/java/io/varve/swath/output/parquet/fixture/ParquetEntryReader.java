/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.fixture;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.RowType;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/**
 * Forward-only reader over one sorted Parquet segment. Reads <b>one row group at a time</b>
 * via the low-level {@link ParquetFileReader} (no Hadoop mapreduce {@code InputFormat} path), so a
 * whole segment is never materialized — only the current row group's decompressed pages are in heap
 * (bounded read-ahead). Decodes each row to the canonical {@link ListEntry} by
 * {@code row_type}, the inverse of {@link io.varve.swath.output.parquet.ListEntryWriteSupport}.
 *
 * <p>The next record is pre-loaded so {@link #hasNext()} stays non-throwing; {@link #next()} performs
 * the read that advances to the following record and may fail.
 */
public final class ParquetEntryReader implements AutoCloseable {

    private final ParquetFileReader reader;
    private final MessageColumnIO columnIo;
    private final MessageType schema;

    private RecordReader<Group> rowGroup;   // record reader for the current row group
    private long rowsLeftInGroup;
    private ListEntry head;

    public ParquetEntryReader(Path path) throws IOException {
        this.reader = ParquetFileReader.open(new LocalInputFile(path));
        this.schema = reader.getFooter().getFileMetaData().getSchema();
        this.columnIo = new ColumnIOFactory().getColumnIO(schema);
        this.head = readNext();
    }

    public boolean hasNext() {
        return head != null;
    }

    public ListEntry next() throws IOException {
        ListEntry current = head;
        head = readNext();
        return current;
    }

    private ListEntry readNext() throws IOException {
        while (rowsLeftInGroup == 0) {
            PageReadStore pages = reader.readNextRowGroup();
            if (pages == null) {
                return null;   // exhausted
            }
            rowsLeftInGroup = pages.getRowCount();
            rowGroup = columnIo.getRecordReader(pages, new GroupRecordConverter(schema));
        }
        rowsLeftInGroup--;
        return toEntry(rowGroup.read());
    }

    private static ListEntry toEntry(Group g) {
        KeyBytes key = KeyBytes.of(g.getBinary("key", 0).getBytes());
        RowType rowType = RowType.valueOf(g.getString("row_type", 0));
        return switch (rowType) {
            case OBJECT -> new ObjectEntry(key,
                    g.getLong("size", 0),
                    optLong(g, "last_modified"),
                    optString(g, "etag"),
                    optString(g, "storage_class"),
                    optString(g, "version_id"),
                    optBool(g, "is_latest"),
                    optString(g, "owner_id"),
                    optString(g, "owner_display_name"),
                    optString(g, "checksum_algorithm"),
                    optString(g, "checksum_type"));
            case COMMON_PREFIX -> new CommonPrefixEntry(key);
            case DELETE_MARKER -> new DeleteMarkerEntry(key,
                    optString(g, "version_id"),
                    optBool(g, "is_latest"),
                    optLong(g, "last_modified"),
                    optString(g, "owner_id"));
        };
    }

    private static String optString(Group g, String field) {
        return g.getFieldRepetitionCount(field) == 0 ? null : g.getString(field, 0);
    }

    private static long optLong(Group g, String field) {
        return g.getFieldRepetitionCount(field) == 0 ? 0L : g.getLong(field, 0);
    }

    private static boolean optBool(Group g, String field) {
        return g.getFieldRepetitionCount(field) != 0 && g.getBoolean(field, 0);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
