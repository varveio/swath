/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalOutputFile;

import java.nio.file.Path;

/** Narrow canonical-shape format probe, not the full Swath production writer. */
public final class CanonicalProbe {
    static final int ROWS = 2048;

    static Group row(int i) {
        var g = new SimpleGroupFactory(ParquetSchema.canonical()).newGroup();
        g.append("key", String.format("key/%08d/é/🚀", i));
        g.append("is_delete_marker", i % 3 == 1);
        g.append(
                "row_type", i % 3 == 0 ? "OBJECT" : i % 3 == 1 ? "DELETE_MARKER" : "COMMON_PREFIX");
        if (i % 3 == 0) {
            g.append("size", (long) i * 17)
                    .append("last_modified", 1700000000000000L + i)
                    .append("etag", "etag-" + i)
                    .append("storage_class", "STANDARD")
                    .append("version_id", "version-" + i)
                    .append("is_latest", i % 2 == 0)
                    .append("owner_id", "owner-" + i)
                    .append("owner_display_name", "Owner " + i)
                    .append("checksum_algorithm", "SHA256")
                    .append("checksum_type", "FULL_OBJECT");
        }
        return g;
    }

    public static void main(String[] args) throws Exception {
        var codecs = new ProbeCodecs();
        try (var writer =
                ExampleParquetWriter.builder(new LocalOutputFile(Path.of(args[0])))
                        .withConf(new PlainParquetConfiguration())
                        .withType(ParquetSchema.canonical())
                        .withCodecFactory(codecs)
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .withPageRowCountLimit(64)
                        .withPageSize(4096)
                        .withRowGroupSize(32768)
                        .build()) {
            for (int i = 0; i < ROWS; i++) writer.write(row(i));
        } finally {
            codecs.release();
        }
        System.out.println("PASS canonical-shape ZSTD writing");
    }

    static void verify(ParquetFileReader reader) throws Exception {
        if (!reader.getFooter().getFileMetaData().getSchema().equals(ParquetSchema.canonical()))
            throw new AssertionError("schema");
        int indexColumns = 0;
        int indexPages = 0;
        for (var block : reader.getFooter().getBlocks())
            for (var column : block.getColumns()) {
                var ci = reader.readColumnIndex(column);
                var oi = reader.readOffsetIndex(column);
                if (ci == null || oi == null || ci.getMinValues().size() != oi.getPageCount())
                    throw new AssertionError("indexes");
                indexColumns++;
                indexPages += oi.getPageCount();
            }
        int seen = 0;
        org.apache.parquet.column.page.PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            try (var owned = pages) {
                var records =
                        new ColumnIOFactory()
                                .getColumnIO(ParquetSchema.canonical())
                                .getRecordReader(
                                        owned, new GroupRecordConverter(ParquetSchema.canonical()));
                for (int i = 0; i < owned.getRowCount(); i++) {
                    Group actual = records.read();
                    Group expected = row(seen++);
                    if (!actual.toString().equals(expected.toString()))
                        throw new AssertionError("all fields/nulls row " + (seen - 1));
                }
            }
        }
        if (seen != ROWS || indexPages <= indexColumns)
            throw new AssertionError("rows/multi-page coverage");
        var keySchema =
                new org.apache.parquet.schema.MessageType(
                        "swath_listing", ParquetSchema.canonical().getType("key"));
        reader.setRequestedSchema(keySchema);
        var store = reader.getColumnIndexStore(0);
        var key = org.apache.parquet.hadoop.metadata.ColumnPath.get("key");
        if (store.getColumnIndex(key) == null || store.getOffsetIndex(key) == null)
            throw new AssertionError("index store");
        var predicate =
                org.apache.parquet.filter2.predicate.FilterApi.and(
                        org.apache.parquet.filter2.predicate.FilterApi.gtEq(
                                org.apache.parquet.filter2.predicate.FilterApi.binaryColumn("key"),
                                org.apache.parquet.io.api.Binary.fromString(
                                        row(100).getString("key", 0))),
                        org.apache.parquet.filter2.predicate.FilterApi.lt(
                                org.apache.parquet.filter2.predicate.FilterApi.binaryColumn("key"),
                                org.apache.parquet.io.api.Binary.fromString(
                                        row(120).getString("key", 0))));
        var ranges =
                org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter
                        .calculateRowRanges(
                                org.apache.parquet.filter2.compat.FilterCompat.get(predicate),
                                store,
                                java.util.Set.of(key),
                                reader.getFooter().getBlocks().get(0).getRowCount());
        int exact = 0;
        try (org.apache.parquet.column.page.PageReadStore filtered =
                reader.readFilteredRowGroup(0, ranges)) {
            if (filtered.getRowCount() <= 0
                    || filtered.getRowCount()
                            >= reader.getFooter().getBlocks().get(0).getRowCount())
                throw new AssertionError("bounded pages");
            var ordinals = ranges.iterator();
            var records =
                    new ColumnIOFactory()
                            .getColumnIO(keySchema)
                            .getRecordReader(filtered, new GroupRecordConverter(keySchema));
            for (int i = 0; i < filtered.getRowCount(); i++) {
                int ordinal = Math.toIntExact(ordinals.nextLong());
                Group actual = records.read();
                if (!actual.getString("key", 0).equals(row(ordinal).getString("key", 0)))
                    throw new AssertionError("filtered key " + ordinal);
                if (ordinal >= 100 && ordinal < 120) exact++;
            }
            if (ordinals.hasNext() || exact != 20)
                throw new AssertionError("filtered row coverage");
            System.out.println(
                    "filtered_page_rows=" + filtered.getRowCount() + " exact_range_rows=" + exact);
        }
        reader.setRequestedSchema(ParquetSchema.canonical());
        try (var full = reader.readRowGroup(0)) {
            if (full.getRowCount() != reader.getFooter().getBlocks().get(0).getRowCount())
                throw new AssertionError("readRowGroup");
            var records =
                    new ColumnIOFactory()
                            .getColumnIO(ParquetSchema.canonical())
                            .getRecordReader(
                                    full, new GroupRecordConverter(ParquetSchema.canonical()));
            if (!records.read().toString().equals(row(0).toString()))
                throw new AssertionError("projection restore");
        }
        System.out.println(
                "rows="
                        + seen
                        + " indexed_columns="
                        + indexColumns
                        + " indexed_pages="
                        + indexPages);
    }
}
