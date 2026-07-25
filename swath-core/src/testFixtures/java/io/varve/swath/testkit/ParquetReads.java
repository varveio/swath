/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 * Reads Parquet via the low-level {@link ParquetFileReader} + {@link
 * LocalInputFile} — no Hadoop {@code FileSystem} / mapreduce dependency. Used by
 * the round-trip and INT-6 assertions.
 */
public final class ParquetReads {

    private ParquetReads() {
    }

    public static MessageType schema(Path file) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
            return r.getFooter().getFileMetaData().getSchema();
        }
    }

    public static List<Group> readAll(Path file) throws IOException {
        List<Group> out = new ArrayList<>();
        try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = r.getFooter().getFileMetaData().getSchema();
            MessageColumnIO io = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = r.readNextRowGroup()) != null) {
                long n = pages.getRowCount();
                RecordReader<Group> rr = io.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < n; i++) {
                    out.add(rr.read());
                }
            }
        }
        return out;
    }

    /** All {@code key} column values across a part file, decoded UTF-8. */
    public static List<String> keys(Path file) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Group g : readAll(file)) {
            keys.add(new String(g.getBinary("key", 0).getBytes(), StandardCharsets.UTF_8));
        }
        return keys;
    }
}
