/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.ListEntryWriteSupport;
import io.varve.swath.output.parquet.ParquetSchema;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * One internally-sorted Parquet staging segment. Reuses the canonical schema and
 * {@link ListEntryWriteSupport} — the same high-level writer path as production
 * parts — but with a deliberately SMALL row-group size ({@link SortConfig#segmentRowGroupBytes()},
 * default 1&nbsp;MB) so the encoder's uncompressed row-group buffer stays inside the memory corridor
 * <b>and</b> so {@link SegmentReader}, which preloads one full row group per open merge stream, keeps
 * merge-phase peak memory small even with many streams open at once (see {@link KWayMerge}'s class
 * javadoc and {@link SortConfig#effectiveFanIn()}). {@link #close()}
 * writes the footer and fsyncs the file and its parent directory: a segment is durable <b>iff</b>
 * finalized (I6), the point the checkpoint's {@code durable_cursor} advances on. The
 * writer construction and close/fsync sequence themselves live in {@link ListEntryParquetWriters},
 * shared with {@link SortedParquetWriter}.
 */
final class SegmentParquetSink implements AutoCloseable {

    private final Path path;
    private final ParquetWriter<ListEntry> writer;

    SegmentParquetSink(Path path, long rowGroupBytes) throws IOException {
        this.path = path;
        this.writer = ListEntryParquetWriters.build(
                path, new ListEntryWriteSupport(ParquetSchema.canonical()), rowGroupBytes,
                ListEntryParquetWriters.PageLayout.staging());
    }

    void write(ListEntry e) throws IOException {
        writer.write(e);
    }

    @Override
    public void close() throws IOException {
        ListEntryParquetWriters.closeWithDurability(path, writer);
    }
}
