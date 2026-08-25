/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.dataset.DatasetPartWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

/**
 * One rotating Parquet part file (I6): pinned writer settings
 * (ZSTD-3, 64 MB row group, 1 MB page, dictionary on). A part's rows are durable
 * <b>iff</b> it is finalized — {@link #close()} writes the footer and fsyncs the
 * file before the part is recorded in the checkpoint and retained for completion publication.
 */
public final class PartWriter implements AutoCloseable, DatasetPartWriter {

    /** Pinned writer settings. */
    public static final long ROW_GROUP_BYTES = 64L * 1024 * 1024;
    public static final int PAGE_BYTES = ListEntryParquetWriters.PAGE_BYTES;
    public static final int ZSTD_LEVEL = ListEntryParquetWriters.ZSTD_LEVEL;

    private final Path path;
    private final ParquetWriter<ListEntry> writer;
    private final ListEntryParquetWriters.TrackedWriter tracked;
    private long rows;

    public PartWriter(Path path, MessageType schema) throws IOException {
        this(path, schema, 0L, null);
    }

    PartWriter(Path path, MessageType schema, long writebackBytes) throws IOException {
        this(path, schema, writebackBytes, null);
    }

    PartWriter(Path path, MessageType schema, long writebackBytes,
               SyncableLocalOutputFile.DataForcer dataForcer) throws IOException {
        this.path = path;
        tracked = ListEntryParquetWriters.buildTrackedWithChannelForcer(path, new ListEntryWriteSupport(schema),
                ROW_GROUP_BYTES, ListEntryParquetWriters.PageLayout.staging(),
                writebackBytes, dataForcer);
        writer = tracked.writer();
    }

    public void write(ListEntry e) throws IOException {
        writer.write(e);
        rows++;
    }

    public long rows() {
        return rows;
    }

    /** Uncompressed bytes buffered so far — the rotation trigger. */
    public long bufferedDataSize() {
        return writer.getDataSize();
    }

    public Path path() {
        return path;
    }

    @Override public boolean periodicSyncEnabled() { return tracked.periodicSyncEnabled(); }

    @Override public long maybeSyncData() throws IOException {
        return tracked.maybeSyncData();
    }

    /**
     * Write the footer, then fsync the file <b>and its parent directory</b> so the part is
     * durable (I6): the bytes and the directory entry that names the new part must both reach
     * disk before the part can be recorded in the checkpoint.
     */
    @Override
    public void close() throws IOException {
        tracked.closeWithDurability();
    }

    /**
     * Release this part's resources <b>without finalizing it durably</b> — for the
     * abort/discard path (I6/RES-4), where the caller deletes the file immediately
     * afterwards. We close the underlying parquet-mr writer (the only way to free
     * its open file handle) but deliberately <b>skip the {@code fsync}</b> that
     * {@link #close()} performs: an un-fsynced part is not promised to survive a
     * crash, so we never produce a durably-finalized file that we then intend to
     * throw away. The part is also never recorded in checkpoint {@code part_file}, the resume
     * durability authority, so resume ignores any byte that does linger.
     * Best-effort: an {@link IOException} from the writer close is swallowed since
     * the file is about to be deleted.
     */
    public void discard() {
        tracked.discard();
    }

    @Override
    public String md5() {
        return tracked.md5();
    }

    @Override
    public long digestNanos() {
        return tracked.digestNanos();
    }

    @Override public OptionalLong periodicSyncResidualBytes() {
        return tracked.periodicSyncResidualBytes();
    }
}
