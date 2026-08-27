/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.dataset.DatasetDataSyncMetrics;
import io.varve.swath.output.parquet.ListEntryParquetWriters;
import io.varve.swath.output.parquet.ListEntryWriteSupport;
import io.varve.swath.output.parquet.ParquetSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * The final-file writer: the high-level {@link ParquetWriter} path with small, seek-friendly row
 * groups ({@code final-row-group-bytes}) and pages ({@code final-page-rows}), plus the static
 * sortedness stamp in the footer key-value
 * metadata, written via {@link WriteSupport#finalizeWrite()}, the one footer-KV hook the high-level
 * writer supports. {@link #close()} finalizes + fsyncs, matching
 * {@link io.varve.swath.output.parquet.PartWriter}'s durability discipline —
 * the writer construction and close/fsync sequence itself live in {@link ListEntryParquetWriters},
 * shared between the two.
 *
 * <p>Stamps the five footer KV keys documented at each constant below (no per-row-group routing
 * data is embedded — the replay server derives its routing index at startup instead). The
 * {@link #FILE_INDEX_KEY}/{@link #FILE_FINAL_KEY} pair lets a reader prove multi-file completeness
 * from the resolved file set alone — contiguous {@code file_index} values {@code 1..N} with exactly
 * one file, at index N, carrying {@link #FILE_FINAL_KEY} — rather than trusting a sidecar; a
 * truncated prefix left by a mid-roll crash fails that check and falls back to role-1 serving. Full
 * schema and the reader-side completeness check: {@code docs/internals/contracts.md} §6 and
 * {@code docs/swath-replay.md} ("Footer sortedness stamp").
 *
 * <p>Only <b>final</b> output files use this writer. Staging uses the distinct page-run format, so
 * internal working state cannot be mistaken for served Parquet output.
 */
public final class SortedParquetWriter implements SortedFileWriter {

    private static final int DATA_SYNC_CHECK_ROWS = 1024;

    /** Footer KV key naming the total order the file is sorted by. */
    public static final String ORDER_KEY = "swath.sort.order";

    /**
     * The stamp's order value — stable across releases: key bytes unsigned lexicographic, then
     * {@code version_id} (absent/null first, then unsigned UTF-8 bytes), then {@code row_type}
     * rank ({@code OBJECT < COMMON_PREFIX < DELETE_MARKER}) as the final deterministic tail.
     */
    public static final String ORDER_VALUE = "key_bytes_unsigned,version_id_null_first,row_type_rank";

    /** Footer KV key for the mode: {@code "objects"} or {@code "versions"}. */
    public static final String MODE_KEY = "swath.sort.mode";

    /** Footer KV key for the stamp schema's own version. */
    public static final String FORMAT_VERSION_KEY = "swath.sort.format_version";

    /** Current stamp format version. */
    public static final String FORMAT_VERSION_VALUE = "1";

    /** Footer KV key for this file's 1-based position in the output's roll sequence. */
    public static final String FILE_INDEX_KEY = "swath.sort.file_index";

    /** Footer KV key present (value {@link #FILE_FINAL_VALUE}) ONLY on the last file of the output. */
    public static final String FILE_FINAL_KEY = "swath.sort.file_final";

    /** The only value {@link #FILE_FINAL_KEY} is ever written with — its absence is the negative case. */
    public static final String FILE_FINAL_VALUE = "true";

    private final ParquetWriter<ListEntry> writer;
    private final ListEntryParquetWriters.TrackedWriter tracked;
    private final DatasetDataSyncMetrics syncMetrics;
    private long rows;
    private int rowsSinceSyncCheck;
    private long boundsBytes;
    private byte[] firstKey;
    private byte[] lastKey;
    private boolean finalFile;
    private int fileIndex;
    private boolean closed;   // guarded by this (see close())
    private FinalPartMetadata finalMetadata;

    public SortedParquetWriter(Path path, SortConfig config, SortMode mode, int fileIndex) throws IOException {
        this(path, config, mode, fileIndex, WriterOptions.DEFAULT);
    }

    static SortedParquetWriter withWriteback(Path path, SortConfig config, SortMode mode,
            int fileIndex, long writebackBytes, DatasetDataSyncMetrics syncMetrics)
            throws IOException {
        return new SortedParquetWriter(path, config, mode, fileIndex,
                new WriterOptions(writebackBytes, syncMetrics, null));
    }

    static SortedParquetWriter withDataForcer(Path path, SortConfig config, SortMode mode, int fileIndex,
            long writebackBytes, RunMetrics metrics, ListEntryParquetWriters.DataForcer dataForcer)
            throws IOException {
        DatasetDataSyncMetrics syncMetrics = metrics == null ? null : new DatasetDataSyncMetrics(
                metrics, "parquet", DatasetDataSyncMetrics.Classification.SORTED_PARQUET);
        return new SortedParquetWriter(path, config, mode, fileIndex,
                new WriterOptions(writebackBytes, syncMetrics, dataForcer));
    }

    private SortedParquetWriter(Path path, SortConfig config, SortMode mode, int fileIndex,
            WriterOptions options) throws IOException {
        this.fileIndex = fileIndex;
        this.syncMetrics = options.syncMetrics();
        Map<String, String> stamp = Map.of(
                ORDER_KEY, ORDER_VALUE,
                MODE_KEY, mode.value(),
                FORMAT_VERSION_KEY, FORMAT_VERSION_VALUE);
        // finalFile AND fileIndex are both read lazily, not captured here: either may be set any time
        // before close(), which is when the footer — and so the stamp — is actually written. The
        // parallel range merge needs the index late: a range's parts only learn their position in the
        // GLOBAL roll sequence once every earlier range has finished and its part count is known.
        // this.fileIndex, NOT fileIndex: the constructor parameter shadows the field here, and capturing
        // it would freeze the index at construction — silently defeating setFileIndex and reinstating
        // the range-local stamp the parallel path exists to fix.
        WriteSupport<ListEntry> writeSupport = new StampedWriteSupport(
                ParquetSchema.canonical(), stamp, () -> finalFile, () -> this.fileIndex);
        ListEntryParquetWriters.TrackedSpec trackedSpec = new ListEntryParquetWriters.TrackedSpec(
                config.finalRowGroupBytes(),
                ListEntryParquetWriters.PageLayout.served(config.finalPageRows()),
                options.writebackBytes(), options.dataForcer());
        ListEntryParquetWriters.TrackedWriter tracked =
                ListEntryParquetWriters.buildTracked(path, writeSupport, trackedSpec);
        this.tracked = tracked;
        this.writer = tracked.writer();
    }

    @Override
    public void write(ListEntry e) throws IOException {
        writer.write(e);
        byte[] key = e.key().rawUnsafe();
        if (firstKey == null) {
            firstKey = key.clone();
        }
        // ListEntry/KeyBytes are immutable; retaining the latest reference avoids a per-row copy.
        lastKey = key;
        boundsBytes += key.length;
        rows++;
        if (tracked.periodicSyncEnabled() && ++rowsSinceSyncCheck >= DATA_SYNC_CHECK_ROWS) {
            rowsSinceSyncCheck = 0;
            long syncStartedAt = System.nanoTime();
            long syncedBytes = tracked.maybeSyncData();
            if (syncedBytes > 0 && syncMetrics != null) {
                syncMetrics.recordSync(System.nanoTime() - syncStartedAt, syncedBytes);
            }
        }
    }

    @Override
    public long rows() {
        return rows;
    }

    @Override
    public long dataSize() {
        return writer.getDataSize();
    }

    /**
     * Marks this file as the last of the output — call, if at all, before
     * {@link #close()}, which is when the footer (and so {@link #FILE_FINAL_KEY}) is actually written.
     */
    @Override
    public void markFinal() {
        this.finalFile = true;
    }

    /**
     * Set this file's 1-based position in the output's roll sequence, overriding the constructor's
     * value. Like {@link #markFinal()} this is honoured any time before {@link #close()}. The
     * parallel range merge is the caller that needs it: each range writes its parts before it can
     * know how many parts the ranges below it produced, so the global index is assigned afterwards,
     * once every range has drained.
     */
    @Override
    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    /**
     * Finalize (footer, incl. the stamp) and fsync the file and its parent directory (I6).
     *
     * <p>Idempotent. The parallel merge has two owners that may both try to release a part — the
     * publish path that stamps and closes, and the failure path that releases whatever is still open
     * — and which one runs is decided by a race between range threads. Making the second close a
     * no-op is what lets both be unconditional instead of coordinated.
     */
    @Override
    public synchronized void close() throws IOException {
        // synchronized, not a volatile check-then-act: two owners CAN race here (the publish path
        // stamps and closes, the failure path releases whatever is still open), and a check-then-act
        // would let both enter the underlying Parquet close, which is not thread-safe.
        if (closed) {
            return;
        }
        // Set only AFTER the footer write and fsync succeed. Marking first would make a FAILED close
        // look completed, so a retry would silently skip a file whose footer was never written --
        // publishing a part with no stamp at all.
        long closeStart = System.nanoTime();
        tracked.closeWithDurability();
        long closeNanos = System.nanoTime() - closeStart;
        finalMetadata = new FinalPartMetadata(rows, tracked.bytes(), tracked.md5(),
                firstKey == null ? null : new String(firstKey, StandardCharsets.UTF_8),
                lastKey == null ? null : new String(lastKey, StandardCharsets.UTF_8),
                closeNanos, tracked.digestNanos(), boundsBytes);
        if (syncMetrics != null) {
            tracked.periodicSyncResidualBytes().ifPresent(syncMetrics::recordResidual);
        }
        closed = true;
    }

    @Override
    public synchronized Optional<FinalPartMetadata> finalMetadata() {
        return Optional.ofNullable(finalMetadata);
    }

    private record WriterOptions(long writebackBytes, DatasetDataSyncMetrics syncMetrics,
                                 ListEntryParquetWriters.DataForcer dataForcer) {
        private static final WriterOptions DEFAULT = new WriterOptions(0L, null, null);
    }

    /**
     * Delegates row encoding to {@link ListEntryWriteSupport} unchanged; the only addition is
     * {@link #finalizeWrite()}, the high-level writer's sole footer-KV hook — which reads
     * {@code finalFile} at CALL time (during {@code close()}), not at construction, so a
     * {@link #markFinal()} anywhere before {@code close()} is honored.
     */
    private static final class StampedWriteSupport extends WriteSupport<ListEntry> {
        private final ListEntryWriteSupport delegate;
        private final Map<String, String> stamp;
        private final BooleanSupplier finalFile;
        private final IntSupplier fileIndex;

        StampedWriteSupport(MessageType schema, Map<String, String> stamp, BooleanSupplier finalFile,
                            IntSupplier fileIndex) {
            this.delegate = new ListEntryWriteSupport(schema);
            this.stamp = stamp;
            this.finalFile = finalFile;
            this.fileIndex = fileIndex;
        }

        @Override
        @SuppressWarnings("deprecation")   // init(Configuration) is the abstract method in parquet 1.15
        public WriteContext init(Configuration configuration) {
            return delegate.init(configuration);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            delegate.prepareForWrite(recordConsumer);
        }

        @Override
        public void write(ListEntry record) {
            delegate.write(record);
        }

        @Override
        public FinalizedWriteContext finalizeWrite() {
            // Merge, don't replace: today ListEntryWriteSupport.finalizeWrite() contributes nothing
            // (WriteSupport's default is an empty map), so this is a no-op union in practice — but a
            // future delegate-side footer KV must not be silently dropped by this wrapper. The stamp
            // wins on key conflict (it's the seam this class owns).
            Map<String, String> merged = new LinkedHashMap<>(delegate.finalizeWrite().getExtraMetaData());
            merged.putAll(stamp);
            merged.put(FILE_INDEX_KEY, Integer.toString(fileIndex.getAsInt()));
            if (finalFile.getAsBoolean()) {
                merged.put(FILE_FINAL_KEY, FILE_FINAL_VALUE);
            }
            return new FinalizedWriteContext(merged);
        }
    }
}
