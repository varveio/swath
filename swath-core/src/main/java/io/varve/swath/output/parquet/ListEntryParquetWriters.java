/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.dataset.DurableFiles;
import io.varve.swath.output.dataset.PeriodicDataSync;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

/** Shared physical construction and durability boundary for {@link ListEntry} Parquet writers. */
public final class ListEntryParquetWriters {

    public static final int PAGE_BYTES = 1024 * 1024;
    public static final int ZSTD_LEVEL = 3;
    public static final int SERVED_DICTIONARY_BYTES = 8 * 1024;
    /** Keep complete page bounds for every supported general-purpose S3 key (maximum 1,024 bytes). */
    public static final int SERVED_COLUMN_INDEX_TRUNCATE_BYTES = 1024;
    public static final int DEFAULT_PAGE_ROWS = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT;

    public record PageLayout(int pageRows, int dictionaryBytes, int columnIndexTruncateBytes) {
        public static PageLayout direct() {
            return new PageLayout(DEFAULT_PAGE_ROWS, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE,
                    ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);
        }

        public static PageLayout served(int pageRows) {
            return new PageLayout(pageRows, SERVED_DICTIONARY_BYTES,
                    SERVED_COLUMN_INDEX_TRUNCATE_BYTES);
        }
    }

    /** Complete construction options for a tracked final-file writer. */
    public record TrackedSpec(long rowGroupBytes, PageLayout layout, long writebackBytes,
                              DataForcer dataForcer) {
        public TrackedSpec {
            Objects.requireNonNull(layout, "layout");
        }
    }

    private ListEntryParquetWriters() {
    }

    /** Builds a final-file writer with byte/digest tracking and optional same-channel data sync. */
    public static TrackedWriter buildTracked(
            Path path, WriteSupport<ListEntry> writeSupport, TrackedSpec spec) throws IOException {
        PeriodicDataSync periodicSync = new PeriodicDataSync(spec.writebackBytes());
        SyncableLocalOutputFile syncableOutput = null;
        OutputFile physicalOutput;
        if (periodicSync.enabled()) {
            syncableOutput = spec.dataForcer() == null
                    ? new SyncableLocalOutputFile(path)
                    : new SyncableLocalOutputFile(path, spec.dataForcer()::force);
            physicalOutput = syncableOutput;
        } else {
            physicalOutput = new LocalOutputFile(path);
        }
        DigestingOutputFile output = new DigestingOutputFile(physicalOutput);
        return new TrackedWriter(path,
                build(writeSupport, spec.rowGroupBytes(), spec.layout(), output), output,
                syncableOutput, periodicSync);
    }

    private static ParquetWriter<ListEntry> build(WriteSupport<ListEntry> writeSupport,
            long rowGroupBytes, PageLayout layout, OutputFile output) throws IOException {
        ParquetConfiguration conf = ParquetFiles.newConfiguration();
        return new Builder(output, writeSupport)
                .withConf(conf)
                .withCodecFactory(ParquetFiles.newCodecFactory())
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(PAGE_BYTES)
                .withPageRowCountLimit(layout.pageRows())
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(layout.dictionaryBytes())
                .withColumnIndexTruncateLength(layout.columnIndexTruncateBytes())
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    /** Physical state shared by direct and sorted final writers. */
    public static final class TrackedWriter {
        private final Path path;
        private final ParquetWriter<ListEntry> writer;
        private final DigestingOutputFile output;
        private final SyncableLocalOutputFile syncableOutput;
        private final PeriodicDataSync periodicSync;

        private TrackedWriter(Path path, ParquetWriter<ListEntry> writer, DigestingOutputFile output,
                SyncableLocalOutputFile syncableOutput, PeriodicDataSync periodicSync) {
            this.path = path;
            this.writer = writer;
            this.output = output;
            this.syncableOutput = syncableOutput;
            this.periodicSync = periodicSync;
        }

        public ParquetWriter<ListEntry> writer() {
            return writer;
        }

        /** Writes one row, restoring a checked output failure with the affected part's path. */
        public void write(ListEntry entry) throws IOException {
            try {
                writer.write(entry);
            } catch (UncheckedOutputException e) {
                throw new IOException("failed to write Parquet part " + path, e.getCause());
            }
        }

        public boolean periodicSyncEnabled() {
            return periodicSync.enabled();
        }

        public long maybeSyncData() throws IOException {
            if (!periodicSync.enabled()) {
                return 0L;
            }
            return periodicSync.maybeSync(output.physicalBytes(), syncableOutput::syncData);
        }

        public OptionalLong periodicSyncResidualBytes() {
            return periodicSync.enabled()
                    ? OptionalLong.of(periodicSync.residualBytes(output.bytes()))
                    : OptionalLong.empty();
        }

        public long bytes() {
            return output.bytes();
        }

        public String md5() {
            return output.md5();
        }

        public long digestNanos() {
            return output.digestNanos();
        }

        /** Footer close followed by the mandatory file-and-parent durability barrier. */
        public void closeWithDurability() throws IOException {
            try {
                periodicSync.requirePublishable();
            } catch (IOException failure) {
                try {
                    writer.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
            ListEntryParquetWriters.closeWithDurability(path, writer);
            output.markDurable();
        }

        /** Releases the writer without making the file durable or publishable. */
        public void discard() {
            try {
                writer.close();
            } catch (IOException ignored) {
                // The owner deletes this unfinalized file immediately afterwards.
            }
        }
    }

    @FunctionalInterface
    public interface DataForcer {
        void force(FileChannel channel) throws IOException;
    }

    /** Finalizes the footer, then fsyncs the file and its parent directory (I6). */
    public static void closeWithDurability(Path path, ParquetWriter<ListEntry> writer)
            throws IOException {
        writer.close();
        DurableFiles.fileAndParent(path);
    }

    private static final class Builder extends ParquetWriter.Builder<ListEntry, Builder> {
        private final WriteSupport<ListEntry> writeSupport;

        Builder(OutputFile file, WriteSupport<ListEntry> writeSupport) {
            super(file);
            this.writeSupport = writeSupport;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return writeSupport;
        }

        @Override
        protected WriteSupport<ListEntry> getWriteSupport(ParquetConfiguration conf) {
            return writeSupport;
        }
    }
}
