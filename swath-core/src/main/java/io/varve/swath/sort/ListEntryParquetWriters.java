/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * Shared {@link ParquetWriter} scaffolding for {@link SegmentParquetSink} and
 * {@link SortedParquetWriter} — extracted so the parquet property construction and the
 * close/fsync durability discipline (I6) exist in exactly ONE place: splitting it back into two
 * copies risks a future durability fix landing in one copy and not the other. The two writers
 * still differ in what actually matters — their {@link WriteSupport} (plain vs. sortedness-stamped)
 * and their page/row-group geometry — all of which are supplied by the caller.
 */
final class ListEntryParquetWriters {

    static final int PAGE_BYTES = 1024 * 1024;
    static final int ZSTD_LEVEL = 3;
    static final int SERVED_DICTIONARY_BYTES = 8 * 1024;
    static final int SEGMENT_PAGE_ROWS = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT;

    record PageLayout(int pageRows, int dictionaryBytes) {
        static PageLayout staging() {
            return new PageLayout(SEGMENT_PAGE_ROWS, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
        }

        static PageLayout served(int pageRows) {
            return new PageLayout(pageRows, SERVED_DICTIONARY_BYTES);
        }
    }

    private ListEntryParquetWriters() {
    }

    /** Builds a writer with caller-supplied write support and page/row-group geometry. */
    static ParquetWriter<ListEntry> build(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes,
                                          PageLayout layout)
            throws IOException {
        return build(writeSupport, rowGroupBytes, layout, new LocalOutputFile(path));
    }

    /** As {@link #build(Path, WriteSupport, long, PageLayout)}, with emitted-byte tracking. */
    static TrackedWriter buildTracked(Path path, WriteSupport<ListEntry> writeSupport, long rowGroupBytes,
                                      PageLayout layout)
            throws IOException {
        DigestingOutputFile output = new DigestingOutputFile(new LocalOutputFile(path));
        return new TrackedWriter(build(writeSupport, rowGroupBytes, layout, output), output);
    }

    private static ParquetWriter<ListEntry> build(WriteSupport<ListEntry> writeSupport,
                                                   long rowGroupBytes, PageLayout layout, OutputFile output)
            throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", ZSTD_LEVEL);
        return new Builder(output, writeSupport)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(rowGroupBytes)
                .withPageSize(PAGE_BYTES)
                .withPageRowCountLimit(layout.pageRows())
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(layout.dictionaryBytes())
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    record TrackedWriter(ParquetWriter<ListEntry> writer, DigestingOutputFile output) {
    }

    /** Sequential {@link OutputFile} decorator that digests the exact bytes parquet-mr emits. */
    static final class DigestingOutputFile implements OutputFile {
        private final OutputFile delegate;
        private final MessageDigest digest;
        private long bytes;
        private long digestNanos;
        private boolean opened;
        private boolean finished;
        private String md5;

        DigestingOutputFile(OutputFile delegate) {
            this.delegate = delegate;
            try {
                this.digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("JVM has no MD5 provider", e);
            }
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return tracking(delegate.create(blockSizeHint));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return tracking(delegate.createOrOverwrite(blockSizeHint));
        }

        @Override
        public boolean supportsBlockSize() {
            return delegate.supportsBlockSize();
        }

        @Override
        public long defaultBlockSize() {
            return delegate.defaultBlockSize();
        }

        synchronized long bytes() {
            requireFinished();
            return bytes;
        }

        synchronized long digestNanos() {
            requireFinished();
            return digestNanos;
        }

        synchronized String md5() {
            requireFinished();
            if (md5 == null) {
                long start = System.nanoTime();
                md5 = HexFormat.of().formatHex(digest.digest());
                digestNanos += System.nanoTime() - start;
            }
            return md5;
        }

        private synchronized PositionOutputStream tracking(PositionOutputStream out) {
            if (opened) {
                throw new IllegalStateException("Parquet output stream opened more than once");
            }
            opened = true;
            return new PositionOutputStream() {
                @Override
                public long getPos() throws IOException {
                    return out.getPos();
                }

                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                    update(b);
                }

                @Override
                public void write(byte[] b) throws IOException {
                    out.write(b);
                    update(b, 0, b.length);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    update(b, off, len);
                }

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public void close() throws IOException {
                    out.close();
                    synchronized (DigestingOutputFile.this) {
                        finished = true;
                    }
                }
            };
        }

        private synchronized void update(byte[] b, int off, int len) {
            long start = System.nanoTime();
            digest.update(b, off, len);
            digestNanos += System.nanoTime() - start;
            bytes += len;
        }

        private synchronized void update(int b) {
            long start = System.nanoTime();
            digest.update((byte) b);
            digestNanos += System.nanoTime() - start;
            bytes++;
        }

        private void requireFinished() {
            if (!finished) {
                throw new IllegalStateException("final part metadata requested before durable close");
            }
        }
    }

    /**
     * Finalizes the footer, then fsyncs the file and its parent directory (I6): a file is durable
     * <b>iff</b> this has returned. The one durability sequence shared by both writers.
     */
    static void closeWithDurability(Path path, ParquetWriter<ListEntry> writer) throws IOException {
        writer.close();
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Durability.directory(path.getParent());
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
        @SuppressWarnings("deprecation")   // getWriteSupport(Configuration) is the abstract method in parquet 1.15
        protected WriteSupport<ListEntry> getWriteSupport(Configuration conf) {
            return writeSupport;
        }
    }
}
