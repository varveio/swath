/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * parquet-mr {@code LocalOutputFile} semantics with access to the same channel for data-only sync.
 * The 4-KiB transport buffer is below parquet-mr's row-group/page stores: syncing flushes only bytes
 * the writer already emitted and never changes Parquet geometry.
 */
final class SyncableLocalOutputFile implements OutputFile {
    private static final int BUFFER_SIZE = 4096;

    private final Path path;
    private final DataForcer dataForcer;
    private ChannelPositionOutputStream stream;

    SyncableLocalOutputFile(Path path) {
        this(path, channel -> channel.force(false));
    }

    SyncableLocalOutputFile(Path path, DataForcer dataForcer) {
        this.path = path;
        this.dataForcer = dataForcer;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return open(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return open(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override public boolean supportsBlockSize() { return false; }

    @Override public long defaultBlockSize() { return -1L; }

    /** Flush the bottom transport buffer and force the same channel as one poisonable operation. */
    void syncData() throws IOException {
        ChannelPositionOutputStream opened = stream;
        if (opened == null) {
            throw new IOException("Parquet output stream is not open");
        }
        opened.flushAndForce(dataForcer);
    }

    private synchronized PositionOutputStream open(StandardOpenOption... options) throws IOException {
        if (stream != null) {
            throw new IllegalStateException("Parquet output stream opened more than once");
        }
        FileChannel channel = FileChannel.open(path, options);
        boolean success = false;
        try {
            stream = new ChannelPositionOutputStream(channel);
            success = true;
            return stream;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    private static final class ChannelPositionOutputStream extends PositionOutputStream {
        private final FileChannel channel;
        private final OutputStream output;
        private long position;

        ChannelPositionOutputStream(FileChannel channel) {
            this.channel = channel;
            output = new BufferedOutputStream(Channels.newOutputStream(channel), BUFFER_SIZE);
        }

        @Override public long getPos() { return position; }

        @Override public void write(int value) throws IOException {
            position++;
            output.write(value);
        }

        @Override public void write(byte[] bytes) throws IOException {
            position += bytes.length;
            output.write(bytes);
        }

        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            position += length;
            output.write(bytes, offset, length);
        }

        @Override public void flush() throws IOException { output.flush(); }

        @Override public void close() throws IOException { output.close(); }

        void flushAndForce(DataForcer dataForcer) throws IOException {
            output.flush();
            dataForcer.force(channel);
        }
    }

    @FunctionalInterface
    interface DataForcer {
        void force(FileChannel channel) throws IOException;
    }
}
