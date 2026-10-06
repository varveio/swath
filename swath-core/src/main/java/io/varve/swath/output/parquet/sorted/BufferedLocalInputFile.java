/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A local {@link InputFile} whose stream serves small reads from a positional read buffer.
 *
 * <p>parquet-java parses page indexes with Thrift straight off the stream, one {@code read()} per
 * varint byte and field header. {@link org.apache.parquet.io.LocalInputFile} is unbuffered, so each of
 * those is a syscall: thousands per row group on first touch. Only reads smaller than
 * {@code DIRECT_READ_BYTES} use the buffer; page data reads go straight to the channel.
 */
final class BufferedLocalInputFile implements InputFile {

    private static final int BUFFER_BYTES = 16 * 1024;

    private static final int DIRECT_READ_BYTES = 1024;

    private final Path file;

    BufferedLocalInputFile(Path file) {
        this.file = file;
    }

    @Override
    public long getLength() throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return channel.size();
        }
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new Stream(FileChannel.open(file, StandardOpenOption.READ));
    }

    private static final class Stream extends SeekableInputStream {

        private final FileChannel channel;
        private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
        private long bufferStart;
        private long position;

        Stream(FileChannel channel) {
            this.channel = channel;
            buffer.limit(0);
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPosition) {
            position = newPosition;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            if (!buffered() && !fill()) {
                return -1;
            }
            int value = buffer.get((int) (position - bufferStart)) & 0xFF;
            position++;
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            return read(ByteBuffer.wrap(bytes, offset, length));
        }

        @Override
        public int read(ByteBuffer target) throws IOException {
            ensureOpen();
            int wanted = target.remaining();
            if (wanted == 0) {
                return 0;
            }
            if (!buffered() && wanted >= DIRECT_READ_BYTES) {
                int n = channel.read(target, position);
                if (n > 0) {
                    position += n;
                }
                return n;
            }
            if (!buffered() && !fill()) {
                return -1;
            }
            int offset = (int) (position - bufferStart);
            int n = Math.min(wanted, buffer.limit() - offset);
            target.put(buffer.slice(offset, n));
            position += n;
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(ByteBuffer.wrap(bytes));
        }

        @Override
        public void readFully(byte[] bytes, int offset, int length) throws IOException {
            readFully(ByteBuffer.wrap(bytes, offset, length));
        }

        @Override
        public void readFully(ByteBuffer target) throws IOException {
            while (target.hasRemaining()) {
                if (read(target) < 0) {
                    throw new EOFException("reached end of file with " + target.remaining() + " bytes left");
                }
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        /** A closed stream fails every read, including one its buffer could still answer. */
        private void ensureOpen() throws IOException {
            if (!channel.isOpen()) {
                throw new ClosedChannelException();
            }
        }

        private boolean buffered() {
            return position >= bufferStart && position < bufferStart + buffer.limit();
        }

        private boolean fill() throws IOException {
            buffer.clear();
            bufferStart = position;
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer, bufferStart + buffer.position());
                if (n < 0) {
                    break;
                }
            }
            buffer.flip();
            return buffer.hasRemaining();
        }
    }
}
