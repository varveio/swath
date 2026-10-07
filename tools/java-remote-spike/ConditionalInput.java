/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Demand-only buffered input. The provider validates identity and range before returning bytes. */
final class ConditionalInput implements InputFile, AutoCloseable {
    interface Source extends AutoCloseable {
        long length();

        byte[] read(long position, int count) throws IOException;

        void close() throws IOException;
    }

    private final Source source;
    private volatile boolean closed;

    ConditionalInput(Source source) {
        this.source = Objects.requireNonNull(source);
    }

    public long getLength() {
        return source.length();
    }

    public SeekableInputStream newStream() throws IOException {
        checkOpen();
        return new SeekableInputStream() {
            long position;
            long start = -1;
            byte[] window = new byte[0];
            boolean streamClosed;

            void check() throws IOException {
                checkOpen();
                if (streamClosed) throw new IOException("stream closed");
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedIOException("read interrupted");
            }

            public long getPos() throws IOException {
                check();
                return position;
            }

            public void seek(long value) throws IOException {
                check();
                if (value < 0 || value > getLength()) throw new EOFException("seek outside object");
                position = value;
            }

            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
            }

            public int read(byte[] target, int offset, int count) throws IOException {
                Objects.checkFromIndexSize(offset, count, target.length);
                check();
                if (count == 0) return 0;
                if (position == getLength()) return -1;
                if (position < start || position >= start + window.length) {
                    int wanted = (int) Math.min(65536, getLength() - position);
                    byte[] fetched = source.read(position, wanted);
                    check();
                    if (fetched.length != wanted) throw new EOFException("short provider range");
                    window = fetched;
                    start = position;
                }
                int copied = Math.min(count, window.length - (int) (position - start));
                System.arraycopy(window, (int) (position - start), target, offset, copied);
                position += copied;
                return copied;
            }

            public void readFully(byte[] b) throws IOException {
                readFully(b, 0, b.length);
            }

            public void readFully(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                while (len > 0) {
                    int n = read(b, off, len);
                    if (n < 0) throw new EOFException();
                    off += n;
                    len -= n;
                }
            }

            public int read(ByteBuffer target) throws IOException {
                if (!target.hasRemaining()) return 0;
                byte[] bytes = new byte[Math.min(target.remaining(), 65536)];
                int n = read(bytes);
                if (n > 0) target.put(bytes, 0, n);
                return n;
            }

            public void readFully(ByteBuffer target) throws IOException {
                while (target.hasRemaining()) if (read(target) < 0) throw new EOFException();
            }

            public void close() {
                streamClosed = true;
                window = new byte[0];
            }
        };
    }

    private void checkOpen() throws IOException {
        if (closed) throw new IOException("input closed");
    }

    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        source.close();
    }
}
