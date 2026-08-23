/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.output.dataset.PartDigest;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/** Sequential {@link OutputFile} decorator that digests the exact bytes parquet-mr emits. */
public final class DigestingOutputFile implements OutputFile {
    private final OutputFile delegate;
    private final PartDigest digest = new PartDigest();
    private boolean opened;

    public DigestingOutputFile(OutputFile delegate) {
        this.delegate = delegate;
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

    /** Called only after the parquet footer and the file/parent fsync barrier have succeeded. */
    public void markDurable() {
        digest.markDurable();
    }

    public long bytes() {
        return digest.bytes();
    }

    public long digestNanos() {
        return digest.digestNanos();
    }

    public String md5() {
        return digest.md5();
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
            public void write(int value) throws IOException {
                out.write(value);
                digest.update(value);
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                out.write(bytes);
                digest.update(bytes, 0, bytes.length);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                out.write(bytes, offset, length);
                digest.update(bytes, offset, length);
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
                digest.streamClosed();
            }
        };
    }
}
