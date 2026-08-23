/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** An output-stream decorator that records only bytes successfully accepted by its delegate. */
public final class DigestingOutputStream extends FilterOutputStream {
    private final PartDigest digest;

    public DigestingOutputStream(OutputStream delegate, PartDigest digest) {
        super(delegate);
        this.digest = digest;
    }

    @Override
    public void write(int value) throws IOException {
        out.write(value);
        digest.update(value);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        out.write(bytes, offset, length);
        digest.update(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        out.close();
        digest.streamClosed();
    }
}
