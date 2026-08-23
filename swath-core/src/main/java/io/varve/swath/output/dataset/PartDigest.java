/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Incremental MD5 of a physical dataset part, exposed only after its writer has made that part
 * durable. Format adapters feed this with the bytes that reach the file: parquet feeds its
 * {@code OutputFile}; compressed text feeds the stream below its compressor.
 */
public final class PartDigest {
    private final MessageDigest digest;
    private long bytes;
    private long digestNanos;
    private boolean streamClosed;
    private boolean durable;
    private String md5;

    public PartDigest() {
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM has no MD5 provider", e);
        }
    }

    public synchronized void update(byte[] bytes, int offset, int length) {
        requireOpenStream();
        long startedAt = System.nanoTime();
        digest.update(bytes, offset, length);
        digestNanos += System.nanoTime() - startedAt;
        this.bytes += length;
    }

    public synchronized void update(int value) {
        requireOpenStream();
        long startedAt = System.nanoTime();
        digest.update((byte) value);
        digestNanos += System.nanoTime() - startedAt;
        bytes++;
    }

    /** Marks that the producing stream closed successfully; this alone does not expose metadata. */
    public synchronized void streamClosed() {
        streamClosed = true;
    }

    /** Makes metadata available only after close and the writer's durability barrier both succeeded. */
    public synchronized void markDurable() {
        if (!streamClosed) {
            throw new IllegalStateException("part digest marked durable before its output stream closed");
        }
        durable = true;
    }

    public synchronized long bytes() {
        requireDurable();
        return bytes;
    }

    /** Digest CPU time accrued on the physical-write path, not a post-close reread. */
    public synchronized long digestNanos() {
        requireDurable();
        return digestNanos;
    }

    public synchronized String md5() {
        requireDurable();
        if (md5 == null) {
            long startedAt = System.nanoTime();
            md5 = HexFormat.of().formatHex(digest.digest());
            digestNanos += System.nanoTime() - startedAt;
        }
        return md5;
    }

    private void requireDurable() {
        if (!durable) {
            throw new IllegalStateException("part digest requested before durable close");
        }
    }

    private void requireOpenStream() {
        if (streamClosed) {
            throw new IllegalStateException("part digest updated after its output stream closed");
        }
    }
}
