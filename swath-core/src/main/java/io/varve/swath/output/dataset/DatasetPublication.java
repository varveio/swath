/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.error.OutputException;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.PartInfo;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Single owner of an unsorted dataset's published part set and root artifacts.
 *
 * <p>Lane threads close and fsync their own parts, compute immutable metadata, and commit the
 * checkpoint before calling {@link #publishFinalizedPart}. This object then serializes the
 * consumer-visible transition: add the part to its monotone in-memory set and atomically replace
 * {@code manifest.json}. It deliberately owns no thread or queue; adding either would create a
 * second failure and shutdown protocol merely to serialize work that already occurs only at part
 * boundaries.
 *
 * <p>{@link #publishSuccess} is the terminal transition. It rewrites the full manifest, writes the
 * state and symlink artifacts, and writes {@code _SUCCESS} last. Once that transition succeeds or
 * fails, no later part can be published.
 */
final class DatasetPublication {
    private final Path dir;
    private final String bucket;
    private final DatasetFormat format;
    private final String argsHash;
    private final LongSupplier nanoClock;
    private final List<PartInfo> committedParts;
    private final AtomicLong committedPartCount;
    private final AtomicLong committedBytes;
    private final AtomicLong manifestWriteCount = new AtomicLong();
    private final AtomicLong manifestWriteNanos = new AtomicLong();

    private boolean successPublished;
    private Throwable terminalFailure;

    DatasetPublication(Path dir, String bucket, DatasetFormat format, String argsHash,
            List<PartInfo> existingParts, LongSupplier nanoClock) {
        this.dir = dir;
        this.bucket = bucket;
        this.format = format;
        this.argsHash = argsHash;
        this.nanoClock = nanoClock;
        this.committedParts = new ArrayList<>(existingParts);
        this.committedPartCount = new AtomicLong(existingParts.size());
        this.committedBytes = new AtomicLong(
                existingParts.stream().mapToLong(PartInfo::bytes).sum());
    }

    /**
     * Publish one already-durable, already-checkpointed part.
     *
     * <p>The part stays in the owned set if the manifest replacement fails. Its checkpoint record
     * is already authoritative, so rolling it back here would make a later full publication omit a
     * committed part. Another lane's later replacement may therefore make this part visible even
     * though this call failed; the pool still records and surfaces the original failure.
     */
    void publishFinalizedPart(PartInfo part) throws IOException {
        long startedAt = nanoClock.getAsLong();
        boolean attemptedWrite = false;
        try {
            synchronized (this) {
                ensureAcceptingParts();
                committedParts.add(part);
                committedPartCount.incrementAndGet();
                committedBytes.addAndGet(part.bytes());
                attemptedWrite = true;
                writeManifestLocked();
            }
        } finally {
            if (attemptedWrite) {
                recordManifestWrite(startedAt);
            }
        }
    }

    /** Publish the whole-snapshot artifacts exactly once; concurrent callers serialize here. */
    synchronized void publishSuccess() throws OutputException {
        if (successPublished) {
            return;
        }
        if (terminalFailure != null) {
            throw publicationFailure(terminalFailure);
        }
        try {
            long startedAt = nanoClock.getAsLong();
            try {
                writeManifestLocked();
            } finally {
                recordManifestWrite(startedAt);
            }
            List<PartInfo> snapshot = List.copyOf(committedParts);
            Manifest.writeState(dir, argsHash, null);
            Manifest.writeSymlink(dir, snapshot);
            Manifest.writeSuccess(dir);   // LAST — the whole-snapshot completion marker
            successPublished = true;
        } catch (Throwable failure) {
            terminalFailure = failure;
            throw publicationFailure(failure);
        }
    }

    long committedPartCount() {
        return committedPartCount.get();
    }

    long committedBytes() {
        return committedBytes.get();
    }

    long manifestWriteCount() {
        return manifestWriteCount.get();
    }

    long manifestWriteNanos() {
        return manifestWriteNanos.get();
    }

    private void ensureAcceptingParts() {
        if (successPublished) {
            throw new IllegalStateException("cannot publish a dataset part after _SUCCESS");
        }
        if (terminalFailure != null) {
            throw new IllegalStateException("cannot publish a dataset part after publication failed",
                    terminalFailure);
        }
    }

    /** Caller holds this object's monitor, making every emitted manifest a monotone snapshot. */
    private void writeManifestLocked() throws IOException {
        Manifest.write(dir, bucket, format.manifestFormat(), format.manifestSchema(),
                committedParts, false, null);
    }

    private void recordManifestWrite(long startedAt) {
        manifestWriteCount.incrementAndGet();
        manifestWriteNanos.addAndGet(Math.max(0L, nanoClock.getAsLong() - startedAt));
    }

    private static OutputException publicationFailure(Throwable failure) {
        return new OutputException("failed to write manifest", failure);
    }
}
