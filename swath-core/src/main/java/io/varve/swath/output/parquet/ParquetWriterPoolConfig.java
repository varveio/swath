/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.observability.RunMetrics;
import java.util.List;

/**
 * The optional wiring/knob clump for a {@link ParquetWriterPool}: everything beyond the pool's
 * required resources ({@code dir}, {@code schema}, {@code argsHash}, {@code numWriters},
 * {@code targetBytes}, {@code queueCapacity}). Prod wires the whole clump once (via the canonical
 * constructor); tests derive a single-knob variant from {@link #DEFAULT} with the {@code withX}
 * methods. The pure test-seam monotonic clock is <em>not</em> here — it stays a package-private
 * {@link ParquetWriterPool} constructor overload, so injecting a fake clock never widens this public
 * surface.
 *
 * @param bucket threaded into the consumer {@code manifest.json}'s {@code sourceBucket} (via
 *               {@code Manifest#write}); {@code null} is normalized to {@code ""}
 * @param partListener notified on each finalize (the I6 durable-commit point); {@link
 *                     PartListener#NONE} when not checkpointed
 * @param existingParts finalized parts carried over from a prior run (resume) so the end-of-run
 *                      manifest stays complete; never re-emitted, never rewritten
 * @param rotationIntervalNanos rotate a lane's open (non-empty) part once it has been open this
 *                              long, even below {@code targetBytes} (resume-RPO cadence); {@code 0}
 *                              disables the time trigger
 * @param rotationMaxRows rotate once a lane's open part has this many rows, even below {@code
 *                        targetBytes}; {@code 0} disables the row-count trigger
 * @param metrics rotation-trigger attribution, finalize/discard counters, footer-fsync latency;
 *                {@code null} (the default) attaches no metrics
 */
public record ParquetWriterPoolConfig(
        String bucket,
        PartListener partListener,
        List<PartInfo> existingParts,
        long rotationIntervalNanos,
        long rotationMaxRows,
        RunMetrics metrics) {

    /**
     * The canonical default: no bucket, no listener, no carried-over parts, both cadence triggers
     * disabled ({@code 0}), and no metrics — the minimal-pool wiring. Callers derive a variant with
     * the {@code withX} methods (a single differing knob is {@code DEFAULT.withRotationMaxRows(5)}).
     */
    public static final ParquetWriterPoolConfig DEFAULT =
            new ParquetWriterPoolConfig("", PartListener.NONE, List.of(), 0L, 0L, null);

    public ParquetWriterPoolConfig withBucket(String bucket) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }

    public ParquetWriterPoolConfig withPartListener(PartListener partListener) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }

    public ParquetWriterPoolConfig withExistingParts(List<PartInfo> existingParts) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }

    public ParquetWriterPoolConfig withRotationIntervalNanos(long rotationIntervalNanos) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }

    public ParquetWriterPoolConfig withRotationMaxRows(long rotationMaxRows) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }

    public ParquetWriterPoolConfig withMetrics(RunMetrics metrics) {
        return new ParquetWriterPoolConfig(bucket, partListener, existingParts,
                rotationIntervalNanos, rotationMaxRows, metrics);
    }
}
