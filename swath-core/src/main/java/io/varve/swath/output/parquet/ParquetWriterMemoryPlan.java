/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.output.dataset.SharedDatasetWriterPool;

/**
 * Conservative admission plan for Parquet writer concurrency above the measured release envelope.
 *
 * <p>The first four lanes retain the compatibility contract established by PERF-2. Counts above
 * four are expert configuration: they are admitted only when {@code -Xmx} can cover a deliberately
 * conservative planning allowance for every row-group buffer plus a fixed JVM/pipeline reserve.
 * This is an admission guard, not a claim about actual peak heap; parquet-mr, compression, key
 * shape, and the JVM still require measurement on the target workload.
 */
public final class ParquetWriterMemoryPlan {
    public static final int RELEASE_ENVELOPE_MAX_WRITERS = 4;
    public static final int ABSOLUTE_MAX_WRITERS = SharedDatasetWriterPool.MAX_WRITERS;

    /** JVM, queue, model, and orchestration headroom retained before admitting extra lanes. */
    public static final long BASE_HEAP_RESERVE_BYTES = 256L * 1024 * 1024;

    /** parquet-mr can materially exceed the nominal uncompressed row-group size. */
    public static final int ROW_GROUP_ALLOWANCE_MULTIPLIER = 4;

    private ParquetWriterMemoryPlan() {
    }

    /** Planning allowance reported to operators; it is intentionally not an observed peak. */
    public static long plannedHeapBytes(int writers) {
        requireWriterRange(writers);
        return BASE_HEAP_RESERVE_BYTES
                + (long) writers * PartWriter.ROW_GROUP_BYTES * ROW_GROUP_ALLOWANCE_MULTIPLIER;
    }

    /** Largest expert count this heap admits, always preserving the established 2-4 envelope. */
    public static int maxWritersForHeap(long maxHeapBytes) {
        if (maxHeapBytes <= BASE_HEAP_RESERVE_BYTES) {
            return RELEASE_ENVELOPE_MAX_WRITERS;
        }
        long perWriter = PartWriter.ROW_GROUP_BYTES * ROW_GROUP_ALLOWANCE_MULTIPLIER;
        long heapLimited = (maxHeapBytes - BASE_HEAP_RESERVE_BYTES) / perWriter;
        return (int) Math.min(ABSOLUTE_MAX_WRITERS,
                Math.max(RELEASE_ENVELOPE_MAX_WRITERS, heapLimited));
    }

    private static void requireWriterRange(int writers) {
        if (writers < 1 || writers > ABSOLUTE_MAX_WRITERS) {
            throw new IllegalArgumentException(
                    "writers must be 1.." + ABSOLUTE_MAX_WRITERS + " (got " + writers + ")");
        }
    }
}
