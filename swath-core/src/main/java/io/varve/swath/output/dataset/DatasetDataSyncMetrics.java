/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.observability.RunMetrics;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared format-tagged observations for {@link PeriodicDataSync} adapters. */
public final class DatasetDataSyncMetrics {
    public enum Classification {
        TEXT_UNCOMPRESSED,
        TEXT_COMPRESSED,
        PARQUET
    }

    private final RunMetrics metrics;
    private final String format;
    private final Classification classification;
    private final AtomicBoolean engaged = new AtomicBoolean();

    public DatasetDataSyncMetrics(RunMetrics metrics, String format, Classification classification) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.format = Objects.requireNonNull(format, "format");
        this.classification = Objects.requireNonNull(classification, "classification");
    }

    /** Record one actual sync and emit the generic plus classification engagement pair once. */
    public void recordSync(long elapsedNanos, long bytes) {
        metrics.recordDatasetDataSync(format, elapsedNanos, bytes);
        if (engaged.compareAndSet(false, true)) {
            metrics.recordStealReason("OUTPUT", "data_sync");
            switch (classification) {
                case TEXT_UNCOMPRESSED ->
                        metrics.recordStealReason("OUTPUT", "data_sync_text_uncompressed");
                case TEXT_COMPRESSED ->
                        metrics.recordStealReason("OUTPUT", "data_sync_text_compressed");
                case PARQUET -> metrics.recordStealReason("OUTPUT", "data_sync_parquet");
            }
        }
    }

    /** Record the physical tail left to the final close barrier. */
    public void recordResidual(long bytes) {
        metrics.recordDatasetDataSyncResidual(format, bytes);
    }
}
