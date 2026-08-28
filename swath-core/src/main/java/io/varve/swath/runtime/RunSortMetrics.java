/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.sort.SortMetrics;

/**
 * Wires the sort library's {@link SortMetrics} hook to the live {@link RunMetrics}.
 *
 * <p>The explicit adapter keeps the sort package independent of Micrometer while ensuring every
 * {@link SortMetrics} hook reaches the live run metrics.
 */
record RunSortMetrics(RunMetrics metrics) implements SortMetrics {

    @Override
    public void recordStealReason(String outcome, String reason) {
        metrics.recordStealReason(outcome, reason);
    }

    @Override
    public void markProgress() {
        metrics.markProgress();
    }

    @Override
    public void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        metrics.recordSortMergeBoundaryIo(embeddedEntries, embeddedBytes, scanBytes);
    }

    @Override
    public void recordPageAwareOverlapCluster() {
        metrics.recordSortMergeOverlapCluster();
    }

    @Override
    public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        metrics.recordSortMergeOverlapState(activePages, retainedRows);
    }

    @Override
    public void recordRangeIndexBytes(long bytes) {
        metrics.recordSortMergeRangeIndexBytes(bytes);
    }

    @Override
    public void recordProofSpool(long operations, long bytes, long nanos) {
        metrics.recordSortMergeProofSpool(operations, bytes, nanos);
    }
}
