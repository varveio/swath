/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.sort.SortMetrics;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void recordPipelinePagesForwarded(long pages) {
        metrics.recordSortPipelinePagesForwarded(pages);
    }

    @Override
    public void recordPipelineCluster(long pages, long rows) {
        metrics.recordSortPipelineCluster(pages, rows);
    }

    @Override
    public void recordPipelineRouterWait(long nanos) {
        metrics.recordSortPipelineRouterWait(nanos);
    }

    @Override
    public void recordPipelineHeaderScan(long nanos) {
        metrics.recordSortPipelineHeaderScan(nanos);
    }

    @Override
    public void recordPipelinePlanQueueWait(long nanos) {
        metrics.recordSortPipelinePlanQueueWait(nanos);
    }

    @Override
    public void recordPipelineEncoderPageReads(long pages) {
        metrics.recordSortPipelineEncoderPageReads(pages);
    }

    @Override
    public void recordPipelineEncoderReadWait(long nanos) {
        metrics.recordSortPipelineEncoderReadWait(nanos);
    }

    @Override
    public void recordPipelineDecodedPagePeak(long bytes) {
        metrics.recordSortPipelineDecodedPagePeak(bytes);
    }

    @Override
    public void bindPipelinePartsOpen(AtomicInteger partsOpen) {
        metrics.bindSortPipelinePartsOpen(partsOpen);
    }
}
