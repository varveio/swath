/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Engagement-counter hook for the sort library (metrics discipline,
 * {@code docs/internals/metrics-internals.md} §5). Mirrors the signature of
 * {@code RunMetrics.recordStealReason(outcome, reason)} so the pipeline can wire the
 * live {@code RunMetrics} in with a method reference and this package never depends on Micrometer.
 *
 * <p>Null-safe by construction: the {@link #NO_OP} null object is the default whenever no recorder
 * is injected (nearly every unit test), so the hot paths never branch on {@code null} (§1 idiom).
 * This library's engagement categories all use {@code outcome = "SORT"}; the authoritative reason
 * registry is the §5a drift table rather than an independently maintained list here. The pipeline
 * adds first-class Micrometer meters; this library only emits through the hook.
 */
public interface SortMetrics {

    /** Null object: records nothing. */
    SortMetrics NO_OP = new SortMetrics() {
        @Override
        public void recordStealReason(String outcome, String reason) {
        }

        @Override
        public void markProgress() {
        }

        @Override
        public void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        }

        @Override
        public void recordPageAwareOverlapCluster() {
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        }

        @Override
        public void recordRangeIndexBytes(long bytes) {
        }

        @Override
        public void recordRangeFramedBytes(long bytes) {
        }

        @Override
        public void recordProofSpool(long logicalExtentBytes,
                                     long preallocationOperations,
                                     long preallocationAttemptedBytes,
                                     long mappedOperations,
                                     long mappedBytes,
                                     long serviceNanos) {
        }

    };

    /** Record one engagement-counter increment, exactly as {@code RunMetrics.recordStealReason}. */
    void recordStealReason(String outcome, String reason);

    /**
     * Advance the liveness progress signal, exactly as {@code RunMetrics.markProgress()}.
     *
     * <p><b>Call this from any loop that does real work without emitting a row.</b> The parallel
     * range merge can have two such loops — a legacy/fallback boundary scan walks every page of its
     * segment, and each range's frontier walks its own prefix — and both were silent. The liveness
     * watchdog's total-freeze tripwire defaults to 120 s, so on a billion-object listing these phases
     * halted a perfectly healthy JVM before the merge wrote its first row. The row-emitting path was
     * never the problem; it ticks through a writer decorator.
     *
     */
    void markProgress();

    /**
     * Record page-run boundary-selection IO: extension bytes actually read and framed record bytes
     * traversed by fallback scans. Parquet index metadata is deliberately outside these byte totals.
     */
    void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes);

    /** Record one page-aware overlap cluster engagement. */
    void recordPageAwareOverlapCluster();

    /** Observe one active overlap-cluster size for peak gauges. */
    void recordPageAwareOverlapState(long activePages, long retainedRows);

    /** Record page-index metadata bytes read by seek planning and exact worker proof reads. */
    void recordRangeIndexBytes(long bytes);

    /** Record logical page-frame bytes read by parallel range workers, including cascades. */
    void recordRangeFramedBytes(long bytes);

    /**
     * Record one aggregate delta of bounded proof-spool work.
     *
     * @param logicalExtentBytes fixed-slot address space requested, not bytes transferred
     * @param preallocationOperations physical write/force attempts made while materializing backing
     *        space
     * @param preallocationAttemptedBytes bytes submitted to physical preallocation writes, including
     *        a failed attempt
     * @param mappedOperations exact mapped field/key reads and updates
     * @param mappedBytes exact bytes covered by those mapped reads and updates
     * @param serviceNanos sum of operation service times; concurrent worker times intentionally add
     */
    void recordProofSpool(long logicalExtentBytes,
                          long preallocationOperations,
                          long preallocationAttemptedBytes,
                          long mappedOperations,
                          long mappedBytes,
                          long serviceNanos);

    /** Count pages routed to an encoder without materializing rows on the router thread. */
    default void recordPipelinePagesForwarded(long pages) {
    }

    /** Count pages and rows processed by the router's shared overlap-cluster row heap. */
    default void recordPipelineCluster(long pages, long rows) {
    }

    /** Record total time the router spent blocked on reader input or encoder back-pressure. */
    default void recordPipelineRouterWait(long nanos) {
    }

    /** Record time the router spent waiting for a reader slot's next decoded page. */
    default void recordPipelineReaderWait(long nanos) {
    }

    /** Record a full encoder queue's blocking duration. */
    default void recordPipelineEncoderQueueFull(long nanos) {
    }

    /** Bind the pipeline's owning counter directly to the live open-part gauge. */
    default void bindPipelinePartsOpen(AtomicInteger partsOpen) {
    }
}
