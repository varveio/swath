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

    };

    /** Record one engagement-counter increment, exactly as {@code RunMetrics.recordStealReason}. */
    void recordStealReason(String outcome, String reason);

    /**
     * Advance the liveness progress signal, exactly as {@code RunMetrics.markProgress()}.
     *
     * <p><b>Call this from any loop that does real work without emitting a row.</b>
     */
    void markProgress();

    /** Count pages routed to an encoder without materializing rows on the router thread. */
    default void recordPipelinePagesForwarded(long pages) {
    }

    /** Count pages and rows processed by the router's shared overlap-cluster row heap. */
    default void recordPipelineCluster(long pages, long rows) {
    }

    /** Record total time the router spent blocked on reader input or encoder back-pressure. */
    default void recordPipelineRouterWait(long nanos) {
    }

    /** Record one header cursor's positional metadata-read service time. */
    default void recordPipelineHeaderScan(long nanos) {
    }

    /** Record time the router spent waiting for a full plan lane. */
    default void recordPipelinePlanQueueWait(long nanos) {
    }

    /** Count positional page reads performed by encoders. */
    default void recordPipelineEncoderPageReads(long pages) {
    }

    /** Record encoder positional read and CRC service time. */
    default void recordPipelineEncoderReadWait(long nanos) {
    }

    /** Observe one encoder cluster's exact retained decoded-page high-water mark. */
    default void recordPipelineDecodedPagePeak(long bytes) {
    }

    /** Bind the pipeline's owning counter directly to the live open-part gauge. */
    default void bindPipelinePartsOpen(AtomicInteger partsOpen) {
    }
}
