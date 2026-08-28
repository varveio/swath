/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

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
}
