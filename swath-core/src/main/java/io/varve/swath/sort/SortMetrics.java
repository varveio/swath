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
@FunctionalInterface
public interface SortMetrics {

    /** Null object: records nothing. */
    SortMetrics NO_OP = (outcome, reason) -> { };

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
     * <p><b>The default is a no-op, and that is a trap worth naming.</b> This interface is a
     * {@code @FunctionalInterface} wired in most places as a lambda or a method reference, which
     * cannot supply a second method — so a caller that wires {@code metrics::recordStealReason} gets
     * a silently non-ticking implementation. The pipeline must wire something that overrides this;
     * {@code ListRunner} uses a named bridge with a test asserting both methods forward.
     */
    default void markProgress() {
        // no-op: the null object and every test lambda record nothing.
    }

    /**
     * Record page-run boundary-selection IO: extension bytes actually read and framed record bytes
     * traversed by fallback scans. Parquet index metadata is deliberately outside these byte totals.
     */
    default void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        // no-op: overridden by the live runtime bridge.
    }
}
