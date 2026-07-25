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
 * The categories this library emits are all {@code outcome = "SORT"}: {@code segment_flushed},
 * {@code buffer_byte_gated}, {@code merge_fastpath}, {@code buffer_sort_fallback},
 * {@code merge_pass_cascaded}. The pipeline adds the §5a drift-table rows and the first-class
 * Micrometer meters; this library only emits through the hook.
 */
@FunctionalInterface
public interface SortMetrics {

    /** Null object: records nothing. */
    SortMetrics NO_OP = (outcome, reason) -> { };

    /** Record one engagement-counter increment, exactly as {@code RunMetrics.recordStealReason}. */
    void recordStealReason(String outcome, String reason);
}
