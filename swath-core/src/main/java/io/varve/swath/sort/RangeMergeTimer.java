/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Per-range wall-time hook for the parallel range-merge path (off-by-default,
 * {@code swath.sort.merge-parallelism > 1}). Mirrors {@code RunMetrics.recordSortMergeRange(nanos)}
 * so the pipeline wires the live {@code RunMetrics} in with a method reference and this package never
 * depends on Micrometer — the same seam idiom as {@link SortMetrics}, kept separate so the
 * {@code @FunctionalInterface SortMetrics} single-method contract is untouched.
 *
 * <p>Null-safe by construction: the {@link #NO_OP} null object is the default whenever no recorder is
 * injected (the serial path, and nearly every unit test). {@code nanos} is the per-range merge wall
 * time measured in {@link ParallelRangeMerge}, so no Micrometer {@code Timer.Sample} crosses this seam.
 */
@FunctionalInterface
public interface RangeMergeTimer {

    /** Null object: records nothing. */
    RangeMergeTimer NO_OP = nanos -> { };

    /** Record one range's merge wall time in nanoseconds, exactly as {@code RunMetrics.recordSortMergeRange}. */
    void recordRangeMerge(long nanos);
}
