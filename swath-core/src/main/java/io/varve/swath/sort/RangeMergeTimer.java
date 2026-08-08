/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Per-range wall-time hook for the parallel range-merge path,
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

    /**
     * Record the boundary-sampling prologue's wall time in nanoseconds — the parallel path's own
     * SERIAL fraction, run once before any range starts.
     *
     * <p>Reported separately because it is the term that does not parallelise: on page-run staging it
     * walks every page's frontier across every segment, so it grows with staging size while the ranges
     * beside it get faster as {@code R} rises. Folded into {@code merge_ms} it is invisible, and an
     * A/B reading the run report cannot tell a merge that stopped scaling from one whose prologue grew
     * to dominate it — which is exactly the question tuning {@code R} turns on.
     *
     * <p>{@code default} so {@link #NO_OP} and every test recorder stay one-line lambdas (this
     * interface keeps its single abstract method, and with it {@code @FunctionalInterface}); only the
     * production wiring overrides it.
     */
    default void recordBoundarySampling(long nanos) { }
}
