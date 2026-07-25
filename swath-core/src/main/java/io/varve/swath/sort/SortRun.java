/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;

/**
 * The immutable inputs defining one sort/merge run — the quintet threaded whole through
 * {@link SortTransform} and, from there, {@link ParallelRangeMerge}: the {@link SortConfig knobs},
 * the §0.3 key {@code comparator}, the {@link DuplicateHook dedup hook}, the {@link SortMetrics}
 * sink, and the {@link SortedFileWriterFactory} for the final output. Grouping them replaces the
 * former positional constructor list; behavioural knobs that a caller varies independently
 * ({@code identityVerifiedWideSweep}, the range-merge timer) stay explicit {@link SortTransform}
 * constructor arguments.
 *
 * @param config the sort knobs (segment/merge budgets, fan-in, roll size, codec)
 * @param comparator the §0.3 total order every merge pass runs under
 * @param hook the duplicate-key hook (drop/count/fail); {@link DuplicateHook#NO_OP} when unused
 * @param metrics the sort metrics sink; {@link SortMetrics#NO_OP} off the instrumented path
 * @param finalWriterFactory builds the final sorted-Parquet writers the roll opens
 */
public record SortRun(
        SortConfig config,
        Comparator<ListEntry> comparator,
        DuplicateHook hook,
        SortMetrics metrics,
        SortedFileWriterFactory finalWriterFactory) {
}
