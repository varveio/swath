/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;
import java.util.function.IntSupplier;

/**
 * The immutable inputs defining one sort/merge run — the inputs threaded whole through
 * {@link SortTransform} and its package-private planner/worker/publisher owners: the {@link SortConfig knobs},
 * the §0.3 key {@code comparator}, the {@link DuplicateHook dedup hook}, the final-output
 * {@link EqualKeyPolicy}, the {@link SortMetrics} sink, the {@link SortedFileWriterFactory} for the
 * final output, merge input provenance, timing/fd seams, and the stale-final ownership scope.
 *
 * @param config the sort knobs (segment/merge budgets, fan-in, roll size, codec)
 * @param comparator the §0.3 total order every merge pass runs under
 * @param hook the duplicate-key hook (drop/count/fail); {@link DuplicateHook#NO_OP} when unused
 * @param equalKeyPolicy whether the final drain permits or rejects adjacent equal raw keys
 * @param metrics the sort metrics sink; {@link SortMetrics#NO_OP} off the instrumented path
 * @param finalWriterFactory builds the final sorted-Parquet writers the roll opens
 * @param inputProfile optimizations permitted by the provenance of the staged runs
 * @param rangeMergeTimer records parallel range and boundary-sampling wall time
 * @param softFdLimitSupplier process soft open-file limit source, or a deterministic test value
 * @param staleFinalSweep ownership scope for replacement-publish cleanup
 * @param mergeDiskPolicy merge-start filesystem admission and explicit CLI bypass policy
 * @param pipelinePartTarget internal pipeline part geometry selected by production or benchmark
 */
public record SortRun(
        SortConfig config,
        Comparator<ListEntry> comparator,
        DuplicateHook hook,
        EqualKeyPolicy equalKeyPolicy,
        SortMetrics metrics,
        SortedFileWriterFactory finalWriterFactory,
        MergeInputProfile inputProfile,
        RangeMergeTimer rangeMergeTimer,
        IntSupplier softFdLimitSupplier,
        StaleFinalSweep staleFinalSweep,
        MergeDiskPolicy mergeDiskPolicy,
        PipelinePartSizer.Target pipelinePartTarget) {

    public SortRun {
        PageRunFormat.requireCanonicalComparator(comparator);
        if (pipelinePartTarget == null) {
            throw new NullPointerException("pipelinePartTarget");
        }
    }

    /** Ordering mode persisted into every cascade segment produced by this run. */
    SortMode orderingMode() {
        return equalKeyPolicy == EqualKeyPolicy.REJECT ? SortMode.OBJECTS : SortMode.VERSIONS;
    }

    /** Compatibility constructor for library callers; merge-start disk admission remains enforced. */
    public SortRun(SortConfig config, Comparator<ListEntry> comparator, DuplicateHook hook,
            EqualKeyPolicy equalKeyPolicy, SortMetrics metrics,
            SortedFileWriterFactory finalWriterFactory, MergeInputProfile inputProfile,
            RangeMergeTimer rangeMergeTimer, IntSupplier softFdLimitSupplier,
            StaleFinalSweep staleFinalSweep) {
        this(config, comparator, hook, equalKeyPolicy, metrics, finalWriterFactory, inputProfile,
                rangeMergeTimer, softFdLimitSupplier, staleFinalSweep, MergeDiskPolicy.enforced(),
                PipelinePartSizer.Target.calibrated());
    }

    /** Compatibility constructor with explicit disk admission and production part geometry. */
    public SortRun(SortConfig config, Comparator<ListEntry> comparator, DuplicateHook hook,
            EqualKeyPolicy equalKeyPolicy, SortMetrics metrics,
            SortedFileWriterFactory finalWriterFactory, MergeInputProfile inputProfile,
            RangeMergeTimer rangeMergeTimer, IntSupplier softFdLimitSupplier,
            StaleFinalSweep staleFinalSweep, MergeDiskPolicy mergeDiskPolicy) {
        this(config, comparator, hook, equalKeyPolicy, metrics, finalWriterFactory, inputProfile,
                rangeMergeTimer, softFdLimitSupplier, staleFinalSweep, mergeDiskPolicy,
                PipelinePartSizer.Target.calibrated());
    }

    /** Benchmark-only copy with a different immutable pipeline part target. */
    SortRun withPipelinePartTarget(PipelinePartSizer.Target target) {
        return new SortRun(config, comparator, hook, equalKeyPolicy, metrics, finalWriterFactory,
                inputProfile, rangeMergeTimer, softFdLimitSupplier, staleFinalSweep,
                mergeDiskPolicy, target);
    }

    /** Production soft-fd-limit source; tests pass a fixed supplier when they exercise clamps. */
    public static final IntSupplier PROCESS_SOFT_FD_LIMIT = MergeFdBudget::softOpenFileLimit;
}
