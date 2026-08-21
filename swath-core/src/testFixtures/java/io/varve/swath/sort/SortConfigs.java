/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Object-Mother fixtures for {@link SortConfig} in tests. {@link #base()} is the canonical
 * small-values test config every sort/runtime/replay test derives from; a case that varies one knob
 * writes {@code SortConfigs.base().withFanIn(2)} rather than re-listing all twelve components. The
 * named variants below are the handful of shared shapes that recur across several test files.
 *
 * <p>Built on top of {@link SortConfig#DEFAULT} and its {@code withX} derivations — the same idiom
 * production uses — so there is exactly one place each default lives.
 */
public final class SortConfigs {

    private SortConfigs() {
    }

    /**
     * The canonical test config: a 64&nbsp;MB segment gate, unbounded segment-entries and
     * merge-budget (so the bytes gate and raw fan-in govern), fan-in 512, single-file output, and the
     * shipped defaults for the remaining knobs (LZ4, 64&nbsp;KiB per-stream estimate).
     *
     * <p>Pins the merge SERIAL rather than inheriting {@link SortConfig#DEFAULT_MERGE_PARALLELISM},
     * which is now core-derived and greater than one. Most tests here are about something other than
     * parallelism and assert single-file output; inheriting the shipped default would silently turn
     * each of them into a test of the parallel path instead of the thing it names. A case that wants
     * the parallel merge asks for it — {@code base().withMergeParallelism(R)} — and gets it, because
     * the staged-bytes floor is dropped to zero here (test fixtures are far below the production
     * floor, which exists to stop a small run multiplying its file count for a few seconds' gain).
     */
    public static SortConfig base() {
        return SortConfig.DEFAULT
                .withSegmentBytes(64L << 20)
                .withFanIn(512)
                .withFinalFileBytes(Long.MAX_VALUE)
                .withMergeBudgetBytes(Long.MAX_VALUE)
                .withMergeParallelism(1)
                .withMinParallelStagedBytes(0L);
    }

    /**
     * {@link #base()} rolled into one file per entry (a 1-byte {@code final-file-bytes}) under a
     * bounded 64&nbsp;MB merge budget — exercises range-disjoint multi-file sorted output.
     */
    public static SortConfig rolledPerEntry() {
        return base().withFinalFileBytes(1L).withMergeBudgetBytes(64L << 20);
    }

    /**
     * {@link #base()} with data pages capped at {@code pageRows} rows — the knob a served fixture's
     * cold-read cost turns on. A test that wants several pages inside <em>one</em> row group asks for
     * this rather than shrinking the row group, because those are different granularities and only
     * this one is what a bounded range read pays for.
     */
    public static SortConfig pagesOf(int pageRows) {
        return base().withFinalPageRows(pageRows);
    }

    /**
     * {@link #base()} with tiny (1&nbsp;KiB) final row groups under a bounded 64&nbsp;MB merge budget,
     * so a few hundred keys form many row groups — exercises the multi-row-group serving path.
     */
    public static SortConfig manySmallRowGroups() {
        return base().withFinalRowGroupBytes(1024L).withMergeBudgetBytes(64L << 20);
    }
}
