/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** The explicit entry path that owns a sorted-output measurement or diagnostic. */
public enum SortArm {
    /** No sorted-output path was selected for this listing run. */
    NONE,
    /** A normal {@code swath list --sort} that performed its own listing. */
    LIVE_LIST_SORT,
    /** A checkpoint-authorized {@code --sort --resume} process that only re-ran the merge. */
    MERGE_ONLY_PAGE_RUN,
    /** A checkpoint-authorized resume that found this run already published and did no work. */
    PUBLISHED_REENTRY,
    /** The replay {@code sort-fixture} diagnostic, never a live-listing measurement. */
    SORT_FIXTURE,
    /** The page-run merge benchmark diagnostic, never a live-listing measurement. */
    MERGE_BENCH_PAGE_RUN
}
