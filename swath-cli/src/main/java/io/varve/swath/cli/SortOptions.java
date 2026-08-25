/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.sort.SortConfig;

/** The globally-sorted-output flags (contract §6): {@code --sort} and its disk-guard override. */
final class SortOptions {

    static final int MIN_MERGE_PARALLELISM = 1;
    static final int MAX_MERGE_PARALLELISM = 16;

    boolean sort;

    boolean forceSort;

    Integer mergeParallelism;

    SortConfig resolveConfig() {
        SortConfig config = SortConfig.fromSystemProperties();
        return mergeParallelism == null
                ? config : config.withMergeParallelism(mergeParallelism);
    }
}
