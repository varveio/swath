/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import io.varve.swath.sort.PreparedSortedParts;
import java.nio.file.Path;
import java.util.List;

/** The consumer-visible files and preserved algorithm facts after publication commits. */
public record SortedDatasetCommit(
        List<Path> finalFiles,
        long outputBytes,
        long totalRows,
        PreparedSortedParts.MergeStatistics mergeStatistics) {

    public SortedDatasetCommit {
        finalFiles = List.copyOf(finalFiles);
    }
}
