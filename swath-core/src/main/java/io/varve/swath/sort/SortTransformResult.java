/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.file.Path;
import java.util.List;

/**
 * The outcome of a {@link SortTransform} run: the published final sorted file(s), in key order, and
 * the total row count. Range-disjoint and named {@code part-00001.parquet}… when
 * {@code final-file-bytes} rolls the output; a single file otherwise.
 *
 * <p>{@code mergePasses}/{@code cascadedPasses}/{@code fastPathEmissions} surface the
 * {@link KWayMerge} engagement counts for the {@code sort} JSON run-summary block and the
 * {@code swath.sort.merge.passes} first-class meter.
 */
public record SortTransformResult(List<Path> finalFiles, long totalRows,
                                  long mergePasses, long cascadedPasses, long fastPathEmissions) {
}
