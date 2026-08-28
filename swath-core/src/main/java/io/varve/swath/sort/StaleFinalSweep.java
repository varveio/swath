/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Ownership scope for stale final-output cleanup immediately before replacement publication. */
public enum StaleFinalSweep {
    /** Delete only the sorter's {@code part-*.parquet} files. */
    OWN_PARTS_ONLY,
    /** Delete every Parquet file after the caller has verified dataset ownership. */
    ALL_PARQUET
}
