/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Merge optimizations permitted by the provenance and overlap shape of staged input runs.
 */
public enum MergeInputProfile {
    STRUCTURED_RANGE_OWNED_PAGES,
    ARBITRARY_SORTED_RUNS
}
