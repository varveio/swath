/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Whether a page-aware merge resolves pages across segments or within one segment reader. */
enum MergeScope {
    CROSS_SEGMENT,
    INTRA_SEGMENT
}
