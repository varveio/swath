/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** The role of a page-run segment, which owns its completion accounting. */
enum SegmentKind {
    LISTING,
    CASCADE_INTERMEDIATE,
    FIXTURE_CHUNK
}
