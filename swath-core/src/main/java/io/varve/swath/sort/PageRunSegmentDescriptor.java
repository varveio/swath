/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.file.Path;

/**
 * One kickoff-opened page-run segment's retained primitive file, trailer, and extension metadata.
 * {@link PageRunCatalog} owns name validation, one-open preflight, ordered path/index views, and
 * maximum-record aggregation. Embedded sample keys stream into a separately bounded candidate sink;
 * a descriptor never retains those keys.
 */
record PageRunSegmentDescriptor(Path path, long fileSize, long trailerStart,
                                PageRunTrailer.Trailer trailer,
                                PageRunPageIndex.ReadResult extension) {

    /** Legacy-compatible boundary-sample view used by the existing range-boundary planner. */
    PageRunBoundarySample.ReadResult sample() {
        return extension.boundarySample();
    }
}
