/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.file.Path;

/** One kickoff-opened page-run segment's retained file, trailer, and header metadata. */
record PageRunSegmentDescriptor(Path path, long fileSize, long trailerStart,
                                PageRunTrailer.Trailer trailer,
                                int maxRawPayloadLength,
                                int maxKeyLength,
                                PageRunFormat physicalFormat,
                                int headerBytes,
                                SortMode orderingMode) {
}
