/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;

/** Value type for a page-run segment's CRC-validated fixed completeness trailer. */
public final class PageRunTrailer {

    private PageRunTrailer() {
    }

    /** Exact record and entry totals plus the maximum framed body length. */
    public record Trailer(long totalRecords, long totalEntries, long maxRecordLen) {
    }

    /** Open {@code path}, validate it, and read its fixed trailer. */
    static Trailer read(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            return read(io);
        }
    }

    /** Read the trailer metadata retained by an already-open segment. */
    static Trailer read(PageRunSegmentIo io) {
        return new Trailer(io.totalRecords, io.totalEntries, io.maxRecordLen);
    }
}
