/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.sort.SortMetrics;
import java.io.IOException;
import java.nio.file.Path;

/** Value type for a page-run segment's CRC-validated fixed completeness trailer. */
public final class PageRunTrailer {

    private PageRunTrailer() {
    }

    /** Exact record/entry totals and the allocation/admission maxima from the fixed trailer. */
    public record Trailer(long totalRecords, long totalEntries, long maxRecordLen,
                          int maxRawPayloadLength, int maxKeyLength) {
    }

    /** Open {@code path}, validate it, and read its fixed trailer. */
    static Trailer read(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            return read(io);
        }
    }

    /** Read the trailer metadata retained by an already-open segment. */
    public static Trailer read(PageRunSegmentIo io) {
        return new Trailer(io.totalRecords, io.totalEntries, io.maxRecordLen,
                io.persistedMaxRawPayloadLength, io.persistedMaxKeyLength);
    }
}
