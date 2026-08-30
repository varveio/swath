/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.nio.file.Path;

/** The single parser and value type for a page-run segment's completeness trailer. */
public final class PageRunTrailer {

    private PageRunTrailer() {
    }

    /**
     * Exact segment bounds, the optional-extension offset, and the fixed EOF-tail metadata. The
     * extension starts immediately after the two length-prefixed bounds and ends at the fixed EOF
     * tail.
     */
    public record Trailer(byte[] segMinKey, byte[] segMaxKey, long extensionStart,
                          long totalRecords, long totalEntries, long maxRecordLen) {
    }

    /** Open {@code path}, validate it, and read its trailer without walking framed records. */
    static Trailer read(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            return read(io);
        }
    }

    /** Read the trailer from an already-open segment without walking any framed page record. */
    static Trailer read(PageRunSegmentIo io) throws IOException {
        long fixedTailStart = io.fileSize - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        byte[] segMin = readLenPrefixedKey(io, io.trailerStart, fixedTailStart);
        long maxPrefix = io.trailerStart + Short.BYTES + segMin.length;
        byte[] segMax = readLenPrefixedKey(io, maxPrefix, fixedTailStart);
        long extensionStart = maxPrefix + Short.BYTES + segMax.length;
        if (io.totalRecords == 0 && (segMin.length != 0 || segMax.length != 0)) {
            throw io.fail("empty segment has non-empty trailer bounds");
        }
        return new Trailer(segMin, segMax, extensionStart, io.totalRecords, io.totalEntries,
                io.maxRecordLen);
    }

    private static byte[] readLenPrefixedKey(PageRunSegmentIo io, long position, long limit)
            throws IOException {
        if (position < io.trailerStart || position > limit - Short.BYTES) {
            throw io.fail("trailer key prefix exceeds trailer bounds");
        }
        int length = io.readAt(position, Short.BYTES).getShort() & 0xFFFF;
        if (length > limit - position - Short.BYTES) {
            throw io.fail("trailer key exceeds trailer bounds");
        }
        if (length > ByteMidpoint.MAX_KEY_LEN) {
            throw io.trailerCorruption("trailer key length " + length
                    + " exceeds the S3 key limit of " + ByteMidpoint.MAX_KEY_LEN + " bytes");
        }
        return io.readAt(position + Short.BYTES, length).array();
    }
}
