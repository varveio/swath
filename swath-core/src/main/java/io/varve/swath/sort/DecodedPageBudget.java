/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Exact retained-byte admission for decoded pages. Reservation precedes cursor creation because a
 * compressed page allocates its decompression target lazily; release follows cursor exhaustion.
 */
final class DecodedPageBudget {
    private final long limitBytes;
    private final SortMetrics metrics;
    private long residentBytes;

    DecodedPageBudget(long limitBytes, SortMetrics metrics) {
        if (limitBytes <= 0) {
            throw new IllegalArgumentException("decoded-page budget must be positive");
        }
        this.limitBytes = limitBytes;
        this.metrics = metrics;
    }

    long reserve(PageBlock page) throws MergeMemoryExhaustedException {
        long pageBytes = retainedBytes(page);
        long next = saturatedAdd(residentBytes, pageBytes);
        if (next > limitBytes) {
            metrics.recordStealReason("SORT", "merge_decoded_residency_exhausted");
            throw new MergeMemoryExhaustedException(
                    "decoded-page retained residency exceeds the per-merger merge budget: resident_bytes="
                            + residentBytes + ", next_page_bytes=" + pageBytes
                            + ", budget_bytes=" + limitBytes);
        }
        residentBytes = next;
        return pageBytes;
    }

    void release(long pageBytes) {
        residentBytes -= pageBytes;
        if (residentBytes < 0) {
            throw new IllegalStateException("decoded-page residency accounting underflow");
        }
    }

    private static long retainedBytes(PageBlock page) {
        long bytes = saturatedAdd(page.retainedRecordBytes(), page.dictionaryCoordinateBytes());
        bytes = saturatedAdd(bytes, page.dictionaryCacheBudgetBytes());
        return page.codec() == PageCodec.NONE
                ? bytes : saturatedAdd(bytes, page.rawPayloadLength());
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
