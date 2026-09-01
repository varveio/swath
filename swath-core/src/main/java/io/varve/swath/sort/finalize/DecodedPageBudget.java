/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageBlockCodec;
import io.varve.swath.sort.spill.PageCodec;

/**
 * Exact retained-byte admission for decoded pages. Reservation precedes cursor creation because a
 * compressed page allocates its decompression target lazily; release follows cursor exhaustion.
 */
final class DecodedPageBudget {
    /**
     * Covers the record owner, decompression target, dictionary materialization, and their shallow
     * owners from the larger of the persisted-body and raw-payload ceilings.
     */
    static final long RETAINED_PAGE_FACTOR = 4L;

    private final long limitBytes;
    private final SortMetrics metrics;
    private long residentBytes;
    private long peakResidentBytes;

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
        peakResidentBytes = Math.max(peakResidentBytes, residentBytes);
        return pageBytes;
    }

    void release(long pageBytes) {
        residentBytes -= pageBytes;
        if (residentBytes < 0) {
            throw new IllegalStateException("decoded-page residency accounting underflow");
        }
    }

    long peakResidentBytes() {
        return peakResidentBytes;
    }

    static long retainedBytes(PageBlock page) {
        long bytes = saturatedAdd(page.retainedRecordBytes(), page.dictionaryCoordinateBytes());
        bytes = saturatedAdd(bytes, page.dictionaryCacheBudgetBytes());
        return page.codec() == PageCodec.NONE
                ? bytes : saturatedAdd(bytes, page.rawPayloadLength());
    }

    /**
     * Bound the unit consumed by {@link #reserve(PageBlock)} using trailer/header metadata alone.
     * Four times the larger physical or raw ceiling covers the record, decompression target, and
     * variable dictionary character storage. The additive format-derived term covers the maximum
     * 5-by-64 dictionary cache object graph even when its strings are tiny. Planning deliberately
     * treats NONE as compressed because {@link PageRef} does not retain each page's codec.
     */
    static long retainedPageUpperBound(long rawPayloadBytes, long recordBytes) {
        if (rawPayloadBytes < 1 || recordBytes < 1) {
            throw new IllegalArgumentException("page bounds must be positive");
        }
        return saturatedAdd(
                saturatedMultiply(RETAINED_PAGE_FACTOR,
                        Math.max(rawPayloadBytes, recordBytes)),
                PageBlockCodec.MAX_PERSISTED_DICTIONARY_OVERHEAD_BYTES);
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** Saturation makes corrupt or adversarial metadata fail admission instead of wrapping small. */
    private static long saturatedMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
