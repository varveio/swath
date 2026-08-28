/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Bounded exact-stride type-2 index builder for a listing buffer whose page count is known. */
final class PageRunPageIndexBuilder {

    private final int expectedPages;
    private final long stride;
    private final List<PageRunPageIndex.IndexEntry> entries;
    private long pagesSeen;
    private byte[] prefixMax = new byte[0];

    PageRunPageIndexBuilder(int expectedPages) {
        if (expectedPages < 0) {
            throw new IllegalArgumentException("expectedPages must be >= 0");
        }
        this.expectedPages = expectedPages;
        this.stride = PageRunBoundarySample.stride(expectedPages);
        this.entries = new ArrayList<>(PageRunBoundarySample.expectedCount(expectedPages));
    }

    void recordPage(long pageOrdinal, long frameOffset, long cumulativeEntries,
                    long cumulativeFramedBytes, PageBlock page) {
        if (pageOrdinal != pagesSeen || pageOrdinal >= expectedPages) {
            throw new IllegalStateException("page-run page index ordinal mismatch: expected "
                    + pagesSeen + " but got " + pageOrdinal);
        }
        byte[] pageMax = page.lastKeyUnsafe();
        if (pagesSeen == 0 || Arrays.compareUnsigned(pageMax, prefixMax) > 0) {
            prefixMax = pageMax.clone();
        }
        if (pageOrdinal % stride == 0) {
            entries.add(new PageRunPageIndex.IndexEntry(pageOrdinal, frameOffset, cumulativeEntries,
                    cumulativeFramedBytes, page.firstKeyUnsafe().clone(), prefixMax.clone()));
        }
        pagesSeen++;
    }

    PageRunPageIndex.Snapshot finish() {
        if (pagesSeen != expectedPages) {
            throw new IllegalStateException("page-run page index page count mismatch: expected "
                    + expectedPages + " but saw " + pagesSeen);
        }
        return new PageRunPageIndex.Snapshot(List.copyOf(entries), prefixMax.clone());
    }
}
