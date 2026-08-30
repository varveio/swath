/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Reusable row heap for decoded overlap pages. Cursor nodes are retained and reinserted, so row
 * emission allocates only the {@link ListEntry} materialized by the page decoder.
 */
final class PageRowMerger {

    private final PriorityQueue<PageCursor> pages;
    private long rows;
    private int lastSource;
    private long releasedBytes;

    PageRowMerger(Comparator<ListEntry> comparator) {
        pages = new PriorityQueue<>((left, right) -> comparator.compare(left.head, right.head));
    }

    void add(int source, PageBlock page, long retainedBytes) {
        pages.add(new PageCursor(source, page.cursor(), page.count(), retainedBytes));
        rows += page.count();
    }

    boolean hasNext() {
        return !pages.isEmpty();
    }

    ListEntry next() {
        PageCursor page = pages.poll();
        ListEntry result = page.head;
        lastSource = page.source;
        page.advance();
        rows--;
        if (page.head == null) {
            releasedBytes = page.retainedBytes;
            page.retainedBytes = 0;
        } else {
            releasedBytes = 0;
            pages.add(page);
        }
        return result;
    }

    int lastSource() {
        return lastSource;
    }

    long releasedBytes() {
        return releasedBytes;
    }

    int pageCount() {
        return pages.size();
    }

    long rowCount() {
        return rows;
    }

    RuntimeException drainAndValidate() {
        RuntimeException failure = null;
        for (PageCursor page : pages) {
            try {
                page.cursor.drainAndValidate();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        return failure;
    }

    long releaseAllBytes() {
        long total = 0;
        for (PageCursor page : pages) {
            total = Math.addExact(total, page.retainedBytes);
            page.retainedBytes = 0;
        }
        return total;
    }

    private static final class PageCursor {
        private final int source;
        private final PageBlockCursor cursor;
        private long retainedBytes;
        private ListEntry head;

        PageCursor(int source, PageBlockCursor cursor, int count, long retainedBytes) {
            this.source = source;
            this.cursor = cursor;
            this.retainedBytes = retainedBytes;
            this.head = cursor.hasNext() ? cursor.next() : null;
            if (count < 1 || head == null) {
                throw new IllegalArgumentException("decoded page must contain at least one row");
            }
        }

        void advance() {
            head = cursor.hasNext() ? cursor.next() : null;
        }
    }
}
