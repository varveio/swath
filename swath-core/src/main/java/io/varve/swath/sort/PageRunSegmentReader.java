/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Comparator;

/** Sequential entry stream over one validated page-run segment. */
final class PageRunSegmentReader implements EntryStream {

    private final PageRunSegmentIo io;
    private final PageRowMerger rows;
    private PageRunSegmentIo.Page lookahead;
    private ListEntry head;
    private int source;

    PageRunSegmentReader(Path path, Comparator<ListEntry> comparator,
            SortMetrics metrics, int maxRawPayloadLength) throws IOException {
        this(PageRunSegmentIo.open(path, metrics, maxRawPayloadLength), comparator);
    }

    PageRunSegmentReader(PageRunSegmentIo io, Comparator<ListEntry> comparator) throws IOException {
        this.io = io;
        rows = new PageRowMerger(comparator);
        try {
            lookahead = io.nextPage();
            head = readNext();
        } catch (IOException | RuntimeException failure) {
            try {
                io.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    @Override
    public boolean hasNext() {
        return head != null;
    }

    @Override
    public ListEntry peek() {
        return head;
    }

    @Override
    public ListEntry next() throws IOException {
        ListEntry current = head;
        head = readNext();
        return current;
    }

    private ListEntry readNext() throws IOException {
        try {
            if (!rows.hasNext()) {
                if (lookahead == null) {
                    return null;
                }
                addLookahead();
            }
            while (lookahead != null
                    && java.util.Arrays.compareUnsigned(
                            lookahead.header().minKey(), rows.nextKey()) <= 0) {
                addLookahead();
            }
            return rows.next();
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    private void addLookahead() throws IOException {
        PageRunSegmentIo.Page page = lookahead;
        rows.add(source++, page.decode(io.path()), 0);
        lookahead = io.nextPage();
    }

    @Override
    public void close() throws IOException {
        RuntimeException validationFailure = rows.drainAndValidate();
        try {
            io.close();
        } catch (IOException closeFailure) {
            if (validationFailure != null) {
                validationFailure.addSuppressed(closeFailure);
            } else {
                throw closeFailure;
            }
        }
        if (validationFailure != null) {
            if (validationFailure instanceof UncheckedIOException unchecked) {
                throw unchecked.getCause();
            }
            throw validationFailure;
        }
    }
}
