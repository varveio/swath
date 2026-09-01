/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.model.ListEntry;
import io.varve.swath.sort.SortMetrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only row oracle for page-run staging segments. It deliberately drives the production
 * {@link PageRunSegmentIo#nextPage()} loop instead of maintaining a second segment reader.
 */
public final class PageRunReads {

    private PageRunReads() {
    }

    /** The keys of a page-run staging/intermediate segment, in stored (sorted) order. */
    public static List<String> keys(Path segment) throws IOException {
        return entries(segment).stream()
                .map(entry -> entry.key().asString())
                .toList();
    }

    static List<ListEntry> entries(Path segment) throws IOException {
        return entries(segment, SortMetrics.NO_OP);
    }

    static List<ListEntry> entries(Path segment, SortMetrics metrics) throws IOException {
        List<ListEntry> result = new ArrayList<>();
        try {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, metrics)) {
                PageRunSegmentIo.Page page;
                while ((page = io.nextPage()) != null) {
                    PageBlockCursor cursor = page.decode(segment).cursor();
                    while (cursor.hasNext()) {
                        result.add(cursor.next());
                    }
                    cursor.drainAndValidate();
                }
            }
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
        return result;
    }
}
