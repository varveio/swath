/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.util.List;

/**
 * The row tally of one page: how many objects / common prefixes / delete markers it carries and
 * the summed object size. Computed once, on the thread that built the {@link PageBatch} (a fetch
 * worker), so the single consumer stage merges four longs per page instead of walking every entry.
 *
 * <p>This is the one place a {@link ListEntry} subtype is classified for counting: a new subtype
 * has exactly one switch to extend, and every output stage's statistics follow from it.
 */
public record PageTally(long objects, long commonPrefixes, long deleteMarkers, long objectBytes) {

    public static final PageTally EMPTY = new PageTally(0L, 0L, 0L, 0L);

    /** Tally a page's entries. */
    public static PageTally of(List<ListEntry> entries) {
        long objects = 0L;
        long commonPrefixes = 0L;
        long deleteMarkers = 0L;
        long objectBytes = 0L;
        for (ListEntry entry : entries) {
            switch (entry) {
                case ObjectEntry o -> {
                    objects++;
                    objectBytes += o.size();
                }
                case CommonPrefixEntry ignored -> commonPrefixes++;
                case DeleteMarkerEntry ignored -> deleteMarkers++;
            }
        }
        return new PageTally(objects, commonPrefixes, deleteMarkers, objectBytes);
    }

    /** The tally a {@link PackedPage} accumulated at pack time (same totals a per-entry walk yields). */
    public static PageTally of(PackedPage packed) {
        return new PageTally(packed.objectCount(), packed.commonPrefixCount(),
                packed.deleteMarkerCount(), packed.totalObjectSize());
    }

    public long rows() {
        return objects + commonPrefixes + deleteMarkers;
    }
}
