/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.ListEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.model.PageTally;
import io.varve.swath.observability.RunMetrics;
import java.time.Duration;
import java.util.List;

/**
 * The row tally every output stage keeps: how many objects / common prefixes / delete markers
 * went out and how many bytes they represent. Owned by the stage (single-threaded on the
 * consumer, like the counters it replaces) and turned into the run's {@link ListingStatistics}
 * at the end.
 *
 * <p>The per-page counts arrive pre-computed on the {@link PageBatch} (its {@link PageTally}, built
 * on the fetch worker), so the consumer stage's share is a four-long {@link #merge}, never a walk
 * over the entries; the {@link ListEntry} switch itself exists once, in {@link PageTally#of(List)}.
 */
public final class RowTally {

    private long objects;
    private long commonPrefixes;
    private long deleteMarkers;
    private long estimatedBytes;

    /** Fold one emitted page's tally in, recording its summed object size on {@code metrics}. */
    public void merge(PageTally page, RunMetrics metrics) {
        objects += page.objects();
        commonPrefixes += page.commonPrefixes();
        deleteMarkers += page.deleteMarkers();
        estimatedBytes += page.objectBytes();
        metrics.recordEstimatedBytes(page.objectBytes());
    }

    /**
     * Count one emitted entry, recording an object's size on {@code metrics} as it goes — for the
     * stage that writes entries one at a time and must stop tallying where a broken pipe cut the
     * page ({@link OutputStage}). Same classification as {@link #merge}, one entry at a time.
     */
    public void add(ListEntry entry, RunMetrics metrics) {
        merge(PageTally.of(List.of(entry)), metrics);
    }

    public long objects() {
        return objects;
    }

    public long totalRows() {
        return objects + commonPrefixes + deleteMarkers;
    }

    public long estimatedBytes() {
        return estimatedBytes;
    }

    public ListingStatistics statistics(long apiCalls, Duration elapsed) {
        return new ListingStatistics(objects, commonPrefixes, deleteMarkers, estimatedBytes,
                apiCalls, elapsed);
    }
}
