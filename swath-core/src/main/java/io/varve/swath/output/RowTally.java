/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.RunMetrics;
import java.time.Duration;

/**
 * The per-entry row tally every output stage keeps: how many objects / common prefixes /
 * delete markers went out and how many bytes they represent. Owned by the stage (single-threaded
 * on the consumer, like the counters it replaces) and turned into the run's
 * {@link ListingStatistics} at the end.
 *
 * <p>Shared so the {@link ListEntry} switch exists once: a new {@code ListEntry} subtype has
 * exactly one place to be counted, instead of two that can silently drift apart.
 */
public final class RowTally {

    private long objects;
    private long commonPrefixes;
    private long deleteMarkers;
    private long estimatedBytes;

    /** Count one emitted entry, recording an object's size on {@code metrics} as it goes. */
    public void add(ListEntry entry, RunMetrics metrics) {
        switch (entry) {
            case ObjectEntry o -> {
                objects++;
                estimatedBytes += o.size();
                metrics.recordEstimatedBytes(o.size());
            }
            case CommonPrefixEntry ignored -> commonPrefixes++;
            case DeleteMarkerEntry ignored -> deleteMarkers++;
        }
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
