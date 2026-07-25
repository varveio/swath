/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.time.Duration;

/**
 * End-of-run summary for the cost line. Counters are
 * accumulated by the output stage.
 */
public record ListingStatistics(
        long objects,
        long commonPrefixes,
        long deleteMarkers,
        long estimatedBytes,
        long apiCalls,
        Duration elapsed) {

    public long totalRows() {
        return objects + commonPrefixes + deleteMarkers;
    }
}
