/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.time.Duration;
import java.util.List;

/**
 * One page of results. {@code truncated} is the authoritative
 * "more pages" signal — never infer from {@code entries.size() == maxKeys}
 * (algorithms.md §2, edge 4). {@code commonPrefixes} is populated only in
 * delimiter mode. {@code nextKeyMarker}/{@code nextVersionIdMarker} drive
 * versioned pagination.
 */
public record ListPage(
        List<ListEntry> entries,
        List<KeyBytes> commonPrefixes,
        boolean truncated,
        String nextContinuationToken,
        byte[] nextKeyMarker,
        String nextVersionIdMarker,
        int httpStatus,
        Duration latency) {

    public ListPage withLatency(Duration latency) {
        return new ListPage(entries, commonPrefixes, truncated, nextContinuationToken, nextKeyMarker,
                nextVersionIdMarker, httpStatus, latency);
    }
}
