/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * A loaded worklist node ({@code listing_node}). {@code cursor} is the last
 * <b>emitted</b> key (= {@code start_after} on resume); {@code durableCursor} is
 * the highest key whose pages are in finalized parts (file sinks, I6).
 *
 * <p>Resume start key: text sinks (at-most-once) resume from {@code cursor};
 * file sinks (exactly-once) resume from {@code durableCursor} — the not-yet-durable
 * tail re-lists into new parts (algorithms.md §4.5/§4.6).
 */
public record Node(
        long id,
        long runId,
        Long parentId,
        NodeKind kind,
        byte[] rangeStart,
        byte[] rangeEnd,
        byte[] cursor,
        byte[] durableCursor,
        byte[] keyMarker,
        String versionIdMarker,
        String inventoryUri,
        NodeStatus status,
        long generation,
        boolean unsplittable) {
}
