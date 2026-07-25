/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * A node to insert into the worklist ({@code listing_node}). A fresh seed
 * node has {@code cursor = null} (⊥: nothing emitted yet); {@code rangeStart}/
 * {@code rangeEnd} null mean ⊥/frontier respectively (the half-open
 * {@code (A, B]} model, algorithms.md §1.2).
 */
public record NodeSpec(
        long runId,
        Long parentId,
        NodeKind kind,
        byte[] rangeStart,
        byte[] rangeEnd,
        byte[] cursor,
        String inventoryUri) {

    /** The root RANGE seed covering the whole prefix: {@code (⊥, frontier]}, nothing emitted. */
    public static NodeSpec rootRange(long runId) {
        return new NodeSpec(runId, null, NodeKind.RANGE, null, null, null, null);
    }
}
