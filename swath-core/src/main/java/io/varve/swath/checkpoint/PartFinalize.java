/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import java.util.List;

/**
 * One finalized (footer-fsynced) output part and the {@code durable_cursor}
 * advances it makes durable (algorithms.md §4.5, I6). Recorded in <b>one</b>
 * transaction: insert the {@code part_file} row and, for each node whose pages
 * the part held, advance its {@code durable_cursor} to the highest such key.
 *
 * <p>This — not the on-disk footer — is the exactly-once <b>commit point</b>: a
 * crash after the footer fsync but before this commit leaves the part un-recorded,
 * so resume discards it and re-lists its tail (no finalized row is ever lost or
 * duplicated).
 */
public record PartFinalize(
        long runId,
        int writerId,
        String path,
        String format,
        long rows,
        long bytes,
        List<DurableAdvance> advances) {

    /** A node's highest key whose pages this part made durable. */
    public record DurableAdvance(long nodeId, byte[] maxKey) {
    }
}
