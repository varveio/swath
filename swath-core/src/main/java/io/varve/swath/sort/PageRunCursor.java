/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.List;

/**
 * {@link EntryStream} over one node's run of {@link PageBlock}s — the seal-path input to the
 * {@link StreamingMerger}. A node's pages arrive ascending (S3 pagination), so decoding
 * its blocks in arrival order yields a sorted stream with zero row sorting; the k-way merge then
 * combines the per-node runs. Blocks are decoded lazily one entry at a time (sequential cursors),
 * so no page is fully materialized ahead of its consumption.
 */
final class PageRunCursor implements EntryStream {

    private final List<PageBlock> run;
    private int blockIndex;
    private PageBlock.Cursor current;
    private ListEntry head;

    PageRunCursor(List<PageBlock> run) {
        this.run = run;
        this.head = advance();
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
    public ListEntry next() {
        ListEntry current = head;
        head = advance();
        return current;
    }

    private ListEntry advance() {
        while (true) {
            if (current != null && current.hasNext()) {
                return current.next();
            }
            if (blockIndex >= run.size()) {
                return null;
            }
            current = run.get(blockIndex++).cursor();
        }
    }

    @Override
    public void close() {
        // nothing to release — the run is on-heap
    }
}
