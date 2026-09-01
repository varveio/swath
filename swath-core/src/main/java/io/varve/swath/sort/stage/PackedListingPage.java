/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.model.PackedPage;
import io.varve.swath.sort.spill.PageBlock;

/**
 * The sort package's concrete {@link PackedPage} (pack-on-fetch): a {@link PageBlock} the
 * fetch worker already packed, plus the cheap per-row-type tallies and object-size sum the drain-thread
 * {@code SortOutputStage} needs for its counters without decoding the payload. {@link PageBatch} carries
 * this as an opaque {@link PackedPage} across the {@code swath-model} seam; {@link SpillLane} downcasts to
 * recover {@link #block()} and hands it straight to {@link SortBuffer} (no re-pack on the drain thread).
 */
final class PackedListingPage implements PackedPage {

    private final PageBlock block;
    private final long objectCount;
    private final long commonPrefixCount;
    private final long deleteMarkerCount;
    private final long totalObjectSize;

    PackedListingPage(PageBlock block, long objectCount, long commonPrefixCount,
                    long deleteMarkerCount, long totalObjectSize) {
        this.block = block;
        this.objectCount = objectCount;
        this.commonPrefixCount = commonPrefixCount;
        this.deleteMarkerCount = deleteMarkerCount;
        this.totalObjectSize = totalObjectSize;
    }

    /** The packed block itself — consumed by {@link SpillLane}/{@link SortBuffer} on the drain thread. */
    PageBlock block() {
        return block;
    }

    @Override
    public long entryCount() {
        return block.count();
    }

    @Override
    public long objectCount() {
        return objectCount;
    }

    @Override
    public long commonPrefixCount() {
        return commonPrefixCount;
    }

    @Override
    public long deleteMarkerCount() {
        return deleteMarkerCount;
    }

    @Override
    public long totalObjectSize() {
        return totalObjectSize;
    }

    @Override
    public byte[] firstKey() {
        return block.firstKey();
    }

    @Override
    public byte[] lastKey() {
        return block.lastKey();
    }
}
