/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.sort.spill.SegmentResult;

/**
 * The durability seam invoked by the {@link SortLane}'s encoder <b>after a staging segment is fully
 * written and fsynced</b> — the exact commit point where the checkpoint layer records a
 * {@code part_file} row (staging namespace) and advances each node's {@code durable_cursor} to the
 * segment's per-node max keys ({@link SegmentResult#perNodeMaxKeys()}), exactly as a finalized part
 * does today (§4.1). The lane's single ordered encoder invokes this strictly in seal order, so
 * {@code durable_cursor} never over-advances past keys still sitting in an earlier unfinalized
 * buffer.
 *
 * <p>{@link #NONE} discards — used by {@code --checkpoint none} runs (no resume) and by unit tests.
 * The callback may throw a checked exception (a checkpoint-commit failure); the lane surfaces it on
 * {@link SortLane#close()}.
 */
@FunctionalInterface
public interface SegmentSink {

    /** Discards the result (no durable tracking). */
    SegmentSink NONE = result -> {
    };

    /** Called once per finalized segment, in seal order, on the encoder thread. */
    void onSegmentFinalized(SegmentResult result) throws Exception;
}
