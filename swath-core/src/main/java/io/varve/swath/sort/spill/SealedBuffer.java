/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The state moved out of a sealed {@link io.varve.swath.sort.stage.SortBuffer}: packed pages, distinct-node count, per-node
 * max keys, and the {@link SealTrigger}. {@link PageRunSegmentWriter} owns and sorts the page list
 * in place; merge ordering is resolved by the page-aware external merge.
 */
public final class SealedBuffer {

    private final List<PageBlock> pages;
    private final int runCount;
    private final Map<Long, byte[]> maxKeys;
    private final long entryCount;
    private final SealTrigger trigger;
    private final long estimatedBytes;

    public SealedBuffer(List<PageBlock> pages, int runCount, Map<Long, byte[]> maxKeys, long entryCount,
                 SealTrigger trigger, long estimatedBytes) {
        this.pages = pages;
        this.runCount = runCount;
        this.maxKeys = maxKeys;
        this.entryCount = entryCount;
        this.trigger = trigger;
        this.estimatedBytes = estimatedBytes;
    }

    public long entryCount() {
        return entryCount;
    }

    /**
     * The buffer's admission-time byte estimate (the same §5 estimate that gated this buffer's
     * seal) — NOT the encoded/compressed staging-segment size ({@link SegmentResult#bytes()}).
     * {@link SortLane} keeps this live in memory from seal until the segment is fully encoded +
     * finalized (success or failure), feeding the in-flight staging-bytes high-water mark
     * ({@code swath.sort.staging.bytes.peak}).
     */
    public long estimatedBytes() {
        return estimatedBytes;
    }

    /** How many per-node page runs this buffer holds — the {@code page_runs_per_buffer} signal. */
    public int runCount() {
        return runCount;
    }

    /**
     * The moved page list in admission order, <b>not</b> globally sorted. The page-run writer owns
     * this list and orders it by {@link PageBlock#firstKey()} in place; no live {@link io.varve.swath.sort.stage.SortBuffer}
     * retains an alias after sealing.
     */
    public List<PageBlock> pages() {
        return pages;
    }

    public SealTrigger trigger() {
        return trigger;
    }

    public boolean isEmpty() {
        return entryCount == 0;
    }

    /** Per-node max key for this segment (defensive byte[] copies) — feeds the checkpoint. */
    public Map<Long, byte[]> perNodeMaxKeys() {
        Map<Long, byte[]> out = new LinkedHashMap<>(maxKeys.size());
        for (Map.Entry<Long, byte[]> e : maxKeys.entrySet()) {
            out.put(e.getKey(), e.getValue().clone());
        }
        return out;
    }

}
