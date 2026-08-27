/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable snapshot of a sealed {@link SortBuffer}: per-node {@link PageBlock} runs plus the
 * per-node max keys and the {@link SealTrigger}. {@link PageRunSegmentWriter} consumes the packed
 * pages directly; merge ordering is resolved by the page-aware external merge.
 */
final class SealedBuffer {

    private final Map<Long, List<PageBlock>> runs;
    private final Map<Long, byte[]> maxKeys;
    private final long entryCount;
    private final SealTrigger trigger;
    private final long estimatedBytes;

    SealedBuffer(Map<Long, List<PageBlock>> runs, Map<Long, byte[]> maxKeys, long entryCount,
                 SealTrigger trigger, long estimatedBytes) {
        this.runs = runs;
        this.maxKeys = maxKeys;
        this.entryCount = entryCount;
        this.trigger = trigger;
        this.estimatedBytes = estimatedBytes;
    }

    long entryCount() {
        return entryCount;
    }

    /**
     * The buffer's admission-time byte estimate (the same §5 estimate that gated this buffer's
     * seal) — NOT the encoded/compressed staging-segment size ({@link SegmentResult#bytes()}).
     * {@link SortLane} keeps this live in memory from seal until the segment is fully encoded +
     * finalized (success or failure), feeding the in-flight staging-bytes high-water mark
     * ({@code swath.sort.staging.bytes.peak}).
     */
    long estimatedBytes() {
        return estimatedBytes;
    }

    /** How many per-node page runs this buffer holds — the {@code page_runs_per_buffer} signal. */
    int runCount() {
        return runs.size();
    }

    /**
     * All pages across every node run, flattened (in node-run iteration order, <b>not</b> globally
     * sorted). The minimal seal-path source the page-run writer consumes: it orders these by
     * {@link PageBlock#firstKey()} and concatenates them — it does <b>not</b> k-way merge the buffer
     * since range-disjoint pages let us pack once and write in minKey order.
     */
    List<PageBlock> pages() {
        List<PageBlock> out = new ArrayList<>();
        for (List<PageBlock> run : runs.values()) {
            out.addAll(run);
        }
        return out;
    }

    SealTrigger trigger() {
        return trigger;
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    /** Per-node max key for this segment (defensive byte[] copies) — feeds the checkpoint. */
    Map<Long, byte[]> perNodeMaxKeys() {
        Map<Long, byte[]> out = new LinkedHashMap<>(maxKeys.size());
        for (Map.Entry<Long, byte[]> e : maxKeys.entrySet()) {
            out.put(e.getKey(), e.getValue().clone());
        }
        return out;
    }

}
