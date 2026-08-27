/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The sort lane's in-memory buffer: admitted pages are packed into compact
 * {@link PageBlock}s and grouped by node, so each node's blocks form a pre-sorted run and the
 * per-node max key falls out of the grouping for free (the value the checkpoint's
 * {@code durable_cursor} needs). Byte-gated by the §5 estimate so the corridor is
 * key-size-independent. Single-threaded: the pipeline owns one fill buffer at a time and the
 * double-buffering across sealed buffers.
 *
 * <p>The sorter admits {@code (nodeId, page)} pairs because a sealed buffer mixes many nodes and
 * {@link ListEntry} carries no node id. {@code comparator} is threaded
 * through to {@link PageBlock#pack} so each page records, at admission, whether it is internally
 * ordered under the full §0.3 preorder — not just key bytes. The page-run writer repairs
 * full-comparator disorder only while raw keys remain non-decreasing, which makes the admitted
 * {@link PageBlock#lastKey()} a safe durable maximum; raw-key regression is rejected before the
 * {@link SegmentSink} checkpoint seam.
 */
final class SortBuffer {

    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final Map<Long, List<PageBlock>> runs = new LinkedHashMap<>();
    private final Map<Long, byte[]> maxKeys = new LinkedHashMap<>();
    private long estimatedBytes;
    private long entryCount;

    SortBuffer(SortConfig config, Comparator<ListEntry> comparator) {
        this.config = config;
        this.comparator = comparator;
    }

    /**
     * Fallback (non-pack-on-fetch) admit: pack {@code page} into a block under {@code nodeId}, then group
     * it. Packing happens UPSTREAM, on the fetch worker, for the live {@code --sort} path (see
     * {@link #admit(long, PageBlock)}); this overload survives for any producer that still hands the sort
     * lane a raw {@link ListEntry} list — it packs exactly as before, then delegates to the grouping path.
     */
    void admit(long nodeId, List<ListEntry> page) {
        if (page.isEmpty()) {
            return;
        }
        admit(nodeId, PageBlock.pack(page, comparator, config.segmentCodec()));
    }

    /**
     * Group an ALREADY-PACKED {@code block} under {@code nodeId} and update the per-node max key + gates
     * for pack-on-fetch. The block was packed on the fetch worker (never on this drain thread), so
     * this only tracks the per-node run + the §5 seal gates off the block that now arrives pre-built — the
     * per-node max key, estimated bytes, and entry count are read verbatim, so what gets sealed/checkpointed
     * matches the raw-list admit overload's output byte-for-byte.
     */
    void admit(long nodeId, PageBlock block) {
        runs.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(block);
        byte[] last = block.lastKey();
        maxKeys.merge(nodeId, last,
                (prev, next) -> KeyBytes.compareUnsigned(next, prev) > 0 ? next : prev);
        estimatedBytes += block.estimatedBytes();
        entryCount += block.count();
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    long estimatedBytes() {
        return estimatedBytes;
    }

    long entryCount() {
        return entryCount;
    }

    /** Which gate (if any) says this buffer is full and should be sealed. */
    SealTrigger triggerOrNone() {
        if (estimatedBytes >= config.segmentBytes()) {
            return SealTrigger.BYTE_GATE;
        }
        if (entryCount >= config.segmentEntries()) {
            return SealTrigger.ENTRY_CAP;
        }
        return SealTrigger.DRAIN;   // caller decides whether a partial drain-seal is warranted
    }

    /**
     * Snapshot the accumulated runs into an immutable {@link SealedBuffer} and reset this buffer for
     * reuse (move semantics — the runs are handed off, not copied). {@code trigger} records why the
     * seal happened so the flush can emit {@code SORT.buffer_byte_gated}.
     */
    SealedBuffer seal(SealTrigger trigger) {
        SealedBuffer sealed = new SealedBuffer(new LinkedHashMap<>(runs), new LinkedHashMap<>(maxKeys),
                entryCount, trigger, estimatedBytes);
        runs.clear();
        maxKeys.clear();
        estimatedBytes = 0;
        entryCount = 0;
        return sealed;
    }
}
