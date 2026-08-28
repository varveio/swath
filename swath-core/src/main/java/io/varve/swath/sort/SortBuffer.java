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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The sort lane's in-memory buffer: admitted pages are packed into compact {@link PageBlock}s in
 * admission order, while distinct node ids and per-node max keys track the classification meter
 * and the checkpoint's {@code durable_cursor}. Byte-gated by the §5 estimate so the corridor is
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
    private final SegmentGate gate;
    private final Comparator<ListEntry> comparator;
    private List<PageBlock> pages = new ArrayList<>();
    private Set<Long> nodeIds = new HashSet<>();
    private Map<Long, byte[]> maxKeys = new LinkedHashMap<>();
    private long estimatedBytes;
    private long entryCount;

    SortBuffer(SortConfig config, Comparator<ListEntry> comparator) {
        this.config = config;
        this.gate = new SegmentGate(config);
        this.comparator = comparator;
    }

    /**
     * Fallback (non-pack-on-fetch) admit: pack {@code page} into a block under {@code nodeId}, then
     * retain it. Packing happens UPSTREAM, on the fetch worker, for the live {@code --sort} path (see
     * {@link #admit(long, PageBlock)}); this overload survives for any producer that still hands the
     * sort lane a raw {@link ListEntry} list — it packs exactly as before, then delegates.
     */
    void admit(long nodeId, List<ListEntry> page) {
        if (page.isEmpty()) {
            return;
        }
        admit(nodeId, PageBlock.pack(page, comparator, config.segmentCodec()));
    }

    /**
     * Retain an ALREADY-PACKED {@code block} under {@code nodeId} and update the per-node max key +
     * gates for pack-on-fetch. The block was packed on the fetch worker (never on this drain thread),
     * so this only tracks ownership and the §5 seal gates off the block that now arrives pre-built —
     * the per-node max key, estimated bytes, and entry count are read verbatim, so what gets
     * sealed/checkpointed matches the raw-list admit overload's output byte-for-byte.
     */
    void admit(long nodeId, PageBlock block) {
        pages.add(block);
        nodeIds.add(nodeId);
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
        return gate.trigger(estimatedBytes, entryCount);
    }

    /**
     * Move the accumulated pages and checkpoint metadata into a {@link SealedBuffer}, then install
     * fresh containers before this buffer is reused. {@code trigger} records why the seal happened
     * so the flush can emit {@code SORT.buffer_byte_gated}.
     */
    SealedBuffer seal(SealTrigger trigger) {
        SealedBuffer sealed = new SealedBuffer(pages, nodeIds.size(), maxKeys, entryCount, trigger,
                estimatedBytes);
        pages = new ArrayList<>();
        nodeIds = new HashSet<>();
        maxKeys = new LinkedHashMap<>();
        estimatedBytes = 0;
        entryCount = 0;
        return sealed;
    }
}
