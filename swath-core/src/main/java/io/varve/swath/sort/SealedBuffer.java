/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable snapshot of a sealed {@link SortBuffer}: per-node {@link PageBlock} runs plus the
 * per-node max keys and the {@link SealTrigger}. {@link #merge} produces the sorted stream that
 * {@link SegmentWriter} flushes to a segment.
 *
 * <p><b>Seal-path merge.</b> Each node's run is already ascending, so the happy path
 * k-way merges the per-node {@link PageRunCursor}s with <b>zero row sorting</b> and exploits the
 * {@link StreamingMerger} same-reader fast path over the disjoint ranges. A defensive check —
 * against the <b>full §0.3 comparator</b>, not key bytes alone — guards the
 * pre-sortedness assumption: every page must itself be internally strictly ascending under the
 * full comparator ({@link PageBlock#orderedUnderFullComparator()}, computed once at admission),
 * <b>and</b> each page's first entry must strictly exceed its node's previous page's last entry
 * under the full comparator. A key-bytes-only check would miss a versioned key whose versions
 * arrive out of §0.3 order (S3 delivers `--all-versions` pages last-modified-descending, not in
 * sort order) — same key bytes compare equal, so a bytes-only check sees no violation. A
 * violation falls back to a full stable TimSort ({@code SORT.buffer_sort_fallback}), so
 * correctness never <i>depends</i> on pre-sortedness, it only profits from it.
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
     * (unlike {@link #merge}), since range-disjoint pages let us pack once and write in minKey order.
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

    /**
     * The sorted stream over the buffer's entries: a k-way merge of the per-node runs on the happy
     * path (defensive check passes), else a stable TimSort fallback. Emits {@code SORT.merge_fastpath}
     * once (with the total count) when the returned cursor is fully drained and closed — not per row
     * — and {@code SORT.buffer_sort_fallback} once on fallback.
     */
    SortedCursor merge(Comparator<ListEntry> comparator, DuplicateHook hook, SortMetrics metrics) {
        if (runsArePresorted(comparator)) {
            List<EntryStream> streams = new ArrayList<>(runs.size());
            for (List<PageBlock> run : runs.values()) {
                streams.add(new PageRunCursor(run));
            }
            return new StreamingMerger(streams, comparator, hook,
                    n -> {
                        if (n > 0) {
                            metrics.recordStealReason("SORT", "merge_fastpath");
                        }
                    });
        }
        metrics.recordStealReason("SORT", "buffer_sort_fallback");
        return timSortFallback(comparator, hook);
    }

    /**
     * The class doc's defensive full-comparator check, applied per page via
     * {@link PageBlock#orderedUnderFullComparator()} (computed once at admission — no re-check here)
     * and per page-boundary pair. Catches both a mis-ordered version tiebreak spanning a page
     * boundary AND two mis-ordered versions of one key sitting inside a single page (a
     * key-bytes-only, cross-page-only check could catch neither: same key bytes tie at 0, and an
     * intra-page violation is invisible to a boundary-only check).
     */
    private boolean runsArePresorted(Comparator<ListEntry> comparator) {
        for (List<PageBlock> run : runs.values()) {
            PageBlock prevPage = null;
            for (PageBlock page : run) {
                if (!page.orderedUnderFullComparator()) {
                    return false;
                }
                if (prevPage != null && comparator.compare(prevPage.lastEntry(), page.firstEntry()) >= 0) {
                    return false;
                }
                prevPage = page;
            }
        }
        return true;
    }

    private SortedCursor timSortFallback(Comparator<ListEntry> comparator, DuplicateHook hook) {
        List<ListEntry> all = new ArrayList<>(Math.toIntExact(entryCount));
        for (List<PageBlock> run : runs.values()) {
            for (PageBlock page : run) {
                PageBlock.Cursor c = page.cursor();
                while (c.hasNext()) {
                    all.add(c.next());
                }
            }
        }
        all.sort(comparator);   // Collections/List.sort is a stable TimSort
        return new InMemoryCursor(all, comparator, hook);
    }
}
