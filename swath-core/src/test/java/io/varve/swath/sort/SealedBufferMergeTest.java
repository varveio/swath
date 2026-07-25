/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.drain;
import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Seal-path page-run merge ({@link SealedBuffer}): the happy path k-way merges the per-node runs
 * with zero row sorting (and exploits the fast path); a page-order violation falls back to a stable
 * TimSort and emits {@code SORT.buffer_sort_fallback}. Covers a versioned key's versions arriving
 * out of §0.3 order (S3 delivers `--all-versions` pages last-modified-descending, not sort order)
 * while the KEY bytes stay identical — a key-bytes-only defensive check can't see that, so the check
 * must use the full comparator.
 */
class SealedBufferMergeTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();

    @Test
    void happyPathMergesPerNodeRunsWithoutFallbackAndUsesFastPath() {
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(object("a"), object("b")));
        buffer.admit(1L, List.of(object("c"), object("d")));   // node 1 ascends across pages
        buffer.admit(2L, List.of(object("e"), object("f")));   // disjoint node 2

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"),
                object("d"), object("e"), object("f"));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isZero();
        assertThat(metrics.count("SORT.merge_fastpath")).isGreaterThan(0);   // disjoint ranges ⇒ fast path
    }

    @Test
    void outOfOrderPagesWithinANodeFallBackToTimSort() {
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(object("m"), object("z")));
        buffer.admit(1L, List.of(object("a"), object("c")));   // starts BELOW node 1's previous max ⇒ violation

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        // Correctness never depends on pre-sortedness: the fallback still yields the fully sorted stream.
        assertThat(out).containsExactly(object("a"), object("c"), object("m"), object("z"));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_fastpath")).isZero();
    }

    @Test
    void overlappingNodeRangesStillMergeCorrectlyOnHappyPath() {
        // Each node run is internally ascending (check passes) but the node RANGES overlap — the
        // k-way merge handles that; only intra-node disorder forces the fallback.
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(object("a"), object("c"), object("e")));
        buffer.admit(2L, List.of(object("b"), object("d"), object("f")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        assertThat(out).containsExactly(object("a"), object("b"), object("c"),
                object("d"), object("e"), object("f"));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isZero();
    }

    /** A key's versions out of §0.3 order INSIDE one page (no page boundary at all). */
    @Test
    void versionsOfOneKeyOutOfOrderWithinASinglePageTriggerFallbackAndSortCorrectly() {
        // Real S3 `--all-versions` delivers a key's versions last-modified-descending — here "v2"
        // arrives (and is admitted) before "v1", both on the SAME page. Key bytes tie (a
        // key-bytes-only check sees no violation at all, let alone across a page boundary), but
        // the full comparator says v1 < v2 (§0.3): this must be caught and fall back.
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(objectVersion("k", "v2"), objectVersion("k", "v1")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        assertThat(out).containsExactly(objectVersion("k", "v1"), objectVersion("k", "v2"));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isEqualTo(1);
    }

    /** A key's versions out of §0.3 order ACROSS a page boundary (same key, both pages). */
    @Test
    void versionsOfOneKeySpanningAPageBoundaryOutOfOrderTriggerFallback() {
        // Page 1 ends with (k, v2); page 2 starts with (k, v1). Do not check cross-page order by
        // comparing page.firstKey() to the previous page's lastKey() with compareUnsigned < 0 alone —
        // equal key bytes give 0, which is NOT negative, so a key-bytes-only check would miss this.
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(objectVersion("a", null), objectVersion("k", "v2")));
        buffer.admit(1L, List.of(objectVersion("k", "v1"), objectVersion("z", null)));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        assertThat(out).containsExactly(objectVersion("a", null), objectVersion("k", "v1"),
                objectVersion("k", "v2"), objectVersion("z", null));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isEqualTo(1);
    }

    /** A versioned buffer whose versions DO arrive in §0.3 order stays on the happy path (no over-triggering). */
    @Test
    void versionsOfOneKeyInCorrectOrderStayOnTheHappyPath() {
        SortBuffer buffer = new SortBuffer(config, cmp);
        buffer.admit(1L, List.of(objectVersion("k", "v1"), objectVersion("k", "v2")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        List<ListEntry> out = drain(sealed.merge(cmp, DuplicateHook.NO_OP, metrics));

        assertThat(out).containsExactly(objectVersion("k", "v1"), objectVersion("k", "v2"));
        assertThat(metrics.count("SORT.buffer_sort_fallback")).isZero();
    }

    private static ObjectEntry objectVersion(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, versionId, false, null, null, null, null);
    }
}
