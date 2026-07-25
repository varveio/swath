/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;

/**
 * A <b>decode-free page frontier</b> over one page-run staging segment — the seam the
 * {@link PageAwareMerger} drives instead of an entry-at-a-time {@link EntryStream}. It walks a
 * segment one {@link PageBlock} record at a time and exposes the CURRENT page's key bounds and entry
 * count <b>without materializing any row</b>: the framed record body is read and CRC-verified (cheap)
 * and only its leading {@code [minKeyLen u16][minKey][maxKeyLen u16][maxKey][count u32]} fields are
 * parsed. The expensive front-code row decode ({@link PageBlock#deserialize}/{@link PageBlock#cursor})
 * is deferred to {@link #decodeCurrentPage()}, which the merger calls only when a page is actually
 * emitted (whole, or via a key-level merge over interleaving pages).
 *
 * <p>Pages are visited in the segment's stored order (minKey order for a well-formed segment). The
 * merger uses {@link #minKey()}/{@link #maxKey()} of adjacent pages — both across segments and, on
 * {@link #advance()}, within one segment — to decide whether the minimum page is globally disjoint
 * (emit whole) or interleaves (decode + key-merge); this reader itself makes no ordering judgement.
 */
interface PageFrontierStream extends AutoCloseable {

    /** True iff a current page is loaded (a frontier is available). */
    boolean hasPage();

    /**
     * The current page's minimum key (its first entry's key) — parsed from the record body's leading
     * field, no row decode. The returned array is the reader's internal buffer: <b>read-only</b>, not
     * defensively copied (hot merge path). Valid only while {@link #hasPage()}.
     */
    byte[] minKey();

    /**
     * The current page's maximum key (its last entry's key) — parsed from the record body's leading
     * field, no row decode. Same read-only, no-copy contract as {@link #minKey()}. Valid only while
     * {@link #hasPage()}.
     */
    byte[] maxKey();

    /** The current page's entry count — no row decode. Valid only while {@link #hasPage()}. */
    int count();

    /**
     * Decode the current page's rows into a {@link PageBlock} (the deferred, expensive step). Does
     * NOT advance — call {@link #advance()} afterwards to move on. Valid only while {@link #hasPage()}.
     */
    PageBlock decodeCurrentPage() throws IOException;

    /** Advance to the next page's frontier, or to exhausted ({@link #hasPage()} becomes false). */
    void advance() throws IOException;

    @Override
    void close() throws IOException;
}
