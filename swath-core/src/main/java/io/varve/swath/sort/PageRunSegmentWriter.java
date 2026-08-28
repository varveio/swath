/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Persists a sealed buffer as ONE <b>page-run</b> staging segment: a stream of
 * length-delimited, CRC32C-framed {@link PageBlock} records plus a completeness trailer — the
 * page-oriented staging format used by both live listing and offline fixture sorting.
 *
 * <p><b>Pack once, concatenate — no seal-time merge.</b> This writer orders the buffer's pages by
 * {@link PageBlock#firstKey()} and writes each page's {@link PageBlock#serialize()} bytes verbatim,
 * in minKey order. Correctness rests on the OBJECTS invariant that work-stealing nodes own disjoint
 * key ranges, so pages are range-disjoint by construction and concatenating them in minKey order is
 * globally sorted. The read-time intra-segment guard is the reader's job, not this writer's — and it
 * rejects only a page whose minKey regresses below the previous page's (decreasing minKey); equal or
 * ascending minima whose ranges overlap are legal and resolved by the merger's key-merge fallback.
 *
 * <p><b>Per-page re-sort.</b> Every persisted page must be internally ordered under the full
 * §0.3 comparator. A page that arrived out of order ({@code !orderedUnderFullComparator()}) is
 * re-packed: its entries are drained, sorted with {@code comparator}, and {@link PageBlock#pack}-ed
 * again (pack sets the ordered-bit for a pre-sorted list but never reorders, so the sort must happen
 * first). Repair is allowed only while raw keys remain non-decreasing: {@link SortBuffer}'s
 * checkpoint maximum comes from the admitted page's last raw key, so a raw-key regression is
 * rejected before persistence rather than sorted into a segment whose durable cursor would be too
 * low. This is strictly per-page — it never merges node runs into one global stream.
 *
 * <p><b>On-disk format, big-endian:</b>
 * <pre>
 * [magic u32][format-version u16 = 1]
 * record* : [len u32][crc32c u32][ &lt;PageBlock.serialize() body&gt; ]   // crc32c over the body bytes
 * trailer : [segMinKey u16-len-prefixed][segMaxKey u16-len-prefixed]
 *           [optional trailer extension: type-1 minima, legacy type-2 index, or type-3 page index]
 *           [trailerStart u64][totalRecords u32][totalEntries u64][maxRecordLen u32][magic u32]
 * </pre>
 * {@code segMinKey}/{@code segMaxKey} are the ACTUAL unsigned minimum of all page minima and
 * unsigned maximum of all page maxima — the drop-in for {@code SortedFileIndex.bounds} with no
 * truncated-stats hazard. {@code trailerStart} is the absolute file offset where the trailer begins
 * (where {@code segMinKey}'s length prefix starts) — read from the fixed EOF-relative tail, it lets a
 * reader seek straight to the bounds in O(1) instead of walking every record's length prefix.
 * {@code maxRecordLen} is the largest framed body length (the runtime merge fan-in planner uses it
 * to tighten its configured per-stream estimate, and the reader uses it to bound a claimed length before
 * allocating). A listing-phase segment's optional extension stores the exact capped systematic
 * sparse page-offset index plus the exact largest decoded page payload used by merge planning, while
 * preserving format version 1 and the fixed 28-byte EOF tail; post-boundary cascade intermediates
 * omit it. The trailer is written LAST: with
 * the file-then-directory fsync below, a half-written page-run file has no valid trailer and is
 * discarded whole on resume (I6 — durable iff finalized; segment-granularity, not sub-file).
 */
final class PageRunSegmentWriter {

    /** Segment magic: ASCII "SPGR" (swath Page-Run). Appears in the header and again at trailer end. */
    static final int MAGIC = 0x53504752;

    /** On-disk format version. */
    static final short FORMAT_VERSION = PageRunFormat.CURRENT_FORMAT_VERSION;

    /** The segment-format name constant, wired into the checkpoint/segment-IO selector. */
    static final String FORMAT_NAME = PageRunFormat.NAME;

    /** Header size: magic u32 + version u16. Package-private: the reader shares this constant instead
     *  of open-coding {@code 6}. */
    static final int HEADER_BYTES = 6;

    /** Fixed trailer tail after the two length-prefixed keys: trailerStart u64 + totalRecords u32 +
     *  totalEntries u64 + maxRecordLen u32 + magic u32 = 28 bytes. The reader reads exactly this
     *  (positioned from EOF) to recover {@code trailerStart} (an O(1) seek straight to the key bounds,
     *  no per-record walk), {@code totalRecords}/{@code totalEntries} (end-of-stream completeness
     *  cross-check), {@code maxRecordLen} (the per-record length bound), and validate the trailing
     *  magic (truncation check) — all without scanning records. */
    static final int TRAILER_FIXED_TAIL_BYTES = 8 + 4 + 8 + 4 + 4;

    /** Rows per page when batching an already-sorted {@link SortedCursor} in {@link #writeIntermediate}
     *  (the CASCADE backstop — the multi-pass merge path taken when the runtime-clamped fan-in falls
     *  below the staging-segment count). Matches the S3-pagination page grain. */
    private static final int INTERMEDIATE_PAGE_ENTRIES = 1000;

    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final PageCodec codec;

    PageRunSegmentWriter(Comparator<ListEntry> comparator, DuplicateHook hook, SortMetrics metrics,
                        PageCodec codec) {
        this.comparator = comparator;
        this.hook = hook;
        this.metrics = metrics;
        this.codec = codec;
    }

    /**
     * Write the sealed buffer's pages as a page-run segment at {@code path}: re-sort any
     * out-of-order page, order all pages by {@code firstKey()}, concatenate them framed, then the
     * trailer; fsync file + parent dir. Returns the {@link SegmentResult} the checkpoint consumes
     * ({@code perNodeMaxKeys} comes from the in-memory seal, not the file).
     */
    SegmentResult flush(SealedBuffer buffer, Path path) throws IOException {
        List<PageBlock> pages = buffer.pages();   // already a fresh, mutable, sortable list

        // Re-pack any page with a full-comparator regression. pack() preserves input order and sets
        // the ordered bit; comparator-equal adjacent entries are already ordered and remain stable.
        boolean repacked = false;
        for (int i = 0; i < pages.size(); i++) {
            PageBlock page = pages.get(i);
            if (!page.orderedUnderFullComparator()) {
                List<ListEntry> entries = new ArrayList<>(page.count());
                PageBlockCursor c = page.cursor();
                byte[] previousRawKey = null;
                while (c.hasNext()) {
                    ListEntry entry = c.next();
                    byte[] rawKey = entry.key().rawUnsafe();
                    if (previousRawKey != null
                            && Arrays.compareUnsigned(rawKey, previousRawKey) < 0) {
                        metrics.recordStealReason("SORT", "buffer_page_raw_key_regression");
                        throw new SegmentCorruptionException(path,
                                SegmentCorruptionException.PAGE_RUN_RAW_KEY_REGRESSION,
                                "raw key regressed inside an admitted page; checkpoint durable "
                                        + "cursors require non-decreasing raw keys even when the "
                                        + "full comparator order needs repair");
                    }
                    previousRawKey = rawKey;
                    entries.add(entry);
                }
                entries.sort(comparator);
                pages.set(i, PageBlock.pack(entries, comparator, codec));
                repacked = true;
            }
        }
        if (repacked) {
            metrics.recordStealReason("SORT", "buffer_page_repacked");
        }

        // Concatenation order: minKey (unsigned) across all node runs.
        pages.sort((a, b) -> Arrays.compareUnsigned(a.firstKeyUnsafe(), b.firstKeyUnsafe()));

        long totalEntries;
        try (PageRunSegmentEncoder encoder = PageRunSegmentEncoder.open(path, metrics,
                PageRunPageIndex.exactBuilder(pages.size()))) {
            PageBlock previous = null;
            for (PageBlock page : pages) {
                if (previous != null
                        && comparator.compare(previous.lastEntry(), page.firstEntry()) == 0) {
                    hook.onDuplicate(previous.lastEntry(), page.firstEntry());
                }
                encoder.append(page);
                previous = page;
            }
            totalEntries = encoder.finish(SegmentKind.LISTING);
        }

        if (buffer.trigger() == SealTrigger.BYTE_GATE) {
            metrics.recordStealReason("SORT", "buffer_byte_gated");
        }
        return new SegmentResult(path, totalEntries, Files.size(path), buffer.perNodeMaxKeys(),
                PageRunFormat.currentListing());
    }

    /**
     * The CASCADE backstop (the multi-pass merge path taken when the runtime-clamped fan-in falls below
     * the staging-segment count): batch an already-sorted {@link SortedCursor} into
     * pages of {@link #INTERMEDIATE_PAGE_ENTRIES} and write them as a page-run segment. The caller
     * closes {@code sorted}. Streams one page at a time so memory stays bounded; because boundary
     * selection has already completed, this path omits the unused index extension. Returns total rows.
     */
    long writeIntermediate(SortedCursor sorted, Path path) throws IOException {
        return writeIntermediate(sorted, path, PageBlock.MAX_RAW_PAYLOAD_BYTES);
    }

    /** Write a cascade whose generated pages cannot exceed the merge-planned decoded-page price. */
    long writeIntermediate(SortedCursor sorted, Path path, int maxRawPayloadBytes)
            throws IOException {
        return writeSorted(sorted, path, SegmentKind.CASCADE_INTERMEDIATE, maxRawPayloadBytes);
    }

    /** Write one locally sorted fixture chunk without retaining an unused boundary sample. */
    long writeFixtureChunk(SortedCursor sorted, Path path) throws IOException {
        return writeSorted(sorted, path, SegmentKind.FIXTURE_CHUNK,
                PageBlock.MAX_RAW_PAYLOAD_BYTES);
    }

    private long writeSorted(SortedCursor sorted, Path path, SegmentKind kind,
                             int maxRawPayloadBytes) throws IOException {
        try (PageRunSegmentEncoder encoder = PageRunSegmentEncoder.open(path, metrics, null)) {
            List<ListEntry> batch = new ArrayList<>(INTERMEDIATE_PAGE_ENTRIES);
            while (sorted.hasNext()) {
                batch.add(sorted.next());
                if (batch.size() == INTERMEDIATE_PAGE_ENTRIES) {
                    appendBounded(encoder, batch, maxRawPayloadBytes);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                appendBounded(encoder, batch, maxRawPayloadBytes);
            }
            return encoder.finish(kind);
        }
    }

    /** Split before serialization allocation can cross the admitted decoded-page ceiling. */
    private void appendBounded(PageRunSegmentEncoder encoder, List<ListEntry> entries,
                               int maxRawPayloadBytes) throws IOException {
        try {
            encoder.append(PageBlock.pack(entries, comparator, codec, maxRawPayloadBytes));
        } catch (PageBlock.RawPayloadLimitException tooLarge) {
            if (entries.size() == 1) {
                throw new IOException("one cascade row exceeds the planned decoded-page limit of "
                        + maxRawPayloadBytes + " bytes", tooLarge);
            }
            int split = entries.size() / 2;
            appendBounded(encoder, entries.subList(0, split), maxRawPayloadBytes);
            appendBounded(encoder, entries.subList(split, entries.size()), maxRawPayloadBytes);
        }
    }
}
