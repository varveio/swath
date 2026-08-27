/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32C;

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
 * first). This is strictly per-page — it never merges node runs into one global stream.
 *
 * <p><b>On-disk format, big-endian:</b>
 * <pre>
 * [magic u32][format-version u16 = 1]
 * record* : [len u32][crc32c u32][ &lt;PageBlock.serialize() body&gt; ]   // crc32c over the body bytes
 * trailer : [segMinKey u16-len-prefixed][segMaxKey u16-len-prefixed]
 *           [optional boundary-sample extension]
 *           [trailerStart u64][totalRecords u32][totalEntries u64][maxRecordLen u32][magic u32]
 * </pre>
 * {@code segMinKey}/{@code segMaxKey} are the ACTUAL first/last keys (first page's {@code firstKey()},
 * last page's {@code lastKey()}) — the drop-in for {@code SortedFileIndex.bounds} with no
 * truncated-stats hazard. {@code trailerStart} is the absolute file offset where the trailer begins
 * (where {@code segMinKey}'s length prefix starts) — read from the fixed EOF-relative tail, it lets a
 * reader seek straight to the bounds in O(1) instead of walking every record's length prefix.
 * {@code maxRecordLen} is the largest framed body length (the runtime merge fan-in planner uses it
 * to tighten its configured per-stream estimate, and the reader uses it to bound a claimed length before
 * allocating). A listing-phase segment's optional extension stores the exact capped systematic
 * page-minimum sample used by the parallel merge while preserving format version 1 and the fixed
 * 28-byte EOF tail; post-boundary cascade intermediates omit it. The trailer is written LAST: with
 * the file-then-directory fsync below, a half-written page-run file has no valid trailer and is
 * discarded whole on resume (I6 — durable iff finalized; segment-granularity, not sub-file).
 */
final class PageRunSegmentWriter {

    /** Segment magic: ASCII "SPGR" (swath Page-Run). Appears in the header and again at trailer end. */
    static final int MAGIC = 0x53504752;

    /** On-disk format version. */
    static final short FORMAT_VERSION = 1;

    /** The segment-format name constant, wired into the checkpoint/segment-IO selector. */
    static final String FORMAT_NAME = "page-run";

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

    private static final byte[] EMPTY_KEY = new byte[0];

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

        // Re-pack any page that isn't internally ordered under the full comparator. pack() does
        // NOT reorder — it packs in the given order and sets the ordered-bit — so we must sort first.
        // Note: a page whose entries are correctly ordered but contain an adjacent TIE (equal under
        // the comparator) re-packs with orderedUnderFullComparator() still FALSE (pack() treats any
        // adjacent tie as non-ordered) even though the read-back byte order is fine — so the ordered
        // bit is not a reliable post-seal "is this page sorted" proxy; the reader's min-monotonicity
        // guard must derive from actual key comparisons, not this bit.
        boolean repacked = false;
        for (int i = 0; i < pages.size(); i++) {
            PageBlock page = pages.get(i);
            if (!page.orderedUnderFullComparator()) {
                List<ListEntry> entries = new ArrayList<>(page.count());
                PageBlock.Cursor c = page.cursor();
                while (c.hasNext()) {
                    entries.add(c.next());
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
        pages.sort((a, b) -> Arrays.compareUnsigned(a.firstKey(), b.firstKey()));

        long totalEntries = writePageRun(path, pages);

        metrics.recordStealReason("SORT", "segment_flushed");
        if (buffer.trigger() == SealTrigger.BYTE_GATE) {
            metrics.recordStealReason("SORT", "buffer_byte_gated");
        }
        return new SegmentResult(path, totalEntries, Files.size(path), buffer.perNodeMaxKeys());
    }

    /**
     * The CASCADE backstop (the multi-pass merge path taken when the runtime-clamped fan-in falls below
     * the staging-segment count): batch an already-sorted {@link SortedCursor} into
     * pages of {@link #INTERMEDIATE_PAGE_ENTRIES} and write them as a page-run segment. The caller
     * closes {@code sorted}. Streams one page at a time so memory stays bounded; because boundary
     * selection has already completed, this path omits the unused sample extension. Returns total rows.
     */
    long writeIntermediate(SortedCursor sorted, Path path) throws IOException {
        return writeSorted(sorted, path);
    }

    private long writeSorted(SortedCursor sorted, Path path) throws IOException {
        long totalEntries = 0;
        int totalRecords = 0;
        int maxRecordLen = 0;
        byte[] segMin = EMPTY_KEY;
        byte[] segMax = EMPTY_KEY;

        try (FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writeHeader(ch);
            List<ListEntry> batch = new ArrayList<>(INTERMEDIATE_PAGE_ENTRIES);
            while (sorted.hasNext()) {
                batch.add(sorted.next());
                if (batch.size() == INTERMEDIATE_PAGE_ENTRIES) {
                    PageBlock page = PageBlock.pack(batch, comparator, codec);
                    if (totalRecords == 0) {
                        segMin = page.firstKey();
                    }
                    segMax = page.lastKey();
                    maxRecordLen = Math.max(maxRecordLen, writeFrame(ch, page.serialize()));
                    totalEntries += page.count();
                    totalRecords++;
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                PageBlock page = PageBlock.pack(batch, comparator, codec);
                if (totalRecords == 0) {
                    segMin = page.firstKey();
                }
                segMax = page.lastKey();
                maxRecordLen = Math.max(maxRecordLen, writeFrame(ch, page.serialize()));
                totalEntries += page.count();
                totalRecords++;
            }
            long trailerStart = ch.position();
            writeTrailer(ch, segMin, segMax, null, trailerStart, totalRecords, totalEntries,
                    maxRecordLen);
            ch.force(true);
        }
        Durability.directory(path.getParent());
        return totalEntries;
    }

    /**
     * Write {@code pages} (already per-page re-sorted, already in minKey concatenation order) as a page-run
     * segment: header, framed records, trailer, then fsync file + parent dir. Returns total entries.
     * Applies the {@link DuplicateHook} at page boundaries (adjacent equal {@code prev.lastEntry} /
     * {@code next.firstEntry}) — see the class-level note on why intra-page dups are not scanned.
     */
    private long writePageRun(Path path, List<PageBlock> pages) throws IOException {
        long totalEntries = 0;
        int maxRecordLen = 0;
        PageBlock prev = null;

        try (FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeHeader(ch);
            for (PageBlock page : pages) {
                if (prev != null && comparator.compare(prev.lastEntry(), page.firstEntry()) == 0) {
                    hook.onDuplicate(prev.lastEntry(), page.firstEntry());
                }
                maxRecordLen = Math.max(maxRecordLen, writeFrame(ch, page.serialize()));
                totalEntries += page.count();
                prev = page;
            }
            byte[] segMin = pages.isEmpty() ? EMPTY_KEY : pages.get(0).firstKey();
            byte[] segMax = pages.isEmpty() ? EMPTY_KEY : pages.get(pages.size() - 1).lastKey();
            long trailerStart = ch.position();
            writeTrailer(ch, segMin, segMax, PageRunBoundarySample.select(pages), trailerStart,
                    pages.size(), totalEntries, maxRecordLen);
            ch.force(true);
        }
        Durability.directory(path.getParent());
        return totalEntries;
    }

    private static void writeHeader(FileChannel ch) throws IOException {
        ByteBuffer h = ByteBuffer.allocate(HEADER_BYTES);
        h.putInt(MAGIC);
        h.putShort(FORMAT_VERSION);
        writeFully(ch, h.flip());
    }

    /** Frame one record: {@code [len u32][crc32c u32][body]}. Returns the body length. */
    private static int writeFrame(FileChannel ch, byte[] body) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        ByteBuffer header = ByteBuffer.allocate(8);
        header.putInt(body.length);
        header.putInt((int) crc.getValue());
        writeFully(ch, header.flip());
        // PageBlock.serialize() already returned the exact body array. Write it directly instead of
        // allocating and copying a second page-sized [header + body] buffer on every staged page.
        writeFully(ch, ByteBuffer.wrap(body));
        return body.length;
    }

    /** {@code sample == null} emits the legacy extension-free trailer used by cascade intermediates. */
    private static void writeTrailer(FileChannel ch, byte[] segMin, byte[] segMax, List<byte[]> sample,
                                     long trailerStart, int totalRecords, long totalEntries,
                                     int maxRecordLen) throws IOException {
        ByteBuffer bounds = ByteBuffer.allocate(2 + segMin.length + 2 + segMax.length);
        bounds.putShort((short) segMin.length).put(segMin);
        bounds.putShort((short) segMax.length).put(segMax);
        writeFully(ch, bounds.flip());
        if (sample != null) {
            PageRunBoundarySample.write(ch, sample);
        }
        ByteBuffer t = ByteBuffer.allocate(TRAILER_FIXED_TAIL_BYTES);
        t.putLong(trailerStart);
        t.putInt(totalRecords);
        t.putLong(totalEntries);
        t.putInt(maxRecordLen);
        t.putInt(MAGIC);
        writeFully(ch, t.flip());
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }
}
