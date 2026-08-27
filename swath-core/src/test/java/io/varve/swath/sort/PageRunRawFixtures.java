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
 * RAW (writer-bypassing) page-run segment fixtures — the seam the read-side min-regression guard
 * needs and the rest of the suite structurally cannot reach.
 *
 * <p><b>Why this exists.</b> Every other page-run fixture in the suite stages through
 * {@link PageRunSegmentWriter#flush}, which <em>sorts pages by first key</em> before framing them. That
 * writer-side sort is precisely the precondition the {@link PageAwareMerger}'s whole-page fast path
 * depends on ("within a segment, page {@code minKey}s are non-decreasing"), so a fixture built through
 * {@code flush()} can never express a violation of it — the suite is structurally incapable of testing
 * whether the READ side verifies the precondition or merely trusts it. {@link #writeRawPageRun} frames
 * the pages <b>in the exact order given</b>, producing a segment that is byte-perfect in every physical
 * respect (header magic/version, per-record {@code [len][crc32c]} framing, complete trailer with real
 * {@code trailerStart}/{@code totalRecords}/{@code totalEntries}/{@code maxRecordLen} and the trailing
 * magic) and <b>differs from a writer-produced segment only in page ORDER</b>. Every physical check a
 * reader makes therefore passes, and page order is the single variable under test.
 *
 * <p>{@link #pageMinKeysInFileOrder}, {@link TrustingPageFrontier} and {@link #trustingEntryStream}
 * deliberately re-implement the frame walk instead of reusing {@link PageFrontierReader} /
 * {@link PageRunSegmentReader}: both production readers now <em>reject</em> a min regression,
 * enforced in the shared {@link PageRunSegmentIo#nextPage()} primitive, so a test that needs to observe
 * (rather than trust) what is physically stored — or to reproduce a read path that does not enforce that
 * guard on either route — cannot go through them.
 */
final class PageRunRawFixtures {

    private PageRunRawFixtures() {
    }

    /**
     * Hand-frame {@code pages} into a well-formed {@code .pageseg} at {@code path}, <b>in the given
     * order</b> (no sort by first key — that is the whole point). Each inner list is exactly one
     * {@link PageBlock} record, packed verbatim: entries are NOT reordered, so a caller that wants an
     * internally-ordered page must hand one in (this mirrors {@link PageBlock#pack}, which never
     * reorders either).
     */
    static void writeRawPageRun(Path path, List<List<ListEntry>> pages, Comparator<ListEntry> comparator)
            throws IOException {
        List<PageBlock> blocks = new ArrayList<>(pages.size());
        for (List<ListEntry> page : pages) {
            blocks.add(PageBlock.pack(page, comparator, PageCodec.NONE));
        }

        long totalEntries = 0;
        int maxRecordLen = 0;
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(PageRunSegmentWriter.HEADER_BYTES);
            header.putInt(PageRunSegmentWriter.MAGIC);
            header.putShort(PageRunSegmentWriter.FORMAT_VERSION);
            SortTestSupport.writeFully(ch, header.flip());

            for (PageBlock block : blocks) {
                byte[] body = block.serialize();
                CRC32C crc = new CRC32C();
                crc.update(body, 0, body.length);
                ByteBuffer frame = ByteBuffer.allocate(8 + body.length);
                frame.putInt(body.length);
                frame.putInt((int) crc.getValue());
                frame.put(body);
                SortTestSupport.writeFully(ch, frame.flip());
                totalEntries += block.count();
                maxRecordLen = Math.max(maxRecordLen, body.length);
            }

            byte[] segMin = new byte[0];
            byte[] segMax = new byte[0];
            boolean haveBounds = false;
            for (PageBlock block : blocks) {
                byte[] pageMin = block.firstKey();
                byte[] pageMax = block.lastKey();
                if (!haveBounds || Arrays.compareUnsigned(pageMin, segMin) < 0) {
                    segMin = pageMin;
                }
                if (!haveBounds || Arrays.compareUnsigned(pageMax, segMax) > 0) {
                    segMax = pageMax;
                }
                haveBounds = true;
            }
            long trailerStart = ch.position();
            ByteBuffer trailer = ByteBuffer.allocate(2 + segMin.length + 2 + segMax.length
                    + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
            trailer.putShort((short) segMin.length).put(segMin);
            trailer.putShort((short) segMax.length).put(segMax);
            trailer.putLong(trailerStart);
            trailer.putInt(blocks.size());
            trailer.putLong(totalEntries);
            trailer.putInt(maxRecordLen);
            trailer.putInt(PageRunSegmentWriter.MAGIC);
            SortTestSupport.writeFully(ch, trailer.flip());
            ch.force(true);
        }
    }

    /** The per-page {@code minKey}s in physical FILE order — proves what a fixture actually stores. */
    static List<byte[]> pageMinKeysInFileOrder(Path path) throws IOException {
        List<byte[]> mins = new ArrayList<>();
        for (PageBlock page : readPages(path)) {
            mins.add(page.firstKey());
        }
        return mins;
    }

    /**
     * A read path that trusts page order rather than verifying it, preserved as a test double: a
     * {@link PageFrontierStream} that walks a segment's pages in file order and does not check that
     * they ascend. Driving the real {@link PageAwareMerger} through this shows what the merger does
     * when its precondition is violated (it silently misorders) — the read-time guard in
     * {@link PageFrontierReader#advance()} is load-bearing, not belt-and-braces.
     */
    static final class TrustingPageFrontier implements PageFrontierStream {

        private final List<PageBlock> pages;
        private int index;

        TrustingPageFrontier(Path path) throws IOException {
            this.pages = readPages(path);
        }

        @Override
        public boolean hasPage() {
            return index < pages.size();
        }

        @Override
        public byte[] minKey() {
            return pages.get(index).firstKey();
        }

        @Override
        public byte[] maxKey() {
            return pages.get(index).lastKey();
        }

        @Override
        public int count() {
            return pages.get(index).count();
        }

        @Override
        public PageBlock decodeCurrentPage() {
            return pages.get(index);
        }

        @Override
        public void advance() {
            index++;
        }

        @Override
        public void close() {
        }
    }

    /**
     * The ENTRY-typed reader's read path without the monotonicity check, preserved as a test double: an
     * {@link EntryStream} that decodes a segment's pages in FILE order and concatenates their entries,
     * <b>trusting</b> that the pages ascend — exactly what {@link PageRunSegmentReader#readNext()} did
     * with no monotonicity check. This is the {@link StreamingMerger}-route sibling of
     * {@link TrustingPageFrontier}: driving the real {@link StreamingMerger} through it (as
     * the generic {@code KWayMerge} entry-stream seam does for a non-frontier store, so
     * {@code allSupportPageFrontier} is false) shows what the merge does when a page-run input is not
     * actually a sorted run — it silently misorders, which is why the guard lives in the shared IO layer
     * rather than in the frontier reader alone.
     */
    static EntryStream trustingEntryStream(Path path) throws IOException {
        return new PageRunCursor(readPages(path));
    }

    /** Walk the framed records of a page-run segment (test-owned framing walk — see the class javadoc). */
    private static List<PageBlock> readPages(Path path) throws IOException {
        byte[] all = Files.readAllBytes(path);
        ByteBuffer buf = ByteBuffer.wrap(all);

        // Fixed trailer tail (from EOF): trailerStart u64 tells us where the records stop.
        ByteBuffer tail = ByteBuffer.wrap(all, all.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES,
                PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES).slice();
        long trailerStart = tail.getLong();

        buf.position(PageRunSegmentWriter.HEADER_BYTES);
        List<PageBlock> pages = new ArrayList<>();
        while (buf.position() < trailerStart) {
            int len = buf.getInt();
            buf.getInt();   // crc32c — the production readers verify it; this walk only needs the body
            byte[] body = new byte[len];
            buf.get(body);
            pages.add(PageBlock.deserialize(body));
        }
        return pages;
    }

}
