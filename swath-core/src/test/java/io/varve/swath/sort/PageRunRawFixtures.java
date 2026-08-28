/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
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

    /**
     * Replace one persisted page maximum without changing its payload, then make the record CRC,
     * type-2 prefix maxima, final prefix maximum, and trailer maximum agree with the lie. Keys in
     * this deliberately narrow fixture helper must keep their encoded lengths so no physical offset
     * or cumulative byte claim changes.
     */
    static void understatePageMaxAndRepairIndex(Path path, int pageOrdinal, byte[] forgedMax)
            throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int trailerStart = Math.toIntExact(data.getLong(fixedTailStart));
        List<byte[]> pageMaxima = new ArrayList<>();
        int frameStart = PageRunSegmentWriter.HEADER_BYTES;
        int ordinal = 0;
        boolean changed = false;
        while (frameStart < trailerStart) {
            int bodyLength = data.getInt(frameStart);
            int bodyStart = frameStart + 2 * Integer.BYTES;
            int minLength = unsignedShort(bytes, bodyStart);
            int maxLengthPosition = bodyStart + Short.BYTES + minLength;
            int maxLength = unsignedShort(bytes, maxLengthPosition);
            int maxKeyPosition = maxLengthPosition + Short.BYTES;
            if (ordinal == pageOrdinal) {
                replaceSameLength(bytes, maxKeyPosition, maxLength, forgedMax, "page maxKey");
                rewriteRecordCrc(bytes, frameStart, bodyStart, bodyLength);
                changed = true;
            }
            pageMaxima.add(Arrays.copyOfRange(bytes, maxKeyPosition, maxKeyPosition + maxLength));
            frameStart += 2 * Integer.BYTES + bodyLength;
            ordinal++;
        }
        if (!changed) {
            throw new IllegalArgumentException("page ordinal is outside the segment: " + pageOrdinal);
        }

        byte[] persistedMaximum = pageMaxima.stream()
                .max(Arrays::compareUnsigned)
                .orElseThrow();
        int trailerMinLength = unsignedShort(bytes, trailerStart);
        int trailerMaxLengthPosition = trailerStart + Short.BYTES + trailerMinLength;
        int trailerMaxLength = unsignedShort(bytes, trailerMaxLengthPosition);
        int trailerMaxPosition = trailerMaxLengthPosition + Short.BYTES;
        replaceSameLength(bytes, trailerMaxPosition, trailerMaxLength,
                persistedMaximum, "trailer maxKey");

        int extensionStart = trailerMaxPosition + trailerMaxLength;
        if (data.getInt(extensionStart) != PageRunBoundarySample.MAGIC
                || data.getShort(extensionStart + Integer.BYTES) != PageRunPageIndex.TYPE) {
            throw new IllegalArgumentException("fixture requires a type-2 page index");
        }
        int entryCount = data.getInt(extensionStart + 3 * Integer.BYTES);
        int position = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        for (int entry = 0; entry < entryCount; entry++) {
            long sampleOrdinal = data.getLong(position);
            position += 4 * Long.BYTES;
            int minLength = unsignedShort(bytes, position);
            position += Short.BYTES + minLength;
            int prefixLength = unsignedShort(bytes, position);
            int prefixPosition = position + Short.BYTES;
            byte[] sampledPrefix = pageMaxima.subList(0, Math.toIntExact(sampleOrdinal) + 1)
                    .stream().max(Arrays::compareUnsigned).orElseThrow();
            replaceSameLength(bytes, prefixPosition, prefixLength,
                    sampledPrefix, "page-index prefixMax");
            position = prefixPosition + prefixLength;
        }
        int finalPrefixLength = unsignedShort(bytes, position);
        replaceSameLength(bytes, position + Short.BYTES, finalPrefixLength,
                persistedMaximum, "page-index finalPrefixMax");

        int extensionCrcPosition = fixedTailStart - PageRunBoundarySample.CRC_BYTES;
        CRC32C extensionCrc = new CRC32C();
        extensionCrc.update(bytes, extensionStart, extensionCrcPosition - extensionStart);
        data.putInt(extensionCrcPosition, (int) extensionCrc.getValue());
        Files.write(path, bytes);
    }

    /** Write a checksum-valid segment with a page bound one byte over the S3 key limit. */
    static Path writeCrcRepairedOverlongBound(Path path, boolean overlongMinimum) throws IOException {
        byte[] overlong = new byte[ByteMidpoint.MAX_KEY_LEN + 1];
        if (!overlongMinimum) {
            Arrays.fill(overlong, (byte) 'z');
        }
        List<ListEntry> rows = overlongMinimum
                ? List.of(objectWithKey(overlong), SortTestSupport.object("z"))
                : List.of(SortTestSupport.object("a"), objectWithKey(overlong));
        ListEntryComparator comparator = new ListEntryComparator();
        PageRunSegmentWriter writer = new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP,
                SortMetrics.NO_OP, PageCodec.NONE);
        try (SortedCursor cursor = new InMemoryCursor(rows, comparator, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        byte[] file = Files.readAllBytes(path);
        int frameOffset = PageRunSegmentWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(file, frameOffset, Integer.BYTES).getInt();
        int bodyOffset = frameOffset + 2 * Integer.BYTES;
        ByteBuffer body = ByteBuffer.wrap(file, bodyOffset, bodyLength).slice();
        int keyOffset = overlongMinimum
                ? Short.BYTES
                : Short.BYTES + (body.getShort(0) & 0xFFFF) + Short.BYTES;
        body.put(keyOffset, (byte) (body.get(keyOffset) - 1));
        rewriteRecordCrc(file, frameOffset, bodyOffset, bodyLength);
        Files.write(path, file);
        return path;
    }

    private static void rewriteRecordCrc(byte[] bytes, int frameStart, int bodyStart, int bodyLength) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, bodyStart, bodyLength);
        ByteBuffer.wrap(bytes).putInt(frameStart + Integer.BYTES, (int) crc.getValue());
    }

    private static void replaceSameLength(byte[] bytes, int position, int originalLength,
                                          byte[] replacement, String field) {
        if (replacement.length != originalLength) {
            throw new IllegalArgumentException(field + " replacement must keep encoded length");
        }
        System.arraycopy(replacement, 0, bytes, position, originalLength);
    }

    private static int unsignedShort(byte[] bytes, int position) {
        return ByteBuffer.wrap(bytes).getShort(position) & 0xffff;
    }

    private static ObjectEntry objectWithKey(byte[] key) {
        return new ObjectEntry(KeyBytes.of(key), 0L, 0L, null, null, null,
                false, null, null, null, null);
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
