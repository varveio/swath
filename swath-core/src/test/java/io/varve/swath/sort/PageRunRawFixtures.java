/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.CommonPrefixEntry;
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
public final class PageRunRawFixtures {

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

    /**
     * Write the Blocker-1 crux page: decoded rows {@code [a,z,m]} behind checksum-valid persisted
     * bounds {@code [a,m]}, with a coherent production type-2 index and trailer. The production
     * writer first creates the canonical {@code [a,m,z]} listing page; the fixture then changes only
     * the two row-key payload bytes and repairs every persisted claim around that mutation.
     */
    static Path writeIndexedInteriorRowRegression(Path path) throws IOException {
        ListEntryComparator comparator = new ListEntryComparator();
        SortBuffer buffer = new SortBuffer(
                SortConfigs.base().withSegmentCodec(PageCodec.NONE), comparator);
        buffer.admit(0L, List.of(
                prefix("a"), prefix("m"), prefix("z")));
        new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);

        byte[] bytes = Files.readAllBytes(path);
        int frameStart = PageRunSegmentWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(bytes).getInt(frameStart);
        int bodyStart = frameStart + 2 * Integer.BYTES;
        ByteBuffer body = ByteBuffer.wrap(bytes, bodyStart, bodyLength).slice();
        int payloadStart = pageHeaderLayout(body).payloadOffset();
        // CommonPrefix rows with one-byte keys each encode as [tag][shared=0][suffix=1][key].
        int secondKey = bodyStart + payloadStart + 7;
        int thirdKey = bodyStart + payloadStart + 11;
        if (bytes[secondKey] != 'm' || bytes[thirdKey] != 'z') {
            throw new IllegalStateException("unexpected canonical crux-page payload layout");
        }
        bytes[secondKey] = 'z';
        bytes[thirdKey] = 'm';
        Files.write(path, bytes);

        understatePageMaxAndRepairIndex(path, 0, new byte[]{'m'});
        return path;
    }

    /** Write a checksum-valid segment with a page bound one byte over the S3 key limit. */
    static Path writeCrcRepairedOverlongBound(Path path, boolean overlongMinimum) throws IOException {
        byte[] atLimit = new byte[ByteMidpoint.MAX_KEY_LEN];
        if (!overlongMinimum) {
            Arrays.fill(atLimit, (byte) 'z');
        }
        List<ListEntry> rows = overlongMinimum
                ? List.of(objectWithKey(atLimit), SortTestSupport.object("z"))
                : List.of(SortTestSupport.object("a"), objectWithKey(atLimit));
        ListEntryComparator comparator = new ListEntryComparator();
        PageBlock block = PageBlock.pack(rows, comparator, PageCodec.NONE);
        byte[] validBody = block.serialize();
        int minLength = unsignedShort(validBody, 0);
        int lengthPosition = overlongMinimum ? 0 : Short.BYTES + minLength;
        int oldLength = unsignedShort(validBody, lengthPosition);
        int insertion = lengthPosition + Short.BYTES + oldLength;
        byte[] malformedBody = new byte[validBody.length + 1];
        System.arraycopy(validBody, 0, malformedBody, 0, insertion);
        malformedBody[insertion] = overlongMinimum ? 0 : (byte) 'z';
        System.arraycopy(validBody, insertion, malformedBody, insertion + 1,
                validBody.length - insertion);
        ByteBuffer.wrap(malformedBody).putShort(lengthPosition, (short) (oldLength + 1));

        // Frame the malformed body directly. The production writer now enforces the same key limit
        // as the reader, so corruption fixtures must bypass it rather than relying on asymmetry.
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(PageRunSegmentWriter.HEADER_BYTES)
                    .putInt(PageRunSegmentWriter.MAGIC)
                    .putShort(PageRunSegmentWriter.FORMAT_VERSION);
            SortTestSupport.writeFully(ch, header.flip());
            CRC32C crc = new CRC32C();
            crc.update(malformedBody, 0, malformedBody.length);
            ByteBuffer frame = ByteBuffer.allocate(2 * Integer.BYTES + malformedBody.length)
                    .putInt(malformedBody.length)
                    .putInt((int) crc.getValue())
                    .put(malformedBody);
            SortTestSupport.writeFully(ch, frame.flip());
            long trailerStart = ch.position();
            byte[] segMin = block.firstKey();
            byte[] segMax = block.lastKey();
            ByteBuffer trailer = ByteBuffer.allocate(2 + segMin.length + 2 + segMax.length
                            + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES)
                    .putShort((short) segMin.length).put(segMin)
                    .putShort((short) segMax.length).put(segMax)
                    .putLong(trailerStart).putInt(1).putLong(block.count())
                    .putInt(malformedBody.length).putInt(PageRunSegmentWriter.MAGIC);
            SortTestSupport.writeFully(ch, trailer.flip());
            ch.force(true);
        }
        return path;
    }

    /** Raw test mutation, intentionally independent of the production sparse-index parser. */
    public static void repairCrcAfterSecondEntryCumulativeLie(Path segment) throws IOException {
        byte[] bytes = Files.readAllBytes(segment);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int trailerStart = Math.toIntExact(data.getLong(fixedTailStart));
        int position = trailerStart;
        position += Short.BYTES + unsignedShort(bytes, position);
        position += Short.BYTES + unsignedShort(bytes, position);
        int extensionStart = position;
        if (data.getInt(extensionStart) != PageRunBoundarySample.MAGIC) {
            throw new IllegalStateException("fixture requires the page-index extension magic");
        }
        if (data.getShort(extensionStart + Integer.BYTES)
                != (short) PageRunFormat.PAGE_INDEX_EXTENSION) {
            throw new IllegalStateException("fixture requires the current page-index extension");
        }
        int entryCount = data.getInt(extensionStart + 3 * Integer.BYTES);
        if (entryCount <= 2) {
            throw new IllegalStateException("fixture requires more than two page-index entries");
        }
        int firstEntry = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        int firstMinLength = unsignedShort(bytes, firstEntry + 4 * Long.BYTES);
        int firstPrefixLengthPosition = firstEntry + 4 * Long.BYTES + Short.BYTES
                + firstMinLength;
        int firstPrefixLength = unsignedShort(bytes, firstPrefixLengthPosition);
        int secondEntry = firstPrefixLengthPosition + Short.BYTES + firstPrefixLength;
        long cumulativeEntries = data.getLong(secondEntry + 2 * Long.BYTES);
        data.putLong(secondEntry + 2 * Long.BYTES, cumulativeEntries + 1);
        int crcPosition = fixedTailStart - PageRunBoundarySample.CRC_BYTES;
        CRC32C crc = new CRC32C();
        crc.update(bytes, extensionStart, crcPosition - extensionStart);
        data.putInt(crcPosition, (int) crc.getValue());
        Files.write(segment, bytes);
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

    /** Shared corruption-fixture parser for offsets within one serialized page body. */
    static PageHeaderLayout pageHeaderLayout(byte[] body) {
        return pageHeaderLayout(ByteBuffer.wrap(body));
    }

    /** Shared corruption-fixture parser for offsets within one serialized page body. */
    static PageHeaderLayout pageHeaderLayout(ByteBuffer body) {
        ByteBuffer cursor = body.duplicate();
        int position = Short.BYTES + unsignedShort(cursor, 0);
        position += Short.BYTES + unsignedShort(cursor, position);
        int countOffset = position;
        position += Integer.BYTES + 1; // count and ordered flag
        for (int dictionary = 0; dictionary < PageBlockCodec.DICT_COLUMN_COUNT; dictionary++) {
            int values = unsignedShort(cursor, position);
            position += Short.BYTES;
            for (int value = 0; value < values; value++) {
                position += Short.BYTES + unsignedShort(cursor, position);
            }
        }
        position++; // dictionary-use mask
        int codecOffset = position++;
        int rawLengthOffset = position;
        position += Integer.BYTES;
        int storedLengthOffset = position;
        position += Integer.BYTES;
        return new PageHeaderLayout(
                countOffset, codecOffset, rawLengthOffset, storedLengthOffset, position);
    }

    record PageHeaderLayout(int countOffset, int codecOffset, int rawLengthOffset,
                            int storedLengthOffset, int payloadOffset) {
    }

    private static int unsignedShort(ByteBuffer bytes, int position) {
        return bytes.getShort(position) & 0xffff;
    }

    private static ObjectEntry objectWithKey(byte[] key) {
        return new ObjectEntry(KeyBytes.of(key), 0L, 0L, null, null, null,
                false, null, null, null, null);
    }

    private static CommonPrefixEntry prefix(String key) {
        return new CommonPrefixEntry(KeyBytes.ofUtf8(key));
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
