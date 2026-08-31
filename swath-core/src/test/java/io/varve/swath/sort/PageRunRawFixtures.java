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

/** Writer-bypassing page-run fixtures for corruption tests. */
public final class PageRunRawFixtures {

    private PageRunRawFixtures() {
    }

    /** Frame pages in the supplied physical order without the production writer's page sorting. */
    static void writeRawPageRun(Path path, List<List<ListEntry>> pages,
            Comparator<ListEntry> comparator) throws IOException {
        writeRawPageRun(path, pages, comparator, SortMode.OBJECTS);
    }

    static void writeRawPageRun(Path path, List<List<ListEntry>> pages,
            Comparator<ListEntry> comparator, SortMode orderingMode) throws IOException {
        List<PageBlock> blocks = new ArrayList<>(pages.size());
        for (List<ListEntry> page : pages) {
            blocks.add(PageBlock.pack(page, comparator, PageCodec.NONE));
        }
        long totalEntries = 0;
        int maxRecordLen = 0;
        int maxRawPayloadLength = 0;
        int maxKeyLength = 0;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            PageRunHeader.write(channel, orderingMode);
            for (PageBlock block : blocks) {
                byte[] body = block.serialize();
                writeFrame(channel, body);
                totalEntries += block.count();
                maxRecordLen = Math.max(maxRecordLen, body.length);
                maxRawPayloadLength = Math.max(
                        maxRawPayloadLength, block.rawPayloadLength());
                maxKeyLength = Math.max(maxKeyLength, Math.max(
                        block.firstKeyUnsafe().length, block.lastKeyUnsafe().length));
            }
            writeFixedTrailerTail(
                    channel, channel.position(), blocks.size(), totalEntries, maxRecordLen,
                    maxRawPayloadLength, maxKeyLength);
            channel.force(true);
        }
    }

    /** Write decoded rows {@code [a,z,m]} behind checksum-valid persisted bounds {@code [a,m]}. */
    static Path writeInteriorRowRegression(Path path) throws IOException {
        ListEntryComparator comparator = new ListEntryComparator();
        SortBuffer buffer = new SortBuffer(
                SortConfigs.base().withSegmentCodec(PageCodec.NONE), comparator);
        buffer.admit(0L, List.of(prefix("a"), prefix("m"), prefix("z")));
        new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);

        byte[] bytes = Files.readAllBytes(path);
        int frameStart = PageRunSegmentWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(bytes).getInt(frameStart);
        int bodyStart = frameStart + 2 * Integer.BYTES;
        ByteBuffer body = ByteBuffer.wrap(bytes, bodyStart, bodyLength).slice();
        int payloadStart = pageHeaderLayout(body).payloadOffset();
        int secondKey = bodyStart + payloadStart + 7;
        int thirdKey = bodyStart + payloadStart + 11;
        if (bytes[secondKey] != 'm' || bytes[thirdKey] != 'z') {
            throw new IllegalStateException("unexpected canonical crux-page payload layout");
        }
        bytes[secondKey] = 'z';
        bytes[thirdKey] = 'm';
        int minLength = unsignedShort(bytes, bodyStart);
        int maxLengthPosition = bodyStart + Short.BYTES + minLength;
        int maxLength = unsignedShort(bytes, maxLengthPosition);
        replaceSameLength(bytes, maxLengthPosition + Short.BYTES, maxLength,
                new byte[]{'m'}, "page maxKey");
        rewriteRecordCrc(bytes, frameStart, bodyStart, bodyLength);
        Files.write(path, bytes);
        return path;
    }

    /** Write a checksum-valid segment with a page bound one byte over the S3 key limit. */
    static Path writeCrcRepairedOverlongBound(Path path, boolean overlongMinimum)
            throws IOException {
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

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            PageRunHeader.write(channel, SortMode.OBJECTS);
            writeFrame(channel, malformedBody);
            writeFixedTrailerTail(channel, channel.position(), 1, block.count(), malformedBody.length,
                    block.rawPayloadLength(), Math.max(
                            block.firstKeyUnsafe().length, block.lastKeyUnsafe().length));
            channel.force(true);
        }
        return path;
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
        position += Integer.BYTES + 1;
        for (int dictionary = 0; dictionary < PageBlockCodec.DICT_COLUMN_COUNT; dictionary++) {
            int values = unsignedShort(cursor, position);
            position += Short.BYTES;
            for (int value = 0; value < values; value++) {
                position += Short.BYTES + unsignedShort(cursor, position);
            }
        }
        position++;
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

    private static void writeFrame(FileChannel channel, byte[] body) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        ByteBuffer frame = ByteBuffer.allocate(2 * Integer.BYTES + body.length)
                .putInt(body.length)
                .putInt((int) crc.getValue())
                .put(body);
        SortTestSupport.writeFully(channel, frame.flip());
    }

    private static void writeFixedTrailerTail(FileChannel channel, long trailerStart,
            int totalRecords, long totalEntries, int maxRecordLen,
            int maxRawPayloadLength, int maxKeyLength) throws IOException {
        ByteBuffer tail = ByteBuffer.allocate(PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES)
                .putLong(trailerStart)
                .putInt(totalRecords)
                .putLong(totalEntries)
                .putInt(maxRecordLen)
                .putInt(maxRawPayloadLength)
                .putInt(maxKeyLength);
        CRC32C crc = new CRC32C();
        crc.update(tail.array(), 0, PageRunSegmentWriter.TRAILER_FIELDS_BYTES);
        tail.putInt((int) crc.getValue()).putInt(PageRunSegmentWriter.MAGIC);
        SortTestSupport.writeFully(channel, tail.flip());
    }

    private static void rewriteRecordCrc(
            byte[] bytes, int frameStart, int bodyStart, int bodyLength) {
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
}
