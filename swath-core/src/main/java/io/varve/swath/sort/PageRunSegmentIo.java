/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.zip.CRC32C;

/**
 * Shared read-side IO/framing/validation for one {@link PageRunSegmentWriter} page-run segment.
 * Entry readers, the {@code dump-run} inspector, reference-routing header cursors, and positional
 * encoders share this framing owner.
 *
 * <p><b>What is single-sourced here:</b> the header magic/version check, the fixed trailer-tail read
 * (recovering {@code trailerStart}/{@code totalRecords}/{@code totalEntries}/{@code maxRecordLen},
 * exact decoded-page/key maxima, and the
 * trailing-magic truncation check), the framed-record read ({@code [len u32][crc32c u32][body]} with
 * the {@code len<=0 || len>maxRecordLen} bound applied BEFORE allocation and a CRC32C body verify), the
 * end-of-stream completeness cross-check ({@code seenEntries == totalEntries}), the bounded
 * single-pass page-header validation, the
 * <b>intra-segment min-monotonicity
 * guard</b> ({@link #nextPage()}), and
 * the positional/sequential read primitives.
 *
 * <p>The record-read variants share the same framing validation. The entry reader calls
 * {@link #nextPage()} (CRC-verified body plus the min-monotonicity check),
 * and the inspector calls {@link #nextRecord()} (returns the body plus a {@code crcOk} flag so a debug
 * dump can pinpoint a torn record without aborting the walk, and deliberately does NOT abort on a
 * regression it is being run to diagnose).
 */
final class PageRunSegmentIo implements AutoCloseable {

    private final FileChannel channel;
    private final Path path;
    private final int magic;
    private final short formatVersion;
    private final SortMode orderingMode;
    /** Absolute offset of the first framed page after the variable-length header envelope. */
    final int headerBytes;
    /** Carries the {@code SORT.page_run_min_regression} engagement counter (NO_OP when unwired). */
    private final SortMetrics metrics;
    /** Maximum decoded page payload admitted by kickoff planning for this segment. */
    private final int maxRawPayloadLength;

    /** Largest framed body length (from the trailer): bounds a record's claimed length before alloc. */
    final long maxRecordLen;
    /** Exact largest decoded payload declared by any page header in this segment. */
    final int persistedMaxRawPayloadLength;
    /** Exact largest page-bound key length in this segment. */
    final int persistedMaxKeyLength;
    /** Declared record count (from the trailer): how many framed records precede the trailer. */
    final long totalRecords;
    /** Declared entry count (from the trailer): the end-of-stream completeness cross-check target. */
    final long totalEntries;
    /** Absolute file offset where the fixed trailer begins. */
    final long trailerStart;
    /** Complete file length captured at open. */
    final long fileSize;

    /** Previous page's minKey (monotonicity guard); null until the first page is read. */
    private byte[] previousMin;
    /** Previous page's maxKey (disjointness guard); null until the first page is read. */
    private byte[] previousMax;
    /** Ordinal of the next page returned by {@link #nextPage()}. */
    private long pagesRead;
    /** Entries read before the next page. */
    private long cumulativeEntries;
    /** Framed page bytes physically read by this IO instance. */
    private long framedBytesRead;
    /** Tracked sequential frame offset; avoids a native FileChannel.position() query per page. */
    private long nextFrameOffset;
    private PageRunSegmentIo(FileChannel channel, Path path, SortMetrics metrics, int magic,
                            short formatVersion, SortMode orderingMode, int headerBytes,
                            long maxRecordLen, long totalRecords,
                            long totalEntries, int persistedMaxRawPayloadLength,
                            int persistedMaxKeyLength, long trailerStart, long fileSize) {
        this(channel, path, metrics, magic, formatVersion, orderingMode, headerBytes,
                maxRecordLen, totalRecords,
                totalEntries, persistedMaxRawPayloadLength, persistedMaxKeyLength,
                trailerStart, fileSize, PageBlock.MAX_RAW_PAYLOAD_BYTES);
    }

    private PageRunSegmentIo(FileChannel channel, Path path, SortMetrics metrics, int magic,
                            short formatVersion, SortMode orderingMode, int headerBytes,
                            long maxRecordLen, long totalRecords,
                            long totalEntries, int persistedMaxRawPayloadLength,
                            int persistedMaxKeyLength, long trailerStart, long fileSize,
                            int maxRawPayloadLength) {
        this.channel = channel;
        this.path = path;
        this.metrics = metrics;
        this.maxRawPayloadLength = maxRawPayloadLength;
        this.magic = magic;
        this.formatVersion = formatVersion;
        this.orderingMode = orderingMode;
        this.headerBytes = headerBytes;
        this.maxRecordLen = maxRecordLen;
        this.totalRecords = totalRecords;
        this.totalEntries = totalEntries;
        this.persistedMaxRawPayloadLength = persistedMaxRawPayloadLength;
        this.persistedMaxKeyLength = persistedMaxKeyLength;
        this.trailerStart = trailerStart;
        this.fileSize = fileSize;
        this.nextFrameOffset = headerBytes;
    }

    /**
     * Open {@code path}, validate the header magic/version and the fixed trailer tail (recovering the
     * record/entry counts, {@code maxRecordLen}, exact decoded-page/key maxima,
     * {@code trailerStart}, and the trailing magic /
     * truncation check), then position the channel after the variable-length header at the first record.
     * Closes the channel on any open-time failure. {@code metrics} carries the
     * {@code SORT.page_run_min_regression} engagement counter into the run summary.
     */
    static PageRunSegmentIo open(Path path, SortMetrics metrics) throws IOException {
        return open(path, metrics, PageBlock.MAX_RAW_PAYLOAD_BYTES);
    }

    /** Open with the segment's own CRC-protected fixed-trailer decoded-page maximum. */
    static PageRunSegmentIo openUsingPersistedMaximum(Path path, SortMetrics metrics)
            throws IOException {
        return open(path, metrics, -1);
    }

    /** Open with the decoded-page maximum admitted for this segment by kickoff planning. */
    static PageRunSegmentIo open(Path path, SortMetrics metrics, int maxRawPayloadLength)
            throws IOException {
        if (maxRawPayloadLength < -1 || maxRawPayloadLength > PageBlock.MAX_RAW_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("decoded-page limit is outside the format bound: "
                    + maxRawPayloadLength);
        }
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            long minSize = PageRunHeader.PREFIX_BYTES + PageRunHeader.CRC_BYTES
                    + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
            if (size < minSize) {
                metrics.recordStealReason("SORT", "page_run_header_corruption");
                throw new SegmentCorruptionException(path,
                        SegmentCorruptionException.PAGE_RUN_HEADER_CORRUPTION,
                        "file too small to be a page-run segment (" + size + " bytes)");
            }

            PageRunHeader.Header header = PageRunHeader.read(channel, path, size, metrics);
            int magic = PageRunSegmentWriter.MAGIC;
            short version = header.formatVersion();

            // The fixed trailer tail carries counts, allocation/admission maxima, and the
            // trailing magic — reading it lets us stop after exactly totalRecords records (so we never
            // misread the trailer as a record) and validates the file is complete (a truncated file has
            // no valid trailing magic here).
            byte[] tailBytes = readAt(channel, path,
                    size - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES,
                    PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES).array();
            ByteBuffer tail = ByteBuffer.wrap(tailBytes);
            long trailerStart = tail.getLong();
            long totalRecords = tail.getInt() & 0xFFFFFFFFL;
            long totalEntries = tail.getLong();
            long maxRecordLen = tail.getInt() & 0xFFFFFFFFL;
            long persistedMaxRawPayloadLength = tail.getInt() & 0xFFFFFFFFL;
            long persistedMaxKeyLength = tail.getInt() & 0xFFFFFFFFL;
            int expectedTrailerCrc = tail.getInt();
            int trailerMagic = tail.getInt();
            CRC32C trailerCrc = new CRC32C();
            trailerCrc.update(tailBytes, 0, PageRunSegmentWriter.TRAILER_FIELDS_BYTES);
            if ((int) trailerCrc.getValue() != expectedTrailerCrc) {
                metrics.recordStealReason("SORT", "page_run_trailer_corruption");
                throw new SegmentCorruptionException(path,
                        SegmentCorruptionException.PAGE_RUN_TRAILER_CORRUPTION,
                        "fixed trailer CRC32C mismatch");
            }
            if (trailerMagic != PageRunSegmentWriter.MAGIC) {
                metrics.recordStealReason("SORT", "page_run_trailer_corruption");
                throw new SegmentCorruptionException(path,
                        SegmentCorruptionException.PAGE_RUN_TRAILER_CORRUPTION,
                        "bad or missing page-run trailer magic (truncated segment?)");
            }
            long fixedTailStart = size - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
            if (trailerStart != fixedTailStart || trailerStart < header.encodedBytes()) {
                throw failFor(path, "invalid page-run trailer offset " + trailerStart);
            }
            long recordBytes = trailerStart - header.encodedBytes();
            if (totalRecords == 0) {
                if (recordBytes != 0 || totalEntries != 0 || maxRecordLen != 0
                        || persistedMaxRawPayloadLength != 0 || persistedMaxKeyLength != 0) {
                    throw failFor(path, "inconsistent empty page-run trailer counts");
                }
            } else if (totalEntries < totalRecords
                    || maxRecordLen == 0
                    || recordBytes < 9
                    || totalRecords > recordBytes / 9
                    || maxRecordLen > recordBytes - 8
                    || persistedMaxRawPayloadLength == 0
                    || persistedMaxRawPayloadLength > PageBlock.MAX_RAW_PAYLOAD_BYTES
                    || persistedMaxKeyLength > ByteMidpoint.MAX_KEY_LEN) {
                throw failFor(path, "inconsistent page-run trailer record metadata");
            }

            channel.position(header.encodedBytes());
            return new PageRunSegmentIo(channel, path, metrics, magic, version,
                    header.orderingMode(), header.encodedBytes(), maxRecordLen,
                    totalRecords, totalEntries, (int) persistedMaxRawPayloadLength,
                    (int) persistedMaxKeyLength, trailerStart, size,
                    maxRawPayloadLength < 0
                            ? (int) persistedMaxRawPayloadLength : maxRawPayloadLength);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    /** A framed record's read result: its body bytes, framed length, and CRC verdict. */
    record Record(byte[] body, int framedLen, boolean crcOk) {
    }

    /**
     * One CRC-validated page record and its single parsed header. The body array is newly allocated
     * by this IO instance and is thereafter immutable. A decoded {@link PageBlock} retains that body
     * directly, so a frontier may advance or close without invalidating a previously returned block.
     */
    record Page(byte[] body, PageBlockCodec.Header header) {
        PageBlock decode(Path sourcePath) {
            return PageBlockCodec.deserialize(body, header, sourcePath);
        }
    }

    /** Header-pass result for one physical frame; the stored payload was not read. */
    record RoutingPage(long ordinal, long offset, int framedLen,
                       PageBlockCodec.RoutingHeader header) {
    }

    /**
     * <b>The single sequential page-advance primitive</b>: read the next framed
     * record, CRC-verify its body, parse its frontier fields, and enforce the intra-segment
     * min-monotonicity and mode-aware disjointness invariants. Returning the parsed fields alongside the body costs the entry-typed
     * reader nothing (the leading min/max/count parse is a few bytes of the body it already holds) and buys
     * the guarantee that a third reader added later CANNOT skip the LOGICAL guard the way a bare
     * {@code nextBody()} let {@link PageRunSegmentReader} skip it: {@code StreamingMerger} assumes each
     * input run is sorted, so an unguarded page-run reader on that generic seam silently misorders output
     * exactly as the frontier path would.
     */
    Page nextPage() throws IOException {
        if (pagesRead == totalRecords) {
            if (nextFrameOffset != trailerStart) {
                throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                        "page frames end at " + nextFrameOffset
                                + " but trailer starts at " + trailerStart, null);
            }
            checkComplete(cumulativeEntries);
            return null;
        }
        Record record = nextRecord();
        if (!record.crcOk()) {
            throw fail("record CRC32C mismatch (torn or corrupt record)");
        }
        byte[] body = record.body();
        PageBlockCodec.Header header;
        try {
            header = parsePageHeader(body);
        } catch (IllegalArgumentException e) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "malformed page body: " + e.getMessage(), e);
        }
        requirePlannedDecodedPayload(header.rawPayloadLength());
        requirePersistedKeyMaximum(header.minKey(), header.maxKey());
        pagesRead++;
        checkMinMonotonic(header.minKey());
        checkDisjoint(header.minKey());
        previousMin = header.minKey();
        previousMax = header.maxKey();
        framedBytesRead += Math.addExact(8, record.framedLen());
        cumulativeEntries = Math.addExact(cumulativeEntries, header.count());
        return new Page(body, header);
    }

    /**
     * Advance one frame using positional metadata reads only. The body CRC is intentionally deferred
     * to the encoder's positional read; this pass proves frame tiling, routing bounds, and totals.
     */
    RoutingPage nextRoutingPage() throws IOException {
        if (pagesRead == totalRecords) {
            return null;
        }
        long ordinal = pagesRead;
        long frameOffset = nextFrameOffset;
        ByteBuffer prefix = readAt(frameOffset, 8);
        int bodyLength = prefix.getInt();
        validateRecordLength(bodyLength);
        int framedLen = Math.addExact(8, bodyLength);
        long frameEnd = Math.addExact(frameOffset, framedLen);
        if (frameEnd > trailerStart) {
            throw fail("page frame at offset " + frameOffset + " crosses trailer at "
                    + trailerStart);
        }
        PageBlockCodec.RoutingHeader header;
        try {
            long bodyOffset = frameOffset + 8;
            header = PageBlockCodec.parseRoutingHeader(bodyLength,
                    (position, bytes) -> readAt(bodyOffset + position, bytes));
            validateRoutingHeader(header);
        } catch (IllegalArgumentException e) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "malformed page routing header: " + e.getMessage(), e);
        }
        requirePlannedDecodedPayload(header.rawPayloadLength());
        requirePersistedKeyMaximum(header.minKey(), header.maxKey());
        pagesRead++;
        checkMinMonotonic(header.minKey());
        checkDisjoint(header.minKey());
        previousMin = header.minKey();
        previousMax = header.maxKey();
        cumulativeEntries = Math.addExact(cumulativeEntries, header.count());
        framedBytesRead = Math.addExact(framedBytesRead, framedLen);
        nextFrameOffset = frameEnd;
        return new RoutingPage(ordinal, frameOffset, framedLen, header);
    }

    /** Validate the exact header-to-trailer frame chain and both trailer totals. */
    void checkRoutingComplete() throws IOException {
        if (nextFrameOffset != trailerStart) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "page frames end at " + nextFrameOffset
                            + " but trailer starts at " + trailerStart, null);
        }
        checkComplete(cumulativeEntries);
    }

    /** Positional CRC/read/decode input for a reference, safe across concurrent encoder lanes. */
    PageBlock readPage(PageRef ref) throws IOException {
        if (ref.offset() < headerBytes || ref.offset() >= trailerStart
                || ref.ordinal() >= totalRecords) {
            throw fail("page reference is outside the physical frame region");
        }
        ByteBuffer prefix = readAt(ref.offset(), 8);
        int bodyLength = prefix.getInt();
        int expectedCrc = prefix.getInt();
        validateRecordLength(bodyLength);
        int framedLen = Math.addExact(8, bodyLength);
        if (framedLen != ref.framedLen()
                || Math.addExact(ref.offset(), framedLen) > trailerStart) {
            throw fail("page reference frame length disagrees with the segment");
        }
        byte[] body = readAt(ref.offset() + 8, bodyLength).array();
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        if ((int) crc.getValue() != expectedCrc) {
            throw fail("record CRC32C mismatch (torn or corrupt record)");
        }
        PageBlockCodec.Header header;
        try {
            header = parsePageHeader(body);
        } catch (IllegalArgumentException e) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "malformed page body: " + e.getMessage(), e);
        }
        requirePlannedDecodedPayload(header.rawPayloadLength());
        requirePersistedKeyMaximum(header.minKey(), header.maxKey());
        if (!Arrays.equals(header.minKey(), ref.minKey())
                || !Arrays.equals(header.maxKey(), ref.maxKey())
                || header.count() != ref.count()
                || header.rawPayloadLength() != ref.rawPayloadLength()) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "page body metadata disagrees with its routed reference", null);
        }
        return PageBlockCodec.deserialize(body, header, path);
    }

    /**
     * Read-time verification that a segment's page {@code minKey}s never go backwards (unsigned).
     * This diagnostic runs before the stronger mode-aware disjointness check so a regression keeps
     * its established error class. Strictly {@code newMin < previousMin} is corruption — the
     * page-aware merger's whole-page fast path would trust a frontier that is no longer a lower bound on
     * the segment's remaining keys, and the entry-typed {@link StreamingMerger} would trust a run that is
     * not sorted; either way the merged output could be silently misordered. Costs exactly one
     * {@code compareUnsigned} per page advance and is a pure no-op on every well-formed segment
     * ({@link PageRunSegmentWriter#flush()} establishes the ascent by sorting pages on their first key).
     */
    private void checkMinMonotonic(byte[] newMin) throws IOException {
        if (previousMin == null || Arrays.compareUnsigned(newMin, previousMin) >= 0) {
            return;
        }
        // Count BEFORE throwing so the violation survives into summary.json's meters[] even though the
        // run aborts (the terminal write serializes the registry on the unwind path).
        metrics.recordStealReason("SORT", "page_run_min_regression");   // literal: the CI drift guard resolves only literals
        HexFormat hex = HexFormat.of();
        throw new SegmentCorruptionException(path, SegmentCorruptionException.PAGE_RUN_MIN_REGRESSION,
                "page minKey regressed within segment (page " + pagesRead + " of " + totalRecords
                        + ": minKey 0x" + hex.formatHex(newMin) + " < previous page's minKey 0x"
                        + hex.formatHex(previousMin) + ") — page-run pages MUST be stored in non-decreasing"
                        + " minKey order (PageRunSegmentWriter#flush sorts them); every merge path relies on"
                        + " that ascent (the page-aware merger's frontier is a valid lower bound only under"
                        + " it, and the streaming merger assumes each input run is sorted), so a regression"
                        + " would silently misorder the merged output");
    }

    /** Reject true page-range overlap universally, and an equal boundary for OBJECTS. */
    private void checkDisjoint(byte[] newMin) throws IOException {
        if (previousMax == null) {
            return;
        }
        int comparison = Arrays.compareUnsigned(previousMax, newMin);
        if (comparison < 0 || (comparison == 0 && orderingMode == SortMode.VERSIONS)) {
            return;
        }
        metrics.recordStealReason("SORT", "page_run_page_overlap");
        HexFormat hex = HexFormat.of();
        throw new SegmentCorruptionException(path,
                SegmentCorruptionException.PAGE_RUN_PAGE_OVERLAP,
                "adjacent page ranges overlap under " + orderingMode + " ordering (page "
                        + pagesRead + " of " + totalRecords + ": previous maxKey 0x"
                        + hex.formatHex(previousMax) + " >= next minKey 0x"
                        + hex.formatHex(newMin) + ")");
    }

    /**
     * Read the next framed record WITHOUT throwing on a CRC mismatch (the inspection path): the len
     * bound is still enforced (a structurally impossible length is genuine corruption the walk cannot
     * step over and still throws), but a body-only bit-flip is reported via {@link Record#crcOk()}.
     */
    Record nextRecord() throws IOException {
        ByteBuffer frame = readFully(8);
        int len = frame.getInt();
        int expectedCrc = frame.getInt();
        // Bound the claimed length against maxRecordLen (from the trailer) BEFORE allocating: an
        // un-CRC-protected len-prefix bit-flip must not be able to drive an up-to-~2GB allocation
        // ahead of the CRC check.
        validateRecordLength(len);
        byte[] body = readFully(len).array();
        nextFrameOffset = Math.addExact(nextFrameOffset, 8L + len);
        CRC32C crc = new CRC32C();
        crc.update(body, 0, len);
        boolean crcOk = (int) crc.getValue() == expectedCrc;
        return new Record(body, len, crcOk);
    }

    private void validateRecordLength(int length) throws IOException {
        if (length <= 0 || length > maxRecordLen) {
            throw fail("record length " + length
                    + " out of bounds (maxRecordLen=" + maxRecordLen + ")");
        }
    }

    private void validateRoutingHeader(PageBlockCodec.RoutingHeader header) {
        if (Arrays.compareUnsigned(header.minKey(), header.maxKey()) > 0) {
            throw new IllegalArgumentException(
                    "malformed PageBlock: minKey exceeds maxKey under unsigned byte order");
        }
    }

    private void requirePlannedDecodedPayload(int rawPayloadLength) throws IOException {
        if (rawPayloadLength > maxRawPayloadLength) {
            metrics.recordStealReason("SORT", "page_run_decoded_page_limit");
            throw corruption(SegmentCorruptionException.PAGE_RUN_DECODED_PAGE_LIMIT,
                    "decoded page payload " + rawPayloadLength
                            + " exceeds the planned segment maximum " + maxRawPayloadLength, null);
        }
    }

    private void requirePersistedKeyMaximum(byte[] minKey, byte[] maxKey) throws IOException {
        int observed = Math.max(minKey.length, maxKey.length);
        if (observed > persistedMaxKeyLength) {
            metrics.recordStealReason("SORT", "page_run_key_length_limit");
            throw corruption(SegmentCorruptionException.PAGE_RUN_KEY_LENGTH_LIMIT,
                    "page-bound key length " + observed
                            + " exceeds the persisted segment maximum "
                            + persistedMaxKeyLength, null);
        }
    }

    /**
     * End-of-stream completeness cross-check (closes the silent-truncation-downward gap): a bit-flipped
     * totalRecords/totalEntries that lowered the advertised count would otherwise let the stream end
     * early without any error — compare what the caller actually read against the trailer's declared
     * {@code totalEntries}, once, at end-of-stream. Runs UNCONDITIONALLY regardless of whether any body
     * was loaded.
     */
    void checkComplete(long seenEntries) throws IOException {
        if (seenEntries != totalEntries) {
            throw corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "entry count mismatch: saw " + seenEntries
                            + " but trailer declared totalEntries=" + totalEntries, null);
        }
    }

    /** Structurally validate a body once and return its zero-copy persisted-page header. */
    static PageBlockCodec.Header parsePageHeader(byte[] body) {
        PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
        if (header.minKey().length > ByteMidpoint.MAX_KEY_LEN
                || header.maxKey().length > ByteMidpoint.MAX_KEY_LEN) {
            throw new IllegalArgumentException("malformed PageBlock: minKey/maxKey exceeds the S3 key limit of "
                    + ByteMidpoint.MAX_KEY_LEN + " bytes");
        }
        if (Arrays.compareUnsigned(header.minKey(), header.maxKey()) > 0) {
            throw new IllegalArgumentException(
                    "malformed PageBlock: minKey exceeds maxKey under unsigned byte order");
        }
        if (header.codec() == PageCodec.NONE
                && header.payloadLength() != header.rawPayloadLength()) {
            throw new IllegalArgumentException("malformed PageBlock: NONE payload lengths differ: raw="
                    + header.rawPayloadLength() + " stored=" + header.payloadLength());
        }
        return header;
    }

    /** Positional read of exactly {@code n} bytes (does not move the channel position). */
    ByteBuffer readAt(long position, int n) throws IOException {
        return readAt(channel, path, position, n);
    }

    /** Positional read into caller-owned scratch storage (does not move the channel position). */
    void readAt(long position, ByteBuffer destination) throws IOException {
        long pos = position;
        while (destination.hasRemaining()) {
            int read = channel.read(destination, pos);
            if (read < 0) {
                throw new EOFException("unexpected EOF reading page-run segment " + path);
            }
            pos += read;
        }
    }

    /** Sequential read of exactly {@code n} bytes from the current channel position. */
    private ByteBuffer readFully(int n) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(n);
        while (buf.hasRemaining()) {
            int r = channel.read(buf);
            if (r < 0) {
                throw new EOFException("unexpected EOF reading page-run segment " + path);
            }
        }
        return buf.flip();
    }

    private static ByteBuffer readAt(FileChannel ch, Path path, long position, int n) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(n);
        long pos = position;
        while (buf.hasRemaining()) {
            int r = ch.read(buf, pos);
            if (r < 0) {
                throw new EOFException("unexpected EOF reading page-run segment " + path);
            }
            pos += r;
        }
        return buf.flip();
    }

    IOException fail(String message) {
        return failFor(path, message);
    }

    SegmentCorruptionException corruption(String errorClass, String message, Throwable cause) {
        return new SegmentCorruptionException(path, errorClass, message, cause);
    }

    Path path() {
        return path;
    }

    long framedBytesRead() {
        return framedBytesRead;
    }

    long nextFrameOffset() {
        return nextFrameOffset;
    }

    int magic() {
        return magic;
    }

    short formatVersion() {
        return formatVersion;
    }

    SortMode orderingMode() {
        return orderingMode;
    }

    private static IOException failFor(Path path, String message) {
        return new IOException("page-run segment " + path + ": " + message);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
