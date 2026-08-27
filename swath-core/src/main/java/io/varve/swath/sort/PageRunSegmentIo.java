/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

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
 * The three consumers — {@link PageRunSegmentReader} (entry-typed {@link EntryStream}),
 * {@link PageFrontierReader} (decode-free {@link PageFrontierStream}), and
 * {@link PageRunSegmentInspector} (the {@code dump-run} debug tool) — all frame, bound, and CRC-verify
 * records identically; this class owns that single corruption-detection contract so it lives in exactly
 * one place instead of being hand-copied per reader.
 *
 * <p><b>What is single-sourced here:</b> the header magic/version check, the fixed trailer-tail read
 * (recovering {@code trailerStart}/{@code totalRecords}/{@code totalEntries}/{@code maxRecordLen} + the
 * trailing-magic truncation check), the framed-record read ({@code [len u32][crc32c u32][body]} with
 * the {@code len<=0 || len>maxRecordLen} bound applied BEFORE allocation and a CRC32C body verify), the
 * end-of-stream completeness cross-check ({@code seenEntries == totalEntries}), the decode-free
 * frontier-field parse, the <b>intra-segment min-monotonicity guard</b> ({@link #nextPage()}), and
 * the positional/sequential read primitives.
 *
 * <p>Three record-read variants share the same len/crc read: the two streaming readers call
 * {@link #nextPage()} (CRC-verified body + parsed frontier fields + the min-monotonicity check — the ONE
 * page-advance primitive, so no page-run read path can be added later that forgets the LOGICAL guard),
 * and the inspector calls {@link #nextRecord()} (returns the body plus a {@code crcOk} flag so a debug
 * dump can pinpoint a torn record without aborting the walk, and deliberately does NOT abort on a
 * regression it is being run to diagnose).
 */
final class PageRunSegmentIo implements AutoCloseable {

    private final FileChannel channel;
    private final Path path;
    /** Carries the {@code SORT.page_run_min_regression} engagement counter (NO_OP when unwired). */
    private final SortMetrics metrics;

    /** Largest framed body length (from the trailer): bounds a record's claimed length before alloc. */
    final long maxRecordLen;
    /** Declared record count (from the trailer): how many framed records precede the trailer. */
    final long totalRecords;
    /** Declared entry count (from the trailer): the end-of-stream completeness cross-check target. */
    final long totalEntries;
    /** Absolute file offset where the trailer begins (the O(1) seek target for the key bounds). */
    final long trailerStart;
    /** Complete file length captured at open; bounds the optional trailer extension. */
    final long fileSize;

    /** Previous page's minKey (monotonicity guard); null until the first page is read. */
    private byte[] previousMin;
    /** Pages read via {@link #nextPage()} so far — the 1-based page number in a violation message. */
    private long pagesRead;

    private PageRunSegmentIo(FileChannel channel, Path path, SortMetrics metrics, long maxRecordLen,
                            long totalRecords, long totalEntries, long trailerStart, long fileSize) {
        this.channel = channel;
        this.path = path;
        this.metrics = metrics;
        this.maxRecordLen = maxRecordLen;
        this.totalRecords = totalRecords;
        this.totalEntries = totalEntries;
        this.trailerStart = trailerStart;
        this.fileSize = fileSize;
    }

    /**
     * Open {@code path} with no metrics recorder (the trailer/inspection readers and tests): as
     * {@link #open(Path, SortMetrics)} with {@link SortMetrics#NO_OP}.
     */
    static PageRunSegmentIo open(Path path) throws IOException {
        return open(path, SortMetrics.NO_OP);
    }

    /**
     * Open {@code path}, validate the header magic/version and the fixed trailer tail (recovering the
     * record/entry counts, {@code maxRecordLen}, {@code trailerStart}, and the trailing magic /
     * truncation check), then position the channel at the first record ({@link PageRunSegmentWriter#HEADER_BYTES}).
     * Closes the channel on any open-time failure. {@code metrics} carries the
     * {@code SORT.page_run_min_regression} engagement counter into the run summary.
     */
    static PageRunSegmentIo open(Path path, SortMetrics metrics) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            long minSize = PageRunSegmentWriter.HEADER_BYTES + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
            if (size < minSize) {
                throw failFor(path, "file too small to be a page-run segment (" + size + " bytes)");
            }

            ByteBuffer header = readAt(channel, path, 0, PageRunSegmentWriter.HEADER_BYTES);
            int magic = header.getInt();
            short version = header.getShort();
            if (magic != PageRunSegmentWriter.MAGIC) {
                throw failFor(path, "bad page-run magic 0x" + Integer.toHexString(magic));
            }
            if (version != PageRunSegmentWriter.FORMAT_VERSION) {
                throw failFor(path, "unsupported page-run format version " + version);
            }

            // The fixed trailer tail carries trailerStart/totalRecords/totalEntries/maxRecordLen and the
            // trailing magic — reading it lets us stop after exactly totalRecords records (so we never
            // misread the trailer as a record) and validates the file is complete (a truncated file has
            // no valid trailing magic here).
            ByteBuffer tail = readAt(channel, path, size - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES,
                    PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
            long trailerStart = tail.getLong();
            long totalRecords = tail.getInt() & 0xFFFFFFFFL;
            long totalEntries = tail.getLong();
            long maxRecordLen = tail.getInt() & 0xFFFFFFFFL;
            int trailerMagic = tail.getInt();
            if (trailerMagic != PageRunSegmentWriter.MAGIC) {
                throw failFor(path, "bad or missing page-run trailer (truncated segment?)");
            }
            long fixedTailStart = size - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
            if (trailerStart < PageRunSegmentWriter.HEADER_BYTES
                    || trailerStart > fixedTailStart - 4) {
                throw failFor(path, "invalid page-run trailer offset " + trailerStart);
            }
            long recordBytes = trailerStart - PageRunSegmentWriter.HEADER_BYTES;
            if (totalRecords == 0) {
                if (recordBytes != 0 || totalEntries != 0 || maxRecordLen != 0) {
                    throw failFor(path, "inconsistent empty page-run trailer counts");
                }
            } else if (totalEntries < totalRecords
                    || maxRecordLen == 0
                    || recordBytes < 9
                    || totalRecords > recordBytes / 9
                    || maxRecordLen > recordBytes - 8) {
                throw failFor(path, "inconsistent page-run trailer record metadata");
            }

            channel.position(PageRunSegmentWriter.HEADER_BYTES);
            return new PageRunSegmentIo(channel, path, metrics, maxRecordLen, totalRecords, totalEntries,
                    trailerStart, size);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    /** A framed record's read result: its body bytes, framed length, and CRC verdict. */
    record Record(byte[] body, int framedLen, boolean crcOk) {
    }

    /** The decode-free leading fields of a page record body: {@code [minKeyLen u16][minKey][maxKeyLen u16][maxKey][count u32]}. */
    record FrontierFields(byte[] minKey, byte[] maxKey, int count) {
    }

    /** One validated page record: the CRC-verified body bytes plus its parsed {@link FrontierFields}. */
    record Page(byte[] body, FrontierFields fields) {
    }

    /**
     * <b>The single page-advance primitive for every page-run READ path</b> ({@link PageFrontierReader}'s
     * decode-free frontier AND {@link PageRunSegmentReader}'s entry-typed stream): read the next framed
     * record, CRC-verify its body, parse its frontier fields, and enforce the intra-segment
     * min-monotonicity invariant. Returning the parsed fields alongside the body costs the entry-typed
     * reader nothing (the leading min/max/count parse is a few bytes of the body it already holds) and buys
     * the guarantee that a third reader added later CANNOT skip the LOGICAL guard the way a bare
     * {@code nextBody()} let {@link PageRunSegmentReader} skip it: {@code StreamingMerger} assumes each
     * input run is sorted, so an unguarded page-run reader on that generic seam silently misorders output
     * exactly as the frontier path would.
     */
    Page nextPage() throws IOException {
        byte[] body = nextBody();
        FrontierFields fields = parseFrontierFields(body);
        pagesRead++;
        checkMinMonotonic(fields.minKey());
        previousMin = fields.minKey();
        return new Page(body, fields);
    }

    /**
     * Read-time verification of the merger's ONE logical precondition: a segment's page {@code
     * minKey}s never go backwards (unsigned). Strictly {@code newMin < previousMin} is corruption — the
     * page-aware merger's whole-page fast path would trust a frontier that is no longer a lower bound on
     * the segment's remaining keys, and the entry-typed {@link StreamingMerger} would trust a run that is
     * not sorted; either way the merged output is silently misordered (the {@code page_overlap_keymerge}
     * alarm fires only AFTER such damage, so it is an alarm, not a proof). Equal mins and OVERLAPPING page
     * ranges (a page starting at/inside the previous page's span, as long as its min does not regress) are
     * LEGAL and pass untouched — the merger resolves those with its key-merge fallback. Costs exactly one
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

    /**
     * Read the next framed record and CRC-verify its body, throwing on a mismatch (fail-fast, no silent
     * skip). Private: page-run readers go through {@link #nextPage()} so the guard cannot be bypassed.
     */
    private byte[] nextBody() throws IOException {
        Record r = nextRecord();
        if (!r.crcOk()) {
            throw fail("record CRC32C mismatch (torn or corrupt record)");
        }
        return r.body();
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
        if (len <= 0 || len > maxRecordLen) {
            throw fail("record length " + len + " out of bounds (maxRecordLen=" + maxRecordLen + ")");
        }
        byte[] body = readFully(len).array();
        CRC32C crc = new CRC32C();
        crc.update(body, 0, len);
        boolean crcOk = (int) crc.getValue() == expectedCrc;
        return new Record(body, len, crcOk);
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
            throw fail("entry count mismatch: saw " + seenEntries
                    + " but trailer declared totalEntries=" + totalEntries);
        }
    }

    /** Parse ONLY the leading frontier fields of a record body — no row decode (see {@link PageBlock#serialize}). */
    static FrontierFields parseFrontierFields(byte[] body) {
        ByteBuffer b = ByteBuffer.wrap(body);
        byte[] min = new byte[b.getShort() & 0xFFFF];
        b.get(min);
        byte[] max = new byte[b.getShort() & 0xFFFF];
        b.get(max);
        int count = b.getInt();
        return new FrontierFields(min, max, count);
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

    private static IOException failFor(Path path, String message) {
        return new IOException("page-run segment " + path + ": " + message);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
