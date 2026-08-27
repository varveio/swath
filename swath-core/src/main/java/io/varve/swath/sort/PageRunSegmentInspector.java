/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only structural inspector for one {@link PageRunSegmentWriter} page-run segment
 * — the seam the {@code swath dump-run} debug subcommand consumes to give a page-run segment the
 * same on-disk inspectability {@code duckdb} gives Parquet staging. It reports the header
 * version, one {@link RecordInfo} per framed record ({@code [minKey, maxKey, count, codec, framedLen]}
 * plus a per-record CRC32C verdict), and the completeness {@link TrailerInfo}.
 *
 * <p><b>Reuses the shared read contract.</b> The trailer is read via
 * {@link PageRunSegmentReader#readTrailer} (O(1), no record walk); the framing/len-bound/CRC read and
 * the decode-free structural/min/max/count parse are the single-sourced {@link PageRunSegmentIo} primitives the
 * streaming readers use (no row materialization); the codec is read from a {@link PageBlock#deserialize}
 * of the (verified) body. Unlike the streaming readers this inspector does <b>not</b> throw on a
 * per-record CRC mismatch — it records {@code crcOk=false} and keeps walking (via
 * {@link PageRunSegmentIo#nextRecord()}), so a debug dump can show exactly which record is torn. It
 * never mutates the file.
 */
public final class PageRunSegmentInspector {

    private PageRunSegmentInspector() {
    }

    /** One framed record's structural summary. {@code codec} is {@code "?"} when the CRC failed. */
    public record RecordInfo(long index, byte[] minKey, byte[] maxKey, int count, String codec,
                             int framedLen, boolean crcOk) {
    }

    /** The completeness trailer fields (key bounds + counts + the per-record length bound). */
    public record TrailerInfo(byte[] segMinKey, byte[] segMaxKey, long totalRecords, long totalEntries,
                              long maxRecordLen) {
    }

    /** A whole segment's structural dump: header magic/version, per-record summaries, and the trailer. */
    public record Dump(int magic, short formatVersion, List<RecordInfo> records, TrailerInfo trailer) {
    }

    /**
     * Walk {@code path} read-only and return its structural {@link Dump}. Validates the header and
     * trailing magic (via {@link PageRunSegmentReader#readTrailer}), then reads each framed record's
     * length/CRC, CRC-verifies the body, and parses its leading min/max/count decode-free. A record
     * whose framed length is structurally impossible (non-positive or past {@code maxRecordLen}) still
     * throws — that is genuine structural corruption the walk cannot step over; a body-only bit-flip
     * is reported as {@code crcOk=false} and the walk continues.
     */
    public static Dump inspect(Path path) throws IOException {
        PageRunSegmentReader.Trailer t = PageRunSegmentReader.readTrailer(path);
        List<RecordInfo> records = new ArrayList<>();
        int magic;
        short version;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path)) {
            ByteBuffer header = io.readAt(0, PageRunSegmentWriter.HEADER_BYTES);
            magic = header.getInt();   // already validated by open()
            version = header.getShort();

            // open() positioned the channel at the first record; walk exactly totalRecords records.
            for (long i = 0; i < t.totalRecords(); i++) {
                PageRunSegmentIo.Record rec = io.nextRecord();   // does NOT throw on a body CRC mismatch
                byte[] body = rec.body();
                boolean crcOk = rec.crcOk();

                byte[] min = new byte[0];
                byte[] max = new byte[0];
                int count = -1;
                String codec = "?";
                try {
                    PageRunSegmentIo.FrontierFields f = PageRunSegmentIo.parseFrontierFields(body);
                    min = f.minKey();
                    max = f.maxKey();
                    count = f.count();
                    if (crcOk) {
                        codec = PageBlock.deserialize(body).codec().name();
                    }
                } catch (RuntimeException parseFailed) {
                    // A CRC-bad body stays diagnosable with placeholder fields; a CRC-valid malformed
                    // body is typed corruption and must not be normalized by the inspector.
                    if (crcOk) {
                        throw io.corruption(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                                "malformed page body", parseFailed);
                    }
                }
                records.add(new RecordInfo(i, min, max, count, codec, rec.framedLen(), crcOk));
            }
        }
        TrailerInfo trailer = new TrailerInfo(t.segMinKey(), t.segMaxKey(), t.totalRecords(),
                t.totalEntries(), t.maxRecordLen());
        return new Dump(magic, version, records, trailer);
    }
}
