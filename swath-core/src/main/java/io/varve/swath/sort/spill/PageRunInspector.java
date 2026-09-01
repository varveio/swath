/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.sort.SortMetrics;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only structural inspector for one {@link PageRunWriter} page-run segment
 * — the seam the {@code swath dump-run} debug subcommand consumes to give a page-run segment the
 * same on-disk inspectability {@code duckdb} gives Parquet staging. It reports the header
 * version, one {@link RecordInfo} per framed record ({@code [minKey, maxKey, count, codec, framedLen]}
 * plus a per-record CRC32C verdict), and the completeness {@link PageRunTrailer.Trailer}.
 *
 * <p><b>Reuses the shared read contract.</b> The trailer is read via {@link PageRunTrailer} (O(1), no
 * record walk); the framing/len-bound/CRC read and
 * the decode-free structural/min/max/count/codec parse are the single-sourced {@link
 * PageRunReader} primitives the merge/test consumers use (no row materialization and one header
 * parse). Unlike those consumers, this inspector does <b>not</b> throw on a
 * per-record CRC mismatch — it records {@code crcOk=false} and keeps walking (via
 * {@link PageRunReader#nextRecord()}), so a debug dump can show exactly which record is torn. It
 * never mutates the file.
 */
public final class PageRunInspector {

    private PageRunInspector() {
    }

    /** One framed record's structural summary. {@code codec} is {@code "?"} when the CRC failed. */
    public record RecordInfo(long index, byte[] minKey, byte[] maxKey, int count, String codec,
                             int framedLen, boolean crcOk) {
    }

    /** A whole segment's structural dump: header, records, and fixed trailer. */
    public record Dump(int magic, short formatVersion, List<RecordInfo> records,
                       PageRunTrailer.Trailer trailer) {
    }

    /**
     * Walk {@code path} read-only and return its structural {@link Dump}. Validates the header and
     * trailing magic, then reads each framed record's
     * length/CRC, CRC-verifies the body, and parses its leading min/max/count decode-free. A record
     * whose framed length is structurally impossible (non-positive or past {@code maxRecordLen}) still
     * throws — that is genuine structural corruption the walk cannot step over; a body-only bit-flip
     * is reported as {@code crcOk=false} and the walk continues.
     */
    public static Dump inspect(Path path) throws IOException {
        List<RecordInfo> records = new ArrayList<>();
        try (PageRunReader io = PageRunReader.open(path, SortMetrics.NO_OP)) {
            PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);

            // open() positioned the channel at the first record; walk exactly totalRecords records.
            for (long i = 0; i < trailer.totalRecords(); i++) {
                PageRunReader.Record rec = io.nextRecord();   // does NOT throw on a body CRC mismatch
                byte[] body = rec.body();
                boolean crcOk = rec.crcOk();

                byte[] min = new byte[0];
                byte[] max = new byte[0];
                int count = -1;
                String codec = "?";
                try {
                    PageBlockFormat.Header header = PageRunReader.parsePageHeader(body);
                    min = header.minKey();
                    max = header.maxKey();
                    count = header.count();
                    if (crcOk) {
                        codec = header.codec().name();
                    }
                } catch (RuntimeException parseFailed) {
                    // A CRC-bad body stays diagnosable with placeholder fields; a CRC-valid malformed
                    // body is typed corruption and must not be normalized by the inspector.
                    if (crcOk) {
                        throw io.corruption(PageRunCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                                "malformed page body", parseFailed);
                    }
                }
                records.add(new RecordInfo(i, min, max, count, codec, rec.framedLen(), crcOk));
            }
            return new Dump(io.magic(), io.formatVersion(), records, trailer);
        }
    }
}
