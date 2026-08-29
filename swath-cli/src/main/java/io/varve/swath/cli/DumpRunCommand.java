/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.sort.PageRunSegmentInspector;
import io.varve.swath.sort.PageRunTrailer;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * {@code swath dump-run <file.pageseg>} — a hidden, read-only debug inspector for a page-run
 * staging segment. It prints the header (magic/version), one line per framed record
 * ({@code [minKey, maxKey, count, codec, len]} with a CRC32C {@code OK}/{@code FAIL} verdict), and the
 * trailer-extension index summary, and completeness trailer ({@code segMin}/{@code segMax}/
 * {@code totalRecords}/{@code totalEntries}/{@code maxRecordLen}). This is the page-run equivalent of
 * the inspectability {@code duckdb} gives
 * columnar Parquet staging; the file is never modified.
 *
 * <p>Exit code: {@code 0} when every record's CRC verifies, {@code 1} when any record reports
 * {@code FAIL} or the segment could not be read (bad magic/version, truncation, structural
 * corruption).
 */
@Command(name = "dump-run", hidden = true,
        description = "Debug: dump a page-run staging segment (header, records, trailer, CRC).")
public final class DumpRunCommand implements Callable<Integer>, GlobalOptions.Carrier {

    @Spec
    CommandSpec spec;

    @Parameters(index = "0", paramLabel = "<file.pageseg>", description = "Page-run segment file to inspect.")
    Path file;

    @Mixin
    final GlobalOptions global = new GlobalOptions();

    @Override
    public GlobalOptions globalOptions() {
        return global;
    }

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PageRunSegmentInspector.Dump dump;
        try {
            dump = PageRunSegmentInspector.inspect(file);
        } catch (IOException e) {
            spec.commandLine().getErr().println("dump-run: " + e.getMessage());
            return 1;
        }

        out.println("page-run segment: " + file);
        out.printf("header: magic=0x%08x version=%d%n", dump.magic(), dump.formatVersion());
        out.println("records: " + dump.records().size());
        boolean anyFail = false;
        for (PageRunSegmentInspector.RecordInfo r : dump.records()) {
            anyFail |= !r.crcOk();
            out.printf("  [%d] min=%s max=%s count=%d codec=%s len=%d crc=%s%n",
                    r.index(), hex(r.minKey()), hex(r.maxKey()), r.count(), r.codec(),
                    r.framedLen(), r.crcOk() ? "OK" : "FAIL");
        }
        PageRunSegmentInspector.PageIndexInfo index = dump.pageIndex();
        out.printf("page-index: type=%d status=%s entries=%d firstOffset=%d lastOffset=%d%n",
                index.type(), index.status(), index.entries(), index.firstOffset(), index.lastOffset());
        PageRunTrailer.Trailer t = dump.trailer();
        out.printf("trailer: segMin=%s segMax=%s totalRecords=%d totalEntries=%d maxRecordLen=%d%n",
                hex(t.segMinKey()), hex(t.segMaxKey()), t.totalRecords(), t.totalEntries(),
                t.maxRecordLen());
        out.flush();
        return anyFail ? 1 : 0;
    }

    /** Lowercase hex, so an arbitrary (possibly non-UTF-8) key renders unambiguously. */
    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
