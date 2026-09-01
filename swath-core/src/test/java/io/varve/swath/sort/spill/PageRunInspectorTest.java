/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import static io.varve.swath.sort.finalize.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.stage.PageRunFixtures;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link PageRunInspector} (the {@code swath dump-run} engine): a page-run segment
 * dumps its header magic/version, one {@link PageRunInspector.RecordInfo} per framed record
 * (min/max/count/codec/len matching {@link PageRunReader}), and the completeness
 * trailer — with each record CRC-verified. A body-only bit-flip is reported as {@code crcOk=false}
 * (not thrown), so a debug dump pinpoints the torn record.
 */
class PageRunInspectorTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();

    private PageRunWriter writer() {
        return new PageRunWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCompression.NONE);
    }

    @Test
    void dumpMatchesWhatWasWrittenAndReportsCrcOk(@TempDir Path dir) throws IOException {
        // Two disjoint node runs → two pages ordered by minKey: [a,b] then [m,n].
        PageRunFixtures.Buffer buffer = PageRunFixtures.buffer(config, CMP);
        buffer.admit(1L, List.of(object("m"), object("n")));
        buffer.admit(2L, List.of(object("a"), object("b")));
        Path path = dir.resolve("seg.pageseg");
        writer().flush(buffer.seal(SealTrigger.DRAIN), path);

        PageRunInspector.Dump dump = PageRunInspector.inspect(path);

        assertThat(dump.magic()).isEqualTo(PageRunWriter.MAGIC);
        assertThat(dump.formatVersion()).isEqualTo(PageRunWriter.FORMAT_VERSION);
        // The inspector exposes the shared trailer, and the record count/entry totals agree.
        PageRunTrailer.Trailer canonical = PageRunTrailer.read(path);
        assertThat(dump.trailer().totalRecords()).isEqualTo(canonical.totalRecords());
        assertThat(dump.trailer().totalEntries()).isEqualTo(canonical.totalEntries());
        assertThat(dump.trailer().maxRecordLen()).isEqualTo(canonical.maxRecordLen());
        assertThat(dump.records()).hasSize((int) canonical.totalRecords());

        // Every record's decode-free min/max/count matches the frontier reader, CRC verifies, and the
        // codec resolved to a real PageCompression (never the "?" corruption sentinel).
        List<PageFrontierView> frontier = frontierWalk(path);
        assertThat(dump.records()).hasSameSizeAs(frontier);
        long entrySum = 0;
        for (int i = 0; i < frontier.size(); i++) {
            PageRunInspector.RecordInfo rec = dump.records().get(i);
            PageFrontierView exp = frontier.get(i);
            assertThat(rec.index()).isEqualTo(i);
            assertThat(rec.minKey()).as("record %d minKey", i).containsExactly(exp.min);
            assertThat(rec.maxKey()).as("record %d maxKey", i).containsExactly(exp.max);
            assertThat(rec.count()).isEqualTo(exp.count);
            assertThat(rec.framedLen()).isPositive();
            assertThat(rec.crcOk()).isTrue();
            assertThat(rec.codec()).isIn("NONE", "LZ4", "ZSTD1");
            entrySum += rec.count();
        }
        assertThat(entrySum).isEqualTo(dump.trailer().totalEntries());
    }

    @Test
    void bodyBitFlipIsReportedAsCrcFailNotThrown(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pageseg");
        PageRunFixtures.Buffer buffer = PageRunFixtures.buffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("b"), object("c")));
        writer().flush(buffer.seal(SealTrigger.DRAIN), path);

        // Flip a byte inside the first record body (offset HEADER_BYTES + 8-byte [len][crc] frame) so
        // its CRC32C no longer matches — the inspector must REPORT the failure, not throw.
        byte[] raw = Files.readAllBytes(path);
        raw[PageRunWriter.HEADER_BYTES + 8] ^= 0x7F;
        Files.write(path, raw);

        PageRunInspector.Dump dump = PageRunInspector.inspect(path);
        assertThat(dump.records()).isNotEmpty();
        assertThat(dump.records().get(0).crcOk()).isFalse();
        assertThat(dump.records().get(0).codec()).isEqualTo("?");   // not trusted after a CRC failure
    }

    private record PageFrontierView(byte[] min, byte[] max, int count) {
    }

    private static List<PageFrontierView> frontierWalk(Path path) throws IOException {
        List<PageFrontierView> out = new ArrayList<>();
        try (PageRunReader reader = PageRunReader.open(path, SortMetrics.NO_OP)) {
            PageRunReader.Page page;
            while ((page = reader.nextPage()) != null) {
                out.add(new PageFrontierView(
                        page.header().minKey().clone(), page.header().maxKey().clone(),
                        page.header().count()));
            }
        }
        return out;
    }
}
