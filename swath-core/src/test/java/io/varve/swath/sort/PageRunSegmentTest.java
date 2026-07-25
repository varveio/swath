/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link PageRunSegmentWriter} + {@link PageRunSegmentReader} (page-run segment): round-trip,
 * exact-key trailer bounds, CRC/truncation fail-fast, the per-page re-sort of an out-of-order
 * page, multi-node minKey concatenation ordering, and the {@code writeIntermediate} cascade path.
 */
class PageRunSegmentTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();

    private PageRunSegmentWriter writer() {
        return new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
    }

    private static List<ListEntry> readBack(Path path) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        try (PageRunSegmentReader reader = new PageRunSegmentReader(path)) {
            while (reader.hasNext()) {
                out.add(reader.next());
            }
        }
        return out;
    }

    @Test
    void roundTripsSealedBufferInSortedOrder(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("b")));
        buffer.admit(1L, List.of(object("c"), object("d")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);

        Path path = dir.resolve("seg.pgr");
        SegmentResult result = writer().flush(sealed, path);

        assertThat(result.rows()).isEqualTo(4);
        assertThat(result.bytes()).isEqualTo(Files.size(path));
        assertThat(readBack(path)).containsExactly(object("a"), object("b"), object("c"), object("d"));
    }

    @Test
    void trailerBoundsAreTheExactFirstAndLastKeys(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("alpha"), object("bravo")));
        buffer.admit(2L, List.of(object("yankee"), object("zulu")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);

        Path path = dir.resolve("seg.pgr");
        writer().flush(sealed, path);

        PageRunSegmentReader.Trailer trailer = PageRunSegmentReader.readTrailer(path);
        // Exact keys, not truncated stats (§9.1): first page's firstKey / last page's lastKey.
        assertThat(trailer.segMinKey()).containsExactly(bytes("alpha"));
        assertThat(trailer.segMaxKey()).containsExactly(bytes("zulu"));
        assertThat(trailer.totalRecords()).isEqualTo(2);
        assertThat(trailer.totalEntries()).isEqualTo(4);
        assertThat(trailer.maxRecordLen()).isGreaterThan(0);
    }

    @Test
    void corruptRecordBodyFailsFastOnRead(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // Flip a byte inside the first record body (offset 14 = just past the 6-byte header + 8-byte
        // frame [len][crc]) — the CRC32C over the body no longer matches, so the read must throw.
        byte[] raw = Files.readAllBytes(path);
        raw[14] ^= 0x7F;
        Files.write(path, raw);

        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");
    }

    @Test
    void truncationBeforeTrailerFailsFastOnOpen(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // Chop the tail so the trailing magic is gone — a half-written page-run file has no valid
        // trailer and must be rejected whole (I6), not silently read as far as it goes.
        byte[] raw = Files.readAllBytes(path);
        Files.write(path, Arrays.copyOf(raw, raw.length / 2));

        assertThatThrownBy(() -> new PageRunSegmentReader(path)).isInstanceOf(IOException.class);
    }

    @Test
    void outOfOrderPageIsReSortedAndRepackedSoItReadsBackAscending(@TempDir Path dir) throws IOException {
        // A single page admitted out of order — pack() records it as !orderedUnderFullComparator, so
        // the writer must drain-sort-repack it before persisting.
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("d"), object("b"), object("a"), object("c")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        assertThat(sealed.pages().get(0).orderedUnderFullComparator()).isFalse();   // precondition

        Path path = dir.resolve("seg.pgr");
        writer().flush(sealed, path);

        List<ListEntry> out = readBack(path);
        assertThat(out).containsExactly(object("a"), object("b"), object("c"), object("d"));
        assertThat(isAscending(out)).isTrue();
    }

    @Test
    void outOfOrderPageReSortIsPackedWithTheWriterConfiguredCodec(@TempDir Path dir) throws IOException {
        // The re-pack path (an out-of-order page, drained/sorted/repacked at flush time) must honor
        // the writer's configured codec. Do not hardcode NONE here via the 2-arg
        // PageBlock.pack(entries, comparator) overload — it silently discards whatever codec the page
        // was originally admitted with.
        SortConfig zstdConfig = configWithCodec(PageCodec.ZSTD1);
        SortBuffer buffer = new SortBuffer(zstdConfig, CMP);
        buffer.admit(1L, List.of(object("d"), object("b"), object("a"), object("c")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        assertThat(sealed.pages().get(0).orderedUnderFullComparator()).isFalse();   // precondition: forces the re-pack path

        Path path = dir.resolve("seg.pgr");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.ZSTD1).flush(sealed, path);

        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(path);
        assertThat(dump.records()).hasSize(1);
        assertThat(dump.records().get(0).codec()).isEqualTo("ZSTD1");
        assertThat(readBack(path)).containsExactly(object("a"), object("b"), object("c"), object("d"));
    }

    @Test
    void outOfOrderPageReSortWithNoneConfiguredCodecStillWritesNone(@TempDir Path dir) throws IOException {
        SortConfig noneConfig = configWithCodec(PageCodec.NONE);
        SortBuffer buffer = new SortBuffer(noneConfig, CMP);
        buffer.admit(1L, List.of(object("d"), object("b"), object("a"), object("c")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        assertThat(sealed.pages().get(0).orderedUnderFullComparator()).isFalse();   // precondition: forces the re-pack path

        Path path = dir.resolve("seg.pgr");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE).flush(sealed, path);

        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(path);
        assertThat(dump.records()).hasSize(1);
        assertThat(dump.records().get(0).codec()).isEqualTo("NONE");
    }

    @Test
    void writeIntermediateCascadePagesArePackedWithTheWriterConfiguredCodec(@TempDir Path dir) throws IOException {
        // The cascade backstop's writeIntermediate must honor the writer's configured codec too — do
        // not hardcode NONE in either PageBlock.pack call here.
        List<ListEntry> sorted = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            sorted.add(object(String.format("k%06d", i)));
        }
        Path path = dir.resolve("intermediate.pgr");
        try (SortedCursor cursor = new InMemoryCursor(sorted, CMP, DuplicateHook.NO_OP)) {
            new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.ZSTD1)
                    .writeIntermediate(cursor, path);
        }

        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(path);
        assertThat(dump.records()).hasSizeGreaterThan(1);   // > INTERMEDIATE_PAGE_ENTRIES so multiple pages
        assertThat(dump.records()).allMatch(r -> r.codec().equals("ZSTD1"));
        assertThat(readBack(path)).containsExactlyElementsOf(sorted);
    }

    @Test
    void writeIntermediateWithNoneConfiguredCodecStillWritesNone(@TempDir Path dir) throws IOException {
        List<ListEntry> sorted = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            sorted.add(object(String.format("k%06d", i)));
        }
        Path path = dir.resolve("intermediate.pgr");
        try (SortedCursor cursor = new InMemoryCursor(sorted, CMP, DuplicateHook.NO_OP)) {
            new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                    .writeIntermediate(cursor, path);
        }

        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(path);
        assertThat(dump.records()).isNotEmpty();
        assertThat(dump.records()).allMatch(r -> r.codec().equals("NONE"));
    }

    private static SortConfig configWithCodec(PageCodec codec) {
        return SortConfigs.base().withSegmentCodec(codec);
    }

    @Test
    void multiNodeDisjointRangesEmitPagesInMinKeyOrder(@TempDir Path dir) throws IOException {
        // Node runs admitted with node 2 holding the LOWER range — the writer must order pages by
        // firstKey across all node runs, so the concatenation is globally sorted regardless.
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("m"), object("n")));
        buffer.admit(2L, List.of(object("a"), object("b")));
        buffer.admit(1L, List.of(object("x"), object("y")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);

        Path path = dir.resolve("seg.pgr");
        writer().flush(sealed, path);

        List<ListEntry> out = readBack(path);
        assertThat(out).containsExactly(object("a"), object("b"), object("m"),
                object("n"), object("x"), object("y"));
        assertThat(isAscending(out)).isTrue();
    }

    @Test
    void writeIntermediateRoundTripsASortedCursorAcrossManyPages(@TempDir Path dir) throws IOException {
        // > INTERMEDIATE_PAGE_ENTRIES so multiple pages are framed; zero-padded keys keep lexical ==
        // numeric order so the input list is already sorted.
        List<ListEntry> sorted = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            sorted.add(object(String.format("k%06d", i)));
        }

        Path path = dir.resolve("intermediate.pgr");
        long rows;
        try (SortedCursor cursor = new InMemoryCursor(sorted, CMP, DuplicateHook.NO_OP)) {
            rows = writer().writeIntermediate(cursor, path);
        }

        assertThat(rows).isEqualTo(2500);
        assertThat(readBack(path)).containsExactlyElementsOf(sorted);
        assertThat(PageRunSegmentReader.readTrailer(path).totalRecords()).isEqualTo(3);   // 1000+1000+500
    }

    @Test
    void zeroRecordSegmentRoundTripsToAnEmptyStream(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(config, CMP);
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);   // no admits at all
        assertThat(sealed.isEmpty()).isTrue();

        Path path = dir.resolve("empty.pgr");
        SegmentResult result = writer().flush(sealed, path);

        assertThat(result.rows()).isEqualTo(0);
        assertThat(readBack(path)).isEmpty();
        PageRunSegmentReader.Trailer trailer = PageRunSegmentReader.readTrailer(path);
        assertThat(trailer.totalRecords()).isEqualTo(0);
        assertThat(trailer.totalEntries()).isEqualTo(0);
    }

    @Test
    void writeIntermediateWithAnEmptyCursorRoundTripsToAnEmptyStream(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("empty-intermediate.pgr");
        long rows;
        try (SortedCursor cursor = new InMemoryCursor(List.of(), CMP, DuplicateHook.NO_OP)) {
            rows = writer().writeIntermediate(cursor, path);
        }
        assertThat(rows).isEqualTo(0);
        assertThat(readBack(path)).isEmpty();
    }

    @Test
    void hugeLenPrefixFailsFastViaTheMaxRecordLenBoundInsteadOfAllocating(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // The len prefix is NOT CRC-protected — a flipped byte driving it to ~2GB must be rejected by
        // the maxRecordLen bound BEFORE any allocation, not caught later by the (correct) CRC.
        byte[] raw = Files.readAllBytes(path);
        ByteBuffer.wrap(raw, PageRunSegmentWriter.HEADER_BYTES, 4).putInt(Integer.MAX_VALUE - 16);
        Files.write(path, raw);

        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("out of bounds");
    }

    @Test
    void corruptedTotalEntriesDownwardFailsTheEndOfStreamCrossCheck(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // Fixed trailer tail (28 bytes from EOF): [trailerStart u64][totalRecords u32]
        // [totalEntries u64][maxRecordLen u32][magic u32] — totalEntries sits at [size-16, size-8).
        // Lowering it by one must NOT let the stream silently end one entry short; it must throw.
        byte[] raw = Files.readAllBytes(path);
        int totalEntriesOffset = raw.length - 16;
        long declared = ByteBuffer.wrap(raw, totalEntriesOffset, 8).getLong();
        ByteBuffer.wrap(raw, totalEntriesOffset, 8).putLong(declared - 1);
        Files.write(path, raw);

        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("entry count mismatch");
    }

    @Test
    void frontierReaderFailsWhenTotalRecordsCorruptedToZeroInsteadOfSilentlyDroppingThePage(
            @TempDir Path dir) throws IOException {
        // A valid one-page segment (totalRecords=1, totalEntries=2) whose totalRecords is
        // bit-flipped DOWNWARD to 0 while totalEntries and the trailing magic stay valid. The frontier
        // reader's first advance() then loads NO page (recordsLeft==0 immediately) — the end-of-stream
        // completeness cross-check must fire unconditionally, not only when a body was loaded, so the
        // segment is never silently reported empty (which would drop its 2 rows in PageAwareMerger).
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("b")));
        Path path = dir.resolve("seg.pgr");
        writer().flush(buffer.seal(SealTrigger.DRAIN), path);

        PageRunSegmentReader.Trailer before = PageRunSegmentReader.readTrailer(path);
        assertThat(before.totalRecords()).isEqualTo(1);   // precondition: exactly one page / one record
        assertThat(before.totalEntries()).isEqualTo(2);

        // Fixed trailer tail (28 bytes from EOF): [trailerStart u64][totalRecords u32][totalEntries u64]
        // [maxRecordLen u32][magic u32] — totalRecords is the u32 at [size-20, size-16), just before
        // totalEntries at [size-16, size-8). Zeroing it leaves totalEntries=2 and the trailing magic valid.
        byte[] raw = Files.readAllBytes(path);
        ByteBuffer.wrap(raw, raw.length - 20, 4).putInt(0);
        Files.write(path, raw);

        // The decode-free frontier reader must throw here, never silently report empty (which would
        // drop 2 rows).
        assertThatThrownBy(() -> driveFrontierToEnd(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("entry count mismatch");

        // The entry-typed reader rejects the same corruption too, since it drives the same frontier
        // reader underneath.
        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("entry count mismatch");
    }

    private static void driveFrontierToEnd(Path path) throws IOException {
        try (PageFrontierReader reader = new PageFrontierReader(path)) {
            while (reader.hasPage()) {
                reader.advance();
            }
        }
    }

    @Test
    void readTrailerReturnsExactBoundsForAMultiPageSegmentViaTheOOneSeek(@TempDir Path dir) throws IOException {
        List<ListEntry> sorted = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            sorted.add(object(String.format("k%06d", i)));
        }
        Path path = dir.resolve("multi.pgr");
        try (SortedCursor cursor = new InMemoryCursor(sorted, CMP, DuplicateHook.NO_OP)) {
            writer().writeIntermediate(cursor, path);
        }

        PageRunSegmentReader.Trailer trailer = PageRunSegmentReader.readTrailer(path);
        assertThat(trailer.segMinKey()).containsExactly(bytes("k000000"));
        assertThat(trailer.segMaxKey()).containsExactly(bytes("k002499"));
        assertThat(trailer.totalRecords()).isEqualTo(3);   // 1000 + 1000 + 500
        assertThat(trailer.totalEntries()).isEqualTo(2500);
    }

    private void writeSimpleSegment(Path path, int entries) throws IOException {
        SortBuffer buffer = new SortBuffer(config, CMP);
        List<ListEntry> page = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            page.add(object(String.format("k%06d", i)));
        }
        buffer.admit(1L, page);
        writer().flush(buffer.seal(SealTrigger.DRAIN), path);
    }

    private static boolean isAscending(List<ListEntry> entries) {
        for (int i = 1; i < entries.size(); i++) {
            if (CMP.compare(entries.get(i - 1), entries.get(i)) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
