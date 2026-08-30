/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.zip.CRC32C;
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
        try (PageRunSegmentReader reader = reader(path, SortMetrics.NO_OP)) {
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

        PageRunTrailer.Trailer trailer = PageRunTrailer.read(path);
        // Exact unsigned extrema, not truncated stats (§9.1).
        assertThat(trailer.segMinKey()).containsExactly(bytes("alpha"));
        assertThat(trailer.segMaxKey()).containsExactly(bytes("zulu"));
        assertThat(trailer.totalRecords()).isEqualTo(2);
        assertThat(trailer.totalEntries()).isEqualTo(4);
        assertThat(trailer.maxRecordLen()).isGreaterThan(0);
    }

    @Test
    void nestedOverlappingPagesAreRejectedAtSealBeforeCompletion(@TempDir Path dir) {
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("z")));
        buffer.admit(2L, List.of(object("b"), object("c")));
        Path path = dir.resolve("nested.pageseg");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> new PageRunSegmentWriter(
                CMP, DuplicateHook.NO_OP, metrics, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path))
                .isInstanceOfSatisfying(SegmentCorruptionException.class, failure ->
                        assertThat(failure.errorClass())
                                .isEqualTo(SegmentCorruptionException.PAGE_RUN_PAGE_OVERLAP));
        assertThat(metrics.count("SORT.buffer_page_overlap")).isEqualTo(1);
        assertThat(metrics.count("SORT.segment_flushed")).isZero();
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
    void crcFailurePrecedesPersistedPageOwnershipHandoff(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("crc-before-owner.pageseg");
        writeSimpleSegment(path, 4);
        byte[] raw = Files.readAllBytes(path);
        raw[PageRunSegmentWriter.HEADER_BYTES + 8] ^= 0x40;
        Files.write(path, raw);

        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            assertThatThrownBy(io::nextPage)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CRC32C mismatch");
        }
    }

    @Test
    void truncationBeforeTrailerFailsFastOnOpen(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // Chop the tail so the trailing magic is gone — a half-written page-run file has no valid
        // trailer and must be rejected whole (I6), not silently read as far as it goes.
        byte[] raw = Files.readAllBytes(path);
        Files.write(path, Arrays.copyOf(raw, raw.length / 2));

        assertThatThrownBy(() -> reader(path, SortMetrics.NO_OP)).isInstanceOf(IOException.class);
    }

    @Test
    void undersizedSegmentRecordsHeaderRejectionAtTheLiveOpenBoundary(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("undersized.pgr");
        Files.write(path, new byte[PageRunHeader.PREFIX_BYTES]);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> reader(path, metrics))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_header_corruption")
                .hasMessageContaining("file too small");
        assertThat(metrics.count("SORT.page_run_header_corruption")).isEqualTo(1);
    }

    @Test
    void rawKeyRegressionIsRejectedBeforeTheSegmentSinkCanAdvanceACheckpoint(@TempDir Path dir)
            throws Exception {
        Path staging = Files.createDirectories(dir.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortLane lane = new SortLane(config, CMP, DuplicateHook.NO_OP, metrics,
                SortLaneMeters.NO_OP, staging, "regressing", finalized::add);
        lane.admit(1L, List.of(object("d"), object("b"), object("a"), object("c")));

        assertThatThrownBy(lane::close)
                .hasRootCauseInstanceOf(SegmentCorruptionException.class)
                .hasStackTraceContaining("raw key regressed inside an admitted page");
        assertThat(finalized)
                .as("SegmentSink must not publish a durable cursor for a rejected page")
                .isEmpty();
        assertThat(metrics.count("SORT.buffer_page_raw_key_regression")).isEqualTo(1);
        assertThat(staging).isEmptyDirectory();
    }

    @Test
    void fullComparatorDisorderWithMonotonicRawKeysRepacksAndKeepsTheDurableMaximum(
            @TempDir Path dir) throws Exception {
        ObjectEntry kV2 = version("k", "v2");
        ObjectEntry kV1 = version("k", "v1");
        Path staging = Files.createDirectories(dir.resolve("_staging"));
        List<SegmentResult> finalized = new ArrayList<>();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortLane lane = new SortLane(config, CMP, DuplicateHook.NO_OP, metrics,
                SortLaneMeters.NO_OP, staging, "versions", finalized::add);
        lane.admit(7L, List.of(kV2, kV1, object("z")));
        lane.close();

        assertThat(finalized).hasSize(1);
        assertThat(finalized.get(0).perNodeMaxKeys().get(7L)).containsExactly(bytes("z"));
        assertThat(readBack(finalized.get(0).path()))
                .containsExactly(kV1, kV2, object("z"));
        assertThat(metrics.count("SORT.buffer_page_repacked")).isEqualTo(1);
    }

    @Test
    void adjacentComparatorTieIsNotRepackedAndRetainsInputOrder(@TempDir Path dir)
            throws IOException {
        ObjectEntry first = objectWithSize("a", 2L);
        ObjectEntry second = objectWithSize("a", 1L);
        assertThat(CMP.compare(first, second)).as("precondition: payload is not an ordering field").isZero();

        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(first, second, object("c")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        assertThat(sealed.pages().get(0).orderedUnderFullComparator()).isTrue();

        Path path = dir.resolve("seg.pgr");
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, metrics, PageCodec.NONE)
                .flush(sealed, path);

        List<ListEntry> out = readBack(path);
        assertThat(out).containsExactly(first, second, object("c"));
        assertThat(metrics.count("SORT.buffer_page_repacked")).isZero();
    }

    @Test
    void byteGateFlushRemainsObservable(@TempDir Path dir) throws IOException {
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("b")));
        SealedBuffer sealed = buffer.seal(SealTrigger.BYTE_GATE);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, metrics, PageCodec.NONE)
                .flush(sealed, dir.resolve("seg.pgr"));

        assertThat(metrics.count("SORT.buffer_byte_gated")).isEqualTo(1);
    }

    @Test
    void segmentKindsOwnTheirCompletionCounterSemantics(@TempDir Path dir) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, metrics, PageCodec.NONE);
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a")));

        writer.flush(buffer.seal(SealTrigger.DRAIN), dir.resolve("listing.pageseg"));
        assertThat(metrics.count("SORT.segment_flushed")).isEqualTo(1);

        try (SortedCursor cascade = new InMemoryCursor(
                List.of(object("b")), CMP, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cascade, dir.resolve("cascade.pageseg"));
        }
        assertThat(metrics.count("SORT.segment_flushed"))
                .as("cascade intermediates retain their existing separate accounting")
                .isEqualTo(1);

        try (SortedCursor fixture = new InMemoryCursor(
                List.of(object("c")), CMP, DuplicateHook.NO_OP)) {
            writer.writeFixtureChunk(fixture, dir.resolve("fixture.pageseg"));
        }
        assertThat(metrics.count("SORT.segment_flushed")).isEqualTo(2);
    }

    @Test
    void listingFlushReportsThenRejectsAnEqualPageBoundary(@TempDir Path dir) {
        AtomicInteger duplicates = new AtomicInteger();
        SortBuffer buffer = new SortBuffer(config, CMP);
        buffer.admit(1L, List.of(object("a"), object("b")));
        buffer.admit(2L, List.of(object("b"), object("c")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        assertThatThrownBy(() -> new PageRunSegmentWriter(
                CMP, (previous, current) -> duplicates.incrementAndGet(), metrics, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), dir.resolve("boundary-duplicate.pageseg")))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_page_overlap");

        assertThat(duplicates).hasValue(1);
        assertThat(metrics.count("SORT.buffer_page_overlap")).isEqualTo(1);
    }

    @Test
    void outOfOrderPageReSortIsPackedWithTheWriterConfiguredCodec(@TempDir Path dir) throws IOException {
        // The re-pack path (an out-of-order page, drained/sorted/repacked at flush time) must honor
        // the writer's configured codec. Do not hardcode NONE here via the 2-arg
        // PageBlock.pack(entries, comparator) overload — it silently discards whatever codec the page
        // was originally admitted with.
        SortConfig zstdConfig = configWithCodec(PageCodec.ZSTD1);
        SortBuffer buffer = new SortBuffer(zstdConfig, CMP);
        buffer.admit(1L, List.of(version("k", "v2"), version("k", "v1"), object("z")));
        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);
        assertThat(sealed.pages().get(0).orderedUnderFullComparator()).isFalse();   // precondition: forces the re-pack path

        Path path = dir.resolve("seg.pgr");
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.ZSTD1).flush(sealed, path);

        PageRunSegmentInspector.Dump dump = PageRunSegmentInspector.inspect(path);
        assertThat(dump.records()).hasSize(1);
        assertThat(dump.records().get(0).codec()).isEqualTo("ZSTD1");
        assertThat(readBack(path))
                .containsExactly(version("k", "v1"), version("k", "v2"), object("z"));
    }

    @Test
    void outOfOrderPageReSortWithNoneConfiguredCodecStillWritesNone(@TempDir Path dir) throws IOException {
        SortConfig noneConfig = configWithCodec(PageCodec.NONE);
        SortBuffer buffer = new SortBuffer(noneConfig, CMP);
        buffer.admit(1L, List.of(version("k", "v2"), version("k", "v1"), object("z")));
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
        assertThat(PageRunTrailer.read(path).totalRecords()).isEqualTo(3);   // 1000+1000+500
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            assertThat(PageRunBoundarySample.read(
                    io, PageRunTrailer.read(io), ignored -> { }).status())
                    .isEqualTo(PageRunBoundarySample.Status.ABSENT);
        }
    }

    @Test
    void cascadeEncodingIsByteExactToTheIndependentRawFormatFixture(@TempDir Path dir)
            throws IOException {
        List<ListEntry> sorted = new ArrayList<>();
        for (int i = 0; i < 1_001; i++) {
            sorted.add(object(String.format("k%06d", i)));
        }
        Path expected = dir.resolve("expected.pageseg");
        PageRunRawFixtures.writeRawPageRun(expected,
                List.of(sorted.subList(0, 1_000), sorted.subList(1_000, 1_001)), CMP);

        Path actual = dir.resolve("actual.pageseg");
        try (SortedCursor cursor = new InMemoryCursor(sorted, CMP, DuplicateHook.NO_OP)) {
            writer().writeIntermediate(cursor, actual);
        }

        assertThat(Files.readAllBytes(actual)).containsExactly(Files.readAllBytes(expected));
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
        PageRunTrailer.Trailer trailer = PageRunTrailer.read(path);
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
    void crcValidMalformedPageHeadersFailAsTypedCorruption(@TempDir Path dir) throws IOException {
        List<BodyMutation> mutations = List.of(
                new BodyMutation("zero-count", body -> body.putInt(
                        PageRunRawFixtures.pageHeaderLayout(body).countOffset(), 0)),
                new BodyMutation("oversized-dictionary",
                        body -> body.putShort(
                                PageRunRawFixtures.pageHeaderLayout(body).countOffset() + 5,
                                (short) (PageBlock.DICT_CAP + 1))),
                new BodyMutation("unknown-codec",
                        body -> body.put(
                                PageRunRawFixtures.pageHeaderLayout(body).codecOffset(), (byte) 127)),
                new BodyMutation("negative-stored-length",
                        body -> body.putInt(
                                PageRunRawFixtures.pageHeaderLayout(body).storedLengthOffset(), -1)),
                new BodyMutation("inverted-bounds", body -> {
                    int firstKeyByte = 2;
                    body.put(firstKeyByte, (byte) 'z');
                }));

        for (BodyMutation mutation : mutations) {
            Path path = dir.resolve(mutation.name() + ".pageseg");
            writeSimpleSegment(path, 2);
            mutateFirstBodyAndRepairCrc(path, mutation.mutator());

            assertThatThrownBy(() -> readBack(path))
                    .as(mutation.name())
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_body_corruption");
        }
    }

    @Test
    void crcRepairedOverlongPageBoundsFailAtTheSharedReadBoundary(@TempDir Path dir)
            throws IOException {
        for (boolean overlongMinimum : List.of(true, false)) {
            Path path = dir.resolve(overlongMinimum ? "overlong-min.pageseg" : "overlong-max.pageseg");
            PageRunRawFixtures.writeCrcRepairedOverlongBound(path, overlongMinimum);

            assertThatThrownBy(() -> readBack(path))
                    .as(overlongMinimum ? "overlong minKey" : "overlong maxKey")
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_body_corruption")
                    .hasMessageContaining("exceeds the S3 key limit");
        }
    }

    @Test
    void crcValidDecodedRowsMustMatchHeaderCountBoundsAndPayloadLength(@TempDir Path dir)
            throws IOException {
        List<BodyMutation> mutations = List.of(
                new BodyMutation("declared-count-too-large",
                        body -> body.putInt(
                                PageRunRawFixtures.pageHeaderLayout(body).countOffset(), 3)),
                new BodyMutation("decoded-first-key-mismatch", body -> {
                    PageRunRawFixtures.PageHeaderLayout layout =
                            PageRunRawFixtures.pageHeaderLayout(body);
                    // NONE payload begins [object-tag][shared=0][suffixLen=7][first key bytes].
                    body.put(layout.payloadOffset() + 3, (byte) 'x');
                }),
                new BodyMutation("unexpected-trailing-payload",
                        body -> {
                            int offset = PageRunRawFixtures.pageHeaderLayout(body)
                                    .storedLengthOffset();
                            body.putInt(offset, body.getInt(offset) - 1);
                        }));

        for (BodyMutation mutation : mutations) {
            Path path = dir.resolve(mutation.name() + ".pageseg");
            writeSimpleSegment(path, 2);
            mutateFirstBodyAndRepairCrc(path, mutation.mutator());

            assertThatThrownBy(() -> readBack(path))
                    .as(mutation.name())
                    .isInstanceOf(SegmentCorruptionException.class)
                    .hasMessageContaining("error_class=page_run_body_corruption");
        }
    }

    @Test
    void crcValidInteriorRowRegressionFailsDuringEmissionAndCloseTimeDrain(@TempDir Path dir)
            throws IOException {
        Path path = PageRunRawFixtures.writeIndexedInteriorRowRegression(
                dir.resolve("interior-regression.pageseg"));

        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_body_corruption")
                .hasStackTraceContaining("decoded row order regressed inside persisted page");

        try (PageFrontierReader frontier = new PageFrontierReader(path, SortMetrics.NO_OP)) {
            PageBlockCursor cursor = frontier.decodeCurrentPage().cursor();
            assertThat(cursor.next()).isEqualTo(prefix("a"));
            assertThatThrownBy(cursor::drainAndValidate)
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasStackTraceContaining("decoded row order regressed inside persisted page");
        }
    }

    @Test
    void admissionRejectsAKeyThePersistedReaderWouldReject() {
        byte[] overlong = new byte[ByteMidpoint.MAX_KEY_LEN + 1];
        SortBuffer buffer = new SortBuffer(config, CMP);

        assertThatThrownBy(() -> buffer.admit(1L, List.of(
                new CommonPrefixEntry(KeyBytes.of(overlong)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the S3 key limit");
    }

    @Test
    void trailerRejectsAnOverlongDeclaredKeyAsTypedCorruption(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("overlong-trailer-key.pageseg");
        writeSimpleSegment(path, 2);
        byte[] raw = Files.readAllBytes(path);
        int fixedTailStart = raw.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long trailerStart = ByteBuffer.wrap(raw, fixedTailStart, Long.BYTES).getLong();
        byte[] padded = new byte[raw.length + ByteMidpoint.MAX_KEY_LEN + 1];
        System.arraycopy(raw, 0, padded, 0, fixedTailStart);
        System.arraycopy(raw, fixedTailStart, padded,
                fixedTailStart + ByteMidpoint.MAX_KEY_LEN + 1,
                PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
        ByteBuffer.wrap(padded, Math.toIntExact(trailerStart), Short.BYTES)
                .putShort((short) (ByteMidpoint.MAX_KEY_LEN + 1));
        Files.write(path, padded);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(path, metrics)) {
                PageRunTrailer.read(io);
            }
        }).isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("error_class=page_run_trailer_corruption")
                .hasMessageContaining("trailer key length 1025");
        assertThat(metrics.count("SORT.page_run_trailer_corruption")).isEqualTo(1);
    }

    @Test
    void admissionRejectsADictionaryValueThatCannotFitItsPersistedLength() {
        ObjectEntry overlongStorageClass = new ObjectEntry(KeyBytes.ofUtf8("a"), 1L, 0L, null,
                "x".repeat(0x1_0000), null, false, null, null, null, null);
        SortBuffer buffer = new SortBuffer(config, CMP);

        assertThatThrownBy(() -> buffer.admit(1L, List.of(overlongStorageClass)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dictionary value exceeds the persisted u16 length limit");
    }

    @Test
    void crcValidLowerTotalEntriesFailsTheEndOfStreamCrossCheck(@TempDir Path dir)
            throws IOException {
        Path path = dir.resolve("seg.pgr");
        writeSimpleSegment(path, 200);

        // Keep the fixed trailer CRC valid so this reaches the logical body/trailer cross-check,
        // rather than stopping at the outer corruption gate.
        byte[] raw = Files.readAllBytes(path);
        int totalEntriesOffset = raw.length - 20;
        long declared = ByteBuffer.wrap(raw, totalEntriesOffset, 8).getLong();
        ByteBuffer.wrap(raw, totalEntriesOffset, 8).putLong(declared - 1);
        rewriteFixedTrailerCrc(raw);
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

        PageRunTrailer.Trailer before = PageRunTrailer.read(path);
        assertThat(before.totalRecords()).isEqualTo(1);   // precondition: exactly one page / one record
        assertThat(before.totalEntries()).isEqualTo(2);

        // totalRecords is the u32 at [size-24, size-20). Repair the fixed-trailer CRC so the
        // independent empty/count consistency check is exercised.
        byte[] raw = Files.readAllBytes(path);
        ByteBuffer.wrap(raw, raw.length - 24, 4).putInt(0);
        rewriteFixedTrailerCrc(raw);
        Files.write(path, raw);

        // The decode-free frontier reader must throw here, never silently report empty (which would
        // drop 2 rows).
        assertThatThrownBy(() -> driveFrontierToEnd(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("inconsistent empty page-run trailer counts");

        // The entry-typed reader rejects the same corruption too, since it drives the same frontier
        // reader underneath.
        assertThatThrownBy(() -> readBack(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("inconsistent empty page-run trailer counts");
    }

    private static void driveFrontierToEnd(Path path) throws IOException {
        try (PageFrontierReader reader = new PageFrontierReader(path, SortMetrics.NO_OP)) {
            while (reader.hasPage()) {
                reader.advance();
            }
        }
    }

    private static PageRunSegmentReader reader(Path path, SortMetrics metrics) throws IOException {
        return new PageRunSegmentReader(new PageFrontierReader(path, metrics), CMP, metrics);
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

        PageRunTrailer.Trailer trailer = PageRunTrailer.read(path);
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

    private static void mutateFirstBodyAndRepairCrc(Path path, Consumer<ByteBuffer> mutator)
            throws IOException {
        byte[] file = Files.readAllBytes(path);
        int frameOffset = PageRunSegmentWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(file, frameOffset, 4).getInt();
        int bodyOffset = frameOffset + 8;
        ByteBuffer body = ByteBuffer.wrap(file, bodyOffset, bodyLength).slice();
        mutator.accept(body);
        CRC32C crc = new CRC32C();
        crc.update(file, bodyOffset, bodyLength);
        ByteBuffer.wrap(file, frameOffset + 4, 4).putInt((int) crc.getValue());
        Files.write(path, file);
    }

    private static void rewriteFixedTrailerCrc(byte[] file) {
        int fixedTailStart = file.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        CRC32C crc = new CRC32C();
        crc.update(file, fixedTailStart, PageRunSegmentWriter.TRAILER_FIELDS_BYTES);
        ByteBuffer.wrap(file).putInt(
                fixedTailStart + PageRunSegmentWriter.TRAILER_FIELDS_BYTES,
                (int) crc.getValue());
    }

    private record BodyMutation(String name, Consumer<ByteBuffer> mutator) {
    }

    private static ObjectEntry version(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, versionId,
                false, null, null, null, null);
    }

    private static ObjectEntry objectWithSize(String key, long size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, 0L, null, null, null,
                false, null, null, null, null);
    }

    private static CommonPrefixEntry prefix(String key) {
        return new CommonPrefixEntry(KeyBytes.ofUtf8(key));
    }

}
