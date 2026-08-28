/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Adversarial coverage for type-3 decoded-page metadata and pre-decompression admission. */
class PageRunDecodedResidencyTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void currentWriterPersistsExactMaximumRawPayload(@TempDir Path dir) throws IOException {
        Path segment = writeCompressed(dir.resolve("current.pageseg"), List.of(
                List.of(object("a"), object("a-long-repeated-suffix")),
                List.of(object("z"))));

        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
            PageRunPageIndex.ReadResult index = PageRunPageIndex.read(io, trailer, ignored -> { });
            int physicalMaximum = 0;
            for (int i = 0; i < trailer.totalRecords(); i++) {
                physicalMaximum = Math.max(physicalMaximum,
                        io.nextPage().header().rawPayloadLength());
            }

            assertThat(index.extensionType()).isEqualTo(PageRunPageIndex.TYPE);
            assertThat(index.hasDecodedPageMaximum()).isTrue();
            assertThat(index.maxRawPayloadLength()).isEqualTo(physicalMaximum);
            assertThat(PageRunFormat.currentListing().extensionType())
                    .isEqualTo(PageRunFormat.PAGE_INDEX_EXTENSION);
        }
    }

    @Test
    void crcValidUnderclaimFailsTypedBeforeCompressedPayloadDecode(@TempDir Path dir)
            throws IOException {
        Path segment = writeCompressed(dir.resolve("underclaim.pageseg"),
                List.of(List.of(object("compressible-key-" + "x".repeat(900)))));
        PageRunSegmentDescriptor descriptor = catalog(segment).descriptors().getFirst();
        int declared = descriptor.maxRawPayloadLength();
        underclaimDecodedMaximum(segment, declared - 1);
        PageRunSegmentDescriptor forged = catalog(segment).descriptors().getFirst();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> new PageFrontierReader(
                segment, metrics, forged.maxRawPayloadLength()))
                .isInstanceOfSatisfying(SegmentCorruptionException.class, failure ->
                        assertThat(failure.errorClass())
                                .isEqualTo(SegmentCorruptionException.PAGE_RUN_DECODED_PAGE_LIMIT));
        assertThat(metrics.count("SORT.page_run_decoded_page_limit")).isEqualTo(1);
    }

    @Test
    void tinyCompressedBodyWithHugeRawClaimIsRejectedBeforeAllocation(@TempDir Path dir)
            throws IOException {
        Path segment = writeCompressed(dir.resolve("bomb.pageseg"),
                List.of(List.of(object("a" + "x".repeat(900)))));
        PageRunSegmentDescriptor descriptor = catalog(segment).descriptors().getFirst();
        forgeRawPayloadLength(segment, 128 * 1024 * 1024);

        assertThatThrownBy(() -> new PageFrontierReader(
                segment, SortMetrics.NO_OP, descriptor.maxRawPayloadLength()))
                .isInstanceOfSatisfying(SegmentCorruptionException.class, failure ->
                        assertThat(failure.errorClass())
                                .isEqualTo(SegmentCorruptionException.PAGE_RUN_DECODED_PAGE_LIMIT));
    }

    @Test
    void legacyType2AndExtensionlessSegmentsRemainReadableWithoutBodyPreflight(@TempDir Path dir)
            throws IOException {
        Path type2 = writeCompressed(dir.resolve("type2.pageseg"),
                List.of(List.of(object("a"), object("b"))));
        convertCurrentIndexToLegacyType2(type2);
        PageRunCatalog type2Catalog = catalog(type2);
        assertThat(type2Catalog.descriptors().getFirst().extension().extensionType())
                .isEqualTo(PageRunPageIndex.LEGACY_TYPE);
        assertThat(type2Catalog.descriptors().getFirst().hasDecodedPageMaximum()).isFalse();
        assertThat(PageRunReads.keys(type2)).containsExactly("a", "b");

        Path extensionless = writeCompressed(dir.resolve("extensionless.pageseg"),
                List.of(List.of(object("c"), object("d"))));
        removeExtension(extensionless);
        PageRunCatalog absentCatalog = catalog(extensionless);
        assertThat(absentCatalog.descriptors().getFirst().extension().status())
                .isEqualTo(PageRunPageIndex.Status.ABSENT);
        assertThat(absentCatalog.descriptors().getFirst().hasDecodedPageMaximum()).isFalse();
        assertThat(PageRunReads.keys(extensionless)).containsExactly("c", "d");
    }

    @Test
    void plannerPricesRetainedEncodedBodyPlusDecodedPayloadAndRejectsSingleOversizePage(
            @TempDir Path dir) throws IOException {
        Path first = writeCompressed(dir.resolve("first.pageseg"),
                List.of(List.of(object("a" + "x".repeat(900)))));
        Path second = writeCompressed(dir.resolve("second.pageseg"),
                List.of(List.of(object("z" + "x".repeat(900)))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(first, second),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty());
        long streamPrice = 2 * catalog.maxRecordLen() + catalog.maxRawPayloadLength();
        SortConfig clampedConfig = SortConfigs.base().withFanIn(10)
                .withMergePerStreamBytes(1).withMergeBudgetBytes(streamPrice * 2 + streamPrice / 2);
        MergePlanner clamped = new MergePlanner(clampedConfig, SortMetrics.NO_OP, () -> -1);

        assertThat(clamped.serialFanIn(catalog)).isEqualTo(2);

        MergePlanner refused = new MergePlanner(
                clampedConfig.withMergeBudgetBytes(catalog.maxRawPayloadLength() - 1L),
                SortMetrics.NO_OP, () -> -1);
        assertThatThrownBy(() -> refused.effectiveRanges(4, catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum merge width does not fit decoded-page residency");

        MergePlanner floorRefused = new MergePlanner(
                clampedConfig.withMergeBudgetBytes(streamPrice * 2 - 1),
                SortMetrics.NO_OP, () -> -1);
        assertThatThrownBy(() -> floorRefused.serialFanIn(catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum_streams=2");
    }

    @Test
    void overlapClusterAggregateIsReservedBeforeEachDecompression(@TempDir Path dir)
            throws IOException {
        Path segment = writeCompressed(dir.resolve("overlap.pageseg"), List.of(
                List.of(object("a"), object("z" + "x".repeat(900))),
                List.of(object("b"), object("y" + "x".repeat(900)))));
        PageRunSegmentDescriptor descriptor = catalog(segment).descriptors().getFirst();
        long onePageOnly = descriptor.trailer().maxRecordLen()
                + descriptor.maxRawPayloadLength();
        PageFrontierReader reader = new PageFrontierReader(
                segment, SortMetrics.NO_OP, descriptor.maxRawPayloadLength());

        assertThatThrownBy(() -> new PageAwareMerger(List.of(reader), CMP,
                MergeScope.INTRA_SEGMENT, SortMetrics.NO_OP, MergeRunSink.NO_OP, onePageOnly))
                .isInstanceOfSatisfying(UncheckedIOException.class, failure ->
                        assertThat(failure.getCause())
                                .isInstanceOf(MergeMemoryExhaustedException.class));
    }

    @Test
    void transformAggregateRefusalPreservesOriginalsAndPriorPublication(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = writeCompressed(staging.resolve("overlap.pageseg"), List.of(
                List.of(object("a"), object("z" + "x".repeat(900))),
                List.of(object("b"), object("y" + "x".repeat(900)))));
        PageRunCatalog catalog = catalog(segment);
        long oneStreamPrice = 2 * catalog.maxRecordLen() + catalog.maxRawPayloadLength();
        SortConfig config = SortConfigs.base().withMergeParallelism(1)
                .withMergePerStreamBytes(1).withMergeBudgetBytes(oneStreamPrice);
        SortRun run = new SortRun(config, CMP, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY,
                MergeDiskPolicy.bypassed());
        Path priorFinal = Files.writeString(output.resolve(StagingNames.finalPart(0)), "prior");
        AtomicInteger publications = new AtomicInteger();

        assertThatThrownBy(() -> new SortTransform(run).transform(List.of(segment), output,
                staging, (parts, rows) -> publications.incrementAndGet(), ignored -> { },
                FinalPassListener.NO_OP))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("decoded-page retained residency exceeds");

        assertThat(publications).hasValue(0);
        assertThat(priorFinal).hasContent("prior");
        assertThat(segment).exists();
        try (var finals = Files.newDirectoryStream(output, StagingNames.OWN_FINAL_GLOB)) {
            List<Path> remaining = new ArrayList<>();
            finals.forEach(remaining::add);
            assertThat(remaining).containsExactly(priorFinal);
        }
    }

    private static Path writeCompressed(Path path, List<List<io.varve.swath.model.ListEntry>> pages)
            throws IOException {
        SortBuffer buffer = new SortBuffer(
                SortConfigs.base().withSegmentCodec(PageCodec.LZ4), CMP);
        long node = 0;
        for (List<io.varve.swath.model.ListEntry> page : pages) {
            buffer.admit(node++, page);
        }
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.LZ4)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static PageRunCatalog catalog(Path segment) throws IOException {
        return PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty());
    }

    private static void underclaimDecodedMaximum(Path path, int value) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        ByteBuffer.wrap(bytes).putInt(fixedTailStart - PageRunBoundarySample.CRC_BYTES
                - Integer.BYTES, value);
        rewriteExtensionCrc(bytes, extensionStart(bytes), fixedTailStart);
        Files.write(path, bytes);
    }

    private static void forgeRawPayloadLength(Path path, int rawPayloadLength) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int frameStart = PageRunSegmentWriter.HEADER_BYTES;
        int bodyLength = ByteBuffer.wrap(bytes).getInt(frameStart);
        int bodyStart = frameStart + 2 * Integer.BYTES;
        byte[] body = java.util.Arrays.copyOfRange(bytes, bodyStart, bodyStart + bodyLength);
        PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
        ByteBuffer.wrap(bytes).putInt(bodyStart + header.payloadOffset() - 2 * Integer.BYTES,
                rawPayloadLength);
        CRC32C crc = new CRC32C();
        crc.update(bytes, bodyStart, bodyLength);
        ByteBuffer.wrap(bytes).putInt(frameStart + Integer.BYTES, (int) crc.getValue());
        Files.write(path, bytes);
    }

    private static void convertCurrentIndexToLegacyType2(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int extensionStart = extensionStart(bytes);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int metadataStart = fixedTailStart - PageRunBoundarySample.CRC_BYTES - Integer.BYTES;
        byte[] legacy = new byte[bytes.length - Integer.BYTES];
        System.arraycopy(bytes, 0, legacy, 0, metadataStart);
        System.arraycopy(bytes, metadataStart + Integer.BYTES, legacy, metadataStart,
                bytes.length - metadataStart - Integer.BYTES);
        ByteBuffer data = ByteBuffer.wrap(legacy);
        data.putShort(extensionStart + Integer.BYTES, PageRunPageIndex.LEGACY_TYPE);
        data.putInt(extensionStart + 2 * Integer.BYTES,
                data.getInt(extensionStart + 2 * Integer.BYTES) - Integer.BYTES);
        int legacyFixedTailStart = fixedTailStart - Integer.BYTES;
        rewriteExtensionCrc(legacy, extensionStart, legacyFixedTailStart);
        Files.write(path, legacy);
    }

    private static void removeExtension(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int extensionStart = extensionStart(bytes);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        byte[] extensionless = new byte[extensionStart
                + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES];
        System.arraycopy(bytes, 0, extensionless, 0, extensionStart);
        System.arraycopy(bytes, fixedTailStart, extensionless, extensionStart,
                PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
        Files.write(path, extensionless);
    }

    private static int extensionStart(byte[] bytes) {
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int trailerStart = Math.toIntExact(ByteBuffer.wrap(bytes).getLong(fixedTailStart));
        int position = trailerStart;
        position += Short.BYTES + (ByteBuffer.wrap(bytes).getShort(position) & 0xffff);
        position += Short.BYTES + (ByteBuffer.wrap(bytes).getShort(position) & 0xffff);
        return position;
    }

    private static void rewriteExtensionCrc(
            byte[] bytes, int extensionStart, int fixedTailStart) {
        int crcPosition = fixedTailStart - PageRunBoundarySample.CRC_BYTES;
        CRC32C crc = new CRC32C();
        crc.update(bytes, extensionStart, crcPosition - extensionStart);
        ByteBuffer.wrap(bytes).putInt(crcPosition, (int) crc.getValue());
    }
}
