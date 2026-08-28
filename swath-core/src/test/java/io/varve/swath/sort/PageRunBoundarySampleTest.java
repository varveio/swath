/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRunBoundarySampleTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void embeddedSelectionReadsNoPageBodyWhileLegacyFallbackStillDoes(@TempDir Path dir)
            throws IOException {
        Path extended = writePages(dir.resolve("extended.pageseg"), 4);
        Path legacy = stripExtension(extended, dir.resolve("legacy.pageseg"));
        corruptFirstPageBody(extended);
        corruptFirstPageBody(legacy);

        CountingMetrics metrics = new CountingMetrics();
        assertThat(hex(boundaries(descriptors(extended), 3, metrics))).hasSize(2);
        assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_boundary_global_capped")).isZero();
        assertThat(metrics.scanBytes.sum()).isZero();
        assertThat(metrics.embeddedEntries.sum()).isEqualTo(4);
        assertThat(metrics.progress.sum()).isEqualTo(1);

        assertThatThrownBy(() -> boundaries(
                descriptors(legacy), 3, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");
    }

    @Test
    void kickoffOpensSegmentExactlyOnceAndBoundarySelectionDoesNotReopen(@TempDir Path dir)
            throws IOException {
        List<Path> segments = List.of(
                writePages(dir.resolve("descriptor-0.pageseg"), 4),
                writePages(dir.resolve("descriptor-1.pageseg"), 4, 100),
                writePages(dir.resolve("descriptor-2.pageseg"), 4, 200));
        Map<Path, AtomicInteger> opens = new ConcurrentHashMap<>();
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                segments, path -> {
                    opens.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
                    return PageRunSegmentIo.open(path, SortMetrics.NO_OP);
                }, Optional.of(candidates::add)).descriptors();
        PageRunSegmentDescriptor descriptor = descriptors.getFirst();
        assertThat(opens).hasSize(segments.size());
        assertThat(opens.values()).allMatch(count -> count.get() == 1);
        assertThat(descriptor.fileSize()).isEqualTo(Files.size(segments.getFirst()));
        assertThat(descriptor.trailerStart()).isPositive();
        assertThat(descriptor.sample().status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);
        assertThat(descriptor.sample().entryCount()).isEqualTo(4);
        assertThat(descriptor.extension().status()).isEqualTo(PageRunPageIndex.Status.EMBEDDED);
        assertThat(descriptor.extension().locator()).isNotNull();

        for (Path segment : segments) {
            Files.delete(segment);
        }
        CountingMetrics metrics = new CountingMetrics();

        assertThat(hex(MergePlanner.boundaries(descriptors, candidates, 3, metrics)))
                .hasSize(2);
        assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isEqualTo(1);
        assertThat(metrics.embeddedEntries.sum()).isEqualTo(12);
        assertThat(metrics.scanBytes.sum()).isZero();
    }

    @Test
    void serialKickoffReadsResourceMetadataWithoutRetainingKeyCollections(
            @TempDir Path dir) throws IOException {
        Path segment = writePages(dir.resolve("serial.pageseg"), 4);
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                List.of(segment), path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.empty()).descriptors();

        PageRunBoundarySample.ReadResult sample = descriptors.getFirst().sample();
        assertThat(sample.status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);
        assertThat(sample.entryCount()).isEqualTo(4);
        assertThat(sample.bytesRead()).isPositive();
        assertThat(Arrays.stream(PageRunSegmentDescriptor.class.getRecordComponents())
                .map(component -> component.getType()))
                .allMatch(type -> type != List.class);
        assertThat(Arrays.stream(PageRunBoundarySample.ReadResult.class.getRecordComponents())
                .map(component -> component.getType()))
                .allMatch(type -> type != List.class && !type.isArray());
        assertThat(descriptors.getFirst().extension().status()).isEqualTo(PageRunPageIndex.Status.EMBEDDED);
        assertThat(descriptors.getFirst().extension().bytesRead()).isPositive();
        assertThat(descriptors.getFirst().extension().locator()).isNotNull();
        assertThat(descriptors.getFirst().maxRawPayloadLength()).isPositive();
    }

    @Test
    void explicitSerialAndArbitraryTransformsDoNotEngageBoundarySampling(@TempDir Path root)
            throws IOException {
        assertTransformSkipsBoundarySampling(root.resolve("serial"), 1,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES);
        assertTransformSkipsBoundarySampling(root.resolve("arbitrary"), 4,
                MergeInputProfile.ARBITRARY_SORTED_RUNS);
    }

    @Test
    void exact4096And4097SystematicSemantics(@TempDir Path dir) throws IOException {
        Path atCap = writePages(dir.resolve("4096.pageseg"), 4_096);
        Path aboveCap = writePages(dir.resolve("4097.pageseg"), 4_097);

        SampleRead full = readSample(atCap);
        SampleRead thinned = readSample(aboveCap);
        assertThat(full.keys()).hasSize(4_096);
        assertThat(full.keys().getFirst()).containsExactly(bytes("k00000"));
        assertThat(full.keys().getLast()).containsExactly(bytes("k04095"));
        assertThat(thinned.keys()).hasSize(2_049);
        assertThat(thinned.keys().get(1)).containsExactly(bytes("k00002"));
        assertThat(thinned.keys().getLast()).containsExactly(bytes("k04096"));
    }

    @Test
    void sampleWriterBatchesMaximumCountIntoChunkBoundedWrites() throws IOException {
        List<byte[]> keys = new ArrayList<>(PageRunBoundarySample.MAX_ENTRIES);
        for (int i = 0; i < PageRunBoundarySample.MAX_ENTRIES; i++) {
            byte[] key = new byte[257];
            ByteBuffer.wrap(key).putInt(i);
            keys.add(key);
        }
        CountingChannel channel = new CountingChannel();

        PageRunBoundarySample.write(channel, keys);

        long expectedBytes = 16L + 4 + keys.stream().mapToLong(key -> 2L + key.length).sum();
        assertThat(channel.bytes).isEqualTo(expectedBytes);
        assertThat(channel.writes)
                .isEqualTo((expectedBytes + PageRunBoundarySample.IO_BUFFER_BYTES - 1)
                        / PageRunBoundarySample.IO_BUFFER_BYTES);
    }

    @Test
    void embeddedLegacyAndMixedInputsChooseIdenticalBoundaries(@TempDir Path dir) throws IOException {
        Path first = writePages(dir.resolve("first.pageseg"), 17);
        Path second = writePages(dir.resolve("second.pageseg"), 13, 100);
        Path firstLegacy = stripExtension(first, dir.resolve("first-legacy.pageseg"));
        Path secondLegacy = stripExtension(second, dir.resolve("second-legacy.pageseg"));

        List<String> embedded = hex(boundaries(descriptors(first, second), 8,
                SortMetrics.NO_OP));
        List<String> legacy = hex(boundaries(
                descriptors(firstLegacy, secondLegacy), 8,
                SortMetrics.NO_OP));
        CountingMetrics mixedMetrics = new CountingMetrics();
        List<String> mixed = hex(boundaries(
                descriptors(first, secondLegacy), 8,
                mixedMetrics));

        assertThat(embedded).containsExactlyElementsOf(legacy);
        assertThat(mixed).containsExactlyElementsOf(legacy);
        assertThat(mixedMetrics.count("SORT.merge_boundary_source_mixed")).isEqualTo(1);
        assertThat(mixedMetrics.count("SORT.merge_boundary_fallback_absent")).isEqualTo(1);
    }

    @Test
    void type1ExtensionRemainsAnEmbeddedMinimaOnlyBoundarySource(@TempDir Path dir)
            throws IOException {
        Path legacy = writeLegacyPages(dir.resolve("type1.pageseg"), 7);
        PreparedDescriptors prepared = descriptors(legacy);
        PageRunSegmentDescriptor descriptor = prepared.descriptors().getFirst();

        assertThat(descriptor.extension().status())
                .isEqualTo(PageRunPageIndex.Status.EMBEDDED_MINIMA_ONLY);
        assertThat(descriptor.extension().extensionType()).isEqualTo(PageRunBoundarySample.TYPE);
        assertThat(descriptor.extension().locator()).isNull();
        assertThat(descriptor.sample().status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);
        assertThat(descriptor.sample().entryCount()).isEqualTo(7);
        assertThat(boundaries(prepared, 3, SortMetrics.NO_OP)).hasSize(2);
    }

    @Test
    void rowsPolicyFallsBackExactlyForLegacyInvalidAndMixedInputs(@TempDir Path dir)
            throws IOException {
        Path type1 = writeLegacyPages(dir.resolve("type1-fallback.pageseg"), 7);
        assertRowsFallback(List.of(type1),
                "SORT.merge_boundary_rows_fallback_type1");

        Path extensionlessSource = writePages(dir.resolve("extensionless-source.pageseg"), 7);
        Path extensionless = stripExtension(
                extensionlessSource, dir.resolve("extensionless-fallback.pageseg"));
        assertRowsFallback(List.of(extensionless),
                "SORT.merge_boundary_rows_fallback_extensionless");

        Path invalid = writePages(dir.resolve("invalid-fallback.pageseg"), 7);
        mutateExtension(invalid, Mutation.CRC);
        assertRowsFallback(List.of(invalid),
                "SORT.merge_boundary_rows_fallback_invalid");

        Path type2 = writePages(dir.resolve("type2-mixed.pageseg"), 7, 100);
        assertRowsFallback(List.of(type2, type1),
                "SORT.merge_boundary_rows_fallback_mixed");
    }

    @Test
    void malformedExtensionsFallBackTransactionallyWithExactReasons(@TempDir Path dir)
            throws IOException {
        for (Mutation mutation : Mutation.values()) {
            Path path = writeLegacyPages(dir.resolve(mutation.name() + ".pageseg"), 5);
            Layout originalLayout = layout(Files.readAllBytes(path));
            Path legacy = stripExtension(path, dir.resolve(mutation.name() + "-legacy.pageseg"));
            List<byte[]> expected = boundaries(descriptors(legacy), 3,
                    SortMetrics.NO_OP);
            mutateExtension(path, mutation);
            CountingMetrics metrics = new CountingMetrics();

            assertByteExact(boundaries(descriptors(path), 3, metrics), expected);
            assertThat(metrics.count("SORT." + mutation.reason)).as(mutation.name()).isEqualTo(1);
            assertThat(metrics.count("SORT.merge_boundary_source_scan")).isEqualTo(1);
            assertThat(metrics.embeddedEntries.sum()).isZero();
            long expectedScanBytes = originalLayout.trailerStart
                    - PageRunSegmentWriter.HEADER_BYTES;
            assertThat(metrics.scanBytes.sum()).isEqualTo(expectedScanBytes);
            long expectedAttemptBytes = switch (mutation) {
                case UNKNOWN, LENGTH, COUNT -> 16;
                case CRC, ORDER, KEY_LENGTH, TRAILING_PAYLOAD, BOUNDS ->
                        originalLayout.extensionBytes();
            };
            assertThat(metrics.embeddedBytes.sum()).as(mutation.name())
                    .isEqualTo(expectedAttemptBytes);
        }
    }

    @Test
    void malformedTrailerBoundsFailDescriptorKickoff(@TempDir Path dir) throws IOException {
        Path path = writePages(dir.resolve("bad-trailer-bounds.pageseg"), 5);
        mutateExtensionStart(path);

        assertThatThrownBy(() -> boundaries(
                descriptors(path), 3, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailer key exceeds trailer bounds");
    }

    @Test
    void invalidFixedTrailerOffsetsAreSegmentCorruptionNotExtensionFallback(@TempDir Path dir)
            throws IOException {
        for (boolean beforeHeader : List.of(true, false)) {
            Path path = writePages(dir.resolve("bad-offset-" + beforeHeader + ".pageseg"), 5);
            byte[] bytes = Files.readAllBytes(path);
            int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
            ByteBuffer.wrap(bytes).putLong(fixedTailStart, beforeHeader ? 0L : bytes.length);
            Files.write(path, bytes);

            assertThatThrownBy(() -> boundaries(
                    descriptors(path), 3, SortMetrics.NO_OP))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("invalid page-run trailer offset");
        }
    }

    @Test
    void emptySegmentCarriesAValidEmptySample(@TempDir Path dir) throws IOException {
        Path path = writePages(dir.resolve("empty.pageseg"), 0);

        SampleRead read = readSample(path);
        PageRunBoundarySample.ReadResult sample = read.result();

        assertThat(sample.status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);
        assertThat(read.keys()).isEmpty();
        // CRC-first validation reads the type-3 block once, including decoded-page metadata.
        assertThat(sample.bytesRead()).isEqualTo(32);
    }

    @Test
    void legacyFallbackCountsOnlyTheExactFramedRecordRegion(@TempDir Path dir) throws IOException {
        Path embedded = writePages(dir.resolve("extended.pageseg"), 7);
        Path legacy = stripExtension(embedded, dir.resolve("legacy.pageseg"));
        Layout layout = layout(Files.readAllBytes(legacy));
        CountingMetrics metrics = new CountingMetrics();

        boundaries(descriptors(legacy), 3, metrics);

        assertThat(metrics.embeddedBytes.sum()).isZero();
        assertThat(metrics.scanBytes.sum())
                .isEqualTo(layout.trailerStart - PageRunSegmentWriter.HEADER_BYTES);
    }

    @Test
    void embeddedAndLegacyBoundariesMatchForRepeatedAndExtremeBinaryMinima(@TempDir Path dir)
            throws IOException {
        byte[] binary = {0x00, (byte) 0x80, (byte) 0xff};
        byte[] s3Max = extremeKey(1_024, (byte) 0x80);
        List<byte[]> minima = List.of(new byte[]{0x00}, binary, binary.clone(), new byte[]{(byte) 0x80},
                s3Max, new byte[]{(byte) 0xff});
        Path embedded = writeBinaryPages(dir.resolve("extremes.pageseg"), minima);
        Path legacy = stripExtension(embedded, dir.resolve("extremes-legacy.pageseg"));

        SampleRead sample = readSample(embedded);
        assertByteExact(sample.keys(), minima);
        assertThat(sample.keys().get(1)).containsExactly(0x00, (byte) 0x80, (byte) 0xff);
        assertThat(sample.keys().get(4)).hasSize(1_024);
        assertByteExact(boundaries(descriptors(embedded), 32, SortMetrics.NO_OP),
                boundaries(descriptors(legacy), 32, SortMetrics.NO_OP));
    }

    @Test
    void wholeRunBoundaryCandidatesStayBoundedAndDistributionRepresentative() {
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        MergePlanner.BoundaryCandidates cloneGuard =
                new MergePlanner.BoundaryCandidates();
        byte[] mutable = KeyBytes.ofUtf8("k999999").rawUnsafe().clone();
        cloneGuard.add(mutable);
        mutable[0] = 'x';
        for (int i = 0; i < 100_000; i++) {
            byte[] key = KeyBytes.ofUtf8(String.format("k%06d", i)).rawUnsafe();
            candidates.add(key);
            candidates.add(key);   // repeated minima do not consume another retained slot
        }

        List<byte[]> retained = candidates.sortedKeys();
        assertThat(candidates.capped()).isTrue();
        assertThat(retained).hasSize(MergePlanner.MAX_BOUNDARY_CANDIDATES);
        for (int i = 1; i < retained.size(); i++) {
            assertThat(KeyBytes.compareUnsigned(retained.get(i - 1), retained.get(i))).isNegative();
        }
        int median = Integer.parseInt(KeyBytes.of(retained.get(retained.size() / 2))
                .asString().substring(1));
        assertThat(median).isBetween(45_000, 55_000);
        assertThat(cloneGuard.sortedKeys().stream().map(KeyBytes::of).map(KeyBytes::asString))
                .contains("k999999")
                .doesNotContain("x999999");
    }

    @Test
    void bottomHashRetentionIsOrderIndependentForLongKeys() {
        int retainedLimit = 4_096;
        List<byte[]> longKeys = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            byte[] key = new byte[1_024];
            Arrays.fill(key, (byte) ('a' + i % 23));
            ByteBuffer.wrap(key).putInt(i);
            longKeys.add(key);
        }

        MergePlanner.BoundaryCandidates ascending =
                new MergePlanner.BoundaryCandidates(retainedLimit);
        MergePlanner.BoundaryCandidates descending =
                new MergePlanner.BoundaryCandidates(retainedLimit);
        longKeys.forEach(ascending::add);
        Collections.reverse(longKeys);
        longKeys.forEach(descending::add);

        assertThat(ascending.capped()).isTrue();
        assertThat(descending.capped()).isTrue();
        assertByteExact(ascending.sortedKeys(), descending.sortedKeys());
        assertThat(ascending.sortedKeys()).hasSize(retainedLimit).allMatch(key -> key.length == 1_024);
    }

    @Test
    void boundarySelectionReportsWholeRunCapEngagement(@TempDir Path dir) throws IOException {
        List<Path> segments = new ArrayList<>();
        int segmentCount = MergePlanner.MAX_BOUNDARY_CANDIDATES
                / PageRunBoundarySample.MAX_ENTRIES + 1;
        for (int segment = 0; segment < segmentCount; segment++) {
            List<byte[]> minima = new ArrayList<>();
            for (int page = 0; page < PageRunBoundarySample.MAX_ENTRIES; page++) {
                minima.add(bytes(String.format("s%02d-k%04d", segment, page)));
            }
            segments.add(writeBinaryPages(dir.resolve("segment-" + segment + ".pageseg"), minima));
        }
        CountingMetrics metrics = new CountingMetrics();

        PreparedDescriptors prepared = descriptors(segments);
        assertThat(boundaries(prepared, 8, metrics)).hasSize(7);
        assertThat(prepared.candidates().size())
                .isEqualTo(MergePlanner.MAX_BOUNDARY_CANDIDATES);
        assertThat(prepared.descriptors())
                .allMatch(descriptor -> descriptor.sample().entryCount()
                        == PageRunBoundarySample.MAX_ENTRIES);
        assertThat(metrics.count("SORT.merge_boundary_global_capped")).isEqualTo(1);
    }

    @Test
    void legacyStyleReaderIgnoresExtensionAndReadsEveryRow(@TempDir Path dir) throws IOException {
        Path path = writePages(dir.resolve("extended.pageseg"), 7);
        List<String> keys = new ArrayList<>();
        try (EntryStream reader = PageRunRawFixtures.trustingEntryStream(path)) {
            while (reader.hasNext()) {
                keys.add(new String(reader.next().key().raw(), java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        assertThat(keys).containsExactly("k00000", "k00001", "k00002", "k00003", "k00004",
                "k00005", "k00006");
    }

    @Test
    void embeddedPathStillRejectsFirstMiddleAndLastPageCorruptionAndSweepsTemps(@TempDir Path dir)
            throws IOException {
        for (int page : List.of(0, 5, 11)) {
            Path root = Files.createDirectories(dir.resolve("corrupt-" + page));
            Path staging = Files.createDirectories(root.resolve("_staging"));
            Path data = Files.createDirectories(root.resolve("data"));
            Path segment = writePages(staging.resolve("segment.pageseg"), 12);
            corruptPageBody(segment, page);
            CountingMetrics metrics = new CountingMetrics();
            SortConfig config = SortConfigs.base().withMergeParallelism(4);
            SortTransform transform = new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP,
                    EqualKeyPolicy.ALLOW, metrics, SortedFileWriterFactory.DEFAULT,
                    MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                    SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));

            assertThatThrownBy(() -> transform.transform(List.of(segment), data, staging,
                    PublishListener.NO_OP, units -> { }, FinalPassListener.NO_OP))
                    .as("page %s", page)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CRC32C mismatch");
            assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isEqualTo(1);
            try (var files = Files.list(data)) {
                assertThat(files).isEmpty();
            }
            try (var files = Files.list(staging)) {
                assertThat(files.map(p -> p.getFileName().toString())
                        .filter(name -> name.startsWith("prange-") || name.startsWith("merge-r")))
                        .isEmpty();
            }
        }
    }

    private static void assertTransformSkipsBoundarySampling(Path root, int mergeParallelism,
            MergeInputProfile inputProfile) throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = List.of(
                writePages(staging.resolve("first.pageseg"), 4),
                writePages(staging.resolve("second.pageseg"), 4, 100));
        CountingMetrics metrics = new CountingMetrics();
        SortConfig config = SortConfigs.base().withMergeParallelism(mergeParallelism);
        SortTransformResult result = new SortTransform(new SortRun(config, CMP,
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, metrics,
                SortedFileWriterFactory.DEFAULT, inputProfile, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY))
                .transform(segments, output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(result.totalRows()).isEqualTo(8);
        assertThat(metrics.embeddedEntries.sum()).isZero();
        assertThat(metrics.embeddedBytes.sum()).isZero();
        assertThat(metrics.scanBytes.sum()).isZero();
        assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isZero();
        assertThat(metrics.count("SORT.merge_boundary_source_scan")).isZero();
        assertThat(metrics.count("SORT.merge_range_index_seek")).isZero();
        assertThat(metrics.count("SORT.merge_range_index_absent")).isZero();
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isZero();
    }

    private static Path writePages(Path path, int count) throws IOException {
        return writePages(path, count, 0);
    }

    private static Path writePages(Path path, int count, int offset) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfig.fromSystemProperties(), CMP);
        for (int i = 0; i < count; i++) {
            buffer.admit(i, List.of(object(String.format("k%05d", offset + i))));
        }
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static Path writeBinaryPages(Path path, List<byte[]> keys) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfig.fromSystemProperties(), CMP);
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            buffer.admit(i, List.of(new ObjectEntry(KeyBytes.of(key), 1L, 0L, null, null, null,
                    false, null, null, null, null)));
        }
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private static byte[] extremeKey(int length, byte first) {
        byte[] key = new byte[length];
        key[0] = first;
        key[1] = 0x00;
        key[2] = (byte) 0x80;
        key[3] = (byte) 0xff;
        return key;
    }

    private static SampleRead readSample(Path path) throws IOException {
        List<byte[]> keys = new ArrayList<>();
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            PageRunPageIndex.ReadResult extension =
                    PageRunPageIndex.read(io, PageRunTrailer.read(io), keys::add);
            return new SampleRead(extension.boundarySample(), List.copyOf(keys));
        }
    }

    private static Path writeLegacyPages(Path path, int pages) throws IOException {
        Path source = path.resolveSibling(path.getFileName() + ".type2");
        writePages(source, pages);
        SampleRead sample = readSample(source);
        byte[] bytes = Files.readAllBytes(source);
        Layout layout = layout(bytes);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            SortTestSupport.writeFully(channel, ByteBuffer.wrap(bytes, 0,
                    Math.toIntExact(layout.extensionStart)));
            PageRunBoundarySample.write(channel, sample.keys());
            SortTestSupport.writeFully(channel, ByteBuffer.wrap(bytes,
                    Math.toIntExact(layout.fixedTailStart),
                    PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES));
        }
        Files.delete(source);
        return path;
    }

    private static PreparedDescriptors descriptors(Path... paths) throws IOException {
        return descriptors(List.of(paths));
    }

    private static PreparedDescriptors descriptors(List<Path> paths) throws IOException {
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(paths,
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();
        return new PreparedDescriptors(descriptors, candidates);
    }

    private static List<byte[]> boundaries(PreparedDescriptors prepared, int desiredRanges,
                                           SortMetrics metrics) throws IOException {
        return MergePlanner.boundaries(prepared.descriptors(), prepared.candidates(),
                desiredRanges, metrics);
    }

    private static void assertRowsFallback(List<Path> paths, String reason) throws IOException {
        PreparedDescriptors expected = descriptors(paths);
        List<byte[]> distinct = MergePlanner.boundaries(
                expected.descriptors(), expected.candidates(), 4,
                MergeBoundaryPolicy.DISTINCT, SortMetrics.NO_OP);
        PreparedDescriptors actual = descriptors(paths);
        CountingMetrics metrics = new CountingMetrics();
        List<byte[]> rows = MergePlanner.boundaries(
                actual.descriptors(), actual.candidates(), 4,
                MergeBoundaryPolicy.ROWS, metrics);

        assertByteExact(rows, distinct);
        assertThat(metrics.count(reason)).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_boundary_rows_on")).isZero();
    }

    private record PreparedDescriptors(List<PageRunSegmentDescriptor> descriptors,
                                       MergePlanner.BoundaryCandidates candidates) {
    }

    private record SampleRead(PageRunBoundarySample.ReadResult result, List<byte[]> keys) {
    }

    private static Path stripExtension(Path source, Path destination) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        Layout layout = layout(bytes);
        byte[] legacy = new byte[Math.toIntExact(layout.extensionStart
                + PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES)];
        System.arraycopy(bytes, 0, legacy, 0, Math.toIntExact(layout.extensionStart));
        System.arraycopy(bytes, Math.toIntExact(layout.fixedTailStart), legacy,
                Math.toIntExact(layout.extensionStart), PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
        Files.write(destination, legacy);
        return destination;
    }

    private static void corruptFirstPageBody(Path path) throws IOException {
        corruptPageBody(path, 0);
    }

    private static void corruptPageBody(Path path, int page) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            long position = PageRunSegmentWriter.HEADER_BYTES;
            for (int i = 0; i < page; i++) {
                ByteBuffer length = ByteBuffer.allocate(4);
                channel.read(length, position);
                position += 8L + length.flip().getInt();
            }
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, position + 8);
            one.flip();
            channel.write(ByteBuffer.wrap(new byte[]{(byte) (one.get() ^ 0x7f)}), position + 8);
        }
    }

    private enum Mutation {
        UNKNOWN("merge_boundary_fallback_unknown"),
        LENGTH("merge_boundary_fallback_invalid_length"),
        COUNT("merge_boundary_fallback_invalid_count"),
        CRC("merge_boundary_fallback_invalid_crc"),
        ORDER("merge_boundary_fallback_invalid_order"),
        KEY_LENGTH("merge_boundary_fallback_invalid_length"),
        TRAILING_PAYLOAD("merge_boundary_fallback_invalid_length"),
        BOUNDS("merge_boundary_fallback_invalid_bounds");

        private final String reason;

        Mutation(String reason) {
            this.reason = reason;
        }
    }

    private static void mutateExtension(Path path, Mutation mutation) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        Layout layout = layout(bytes);
        int extension = Math.toIntExact(layout.extensionStart);
        switch (mutation) {
            case UNKNOWN -> bytes[extension] ^= 1;
            case LENGTH -> ByteBuffer.wrap(bytes).putInt(extension + 8,
                    ByteBuffer.wrap(bytes).getInt(extension + 8) + 1);
            case COUNT -> ByteBuffer.wrap(bytes).putInt(extension + 12,
                    ByteBuffer.wrap(bytes).getInt(extension + 12) + 1);
            case CRC -> bytes[extension + 18] = 'a';
            case ORDER -> {
                int firstLength = ByteBuffer.wrap(bytes).getShort(extension + 16) & 0xffff;
                int secondLengthPos = extension + 18 + firstLength;
                int secondLength = ByteBuffer.wrap(bytes).getShort(secondLengthPos) & 0xffff;
                assertThat(secondLength).isPositive();
                bytes[extension + 18] = 'a';
                bytes[secondLengthPos + 2] = '0';
                rewriteExtensionCrc(bytes, layout);
            }
            case KEY_LENGTH -> {
                ByteBuffer.wrap(bytes).putShort(extension + 16, (short) 0xffff);
                rewriteExtensionCrc(bytes, layout);
            }
            case TRAILING_PAYLOAD -> {
                int count = ByteBuffer.wrap(bytes).getInt(extension + 12);
                int prefix = extension + 16;
                for (int i = 1; i < count; i++) {
                    prefix += 2 + (ByteBuffer.wrap(bytes).getShort(prefix) & 0xffff);
                }
                int length = ByteBuffer.wrap(bytes).getShort(prefix) & 0xffff;
                ByteBuffer.wrap(bytes).putShort(prefix, (short) (length - 1));
                bytes[prefix + 2] = 'z';
                rewriteExtensionCrc(bytes, layout);
            }
            case BOUNDS -> bytes[Math.toIntExact(layout.trailerStart) + 2] = 'a';
        }
        Files.write(path, bytes);
    }

    private static void mutateExtensionStart(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        Layout layout = layout(bytes);
        int minLength = ByteBuffer.wrap(bytes).getShort(Math.toIntExact(layout.trailerStart))
                & 0xffff;
        ByteBuffer.wrap(bytes).putShort(Math.toIntExact(layout.trailerStart) + 2 + minLength,
                (short) 0xffff);
        Files.write(path, bytes);
    }

    private static void rewriteExtensionCrc(byte[] bytes, Layout layout) {
        int extension = Math.toIntExact(layout.extensionStart);
        int crcPosition = Math.toIntExact(layout.fixedTailStart - 4);
        CRC32C crc = new CRC32C();
        crc.update(bytes, extension, crcPosition - extension);
        ByteBuffer.wrap(bytes).putInt(crcPosition, (int) crc.getValue());
    }

    private static Layout layout(byte[] bytes) {
        int tail = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long trailer = ByteBuffer.wrap(bytes).getLong(tail);
        int pos = Math.toIntExact(trailer);
        int min = ByteBuffer.wrap(bytes).getShort(pos) & 0xffff;
        pos += 2 + min;
        int max = ByteBuffer.wrap(bytes).getShort(pos) & 0xffff;
        pos += 2 + max;
        return new Layout(trailer, pos, tail);
    }

    private record Layout(long trailerStart, long extensionStart, long fixedTailStart) {
        long extensionBytes() {
            return fixedTailStart - extensionStart;
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static List<String> hex(List<byte[]> keys) {
        return keys == null ? List.of() : keys.stream().map(HexFormat.of()::formatHex).toList();
    }

    private static void assertByteExact(List<byte[]> actual, List<byte[]> expected) {
        assertThat(actual).isNotNull().hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i)).as("key %s", i).containsExactly(expected.get(i));
        }
    }

    private static final class CountingMetrics implements SortMetrics {
        private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
        private final LongAdder embeddedEntries = new LongAdder();
        private final LongAdder embeddedBytes = new LongAdder();
        private final LongAdder scanBytes = new LongAdder();
        private final LongAdder progress = new LongAdder();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.computeIfAbsent(outcome + "." + reason, ignored -> new LongAdder()).increment();
        }

        @Override
        public void recordBoundaryIo(long entries, long embeddedBytes, long scannedBytes) {
            embeddedEntries.add(entries);
            this.embeddedBytes.add(embeddedBytes);
            scanBytes.add(scannedBytes);
        }

        @Override
        public void recordRangeIndexBytes(long bytes) {
        }

        @Override
        public void recordRangeFramedBytes(long bytes) {
        }

        @Override
        public void recordProofSpool(long logicalExtentBytes, long preallocationOperations,
                long preallocationAttemptedBytes, long mappedOperations, long mappedBytes,
                long serviceNanos) {
        }

        @Override
        public void markProgress() {
            progress.increment();
        }

        @Override
        public void recordPageAwareOverlapCluster() {
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        }

        long count(String name) {
            LongAdder count = counts.get(name);
            return count == null ? 0 : count.sum();
        }
    }

    private static final class CountingChannel implements WritableByteChannel {
        private long writes;
        private long bytes;

        @Override
        public int write(ByteBuffer source) {
            int length = source.remaining();
            source.position(source.limit());
            writes++;
            bytes += length;
            return length;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
