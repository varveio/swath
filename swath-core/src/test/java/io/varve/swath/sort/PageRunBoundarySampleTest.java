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
        assertThat(hex(ParallelRangeMerge.boundaries(descriptors(extended), 3, metrics))).hasSize(2);
        assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_boundary_global_capped")).isZero();
        assertThat(metrics.scanBytes.sum()).isZero();
        assertThat(metrics.embeddedEntries.sum()).isEqualTo(4);
        assertThat(metrics.progress.sum()).isEqualTo(1);

        assertThatThrownBy(() -> ParallelRangeMerge.boundaries(
                descriptors(legacy), 3, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");
    }

    @Test
    void kickoffOpensSegmentExactlyOnceAndBoundarySelectionDoesNotReopen(@TempDir Path dir)
            throws IOException {
        Path segment = writePages(dir.resolve("descriptor.pageseg"), 4);
        AtomicInteger opens = new AtomicInteger();
        List<PageRunSegmentDescriptor> descriptors = PageRunSegmentDescriptor.readAll(List.of(segment), path -> {
            opens.incrementAndGet();
            return PageRunSegmentIo.open(path, SortMetrics.NO_OP);
        });
        PageRunSegmentDescriptor descriptor = descriptors.getFirst();
        assertThat(opens).hasValue(1);
        assertThat(descriptor.fileSize()).isEqualTo(Files.size(segment));
        assertThat(descriptor.trailerStart()).isPositive();
        assertThat(descriptor.sample().status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);

        Files.delete(segment);
        CountingMetrics metrics = new CountingMetrics();

        assertThat(hex(ParallelRangeMerge.boundaries(descriptors, 3, metrics)))
                .hasSize(2);
        assertThat(metrics.count("SORT.merge_boundary_source_embedded")).isEqualTo(1);
        assertThat(metrics.embeddedEntries.sum()).isEqualTo(4);
        assertThat(metrics.scanBytes.sum()).isZero();
    }

    @Test
    void exact4096And4097SystematicSemantics(@TempDir Path dir) throws IOException {
        Path atCap = writePages(dir.resolve("4096.pageseg"), 4_096);
        Path aboveCap = writePages(dir.resolve("4097.pageseg"), 4_097);

        PageRunBoundarySample.ReadResult full = readSample(atCap);
        PageRunBoundarySample.ReadResult thinned = readSample(aboveCap);
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

        List<String> embedded = hex(ParallelRangeMerge.boundaries(descriptors(first, second), 8,
                SortMetrics.NO_OP));
        List<String> legacy = hex(ParallelRangeMerge.boundaries(
                descriptors(firstLegacy, secondLegacy), 8,
                SortMetrics.NO_OP));
        CountingMetrics mixedMetrics = new CountingMetrics();
        List<String> mixed = hex(ParallelRangeMerge.boundaries(
                descriptors(first, secondLegacy), 8,
                mixedMetrics));

        assertThat(embedded).containsExactlyElementsOf(legacy);
        assertThat(mixed).containsExactlyElementsOf(legacy);
        assertThat(mixedMetrics.count("SORT.merge_boundary_source_mixed")).isEqualTo(1);
        assertThat(mixedMetrics.count("SORT.merge_boundary_fallback_absent")).isEqualTo(1);
    }

    @Test
    void malformedExtensionsFallBackTransactionallyWithExactReasons(@TempDir Path dir)
            throws IOException {
        for (Mutation mutation : Mutation.values()) {
            Path path = writePages(dir.resolve(mutation.name() + ".pageseg"), 5);
            Layout originalLayout = layout(Files.readAllBytes(path));
            Path legacy = stripExtension(path, dir.resolve(mutation.name() + "-legacy.pageseg"));
            List<byte[]> expected = ParallelRangeMerge.boundaries(descriptors(legacy), 3,
                    SortMetrics.NO_OP);
            mutateExtension(path, mutation);
            CountingMetrics metrics = new CountingMetrics();

            assertByteExact(ParallelRangeMerge.boundaries(descriptors(path), 3, metrics), expected);
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

        assertThatThrownBy(() -> ParallelRangeMerge.boundaries(
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

            assertThatThrownBy(() -> ParallelRangeMerge.boundaries(
                    descriptors(path), 3, SortMetrics.NO_OP))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("invalid page-run trailer offset");
        }
    }

    @Test
    void emptySegmentCarriesAValidEmptySample(@TempDir Path dir) throws IOException {
        Path path = writePages(dir.resolve("empty.pageseg"), 0);

        PageRunBoundarySample.ReadResult sample = readSample(path);

        assertThat(sample.status()).isEqualTo(PageRunBoundarySample.Status.EMBEDDED);
        assertThat(sample.keys()).isEmpty();
        assertThat(sample.bytesRead()).isEqualTo(20);
    }

    @Test
    void legacyFallbackCountsOnlyTheExactFramedRecordRegion(@TempDir Path dir) throws IOException {
        Path embedded = writePages(dir.resolve("extended.pageseg"), 7);
        Path legacy = stripExtension(embedded, dir.resolve("legacy.pageseg"));
        Layout layout = layout(Files.readAllBytes(legacy));
        CountingMetrics metrics = new CountingMetrics();

        ParallelRangeMerge.boundaries(descriptors(legacy), 3, metrics);

        assertThat(metrics.embeddedBytes.sum()).isZero();
        assertThat(metrics.scanBytes.sum())
                .isEqualTo(layout.trailerStart - PageRunSegmentWriter.HEADER_BYTES);
    }

    @Test
    void embeddedAndLegacyBoundariesMatchForRepeatedAndExtremeBinaryMinima(@TempDir Path dir)
            throws IOException {
        byte[] binary = {0x00, (byte) 0x80, (byte) 0xff};
        byte[] s3Max = extremeKey(1_024, (byte) 0x80);
        byte[] formatMax = extremeKey(0xffff, (byte) 0xff);
        List<byte[]> minima = List.of(new byte[]{0x00}, binary, binary.clone(), new byte[]{(byte) 0x80},
                s3Max, new byte[]{(byte) 0xff}, formatMax);
        Path embedded = writeBinaryPages(dir.resolve("extremes.pageseg"), minima);
        Path legacy = stripExtension(embedded, dir.resolve("extremes-legacy.pageseg"));

        PageRunBoundarySample.ReadResult sample = readSample(embedded);
        assertByteExact(sample.keys(), minima);
        assertThat(sample.keys().get(1)).containsExactly(0x00, (byte) 0x80, (byte) 0xff);
        assertThat(sample.keys().get(4)).hasSize(1_024);
        assertThat(sample.keys().getLast()).hasSize(0xffff);
        assertByteExact(ParallelRangeMerge.boundaries(descriptors(embedded), 32, SortMetrics.NO_OP),
                ParallelRangeMerge.boundaries(descriptors(legacy), 32, SortMetrics.NO_OP));
    }

    @Test
    void wholeRunBoundaryCandidatesStayBoundedAndDistributionRepresentative() {
        ParallelRangeMerge.BoundaryCandidates candidates =
                new ParallelRangeMerge.BoundaryCandidates();
        ParallelRangeMerge.BoundaryCandidates cloneGuard =
                new ParallelRangeMerge.BoundaryCandidates();
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
        assertThat(retained).hasSize(ParallelRangeMerge.MAX_BOUNDARY_CANDIDATES);
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

        ParallelRangeMerge.BoundaryCandidates ascending =
                new ParallelRangeMerge.BoundaryCandidates(retainedLimit);
        ParallelRangeMerge.BoundaryCandidates descending =
                new ParallelRangeMerge.BoundaryCandidates(retainedLimit);
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
        int segmentCount = ParallelRangeMerge.MAX_BOUNDARY_CANDIDATES
                / PageRunBoundarySample.MAX_ENTRIES + 1;
        for (int segment = 0; segment < segmentCount; segment++) {
            List<byte[]> minima = new ArrayList<>();
            for (int page = 0; page < PageRunBoundarySample.MAX_ENTRIES; page++) {
                minima.add(bytes(String.format("s%02d-k%04d", segment, page)));
            }
            segments.add(writeBinaryPages(dir.resolve("segment-" + segment + ".pageseg"), minima));
        }
        CountingMetrics metrics = new CountingMetrics();

        assertThat(ParallelRangeMerge.boundaries(descriptors(segments), 8, metrics)).hasSize(7);
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

    private static PageRunBoundarySample.ReadResult readSample(Path path) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(path, SortMetrics.NO_OP)) {
            return PageRunBoundarySample.read(io, PageRunTrailer.read(io));
        }
    }

    private static List<PageRunSegmentDescriptor> descriptors(Path... paths) throws IOException {
        return PageRunSegmentDescriptor.readAll(List.of(paths));
    }

    private static List<PageRunSegmentDescriptor> descriptors(List<Path> paths) throws IOException {
        return PageRunSegmentDescriptor.readAll(paths);
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
        public void markProgress() {
            progress.increment();
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
