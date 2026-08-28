/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

class PageRunZoneProofAdversarialTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @ParameterizedTest(name = "{0}")
    @EnumSource(LogicalLie.class)
    void crcValidLogicalIndexLiesFailTypedBeforePublishAndCleanEverything(
            LogicalLie lie, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), indexedPages());
        lie.mutate(segment);

        // The mutation deliberately passes the extension's structural/CRC admission. The physical
        // proof, not fallback scanning, must be what rejects it.
        List<PageRunSegmentDescriptor> descriptors = descriptors(segment);
        assertThat(descriptors.getFirst().extension().status())
                .isEqualTo(PageRunPageIndex.Status.EMBEDDED);

        TrackingFileWriterFactory writers = new TrackingFileWriterFactory();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransform transform = transform(4, metrics, writers);

        assertThatThrownBy(() -> transform.transform(List.of(segment), output, staging,
                PublishListener.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(SegmentCorruptionException.class)
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(lie.errorClass);

        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
        if (lie.postWorker) {
            assertThat(writers.opened.get())
                    .as("post-worker proof failure closes already-returned range writers")
                    .isPositive();
        }
        assertNoLiveWorkers();
        assertNoOwnedDebris(staging);
        assertThat(segment).exists();
        try (var finals = Files.newDirectoryStream(output, "part-*.parquet")) {
            assertThat(finals.iterator().hasNext()).isFalse();
        }
        if (SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH.equals(lie.errorClass)) {
            assertThat(metrics.count("SORT.page_run_index_mismatch")).isPositive();
        }
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isZero();
    }

    @Test
    void mixedType2AndExtensionlessSegmentsUseSeekAndHeaderZonesInOneCompleteProof(
            @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path indexed = SortTestSupport.writeIndexedPages(
                staging.resolve("indexed.pageseg"), 12, 0);
        List<ListEntry> legacyRows = List.of(
                SortTestSupport.object("k00100"), SortTestSupport.object("k00101"),
                SortTestSupport.object("k00102"), SortTestSupport.object("k00103"));
        Path legacy = SortTestSupport.writePageRun(
                staging.resolve("legacy.pageseg"), legacyRows, CMP);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Logger logger = (Logger) LoggerFactory.getLogger(ParallelRangeMerge.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        SortTransformResult result;
        try {
            result = transform(3, metrics, SortedFileWriterFactory.DEFAULT)
                    .transform(List.of(indexed, legacy), output, staging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }

        assertThat(result.totalRows()).isEqualTo(16);
        assertThat(readKeys(result.finalFiles())).containsExactly(
                "k00000", "k00001", "k00002", "k00003", "k00004", "k00005",
                "k00006", "k00007", "k00008", "k00009", "k00010", "k00011",
                "k00100", "k00101", "k00102", "k00103");
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_range_index_seek")).isPositive();
        assertThat(metrics.count("SORT.merge_range_index_absent")).isPositive();
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
        assertThat(metrics.rangeIndexBytes.sum()).isEqualTo(1_296);
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_merge_range range=")))
                .hasSize(3)
                .allSatisfy(message -> assertThat(message)
                        .contains("pages_seeked_over=")
                        .contains("bytes_read="));
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_merge_range range="))
                .mapToLong(message -> longField(message, "index_bytes_read"))
                .sum()).isEqualTo(720);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(StructuralLie.class)
    void structurallyImpossibleOrdinalAndCumulativeByteLiesUseFullHeaderProof(
            StructuralLie lie, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), indexedPages());
        lie.mutate(segment);
        List<PageRunSegmentDescriptor> descriptors = descriptors(segment);
        assertThat(descriptors.getFirst().extension().status()).isEqualTo(lie.status);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortTransformResult result = transform(3, metrics, SortedFileWriterFactory.DEFAULT)
                .transform(List.of(segment), output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(result.totalRows()).isEqualTo(24);
        assertThat(metrics.count(lie.reason)).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_range_index_absent")).isPositive();
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
        assertThat(metrics.rangeIndexBytes.sum()).isZero();
    }

    @Test
    void plannedRangeCountRejectsMissingExtraAndDuplicateTopologyEvenWithoutSegments(
            @TempDir Path root) throws IOException {
        PageRunSeekPlan plan = PageRunSeekPlan.plan(List.of(), List.of(bytes("m")),
                SortMetrics.NO_OP);
        assertThat(plan.ranges()).isEqualTo(2);

        Path missingDir = Files.createDirectories(root.resolve("missing"));
        PageRunZoneVerifier.RangeSummary only = emptySummaries(missingDir, 1).getFirst();
        assertThatThrownBy(() -> PageRunZoneVerifier.verify(plan, List.of(only), SortMetrics.NO_OP))
                .isInstanceOf(IOException.class).hasMessageContaining("2 planned ranges");
        Files.deleteIfExists(only.spool());

        Path duplicateDir = Files.createDirectories(root.resolve("duplicate"));
        PageRunZoneVerifier.RangeSummary duplicate = emptySummaries(duplicateDir, 1).getFirst();
        assertThatThrownBy(() -> PageRunZoneVerifier.verify(
                plan, List.of(duplicate, duplicate), SortMetrics.NO_OP))
                .isInstanceOf(IOException.class).hasMessageContaining("duplicate");
        Files.deleteIfExists(duplicate.spool());

        Path extraDir = Files.createDirectories(root.resolve("extra"));
        List<PageRunZoneVerifier.RangeSummary> extra = emptySummaries(extraDir, 3);
        assertThatThrownBy(() -> PageRunZoneVerifier.verify(plan, extra, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class).hasMessageContaining("2 planned ranges");
        for (PageRunZoneVerifier.RangeSummary summary : extra) {
            Files.deleteIfExists(summary.spool());
        }

        Path exactDir = Files.createDirectories(root.resolve("exact"));
        List<PageRunZoneVerifier.RangeSummary> exact = emptySummaries(exactDir, 2);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunZoneVerifier.verify(plan, exact, metrics);
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
        assertThat(exact).allSatisfy(summary -> assertThat(summary.spool()).doesNotExist());
    }

    @Test
    void seekPlanningPollsCancellationBeforeOpeningWorkers(@TempDir Path root)
            throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(root.resolve("segment.pageseg"), 8, 0);
        List<PageRunSegmentDescriptor> descriptors = descriptors(segment);

        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> PageRunSeekPlan.plan(descriptors,
                    List.of(bytes("k00004")), SortMetrics.NO_OP))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }

        assertThat(segment).exists();
        assertNoLiveWorkers();
    }

    @Test
    void zoneVerificationPollsCancellation(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(root.resolve("segment.pageseg"), 8, 0);
        List<PageRunSegmentDescriptor> descriptors = descriptors(segment);
        PageRunSeekPlan plan = PageRunSeekPlan.plan(descriptors,
                List.of(bytes("k00004")), SortMetrics.NO_OP);

        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> PageRunZoneVerifier.verify(plan, List.of(), SortMetrics.NO_OP))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }
    }

    private static SortTransform transform(int ranges, SortMetrics metrics,
                                           SortedFileWriterFactory writers) {
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(ranges)
                .withMergeBudgetBytes(64L << 20);
        return new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, metrics, writers,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private static List<List<ListEntry>> indexedPages() {
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int page = 0; page < 8; page++) {
            String base = String.format("k%05d", page * 3);
            pages.add(List.of(
                    SortTestSupport.object(base + "-a"),
                    SortTestSupport.object(base + "-m"),
                    SortTestSupport.object(base + "-z")));
        }
        return pages;
    }

    private static List<PageRunSegmentDescriptor> descriptors(Path path) throws IOException {
        ParallelRangeMerge.BoundaryCandidates candidates =
                new ParallelRangeMerge.BoundaryCandidates();
        return PageRunSegmentDescriptor.readAll(
                List.of(path), candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add));
    }

    private static List<String> readKeys(List<Path> files) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (SegmentReader reader = new SegmentReader(file)) {
                while (reader.hasNext()) {
                    keys.add(reader.next().key().asString());
                }
            }
        }
        return keys;
    }

    private static void assertNoOwnedDebris(Path staging) throws IOException {
        for (String glob : List.of(StagingNames.RANGE_TMP_GLOB,
                StagingNames.RANGE_LEGACY_CASCADE_PARQUET_GLOB,
                StagingNames.RANGE_CASCADE_PAGE_RUN_GLOB,
                StagingNames.RANGE_PROOF_TMP_GLOB)) {
            try (var files = Files.newDirectoryStream(staging, glob)) {
                assertThat(files.iterator().hasNext()).as("no debris matching %s", glob).isFalse();
            }
        }
    }

    private static void assertNoLiveWorkers() {
        assertThat(Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("swath-sort-range-")))
                .isEmpty();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static long longField(String message, String field) {
        int start = message.indexOf(field + "=") + field.length() + 1;
        int end = message.indexOf(' ', start);
        return Long.parseLong(end < 0 ? message.substring(start) : message.substring(start, end));
    }

    private static List<PageRunZoneVerifier.RangeSummary> emptySummaries(Path dir, int ranges)
            throws IOException {
        Path spoolPath = dir.resolve(StagingNames.rangeProofTmp());
        List<PageRunZoneVerifier.RangeSummary> summaries = new ArrayList<>();
        try (PageRunProofSpool.Writer spool = new PageRunProofSpool.Writer(spoolPath)) {
            for (int range = 0; range < ranges; range++) {
                try (PageRunZoneVerifier.RangeBuilder builder =
                             new PageRunZoneVerifier.RangeBuilder(
                                     spool, spoolPath, range, 0)) {
                    summaries.add(builder.finish());
                }
            }
        }
        return List.copyOf(summaries);
    }

    private enum LogicalLie {
        OFFSET_AND_FRAMED_BYTES(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, false) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                int entry = layout.entries().get(2);
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(entry + 8, data.getLong(entry + 8) + 1);
                data.putLong(entry + 24, data.getLong(entry + 24) + 1);
            }
        },
        CUMULATIVE_ENTRIES(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, true) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                int position = layout.entries().get(2) + 16;
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(position, data.getLong(position) + 1);
            }
        },
        MINIMUM(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, false) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                KeyPositions keys = entryKeys(bytes, layout.entries().get(2));
                bytes[keys.minKey() + keys.minLength() - 1]++;
            }
        },
        PREFIX_MAX(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, true) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                KeyPositions keys = entryKeys(bytes, layout.entries().get(2));
                bytes[keys.prefixKey() + keys.prefixLength() - 1]--;
            }
        },
        FINAL_PREFIX_AND_TRAILER_MAX(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION, true) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                bytes[layout.finalPrefixKey() + layout.finalPrefixLength() - 1]++;
                bytes[layout.trailerMaxKey() + layout.trailerMaxLength() - 1]++;
            }
        },
        TRAILER_TOTAL_ENTRIES(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION, true) {
            @Override
            void apply(byte[] bytes, Layout layout) {
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(layout.fixedTailStart() + 12,
                        data.getLong(layout.fixedTailStart() + 12) + 1);
            }
        };

        private final String errorClass;
        private final boolean postWorker;

        LogicalLie(String errorClass, boolean postWorker) {
            this.errorClass = errorClass;
            this.postWorker = postWorker;
        }

        void mutate(Path path) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            Layout layout = layout(bytes);
            apply(bytes, layout);
            rewriteExtensionCrc(bytes, layout);
            Files.write(path, bytes);
        }

        abstract void apply(byte[] bytes, Layout layout);
    }

    private enum StructuralLie {
        ORDINAL(PageRunPageIndex.Status.INVALID_COUNT,
                "SORT.merge_boundary_fallback_invalid_count") {
            @Override
            void apply(byte[] bytes, Layout layout) {
                int entry = layout.entries().get(2);
                ByteBuffer.wrap(bytes).putLong(entry, 3);
            }
        },
        CUMULATIVE_FRAMED_BYTES(PageRunPageIndex.Status.INVALID_CUMULATIVE,
                "SORT.merge_boundary_fallback_invalid_cumulative") {
            @Override
            void apply(byte[] bytes, Layout layout) {
                int position = layout.entries().get(2) + 24;
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(position, data.getLong(position) + 1);
            }
        };

        private final PageRunPageIndex.Status status;
        private final String reason;

        StructuralLie(PageRunPageIndex.Status status, String reason) {
            this.status = status;
            this.reason = reason;
        }

        void mutate(Path path) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            Layout layout = layout(bytes);
            apply(bytes, layout);
            rewriteExtensionCrc(bytes, layout);
            Files.write(path, bytes);
        }

        abstract void apply(byte[] bytes, Layout layout);
    }

    private static Layout layout(byte[] bytes) {
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        int trailerStart = Math.toIntExact(ByteBuffer.wrap(bytes).getLong(fixedTailStart));
        int position = trailerStart;
        int trailerMinLength = unsignedShort(bytes, position);
        int trailerMinKey = position + 2;
        position = trailerMinKey + trailerMinLength;
        int trailerMaxLength = unsignedShort(bytes, position);
        int trailerMaxKey = position + 2;
        position = trailerMaxKey + trailerMaxLength;
        int extensionStart = position;
        int count = ByteBuffer.wrap(bytes).getInt(extensionStart + 12);
        position = extensionStart + PageRunBoundarySample.HEADER_BYTES;
        List<Integer> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(position);
            KeyPositions keys = entryKeys(bytes, position);
            position = keys.prefixKey() + keys.prefixLength();
        }
        int finalPrefixLength = unsignedShort(bytes, position);
        int finalPrefixKey = position + 2;
        return new Layout(extensionStart, fixedTailStart, entries,
                finalPrefixKey, finalPrefixLength, trailerMinKey, trailerMinLength,
                trailerMaxKey, trailerMaxLength);
    }

    private static KeyPositions entryKeys(byte[] bytes, int entry) {
        int minLengthPosition = entry + 4 * Long.BYTES;
        int minLength = unsignedShort(bytes, minLengthPosition);
        int minKey = minLengthPosition + 2;
        int prefixLengthPosition = minKey + minLength;
        int prefixLength = unsignedShort(bytes, prefixLengthPosition);
        return new KeyPositions(minKey, minLength, prefixLengthPosition + 2, prefixLength);
    }

    private static int unsignedShort(byte[] bytes, int position) {
        return ByteBuffer.wrap(bytes).getShort(position) & 0xffff;
    }

    private static void rewriteExtensionCrc(byte[] bytes, Layout layout) {
        int crcPosition = layout.fixedTailStart() - PageRunBoundarySample.CRC_BYTES;
        CRC32C crc = new CRC32C();
        crc.update(bytes, layout.extensionStart(), crcPosition - layout.extensionStart());
        ByteBuffer.wrap(bytes).putInt(crcPosition, (int) crc.getValue());
    }

    private record KeyPositions(int minKey, int minLength, int prefixKey, int prefixLength) {
    }

    private record Layout(int extensionStart, int fixedTailStart, List<Integer> entries,
                          int finalPrefixKey, int finalPrefixLength,
                          int trailerMinKey, int trailerMinLength,
                          int trailerMaxKey, int trailerMaxLength) {
    }

    /** Minimal final-writer double: creates real tmp files and exposes idempotent close accounting. */
    private static final class TrackingFileWriterFactory implements SortedFileWriterFactory {
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            opened.incrementAndGet();
            openNow.incrementAndGet();
            return new SortedFileWriter() {
                private final AtomicBoolean isClosed = new AtomicBoolean();
                private long rows;

                @Override
                public void write(ListEntry entry) {
                    rows++;
                }

                @Override
                public long rows() {
                    return rows;
                }

                @Override
                public long dataSize() {
                    return rows;
                }

                @Override
                public void setFileIndex(int ignored) {
                }

                @Override
                public void close() throws IOException {
                    if (isClosed.compareAndSet(false, true)) {
                        try {
                            channel.close();
                        } finally {
                            closed.incrementAndGet();
                            openNow.decrementAndGet();
                        }
                    }
                }
            };
        }
    }
}
