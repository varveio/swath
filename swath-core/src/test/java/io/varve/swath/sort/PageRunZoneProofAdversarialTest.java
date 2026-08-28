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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
        EntrySelection selection = entrySelection(segment, 4);
        assertThat(selection.selected()).isNotEqualTo(selection.unselected());
        lie.mutate(segment, selection);

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
        Path type1Source = SortTestSupport.writeIndexedPages(
                staging.resolve("type1-source.pageseg"), 4, 200);
        Path type1 = NonIndexKind.TYPE1.convert(
                type1Source, staging.resolve("type1.pageseg"));
        Files.delete(type1Source);
        List<List<ListEntry>> equalMinPages = new ArrayList<>();
        for (int page = 0; page < 6; page++) {
            equalMinPages.add(List.of(SortTestSupport.object("equal-min")));
        }
        Path equalMin = SortTestSupport.writeIndexedPages(
                staging.resolve("equal-min.pageseg"), equalMinPages);
        PageRunSeekPlan equalMinPlan = PageRunSeekPlan.plan(descriptors(equalMin),
                List.of(bytes("g"), bytes("h"), bytes("i")), SortMetrics.NO_OP);
        assertThat(equalMinPlan.segments().getFirst().zone(1).empty()).isTrue();
        assertThat(equalMinPlan.segments().getFirst().zone(2).empty())
                .as("repeated type-2 starts remain two explicit consecutive empty zones")
                .isTrue();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Logger logger = (Logger) LoggerFactory.getLogger(ParallelRangeMerge.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        SortTransformResult result;
        try {
            result = transform(4, metrics, SortedFileWriterFactory.DEFAULT)
                    .transform(List.of(indexed, legacy, type1, equalMin), output, staging,
                            PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }

        assertThat(result.totalRows()).isEqualTo(26);
        assertThat(readKeys(result.finalFiles())).hasSize(26).isSorted();
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_range_index_seek")).isPositive();
        assertThat(metrics.count("SORT.merge_range_index_absent")).isPositive();
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
        assertThat(metrics.rangeIndexBytes.sum()).isEqualTo(2_208);
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_merge_range range=")))
                .hasSize(4)
                .allSatisfy(message -> assertThat(message)
                        .contains("pages_seeked_over=")
                        .contains("bytes_read="));
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith("sort_merge_range range="))
                .mapToLong(message -> longField(message, "index_bytes_read"))
                .sum()).isEqualTo(1_308);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(StructuralLie.class)
    void structurallyImpossibleOrdinalAndCumulativeByteLiesUseFullHeaderProof(
            StructuralLie lie, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), indexedPages());
        lie.mutate(segment, entrySelection(segment, 3));
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

    @ParameterizedTest(name = "{0}")
    @EnumSource(NonIndexKind.class)
    void downwardTrailerTotalWithoutUsableType2RemainsBodyCorruption(
            NonIndexKind kind, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path source = SortTestSupport.writeIndexedPages(
                staging.resolve("source.pageseg"), indexedPages());
        Path segment = kind.convert(source, staging.resolve("input.pageseg"));
        byte[] bytes = Files.readAllBytes(segment);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        ByteBuffer tail = ByteBuffer.wrap(bytes);
        tail.putLong(fixedTailStart + 12, tail.getLong(fixedTailStart + 12) - 1);
        Files.write(segment, bytes);
        Files.deleteIfExists(source);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> transform(4, metrics, SortedFileWriterFactory.DEFAULT)
                .transform(List.of(segment), output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(SegmentCorruptionException.class)
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION);
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
        assertThat(output.resolve("part-00000.parquet")).doesNotExist();
        assertNoOwnedDebris(staging);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(NonIndexKind.class)
    void coherentDownwardTrailerCountsReachNonIndexPhysicalZoneTilingMismatch(
            NonIndexKind kind, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path source = SortTestSupport.writeIndexedPages(
                staging.resolve("source.pageseg"), indexedPages());
        lowerTrailerByLastPage(source);
        Path segment = kind.convert(source, staging.resolve("input.pageseg"));
        Files.deleteIfExists(source);
        PageRunPageIndex.Status expectedStatus = switch (kind) {
            case EXTENSIONLESS -> PageRunPageIndex.Status.ABSENT;
            case TYPE1 -> PageRunPageIndex.Status.EMBEDDED_MINIMA_ONLY;
            case INVALID_TYPE2 -> PageRunPageIndex.Status.INVALID_COUNT;
        };
        assertThat(descriptors(segment).getFirst().extension().status()).isEqualTo(expectedStatus);
        TrackingFileWriterFactory writers = new TrackingFileWriterFactory();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> transform(4, metrics, writers)
                .transform(List.of(segment), output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(SegmentCorruptionException.class)
                .hasMessageContaining("does not tile its planned physical zone")
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION);
        assertThat(writers.opened.get()).isPositive();
        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
        assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isZero();
        assertNoLiveWorkers();
        assertNoOwnedDebris(staging);
        assertThat(output.resolve("part-00000.parquet")).doesNotExist();
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
        assertThat(only.spool()).doesNotExist();

        Path duplicateDir = Files.createDirectories(root.resolve("duplicate"));
        PageRunZoneVerifier.RangeSummary duplicate = emptySummaries(duplicateDir, 1).getFirst();
        assertThatThrownBy(() -> PageRunZoneVerifier.verify(
                plan, List.of(duplicate, duplicate), SortMetrics.NO_OP))
                .isInstanceOf(IOException.class).hasMessageContaining("duplicate");
        assertThat(duplicate.spool()).doesNotExist();

        Path extraDir = Files.createDirectories(root.resolve("extra"));
        List<PageRunZoneVerifier.RangeSummary> extra = emptySummaries(extraDir, 3);
        assertThatThrownBy(() -> PageRunZoneVerifier.verify(plan, extra, SortMetrics.NO_OP))
                .isInstanceOf(IOException.class).hasMessageContaining("2 planned ranges");
        assertThat(extra).allSatisfy(summary -> assertThat(summary.spool()).doesNotExist());

        Path exactDir = Files.createDirectories(root.resolve("exact"));
        List<PageRunZoneVerifier.RangeSummary> exact = emptySummaries(exactDir, 2);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PageRunZoneVerifier.verify(plan, exact, metrics);
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
        assertThat(exact).allSatisfy(summary -> assertThat(summary.spool()).doesNotExist());
    }

    @Test
    void proofSpoolFixedSlotOperationsScaleWithSegmentsAndRangesNotPages(@TempDir Path root)
            throws IOException {
        int segments = 2;
        int ranges = 4;
        SortTestSupport.CountingMetrics small = proofWorkload(
                root.resolve("small"), segments, ranges, 8);
        SortTestSupport.CountingMetrics large = proofWorkload(
                root.resolve("large"), segments, ranges, 256);
        long slots = (long) segments * ranges;
        long expectedOperations = 2 * slots + 5; // writer/reader lifecycle + one delete
        long expectedBytes = 3 * slots * PageRunProofSpool.slotBytes();

        assertThat(small.proofSpoolOperations.sum()).isEqualTo(expectedOperations);
        assertThat(large.proofSpoolOperations.sum()).isEqualTo(expectedOperations);
        assertThat(small.proofSpoolBytes.sum()).isEqualTo(expectedBytes);
        assertThat(large.proofSpoolBytes.sum()).isEqualTo(expectedBytes);
        assertThat(large.proofSpoolNanos.sum()).isPositive();
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

    @Test
    void cancellationAfterWorkersSpoolAndWritersAreActiveQuiescesAndCleans(@TempDir Path root)
            throws Exception {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), 32, 0);
        CountDownLatch writing = new CountDownLatch(1);
        TrackingFileWriterFactory writers = new TrackingFileWriterFactory(writing);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread transformThread = Thread.ofPlatform().name("wp2-proof-cancel").start(() -> {
            try {
                transform(4, SortMetrics.NO_OP, writers)
                        .transform(List.of(segment), output, staging, PublishListener.NO_OP,
                                units -> { }, FinalPassListener.NO_OP);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        assertThat(writing.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(writers.openNow.get()).isPositive();
        assertThat(staging.resolve(StagingNames.rangeProofTmp())).exists();
        transformThread.interrupt();
        transformThread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(transformThread.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(IOException.class);
        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
        assertNoLiveWorkers();
        assertNoOwnedDebris(staging);
        assertThat(output.resolve("part-00000.parquet")).doesNotExist();
    }

    @Test
    void cancellationDuringCoordinatorProofAfterWorkersCompletedClosesAndCleans(
            @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), 32, 0);
        List<PageRunSegmentDescriptor> descriptors = descriptors(segment);
        List<byte[]> boundaries = List.of(bytes("k00008"), bytes("k00016"), bytes("k00024"));
        TrackingFileWriterFactory writers = new TrackingFileWriterFactory();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(4)
                .withMergeBudgetBytes(64L << 20);
        SortRun run = new SortRun(config, CMP, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writers, MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES,
                RangeMergeTimer.NO_OP, SortRun.PROCESS_SOFT_FD_LIMIT,
                StaleFinalSweep.OWN_PARTS_ONLY);
        AtomicBoolean proofEntered = new AtomicBoolean();
        ParallelRangeMerge merge = new ParallelRangeMerge(run, (spool, stats) -> {
            assertThat(spool).exists();
            assertThat(writers.opened.get()).isEqualTo(4);
            assertThat(writers.openNow.get()).isEqualTo(4);
            assertThat(metrics.count("SORT.merge_range_parallel")).isEqualTo(4);
            assertNoLiveWorkers();
            proofEntered.set(true);
            Thread.currentThread().interrupt();
            return new PageRunProofSpool.Reader(spool, stats);
        });

        try {
            assertThatThrownBy(() -> merge.run(PageRunCatalog.fromDescriptors(descriptors), staging, boundaries, units -> { }))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }

        assertThat(proofEntered).isTrue();
        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get()).isEqualTo(4);
        assertThat(metrics.count("SORT.merge_zone_proof_complete")).isZero();
        assertNoLiveWorkers();
        assertNoOwnedDebris(staging);
        assertThat(staging.resolve(StagingNames.rangeProofTmp())).doesNotExist();
        try (var finals = Files.newDirectoryStream(output, "part-*.parquet")) {
            assertThat(finals.iterator().hasNext()).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(MiddleZoneMutation.class)
    void malformedBodyAndMinRegressionInsideAMiddleOwnedZoneFailTypedAndClean(
            MiddleZoneMutation mutation, @TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("segment.pageseg"), 32, 0);
        long middleOrdinal = middleOwnedOrdinal(segment, 4);
        assertThat(middleOrdinal).isBetween(1L, 30L);
        mutation.mutate(segment, middleOrdinal);
        assertThat(descriptors(segment).getFirst().extension().status())
                .isEqualTo(PageRunPageIndex.Status.EMBEDDED);
        TrackingFileWriterFactory writers = new TrackingFileWriterFactory();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> transform(4, metrics, writers)
                .transform(List.of(segment), output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(SegmentCorruptionException.class)
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(mutation.errorClass);
        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
        assertNoLiveWorkers();
        assertNoOwnedDebris(staging);
        assertThat(output.resolve("part-00000.parquet")).doesNotExist();
    }

    @Test
    void serialR1IsByteExactAcrossType2AndExtensionlessInputsAndDoesZeroProofIo(
            @TempDir Path root) throws IOException {
        Path indexedOutput = Files.createDirectories(root.resolve("indexed-out"));
        Path indexedStaging = Files.createDirectories(indexedOutput.resolve("_staging"));
        Path indexed = SortTestSupport.writeIndexedPages(
                indexedStaging.resolve("indexed.pageseg"), indexedPages());
        SortTestSupport.CountingMetrics indexedMetrics = new SortTestSupport.CountingMetrics();
        SortTransformResult indexedResult = transform(1, indexedMetrics, SortedFileWriterFactory.DEFAULT)
                .transform(List.of(indexed), indexedOutput, indexedStaging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        Path legacyOutput = Files.createDirectories(root.resolve("legacy-out"));
        Path legacyStaging = Files.createDirectories(legacyOutput.resolve("_staging"));
        Path source = SortTestSupport.writeIndexedPages(
                legacyStaging.resolve("source.pageseg"), indexedPages());
        Path legacy = NonIndexKind.EXTENSIONLESS.convert(
                source, legacyStaging.resolve("legacy.pageseg"));
        Files.delete(source);
        SortTestSupport.CountingMetrics legacyMetrics = new SortTestSupport.CountingMetrics();
        SortTransformResult legacyResult = transform(1, legacyMetrics, SortedFileWriterFactory.DEFAULT)
                .transform(List.of(legacy), legacyOutput, legacyStaging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(Files.readAllBytes(indexedResult.finalFiles().getFirst()))
                .containsExactly(Files.readAllBytes(legacyResult.finalFiles().getFirst()));
        for (SortTestSupport.CountingMetrics metrics : List.of(indexedMetrics, legacyMetrics)) {
            assertThat(metrics.rangeIndexBytes.sum()).isZero();
            assertThat(metrics.count("SORT.merge_range_index_seek")).isZero();
            assertThat(metrics.count("SORT.merge_range_index_absent")).isZero();
            assertThat(metrics.count("SORT.merge_zone_proof_complete")).isZero();
            assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
        }
        assertThat(indexedStaging.resolve(StagingNames.rangeProofTmp())).doesNotExist();
        assertThat(legacyStaging.resolve(StagingNames.rangeProofTmp())).doesNotExist();
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

    private static SortTestSupport.CountingMetrics proofWorkload(
            Path root, int segments, int ranges, int pages) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> inputs = new ArrayList<>();
        for (int segment = 0; segment < segments; segment++) {
            List<List<ListEntry>> pageRows = new ArrayList<>();
            for (int page = 0; page < pages; page++) {
                pageRows.add(List.of(SortTestSupport.object(
                        String.format("k%08d", page * segments + segment))));
            }
            inputs.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("segment-" + segment + ".pageseg"), pageRows));
        }
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(ranges, metrics, SortedFileWriterFactory.DEFAULT)
                .transform(inputs, output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);
        assertThat(result.finalizationParallelism()).isEqualTo(ranges);
        return metrics;
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
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        return PageRunCatalog.preflight(
                List.of(path), candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();
    }

    private static EntrySelection entrySelection(Path path, int ranges) throws IOException {
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                List.of(path), candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();
        List<byte[]> boundaries = MergePlanner.boundaries(
                descriptors, candidates, ranges, SortMetrics.NO_OP);
        assertThat(boundaries).isNotNull();
        PageRunSeekPlan.SegmentPlan segment = PageRunSeekPlan.plan(
                descriptors, boundaries, SortMetrics.NO_OP).segments().getFirst();
        List<Integer> selected = new ArrayList<>();
        for (int range = 1; range < segment.ranges(); range++) {
            int sample = segment.start(range).sampleIndex();
            if (sample > 0 && !selected.contains(sample)) {
                selected.add(sample);
            }
        }
        int selectedEntry = selected.getFirst();
        int entryCount = descriptors.getFirst().extension().entryCount();
        int unselectedEntry = -1;
        for (int entry = 1; entry < entryCount; entry++) {
            if (!selected.contains(entry)) {
                unselectedEntry = entry;
                break;
            }
        }
        assertThat(unselectedEntry).isNotNegative();
        return new EntrySelection(selectedEntry, unselectedEntry);
    }

    private static long middleOwnedOrdinal(Path path, int ranges) throws IOException {
        MergePlanner.BoundaryCandidates candidates =
                new MergePlanner.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunCatalog.preflight(
                List.of(path), candidate -> PageRunSegmentIo.open(candidate, SortMetrics.NO_OP),
                Optional.of(candidates::add)).descriptors();
        List<byte[]> boundaries = MergePlanner.boundaries(
                descriptors, candidates, ranges, SortMetrics.NO_OP);
        PageRunSeekPlan.SegmentPlan segment = PageRunSeekPlan.plan(
                descriptors, boundaries, SortMetrics.NO_OP).segments().getFirst();
        PageRunSeekPlan.Zone middle = segment.zone(1);
        assertThat(middle.end().pageOrdinal() - middle.start().pageOrdinal())
                .as("range 1 owns at least two pages, so the mutation is not a seam")
                .isGreaterThan(1);
        return middle.start().pageOrdinal() + 1;
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

    private static void lowerTrailerByLastPage(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int fixedTailStart = bytes.length - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        ByteBuffer tail = ByteBuffer.wrap(bytes);
        assertThat(tail.getInt(fixedTailStart + Long.BYTES)).isEqualTo(8);
        assertThat(tail.getLong(fixedTailStart + Long.BYTES + Integer.BYTES)).isEqualTo(24);
        tail.putInt(fixedTailStart + Long.BYTES, 7);
        tail.putLong(fixedTailStart + Long.BYTES + Integer.BYTES, 21);
        Files.write(path, bytes);
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
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(SortMetrics.NO_OP);
        try (PageRunProofSpool.Writer spool =
                     new PageRunProofSpool.Writer(spoolPath, 0, stats)) {
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
        OFFSET_AND_FRAMED_BYTES(
                SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, false, EntryTarget.SELECTED) {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                int entry = layout.entries().get(entryIndex);
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(entry + 8, data.getLong(entry + 8) + 1);
                data.putLong(entry + 24, data.getLong(entry + 24) + 1);
            }
        },
        CUMULATIVE_ENTRIES(
                SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, true, EntryTarget.SELECTED) {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                int position = layout.entries().get(entryIndex) + 16;
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(position, data.getLong(position) + 1);
            }
        },
        MINIMUM(SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, false, EntryTarget.SELECTED) {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                KeyPositions keys = entryKeys(bytes, layout.entries().get(entryIndex));
                bytes[keys.minKey() + keys.minLength() - 1]++;
            }
        },
        PREFIX_MAX(
                SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, true, EntryTarget.UNSELECTED) {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                KeyPositions keys = entryKeys(bytes, layout.entries().get(entryIndex));
                bytes[keys.prefixKey() + keys.prefixLength() - 1]--;
            }
        },
        FINAL_PREFIX_AND_TRAILER_MAX(
                SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION, true, EntryTarget.NONE) {
            @Override
            void apply(byte[] bytes, Layout layout, int ignored) {
                bytes[layout.finalPrefixKey() + layout.finalPrefixLength() - 1]++;
                bytes[layout.trailerMaxKey() + layout.trailerMaxLength() - 1]++;
            }
        },
        TRAILER_TOTAL_ENTRIES(
                SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION, true, EntryTarget.NONE) {
            @Override
            void apply(byte[] bytes, Layout layout, int ignored) {
                ByteBuffer data = ByteBuffer.wrap(bytes);
                data.putLong(layout.fixedTailStart() + 12,
                        data.getLong(layout.fixedTailStart() + 12) + 1);
            }
        };

        private final String errorClass;
        private final boolean postWorker;
        private final EntryTarget target;

        LogicalLie(String errorClass, boolean postWorker, EntryTarget target) {
            this.errorClass = errorClass;
            this.postWorker = postWorker;
            this.target = target;
        }

        void mutate(Path path, EntrySelection selection) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            Layout layout = layout(bytes);
            apply(bytes, layout, target.index(selection));
            rewriteExtensionCrc(bytes, layout);
            Files.write(path, bytes);
        }

        abstract void apply(byte[] bytes, Layout layout, int entryIndex);
    }

    private enum StructuralLie {
        ORDINAL(PageRunPageIndex.Status.INVALID_COUNT,
                "SORT.merge_boundary_fallback_invalid_count") {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                int entry = layout.entries().get(entryIndex);
                ByteBuffer.wrap(bytes).putLong(entry, 3);
            }
        },
        CUMULATIVE_FRAMED_BYTES(PageRunPageIndex.Status.INVALID_CUMULATIVE,
                "SORT.merge_boundary_fallback_invalid_cumulative") {
            @Override
            void apply(byte[] bytes, Layout layout, int entryIndex) {
                int position = layout.entries().get(entryIndex) + 24;
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

        void mutate(Path path, EntrySelection selection) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            Layout layout = layout(bytes);
            apply(bytes, layout, selection.selected());
            rewriteExtensionCrc(bytes, layout);
            Files.write(path, bytes);
        }

        abstract void apply(byte[] bytes, Layout layout, int entryIndex);
    }

    private enum NonIndexKind {
        EXTENSIONLESS {
            @Override
            Path convert(Path source, Path destination) throws IOException {
                byte[] bytes = Files.readAllBytes(source);
                Layout layout = layout(bytes);
                try (FileChannel out = FileChannel.open(destination, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE)) {
                    SortTestSupport.writeFully(out,
                            ByteBuffer.wrap(bytes, 0, layout.extensionStart()));
                    SortTestSupport.writeFully(out, ByteBuffer.wrap(bytes, layout.fixedTailStart(),
                            PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES));
                }
                return destination;
            }
        },
        TYPE1 {
            @Override
            Path convert(Path source, Path destination) throws IOException {
                List<byte[]> minima = new ArrayList<>();
                try (PageFrontierReader frontier =
                             new PageFrontierReader(source, SortMetrics.NO_OP)) {
                    while (frontier.hasPage()) {
                        minima.add(frontier.minKey().clone());
                        frontier.advance();
                    }
                }
                byte[] bytes = Files.readAllBytes(source);
                Layout layout = layout(bytes);
                try (FileChannel out = FileChannel.open(destination, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE)) {
                    SortTestSupport.writeFully(out,
                            ByteBuffer.wrap(bytes, 0, layout.extensionStart()));
                    PageRunBoundarySample.write(out, minima);
                    SortTestSupport.writeFully(out, ByteBuffer.wrap(bytes, layout.fixedTailStart(),
                            PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES));
                }
                return destination;
            }
        },
        INVALID_TYPE2 {
            @Override
            Path convert(Path source, Path destination) throws IOException {
                byte[] bytes = Files.readAllBytes(source);
                Layout layout = layout(bytes);
                bytes[layout.fixedTailStart() - 1] ^= 0x5a;
                Files.write(destination, bytes);
                return destination;
            }
        };

        abstract Path convert(Path source, Path destination) throws IOException;
    }

    private enum MiddleZoneMutation {
        MALFORMED_BODY(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION) {
            @Override
            void apply(byte[] bytes, int bodyStart, int bodyLength) {
                int minLength = unsignedShort(bytes, bodyStart);
                int maxLengthPosition = bodyStart + 2 + minLength;
                int maxLength = unsignedShort(bytes, maxLengthPosition);
                int orderedPosition = maxLengthPosition + 2 + maxLength + Integer.BYTES;
                bytes[orderedPosition] = 2;
            }
        },
        MIN_REGRESSION(SegmentCorruptionException.PAGE_RUN_MIN_REGRESSION) {
            @Override
            void apply(byte[] bytes, int bodyStart, int bodyLength) {
                int minLength = unsignedShort(bytes, bodyStart);
                java.util.Arrays.fill(bytes, bodyStart + 2, bodyStart + 2 + minLength, (byte) 0);
            }
        };

        private final String errorClass;

        MiddleZoneMutation(String errorClass) {
            this.errorClass = errorClass;
        }

        void mutate(Path path, long pageOrdinal) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            int frameStart = PageRunSegmentWriter.HEADER_BYTES;
            for (long page = 0; page < pageOrdinal; page++) {
                int length = ByteBuffer.wrap(bytes).getInt(frameStart);
                frameStart = Math.addExact(frameStart, 8 + length);
            }
            int bodyLength = ByteBuffer.wrap(bytes).getInt(frameStart);
            int bodyStart = frameStart + 8;
            apply(bytes, bodyStart, bodyLength);
            CRC32C crc = new CRC32C();
            crc.update(bytes, bodyStart, bodyLength);
            ByteBuffer.wrap(bytes).putInt(frameStart + Integer.BYTES, (int) crc.getValue());
            Files.write(path, bytes);
        }

        abstract void apply(byte[] bytes, int bodyStart, int bodyLength);
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
        return new Layout(trailerStart, extensionStart, fixedTailStart, entries,
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

    private enum EntryTarget {
        SELECTED {
            @Override
            int index(EntrySelection selection) {
                return selection.selected();
            }
        },
        UNSELECTED {
            @Override
            int index(EntrySelection selection) {
                return selection.unselected();
            }
        },
        NONE {
            @Override
            int index(EntrySelection ignored) {
                return -1;
            }
        };

        abstract int index(EntrySelection selection);
    }

    private record EntrySelection(int selected, int unselected) {
    }

    private record KeyPositions(int minKey, int minLength, int prefixKey, int prefixLength) {
    }

    private record Layout(int trailerStart, int extensionStart, int fixedTailStart,
                          List<Integer> entries,
                          int finalPrefixKey, int finalPrefixLength,
                          int trailerMinKey, int trailerMinLength,
                          int trailerMaxKey, int trailerMaxLength) {
    }

    /** Minimal final-writer double: creates real tmp files and exposes idempotent close accounting. */
    private static final class TrackingFileWriterFactory implements SortedFileWriterFactory {
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();
        private final CountDownLatch blockAtWrite;

        TrackingFileWriterFactory() {
            this(null);
        }

        TrackingFileWriterFactory(CountDownLatch blockAtWrite) {
            this.blockAtWrite = blockAtWrite;
        }

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
                public void write(ListEntry entry) throws IOException {
                    if (blockAtWrite != null) {
                        blockAtWrite.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("proof cancellation writer interrupted", e);
                        }
                    }
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
