/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageCompression;
import io.varve.swath.sort.spill.PageRef;
import io.varve.swath.sort.spill.PageRunCatalog;
import io.varve.swath.sort.spill.PageRunDescriptor;
import io.varve.swath.sort.spill.PageRunReader;
import io.varve.swath.sort.spill.PageRunTrailer;
import io.varve.swath.sort.spill.SpillTestFixtures;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class FinalizationPipelineTest {
    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void headerCursorAndPlanQueuesUseTheReferenceRoutingDepths() {
        assertThat(PageRunHeaderStreams.QUEUE_DEPTH).isEqualTo(2);
        assertThat(PartEncoders.QUEUE_DEPTH).isEqualTo(2);
    }

    @Test
    void calibratedPartTargetUsesCompletedEncodedToLogicalRatio() {
        PartSizer sizer = new PartSizer(
                PartSizer.Target.calibrated(), 100);

        assertThat(sizer.calibratedLogicalTarget()).isEqualTo(100);
        sizer.completed(100, 1_000);
        assertThat(sizer.encodedToLogicalRatio()).isEqualTo(0.1);
        assertThat(sizer.calibratedLogicalTarget()).isEqualTo(1_000);
        assertThat(sizer.shouldClose(999, Long.MAX_VALUE)).isFalse();
        assertThat(sizer.shouldClose(1_000, 0)).isTrue();
    }

    @Test
    void decodedPageBudgetReleasesAnExhaustedPageBeforeAdmittingItsSuccessor() throws IOException {
        PageBlock page = PageBlock.pack(List.of(SortTestSupport.object("key")), comparator, PageCompression.NONE);
        DecodedPageBudget sizing = new DecodedPageBudget(Long.MAX_VALUE, SortMetrics.NO_OP);
        long pageBytes = sizing.reserve(page);
        DecodedPageBudget budget = new DecodedPageBudget(pageBytes, SortMetrics.NO_OP);

        long first = budget.reserve(page);
        assertThatThrownBy(() -> budget.reserve(page))
                .isInstanceOf(MergeMemoryExhaustedException.class);
        budget.release(first);
        assertThat(budget.reserve(page)).isEqualTo(pageBytes);
    }

    @Test
    void retainedPagePlanningUnitCoversTheRuntimeGuard() {
        PageBlock packed = PageBlock.pack(List.of(
                SortTestSupport.object("repeated-prefix/alpha"),
                SortTestSupport.object("repeated-prefix/bravo")), comparator, PageCompression.LZ4);
        byte[] record = SpillTestFixtures.serialize(packed);
        PageBlock persisted = SpillTestFixtures.deserialize(record);

        long retainedUpper = DecodedPageBudget.retainedPageUpperBound(
                persisted.rawPayloadLength(), record.length);

        assertThat(DecodedPageBudget.retainedBytes(persisted)).isLessThanOrEqualTo(retainedUpper);
    }

    @Test
    void clusteredRowsAttributeEveryRawPayloadByteExactlyOnce() {
        PageBlock page = null;
        for (int count = 2; count < 20; count++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int row = 0; row < count; row++) {
                rows.add(SortTestSupport.object(String.format("key-%02d", row)));
            }
            PageBlock candidate = PageBlock.pack(rows, comparator, PageCompression.NONE);
            if (candidate.rawPayloadLength() % candidate.count() != 0) {
                page = candidate;
                break;
            }
        }
        assertThat(page).as("fixture must exercise logical-byte remainder").isNotNull();
        PageRowMerger merger = new PageRowMerger(comparator);
        merger.add(0, page, 0);

        long attributed = 0;
        while (merger.hasNext()) {
            merger.next();
            attributed += merger.lastLogicalBytes();
        }

        assertThat(attributed).isEqualTo(page.rawPayloadLength());
    }

    @Test
    void moreHeaderCursorsThanScanPermitsCompleteAtDepthOne(@TempDir Path root)
            throws IOException {
        int segmentCount = 4;
        Path staging = Files.createDirectories(root.resolve("header-staging"));
        List<Path> segments = new ArrayList<>(segmentCount);
        for (int segment = 0; segment < segmentCount; segment++) {
            List<List<ListEntry>> pages = new ArrayList<>();
            for (int page = 0; page < 3; page++) {
                pages.add(List.of(SortTestSupport.object(
                        String.format("s%05d-p%d", segment, page))));
            }
            segments.add(SortTestSupport.writePages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), pages));
        }
        List<PageRunReader> channels = new ArrayList<>();
        for (Path segment : segments) {
            channels.add(PageRunReader.open(segment, SortMetrics.NO_OP));
        }
        CountDownLatch firstCursorAtBlockedHandoff = new CountDownLatch(1);
        PageRunHeaderStreams.Hook schedule = new PageRunHeaderStreams.Hook() {
            @Override
            public void beforePermitAcquire(int segment, long page) throws InterruptedException {
                if (segment == 1 && page == 0
                        && !firstCursorAtBlockedHandoff.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("first cursor did not fill its handoff queue");
                }
            }

            @Override
            public void beforeHandoff(int segment, long page) {
                if (segment == 0 && page == 1) {
                    firstCursorAtBlockedHandoff.countDown();
                }
            }
        };
        PageRunHeaderStreams.Settings settings = new PageRunHeaderStreams.Settings(
                1, 1, schedule);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FinalizationFailure failure = new FinalizationFailure();
            try (PageRunHeaderStreams cursors = new PageRunHeaderStreams(
                    channels, settings, SortMetrics.NO_OP, failure)) {
                assertThat(cursors.next(1)).isNotNull();
            }
        });
        for (PageRunReader channel : channels) {
            channel.close();
        }
    }

    @Test
    void headerPassRejectsTruncatedFrameTiling(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("truncated" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a")),
                        List.of(SortTestSupport.object("c"))));
        PageRunReader.RoutingPage first;
        try (PageRunReader io = PageRunReader.open(segment, SortMetrics.NO_OP)) {
            first = io.nextRoutingPage();
        }
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            ByteBuffer shortened = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(first.framedLen() - 9).flip();
            channel.write(shortened, first.offset());
        }

        assertThatThrownBy(() -> {
            try (PageRunReader io = PageRunReader.open(segment, SortMetrics.NO_OP)) {
                while (io.nextRoutingPage() != null) {
                    // Header pass must reject before an encoder is started.
                }
                io.checkRoutingComplete();
            }
        }).isInstanceOf(IOException.class);
    }

    @Test
    void headerPassRejectsSplicedFrameOrder(@TempDir Path root) throws IOException {
        Path target = SortTestSupport.writePages(
                root.resolve("target" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a")),
                        List.of(SortTestSupport.object("c"))));
        Path donor = SortTestSupport.writePages(
                root.resolve("donor" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("0"))));
        PageRunReader.RoutingPage targetSecond;
        try (PageRunReader io = PageRunReader.open(target, SortMetrics.NO_OP)) {
            io.nextRoutingPage();
            targetSecond = io.nextRoutingPage();
        }
        ByteBuffer donorFrame;
        try (PageRunReader io = PageRunReader.open(donor, SortMetrics.NO_OP)) {
            PageRunReader.RoutingPage page = io.nextRoutingPage();
            assertThat(page.framedLen()).isEqualTo(targetSecond.framedLen());
            donorFrame = SpillTestFixtures.readFrame(donor, page.offset(), page.framedLen());
        }
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
            channel.write(donorFrame, targetSecond.offset());
        }

        assertThatThrownBy(() -> {
            try (PageRunReader io = PageRunReader.open(target, SortMetrics.NO_OP)) {
                io.nextRoutingPage();
                io.nextRoutingPage();
            }
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("page minKey regressed");
    }

    @Test
    void disjointSingleRowPagesUseWholePageForwarding(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortedDatasetResult result = run(root,
                List.of(List.of("a", "c"), List.of("b", "d")), Long.MAX_VALUE, metrics);

        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        assertThat(metrics.pipelinePagesForwarded.sum()).isEqualTo(4);
        assertThat(metrics.pipelineClusterPages.sum()).isZero();
        assertThat(metrics.count("SORT.pipeline_whole_page_merge")).isEqualTo(4);
    }

    @Test
    void fullOverlapSingleRowPagesMergeAndNeverSplitEqualRawKeys(@TempDir Path root)
            throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortedDatasetResult result = run(root,
                List.of(List.of("a", "b", "c"), List.of("a", "b", "c")), 1, metrics);

        assertThat(keys(result.finalFiles())).containsExactly("a", "a", "b", "b", "c", "c");
        assertThat(result.finalFiles()).hasSize(3);
        assertThat(metrics.pipelinePagesForwarded.sum()).isZero();
        assertThat(metrics.pipelineClusterPages.sum()).isEqualTo(6);
        for (Path part : result.finalFiles()) {
            List<String> partKeys = keys(List.of(part));
            assertThat(partKeys).hasSize(2);
            assertThat(partKeys.getFirst()).isEqualTo(partKeys.getLast());
        }
    }

    @Test
    void largeFullyOverlappingClusterCompletesWithinDecodedBudget(@TempDir Path root)
            throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<String>> segments = java.util.stream.IntStream.range(0, 65)
                .mapToObj(ignored -> List.of("same-key")).toList();

        SortedDatasetResult result = run(root, segments, Long.MAX_VALUE, metrics);

        assertThat(keys(result.finalFiles())).hasSize(65).containsOnly("same-key");
        assertThat(metrics.pipelineClusterPages.sum()).isEqualTo(65);
    }

    @Test
    void hundredPageTransitiveChainCompletesWithIncrementalEncoderAdmission(@TempDir Path root)
            throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<List<ListEntry>>> segments = new ArrayList<>();
        for (int page = 0; page < 100; page++) {
            List<ListEntry> entries = List.of(
                    SortTestSupport.object(String.format("k%04d", 2 * page)),
                    SortTestSupport.object(String.format("k%04d", 2 * page + 3)));
            segments.add(List.of(entries));
        }
        PageBlock sample = PageBlock.pack(segments.getFirst().getFirst(), comparator, PageCompression.NONE);
        long pageBytes = DecodedPageBudget.retainedBytes(
                SpillTestFixtures.deserialize(SpillTestFixtures.serialize(sample)));
        long plannedPageBytes = DecodedPageBudget.retainedPageUpperBound(
                sample.rawPayloadLength(), SpillTestFixtures.serialize(sample).length);
        long retainedRefs = (100L * (PageRunHeaderStreams.QUEUE_DEPTH + 2L)
                + 100L * (PartEncoders.QUEUE_DEPTH + 2L))
                * PageRef.retainedBytes(SpillTestFixtures.serialize(sample).length);
        long mergeBudget = PartEncoders.writerHeapEstimateBytes(SortConfig.DEFAULT.finalRowGroupBytes())
                + retainedRefs
                + SpillTestFixtures.serialize(sample).length + 3L * plannedPageBytes - 1L;

        DecodedPageBudget eager = new DecodedPageBudget(3L * pageBytes - 1L,
                SortMetrics.NO_OP);
        eager.reserve(SpillTestFixtures.deserialize(SpillTestFixtures.serialize(sample)));
        eager.reserve(SpillTestFixtures.deserialize(SpillTestFixtures.serialize(sample)));
        assertThatThrownBy(() -> eager.reserve(SpillTestFixtures.deserialize(SpillTestFixtures.serialize(sample))))
                .isInstanceOf(MergeMemoryExhaustedException.class);

        SortedDatasetResult result = runPages(root, segments, Long.MAX_VALUE,
                metrics, SortedFileWriterFactory.DEFAULT, 10_000, mergeBudget,
                PageCompression.NONE, 1);

        assertThat(result.totalRows()).isEqualTo(200);
        assertThat(metrics.pipelineDecodedPageBytesPeak.get())
                .isPositive()
                .isLessThan(3L * pageBytes);
    }

    @Test
    void pipelineRunsCascadeBeforeFinalRouting(@TempDir Path root) throws IOException {
        List<List<String>> segments = List.of(
                List.of("a"), List.of("b"), List.of("c"), List.of("d"), List.of("e"));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortedDatasetResult result = run(root, segments, Long.MAX_VALUE, metrics,
                SortedFileWriterFactory.DEFAULT, 2, 64L << 20);

        assertThat(result.cascadedPasses()).isEqualTo(2);
        assertThat(result.mergePasses()).isEqualTo(3);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
        assertThat(metrics.count("SORT.cascade_page_whole_merge")).isPositive();
        assertThat(metrics.pipelineDecodedPageBytesPeak.get()).isPositive();
    }

    @Test
    void cascadeIntermediatesPreserveDuplicateMultiplicityAcrossPasses(@TempDir Path root)
            throws IOException {
        List<List<List<ListEntry>>> segments = List.of(
                List.of(List.of(SortTestSupport.object("a"), SortTestSupport.object("m"),
                        SortTestSupport.object("x"))),
                List.of(List.of(SortTestSupport.object("a"), SortTestSupport.object("n"),
                        SortTestSupport.object("y"))),
                List.of(List.of(SortTestSupport.object("b"), SortTestSupport.object("o"),
                        SortTestSupport.object("z"))),
                List.of(List.of(SortTestSupport.object("a"), SortTestSupport.object("p"))),
                List.of(List.of(SortTestSupport.object("q"))));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortedDatasetResult result = runPages(root, segments, Long.MAX_VALUE, metrics,
                SortedFileWriterFactory.DEFAULT, 2, 64L << 20, PageCompression.LZ4, 2);

        assertThat(result.cascadedPasses()).isEqualTo(2);
        assertThat(keys(result.finalFiles())).containsExactly(
                "a", "a", "a", "b", "m", "n", "o", "p", "q", "x", "y", "z");
        assertThat(metrics.count("SORT.cascade_page_overlap_merge")).isPositive();
    }

    @Test
    void smallCorpusRunsEveryRequestedPipelineEncoder(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<List<ListEntry>>> pages = List.of(
                List.of(List.of(SortTestSupport.object("a"))));

        SortedDatasetResult result = runPages(root, pages, Long.MAX_VALUE, metrics,
                SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20);

        assertThat(result.finalizationParallelism()).isEqualTo(4);
        assertThat(metrics.count("SORT.pipeline_encoders_fd_clamped")).isZero();
        assertThat(metrics.count("SORT.pipeline_encoders_heap_clamped")).isZero();
    }

    @Test
    void pipelineAdmissionUsesOnlySurvivorFdsAndPipelineHeap(@TempDir Path root)
            throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("admission" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortConfig base = SortConfigs.base().withMergeBudgetBytes(64L << 20);
        FinalizationPlanner fdPlanner = new FinalizationPlanner(base, SortMetrics.NO_OP,
                () -> FileDescriptorBudget.FD_HEADROOM + catalog.descriptors().size() + 2);
        assertThat(fdPlanner.pipelineParallelism(4, catalog))
                .extracting(FinalizationPlanner.PipelinePlan::encoders,
                        FinalizationPlanner.PipelinePlan::reason)
                .containsExactly(2, FinalizationPlanner.PipelineClampReason.FD_CLAMPED);

        long readPageBytes = catalog.maxRecordLen();
        long retainedPageBytes = fdPlanner.pipelineParallelism(1, catalog).retainedPageBytes();
        long routerRefs = 4L + catalog.totalRecords()
                * (1L + 2L * (PartEncoders.QUEUE_DEPTH + 1L));
        long routerBytes = routerRefs * PageRef.retainedBytes(catalog.maxKeyLength());
        long perEncoder = readPageBytes + retainedPageBytes
                + PartEncoders.writerHeapEstimateBytes(base.finalRowGroupBytes());
        FinalizationPlanner heapPlanner = new FinalizationPlanner(
                base.withMergeBudgetBytes(routerBytes + 2 * perEncoder),
                SortMetrics.NO_OP, () -> -1);
        assertThat(heapPlanner.pipelineParallelism(4, catalog))
                .extracting(FinalizationPlanner.PipelinePlan::encoders,
                        FinalizationPlanner.PipelinePlan::reason)
                .containsExactly(2, FinalizationPlanner.PipelineClampReason.HEAP_CLAMPED);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        FinalizationPlanner refusingPlanner = new FinalizationPlanner(
                base.withMergeBudgetBytes(1), metrics, () -> -1);
        assertThatThrownBy(() -> refusingPlanner.pipelineParallelism(1, catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum pipeline lane does not fit");
        assertThat(metrics.count("SORT.pipeline_encoder_heap_floor_exhausted")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(longs = {8L << 20, 32L << 20, 64L << 20, 128L << 20})
    void writerHeapEstimateScalesEncoderAdmissionWithConfiguredRowGroupSize(
            long finalRowGroupBytes, @TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("row-group-admission" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortConfig base = SortConfigs.base().withFinalRowGroupBytes(finalRowGroupBytes);

        long readPageBytes = catalog.maxRecordLen();
        FinalizationPlanner probe = new FinalizationPlanner(
                base.withMergeBudgetBytes(Long.MAX_VALUE), SortMetrics.NO_OP, () -> -1);
        long retainedPageBytes = probe.pipelineParallelism(1, catalog).retainedPageBytes();
        long routerRefs = 4L + catalog.totalRecords()
                * (1L + 2L * (PartEncoders.QUEUE_DEPTH + 1L));
        long routerBytes = routerRefs * PageRef.retainedBytes(catalog.maxKeyLength());
        long writerEstimate = PartEncoders.writerHeapEstimateBytes(finalRowGroupBytes);
        long perEncoder = readPageBytes + retainedPageBytes + writerEstimate;

        // Sized to fit exactly two writers at THIS row-group size's estimate. If admission still
        // priced writers at the old fixed constant, a 128 MiB row group would leave this budget
        // large enough to admit all four requested encoders instead of clamping to two.
        FinalizationPlanner planner = new FinalizationPlanner(
                base.withMergeBudgetBytes(routerBytes + 2 * perEncoder), SortMetrics.NO_OP, () -> -1);

        FinalizationPlanner.PipelinePlan plan = planner.pipelineParallelism(4, catalog);

        assertThat(plan.encoders()).isEqualTo(2);
        assertThat(plan.reason()).isEqualTo(FinalizationPlanner.PipelineClampReason.HEAP_CLAMPED);
    }

    @Test
    void millionPageCatalogPricesOnlyBoundedInFlightPlans(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("large-catalog" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog physical = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        PageRunDescriptor base = physical.descriptors().getFirst();
        PageRunTrailer.Trailer largeTrailer = new PageRunTrailer.Trailer(
                1_000_000, 1_000_000,
                base.trailer().maxRecordLen(), base.maxRawPayloadLength(),
                base.maxKeyLength());
        PageRunCatalog large = SpillTestFixtures.catalog(List.of(
                new PageRunDescriptor(base.path(), base.fileSize(), base.trailerStart(),
                        largeTrailer, base.maxRawPayloadLength(), base.maxKeyLength(),
                        base.physicalFormat(), base.headerBytes(),
                        base.orderingMode())));
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(64L << 20)
                .withMergeBudgetBytes(256L << 20);

        FinalizationPlanner planner = new FinalizationPlanner(config, SortMetrics.NO_OP, () -> -1);
        FinalizationPlanner.PipelinePlan plan = planner.pipelineParallelism(4, large);

        assertThat(planner.pipelinePlanRefs(large))
                .isEqualTo(FinalizationPlanner.MAX_PIPELINE_PLAN_REFS);
        assertThat(plan.encoders()).isEqualTo(4);
    }

    @Test
    void longKeyCatalogPricesScannerPendingQueuedAndExecutingReferences(@TempDir Path root)
            throws IOException {
        String maximumKey = "m" + "x".repeat(1_023);
        Path segment = SortTestSupport.writePages(
                root.resolve("long-key" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a")),
                        List.of(SortTestSupport.object(maximumKey)),
                        List.of(SortTestSupport.object("z"))));
        PageRunCatalog physical = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        PageRunDescriptor base = physical.descriptors().getFirst();
        int segments = 8;
        List<PageRunDescriptor> descriptors = new ArrayList<>(segments);
        for (int index = 0; index < segments; index++) {
            PageRunTrailer.Trailer trailer = new PageRunTrailer.Trailer(
                    1_000_000, 1_000_000,
                    base.trailer().maxRecordLen(), base.maxRawPayloadLength(),
                    base.maxKeyLength());
            descriptors.add(new PageRunDescriptor(
                    root.resolve("synthetic-" + index + StagingNames.PAGE_RUN_SUFFIX),
                    base.fileSize(), base.trailerStart(), trailer,
                    base.maxRawPayloadLength(), base.maxKeyLength(), base.physicalFormat(),
                    base.headerBytes(), base.orderingMode()));
        }
        PageRunCatalog catalog = SpillTestFixtures.catalog(descriptors);
        int refBytes = PageRef.retainedBytes(maximumKey.length());
        int budgetedEncoders = 3;
        long cursorRefs = (long) segments * (PageRunHeaderStreams.QUEUE_DEPTH + 2L);
        long planRefs = (long) FinalizationPlanner.MAX_PIPELINE_PLAN_REFS
                * (1L + budgetedEncoders * (PartEncoders.QUEUE_DEPTH + 1L));
        long retainedPageBytes = DecodedPageBudget.retainedPageUpperBound(
                base.maxRawPayloadLength(),
                catalog.maxRecordLen());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(1L << 40);
        long budgetForThree = (cursorRefs + planRefs) * refBytes
                + budgetedEncoders * (PartEncoders.writerHeapEstimateBytes(config.finalRowGroupBytes())
                + catalog.maxRecordLen() + retainedPageBytes);
        config = config.withMergeBudgetBytes(budgetForThree);

        FinalizationPlanner.PipelinePlan plan = new FinalizationPlanner(
                config, SortMetrics.NO_OP, () -> -1).pipelineParallelism(4, catalog);

        assertThat(catalog.maxKeyLength()).isEqualTo(ByteMidpoint.MAX_KEY_LEN);
        assertThat(plan.refBytes()).isEqualTo(refBytes).isGreaterThan(200);
        assertThat(plan.encoders()).isEqualTo(4);
        assertThat(plan.reason()).isEqualTo(FinalizationPlanner.PipelineClampReason.NONE);
        assertThat(plan.planRefLimit()).isLessThan(FinalizationPlanner.MAX_PIPELINE_PLAN_REFS);
        assertThat(plan.clusterBudgetBytes()).isGreaterThanOrEqualTo(retainedPageBytes);
    }

    @Test
    void mediumKeyCatalogAdmitsByReducingTheRuntimePlanCap(@TempDir Path root)
            throws IOException {
        String key = "k".repeat(230);
        Path segment = SortTestSupport.writePages(
                root.resolve("medium-key" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object(key))));
        PageRunCatalog physical = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        PageRunDescriptor base = physical.descriptors().getFirst();
        PageRunTrailer.Trailer largeTrailer = new PageRunTrailer.Trailer(
                1_000_000, 1_000_000,
                base.trailer().maxRecordLen(), base.maxRawPayloadLength(),
                base.maxKeyLength());
        PageRunCatalog catalog = SpillTestFixtures.catalog(List.of(
                new PageRunDescriptor(base.path(), base.fileSize(), base.trailerStart(),
                        largeTrailer, base.maxRawPayloadLength(), base.maxKeyLength(),
                        base.physicalFormat(), base.headerBytes(),
                        base.orderingMode())));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        FinalizationPlanner planner = new FinalizationPlanner(SortConfigs.base()
                .withFinalFileBytes(Long.MAX_VALUE)
                .withMergeBudgetBytes(32L << 20), metrics, () -> -1);

        FinalizationPlanner.PipelinePlan plan = planner.pipelineParallelism(1, catalog);

        assertThat(planner.pipelinePlanRefs(catalog))
                .isEqualTo(FinalizationPlanner.MAX_PIPELINE_PLAN_REFS);
        assertThat(plan.encoders()).isEqualTo(1);
        assertThat(plan.planRefLimit())
                .isBetween(FinalizationPlanner.MIN_PIPELINE_PLAN_REFS,
                        FinalizationPlanner.MAX_PIPELINE_PLAN_REFS - 1);
        assertThat(metrics.count("SORT.pipeline_plan_ref_capped")).isEqualTo(1);
    }

    @Test
    void descriptorFloorRefusesBeforeOpeningAPipelineLane(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("fd-floor" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        int softLimit = FileDescriptorBudget.FD_HEADROOM + catalog.descriptors().size();
        FinalizationPlanner planner = new FinalizationPlanner(
                SortConfigs.base().withMergeBudgetBytes(64L << 20), metrics, () -> softLimit);

        assertThatThrownBy(() -> planner.pipelineParallelism(4, catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum pipeline lane does not fit descriptor budget")
                .hasMessageContaining("reason=fd_exhausted");
        assertThat(metrics.count("SORT.pipeline_encoders_fd_floor_exhausted")).isEqualTo(1);
        assertThat(metrics.count("SORT.pipeline_encoders_fd_clamped")).isZero();
    }

    @Test
    void unboundedByteTargetClosesPlansAtTheReferenceCap(@TempDir Path root)
            throws IOException {
        int pages = FinalizationPlanner.MAX_PIPELINE_PLAN_REFS + 1;
        List<List<ListEntry>> segment = new ArrayList<>(pages);
        for (int page = 0; page < pages; page++) {
            segment.add(List.of(SortTestSupport.object(String.format("key-%05d", page))));
        }
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortedDatasetResult result = runPages(root, List.of(segment), Long.MAX_VALUE,
                metrics, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.NONE, 1);

        assertThat(result.finalFiles()).hasSize(2);
        assertThat(metrics.count("SORT.pipeline_plan_ref_capped")).isEqualTo(1);
        assertThat(keys(result.finalFiles())).hasSize(pages);
    }

    @Test
    void overlapComponentWiderThanTheReferenceCapKeepsItsExactOrderInOnePart(@TempDir Path root)
            throws IOException {
        List<String> componentKeys = componentKeys();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortedDatasetResult result = runPages(root, oversizedComponent(), Long.MAX_VALUE,
                metrics, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.NONE, 4);

        assertThat(metrics.count("SORT.pipeline_cluster_spilled")).isEqualTo(1);
        assertThat(metrics.pipelineClusterPages.sum())
                .isEqualTo(FinalizationPlanner.MAX_PIPELINE_PLAN_REFS + 1L);
        assertThat(result.totalRows()).isEqualTo(componentKeys.size() + 2L);
        // The component is indivisible, so it takes a part of its own and caps the part after it.
        assertThat(result.finalFiles()).hasSize(2);
        assertThat(keys(List.of(result.finalFiles().getFirst())))
                .containsExactlyElementsOf(componentKeys);
        assertThat(keys(List.of(result.finalFiles().getLast())))
                .containsExactly("zzz-0", "zzz-1");
        assertNoClusterReferenceSpills(root);
    }

    @Test
    void routingAnOversizedComponentRetainsNoneOfItsReferences(@TempDir Path root)
            throws IOException {
        int heapLimit = 64;
        int narrowPages = 400;
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<List<ListEntry>> broad = List.of(
                List.of(SortTestSupport.object("a"), SortTestSupport.object("zz")));
        List<List<ListEntry>> narrow = new ArrayList<>(narrowPages);
        for (int page = 0; page < narrowPages; page++) {
            narrow.add(List.of(SortTestSupport.object(String.format("b%05d", page))));
        }
        List<PageRunReader> channels = List.of(
                PageRunReader.open(SortTestSupport.writePages(
                        staging.resolve("broad" + StagingNames.PAGE_RUN_SUFFIX), broad),
                        SortMetrics.NO_OP),
                PageRunReader.open(SortTestSupport.writePages(
                        staging.resolve("narrow" + StagingNames.PAGE_RUN_SUFFIX), narrow),
                        SortMetrics.NO_OP));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<PartPlan> plans = new ArrayList<>();
        FinalizationFailure failure = new FinalizationFailure();
        MergeRouter.Result routed;
        try (PageRunHeaderStreams cursors = new PageRunHeaderStreams(channels,
                PageRunHeaderStreams.planned(channels.size()), metrics, failure)) {
            routed = new MergeRouter(cursors, plans::add,
                    new PartSizer(PartSizer.Target.calibrated(), Long.MAX_VALUE), metrics,
                    failure, () -> { }, heapLimit, staging)
                    .route(channels.size());
        } finally {
            for (PageRunReader channel : channels) {
                channel.close();
            }
        }

        assertThat(routed.refs()).isEqualTo(narrowPages + 1L);
        assertThat(metrics.count("SORT.pipeline_cluster_spilled")).isEqualTo(1);
        assertThat(plans).hasSize(1);
        PartPlan.Cluster cluster = (PartPlan.Cluster) plans.getFirst().items().getFirst();
        assertThat(cluster.refCount()).isEqualTo(narrowPages + 1L);
        // The dispatched plan holds a staging coordinate rather than one PageRef per page, so the
        // queued reference wave stays priced by the cap however wide the component grows.
        assertThat(cluster.refs()).isInstanceOf(ClusterRefs.Spilled.class);
        Path spill = ((ClusterRefs.Spilled) cluster.refs()).file();
        assertThat(spill).exists().hasParent(staging);
        List<String> spilledMinimums = new ArrayList<>();
        try (ClusterRefs.Cursor refs = cluster.refs().open()) {
            while (refs.peek() != null) {
                spilledMinimums.add(new String(refs.next().minKey(), StandardCharsets.UTF_8));
            }
        }
        assertThat(spilledMinimums).hasSize(narrowPages + 1)
                .startsWith("a", "b00000").endsWith("b00399");
        cluster.discard();
        assertThat(spill).doesNotExist();
    }

    @Test
    void clusterReferenceCollectionPromotesExactlyAtTheHeapLimit(@TempDir Path root)
            throws IOException {
        List<PageRef> refs = new ArrayList<>();
        for (int page = 0; page < 5; page++) {
            refs.add(new PageRef(0, page, 64L + page, 32, ("k" + page).getBytes(StandardCharsets.UTF_8),
                    ("k" + page).getBytes(StandardCharsets.UTF_8), 1, 16));
        }

        try (ClusterRefs.Builder atLimit = new ClusterRefs.Builder(4, root, 0)) {
            for (PageRef ref : refs.subList(0, 4)) {
                atLimit.add(ref);
            }
            assertThat(atLimit.spilled()).isFalse();
            assertThat(atLimit.build()).isInstanceOf(ClusterRefs.Heap.class);
        }

        ClusterRefs promoted;
        try (ClusterRefs.Builder builder = new ClusterRefs.Builder(4, root, 1)) {
            for (PageRef ref : refs) {
                builder.add(ref);
            }
            assertThat(builder.spilled()).isTrue();
            promoted = builder.build();
        }
        assertThat(promoted).isEqualTo(new ClusterRefs.Spilled(
                root.resolve(StagingNames.clusterRefsTmp(1)), 5));
        List<PageRef> replayed = new ArrayList<>();
        try (ClusterRefs.Cursor cursor = promoted.open()) {
            while (cursor.peek() != null) {
                replayed.add(cursor.next());
            }
        }
        assertThat(replayed).usingRecursiveFieldByFieldElementComparator()
                .containsExactlyElementsOf(refs);
        promoted.discard();
        assertThat(root.resolve(StagingNames.clusterRefsTmp(1))).doesNotExist();
    }

    @Test
    void partialClusterCollectionLeavesNoSpillBehind(@TempDir Path root) throws IOException {
        PageRef ref = new PageRef(0, 0, 64L, 32, "k".getBytes(StandardCharsets.UTF_8),
                "k".getBytes(StandardCharsets.UTF_8), 1, 16);
        Path spill = root.resolve(StagingNames.clusterRefsTmp(7));

        try (ClusterRefs.Builder builder = new ClusterRefs.Builder(1, root, 7)) {
            builder.add(ref);
            builder.add(ref);
            assertThat(spill).exists();
        }

        assertThat(spill).doesNotExist();
    }

    @Test
    void cancellingAnOversizedComponentReleasesItsSpilledReferences(@TempDir Path root)
            throws Exception {
        CountDownLatch componentEncoderStarted = new CountDownLatch(1);
        CountDownLatch blockWriter = new CountDownLatch(1);
        SortedFileWriterFactory blocking = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    @Override
                    public void write(ListEntry entry) throws IOException {
                        if (index != 1) {
                            super.write(entry);
                            return;
                        }
                        componentEncoderStarted.countDown();
                        try {
                            blockWriter.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("pipeline encoder interrupted", e);
                        }
                        super.write(entry);
                    }
                };
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofPlatform().start(() -> {
            try {
                runPages(root, oversizedComponent(), Long.MAX_VALUE, SortMetrics.NO_OP, blocking,
                        10_000, 64L << 20, PageCompression.NONE, 1);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        try {
            assertThat(componentEncoderStarted.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(clusterReferenceSpills(root)).isNotEmpty();
            caller.interrupt();
            assertThat(caller.join(Duration.ofSeconds(60))).isTrue();
        } finally {
            blockWriter.countDown();
        }
        assertThat(failure.get()).isInstanceOf(IOException.class)
                .hasMessageContaining("sort merge interrupted");
        assertNoPublishedOrTemporaryFiles(root);
        assertNoClusterReferenceSpills(root);
    }

    /**
     * One broad page overlapping every page of a second run: a legal set of sorted runs whose
     * transitive component is wider than the hard plan reference cap, plus two pages above it.
     */
    private static List<List<List<ListEntry>>> oversizedComponent() {
        List<List<ListEntry>> broad = new ArrayList<>();
        broad.add(List.of(SortTestSupport.object("a"), SortTestSupport.object("zz")));
        broad.add(List.of(SortTestSupport.object("zzz-0")));
        broad.add(List.of(SortTestSupport.object("zzz-1")));
        List<List<ListEntry>> narrow = new ArrayList<>();
        for (int page = 0; page < FinalizationPlanner.MAX_PIPELINE_PLAN_REFS; page++) {
            narrow.add(List.of(SortTestSupport.object(String.format("b%05d", page))));
        }
        return List.of(broad, narrow);
    }

    /** The exact row order {@link #oversizedComponent()}'s single component must produce. */
    private static List<String> componentKeys() {
        List<String> keys = new ArrayList<>();
        keys.add("a");
        for (int page = 0; page < FinalizationPlanner.MAX_PIPELINE_PLAN_REFS; page++) {
            keys.add(String.format("b%05d", page));
        }
        keys.add("zz");
        return keys;
    }

    private static List<Path> clusterReferenceSpills(Path root) throws IOException {
        try (var files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".pagerefs.tmp"))
                    .toList();
        }
    }

    private static void assertNoClusterReferenceSpills(Path root) throws IOException {
        assertThat(clusterReferenceSpills(root)).isEmpty();
    }

    @Test
    void cascadeFanInReservesEveryPipelineOutputDescriptor(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("fanin" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortConfig config = SortConfigs.base()
                .withFanIn(100)
                .withMergeBudgetBytes(64L << 20)
                .withMergePerStreamBytes(1);
        FinalizationPlanner planner = new FinalizationPlanner(config, SortMetrics.NO_OP,
                () -> FileDescriptorBudget.FD_HEADROOM + 5);

        assertThat(planner.pipelineFanIn(catalog, 4)).isEqualTo(2);
    }

    @Test
    void cascadeFanInRefusesBeforeOpeningReadersWhenCapacityCannotFitTwoStreams(@TempDir Path root)
            throws IOException {
        Path segA = SortTestSupport.writePages(root.resolve("fanin-a" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        Path segB = SortTestSupport.writePages(root.resolve("fanin-b" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("b"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segA, segB),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortConfig config = SortConfigs.base()
                .withFanIn(100)
                .withMergeBudgetBytes(64L << 20)
                .withMergePerStreamBytes(1);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        // headroom for encoderCount=4 is FD_HEADROOM + 3, so a soft limit of FD_HEADROOM + 4
        // leaves an fd-bounded capacity of 1 — below the two-stream minimum a cascade needs.
        FinalizationPlanner planner = new FinalizationPlanner(config, metrics,
                () -> FileDescriptorBudget.FD_HEADROOM + 4);

        assertThatThrownBy(() -> planner.pipelineFanIn(catalog, 4))
                .isInstanceOf(CascadeCapacityExhaustedException.class)
                .hasMessageContaining("cascade cannot open the minimum two streams");
        assertThat(metrics.count("SORT.merge_fanin_floor_exhausted")).isEqualTo(1);
    }

    @Test
    void singleSourceSegmentBypassesCascadeEvenUnderTheSameStarvedFdBudget(@TempDir Path root)
            throws IOException {
        Path segment = SortTestSupport.writePages(
                root.resolve("fanin-single" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP));
        SortConfig config = SortConfigs.base()
                .withFanIn(100)
                .withMergeBudgetBytes(64L << 20)
                .withMergePerStreamBytes(1);
        FinalizationPlanner planner = new FinalizationPlanner(config, SortMetrics.NO_OP,
                () -> FileDescriptorBudget.FD_HEADROOM + 4);

        // A single source segment never opens a cascade group, so the same starved budget that
        // refuses a two-segment catalog above must not refuse here.
        assertThat(planner.pipelineFanIn(catalog, 4)).isEqualTo(2);
    }

    @Test
    void calibratedCompressedDisjointPartsTrackTargetAfterCalibration(@TempDir Path root)
            throws IOException {
        long targetBytes = 64L << 10;
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int page = 0; page < 600; page++) {
            List<ListEntry> entries = new ArrayList<>();
            for (int row = 0; row < 100; row++) {
                long value = page * 100L + row;
                entries.add(SortTestSupport.object(
                        String.format("prefix/%05d/object-%04d-%016x%016x", page, row,
                                value * 0x9e3779b97f4a7c15L,
                                Long.rotateLeft(value * 0xc2b2ae3d27d4eb4fL, 29))));
            }
            pages.add(entries);
        }

        SortedDatasetResult result = runPages(root, List.of(pages), targetBytes,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.LZ4, 1);

        assertThat(result.finalFiles()).hasSizeGreaterThanOrEqualTo(8);
        long tolerance = Math.round(targetBytes * 0.35);
        List<Long> partSizes = result.finalFiles().stream().map(part -> {
            try {
                return Files.size(part);
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }).toList();
        long outputBytes = partSizes.stream().mapToLong(Long::longValue).sum();
        long expectedParts = (outputBytes + targetBytes - 1L) / targetBytes;
        assertThat((long) result.finalFiles().size())
                .isBetween(expectedParts - 1L, expectedParts + 1L);
        for (Path part : result.finalFiles().subList(1, result.finalFiles().size() - 1)) {
            assertThat(Files.size(part)).as("part sizes: %s", partSizes)
                    .isBetween(targetBytes - tolerance, targetBytes + tolerance);
        }
    }

    @Test
    void equalKeyGroupCrossingPageBoundaryStaysInOnePart(@TempDir Path root)
            throws IOException {
        int equalKeyRows = 4_097;
        List<ListEntry> firstPage = new ArrayList<>(equalKeyRows);
        for (int row = 0; row < equalKeyRows; row++) {
            firstPage.add(SortTestSupport.object("a"));
        }
        List<List<List<ListEntry>>> segmentPages = List.of(
                List.of(firstPage),
                List.of(List.of(SortTestSupport.object("a"))),
                List.of(List.of(SortTestSupport.object("b"))));

        SortedDatasetResult result = runPages(root, segmentPages, 1, SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20);

        assertThat(result.finalFiles()).hasSize(2);
        assertThat(keys(List.of(result.finalFiles().getFirst())))
                .hasSize(equalKeyRows + 1).containsOnly("a");
        assertThat(keys(List.of(result.finalFiles().getLast()))).containsExactly("b");
    }

    @Test
    void sharedQueueLetsAnIdleEncoderBypassABlockedWorkersFuturePlan(@TempDir Path root)
            throws IOException {
        CountDownLatch laterPlanClosed = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> closeOrder = new ConcurrentLinkedQueue<>();
        SortedFileWriterFactory reordered = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    private final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public void write(ListEntry entry) throws IOException {
                        if (index == 2) {
                            try {
                                if (!laterPlanClosed.await(60, TimeUnit.SECONDS)) {
                                    throw new IOException("later encoder did not finish");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("slow encoder interrupted", e);
                            }
                        }
                        super.write(entry);
                    }

                    @Override
                    public void close() throws IOException {
                        super.close();
                        if (closed.compareAndSet(false, true)) {
                            closeOrder.add(index);
                            if (index == 4) {
                                laterPlanClosed.countDown();
                            }
                        }
                    }
                };

        List<List<List<ListEntry>>> pages = List.of(List.of(
                List.of(SortTestSupport.object("a")),
                List.of(SortTestSupport.object("b")),
                List.of(SortTestSupport.object("c")),
                List.of(SortTestSupport.object("d")),
                List.of(SortTestSupport.object("e")),
                List.of(SortTestSupport.object("f"))));
        SortedDatasetResult result = runPages(root, pages, 1,
                SortMetrics.NO_OP, reordered, 10_000, 64L << 20,
                PageCompression.NONE, 2);

        List<Integer> observed = List.copyOf(closeOrder);
        assertThat(observed.indexOf(4)).isLessThan(observed.indexOf(2));
        assertThat(result.finalFiles()).hasSizeGreaterThan(1);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
    }

    @Test
    void fourSharedChannelEncodersMatchOneEncoderOutput(@TempDir Path root) throws IOException {
        List<List<List<ListEntry>>> pages = new ArrayList<>();
        for (int segment = 0; segment < 8; segment++) {
            List<List<ListEntry>> segmentPages = new ArrayList<>();
            for (int page = 0; page < 40; page++) {
                segmentPages.add(List.of(
                        SortTestSupport.object(String.format("k%04d-a-s%02d", page, segment)),
                        SortTestSupport.object(String.format("k%04d-z-s%02d", page, segment))));
            }
            pages.add(segmentPages);
        }
        SortedDatasetResult serial = runPages(root.resolve("serial"), pages, 4_096,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.NONE, 1, PartSizer.Target.fixedRows(160));
        SortedDatasetResult concurrent = runPages(root.resolve("concurrent"), pages, 4_096,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.NONE, 4, PartSizer.Target.fixedRows(160));

        assertThat(concurrent.finalFiles()).hasSameSizeAs(serial.finalFiles());
        for (int part = 0; part < serial.finalFiles().size(); part++) {
            assertThat(Files.mismatch(serial.finalFiles().get(part),
                    concurrent.finalFiles().get(part))).isEqualTo(-1L);
        }
        assertThat(keys(concurrent.finalFiles())).containsExactlyElementsOf(keys(serial.finalFiles()));
        assertThat(concurrent.totalRows()).isEqualTo(serial.totalRows());
    }

    @Test
    void encoderProgressUsesTheSharedRowBatchGranularity(@TempDir Path root) throws IOException {
        List<ListEntry> rows = new ArrayList<>();
        for (int row = 0; row < 2_500; row++) {
            rows.add(SortTestSupport.object(String.format("key-%04d", row)));
        }
        List<Long> progress = new java.util.concurrent.CopyOnWriteArrayList<>();

        SortedDatasetResult result = runPages(root, List.of(List.of(rows)), Long.MAX_VALUE,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                PageCompression.NONE, 1, progress::add);

        assertThat(result.totalRows()).isEqualTo(2_500);
        assertThat(progress).containsExactly(1_000L, 1_000L, 500L);
    }

    @Test
    void corruptReferencedPageAbortsWithoutTemporaryOutput(@TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Files.createDirectories(root.resolve("data"));
        Path segment = SortTestSupport.writePages(
                staging.resolve("crc" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"), SortTestSupport.object("b"))));
        PageRunReader.RoutingPage page;
        try (PageRunReader io = PageRunReader.open(segment, SortMetrics.NO_OP)) {
            page = io.nextRoutingPage();
        }
        long corruptOffset = page.offset() + page.framedLen() - 1L;
        try (FileChannel channel = FileChannel.open(segment,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer value = ByteBuffer.allocate(1);
            channel.read(value, corruptOffset);
            value.flip();
            value.put(0, (byte) (value.get(0) ^ 0x01));
            channel.write(value, corruptOffset);
        }

        assertThatThrownBy(() -> runPaths(root, List.of(segment), Long.MAX_VALUE,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 4))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC32C mismatch");
        assertNoPublishedOrTemporaryFiles(root);
    }

    @Test
    void openDescriptorCountStaysWithinSegmentsPlusEncoders(@TempDir Path root) throws IOException {
        Assumptions.assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")));
        int segments = 8;
        long baseline = openDescriptorCount();
        AtomicLong peak = new AtomicLong();
        SortedFileWriterFactory measuring = (path, index) -> {
            SortedFileWriter writer = SortedFileWriterFactory.DEFAULT.create(path, index);
            peak.accumulateAndGet(openDescriptorCount(), Math::max);
            return writer;
        };
        List<List<String>> keys = new ArrayList<>();
        for (int segment = 0; segment < segments; segment++) {
            List<String> rows = new ArrayList<>();
            for (int row = 0; row < 80; row++) {
                rows.add(String.format("k%04d-s%02d", row, segment));
            }
            keys.add(rows);
        }

        SortedDatasetResult result = run(root, keys, 2_048, SortMetrics.NO_OP, measuring);

        assertThat(result.finalizationParallelism()).isEqualTo(4);
        assertThat(peak.get()).isLessThanOrEqualTo(baseline + segments + 4 + 16);
    }

    @Test
    void encoderFailureDiscardsItsPartAndPublishesNothing(@TempDir Path root) {
        SortedFileWriterFactory failing = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    private int writes;

                    @Override
                    public void write(ListEntry entry) throws IOException {
                        if (++writes == 3) {
                            throw new IOException("injected pipeline encoder failure");
                        }
                        super.write(entry);
                    }
                };

        assertThatThrownBy(() -> run(root,
                List.of(List.of("a", "b", "c", "d", "e")), Long.MAX_VALUE,
                SortMetrics.NO_OP, failing))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected pipeline encoder failure");
        assertNoPublishedOrTemporaryFiles(root);
    }

    @Test
    void callerInterruptCancelsAllFinalizationStagesWithoutPublishing(@TempDir Path root)
            throws Exception {
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch blockWriter = new CountDownLatch(1);
        SortedFileWriterFactory blocking = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    @Override
                    public void write(ListEntry entry) throws IOException {
                        if (index == 1) {
                            super.write(entry);
                            return;
                        }
                        writerStarted.countDown();
                        try {
                            blockWriter.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("pipeline encoder interrupted", e);
                        }
                        super.write(entry);
                    }
                };
        List<String> keys = java.util.stream.IntStream.range(0, 32)
                .mapToObj(i -> String.format("k%03d", i)).toList();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofPlatform().start(() -> {
            try {
                run(root, List.of(keys), 1, metrics, blocking);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        try {
            assertThat(writerStarted.await(60, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (metrics.count("SORT.pipeline_plan_queue_saturated") == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(metrics.count("SORT.pipeline_plan_queue_saturated")).isPositive();
            caller.interrupt();
            assertThat(caller.join(Duration.ofSeconds(60))).isTrue();
        } finally {
            blockWriter.countDown();
        }
        assertThat(failure.get()).isInstanceOf(IOException.class)
                .hasMessageContaining("sort merge interrupted");
        assertNoPublishedOrTemporaryFiles(root);
    }

    private SortedDatasetResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics) throws IOException {
        return run(root, segmentKeys, finalFileBytes, metrics, SortedFileWriterFactory.DEFAULT);
    }

    private SortedDatasetResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory)
            throws IOException {
        return run(root, segmentKeys, finalFileBytes, metrics, writerFactory, 10_000, 64L << 20);
    }

    private SortedDatasetResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes) throws IOException {
        List<List<List<ListEntry>>> segmentPages = segmentKeys.stream()
                .map(keys -> keys.stream()
                        .map(key -> List.<ListEntry>of(SortTestSupport.object(key))).toList())
                .toList();
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes);
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, PageCompression.NONE);
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, PageCompression codec)
            throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, codec, 4);
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, PageCompression codec,
            int encoderCount) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, codec, encoderCount, PartSizer.Target.calibrated(),
                ignored -> { });
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, PageCompression codec,
            int encoderCount, PartSizer.Target partTarget) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, codec, encoderCount, partTarget, ignored -> { });
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, PageCompression codec,
            int encoderCount, LongConsumer progressCallback) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, codec, encoderCount, PartSizer.Target.calibrated(),
                progressCallback);
    }

    private SortedDatasetResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, PageCompression codec,
            int encoderCount, PartSizer.Target partTarget, LongConsumer progressCallback)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = new ArrayList<>();
        for (int segment = 0; segment < segmentPages.size(); segment++) {
            segments.add(SortTestSupport.writePages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX),
                    segmentPages.get(segment), SortMode.OBJECTS, codec));
        }
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(encoderCount)
                .withFanIn(fanIn)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withFinalFileBytes(finalFileBytes);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writerFactory,
                () -> -1, StaleFinalSweep.OWN_PARTS_ONLY, partTarget);
        return new SortedDatasetCoordinator(run).transform(segments, output, staging, SortedDatasetCommitter.NO_OP,
                progressCallback, FinalPassListener.NO_OP);
    }

    private SortedDatasetResult runPaths(Path root, List<Path> segments, long finalFileBytes,
            SortMetrics metrics, SortedFileWriterFactory writerFactory, int encoderCount)
            throws IOException {
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(encoderCount)
                .withMergeBudgetBytes(64L << 20)
                .withFinalFileBytes(finalFileBytes);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writerFactory, () -> -1, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortedDatasetCoordinator(run).transform(segments, root.resolve("data"),
                root.resolve("_staging"), SortedDatasetCommitter.NO_OP, ignored -> { },
                FinalPassListener.NO_OP);
    }

    private static long openDescriptorCount() {
        try (var descriptors = Files.list(Path.of("/proc/self/fd"))) {
            return descriptors.count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertNoPublishedOrTemporaryFiles(Path root) {
        assertThat(root.resolve("data")).isEmptyDirectory();
        try (var files = Files.walk(root)) {
            assertThat(files.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString()))
                    .noneMatch(name -> name.endsWith(".tmp") || name.endsWith(".parquet"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static List<String> keys(List<Path> files) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (ParquetEntryReader reader = new ParquetEntryReader(file)) {
                while (reader.hasNext()) {
                    keys.add(reader.next().key().asString());
                }
            }
        }
        return keys;
    }

}
