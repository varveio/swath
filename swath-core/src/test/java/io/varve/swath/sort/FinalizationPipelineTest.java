/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

final class FinalizationPipelineTest {
    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void headerCursorAndPlanQueuesUseTheReferenceRoutingDepths() {
        assertThat(SegmentHeaderCursors.QUEUE_DEPTH).isEqualTo(2);
        assertThat(PartEncoders.QUEUE_DEPTH).isEqualTo(2);
    }

    @Test
    void calibratedPartTargetUsesCompletedEncodedToLogicalRatio() {
        PipelinePartSizer sizer = new PipelinePartSizer(
                PipelinePartSizer.Target.calibrated(), 100);

        assertThat(sizer.calibratedLogicalTarget()).isEqualTo(100);
        sizer.completed(100, 1_000);
        assertThat(sizer.encodedToLogicalRatio()).isEqualTo(0.1);
        assertThat(sizer.calibratedLogicalTarget()).isEqualTo(1_000);
        assertThat(sizer.shouldClose(999, Long.MAX_VALUE)).isFalse();
        assertThat(sizer.shouldClose(1_000, 0)).isTrue();
    }

    @Test
    void decodedPageBudgetReleasesAnExhaustedPageBeforeAdmittingItsSuccessor() throws IOException {
        PageBlock page = PageBlock.pack(List.of(SortTestSupport.object("key")), comparator);
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
                SortTestSupport.object("repeated-prefix/bravo")), comparator, PageCodec.LZ4);
        byte[] record = packed.serialize();
        PageBlock persisted = PageBlock.deserialize(record);

        long retainedUpper = DecodedPageBudget.retainedPageUpperBound(
                persisted.rawPayloadLength(), record.length);

        assertThat(DecodedPageBudget.retainedBytes(persisted)).isLessThanOrEqualTo(retainedUpper);
    }

    @Test
    void retainedPagePlanningUnitCoversMaximumDictionaryCardinality(@TempDir Path root)
            throws IOException {
        List<ListEntry> entries = new ArrayList<>(PageBlock.DICT_CAP);
        for (int value = 0; value < PageBlock.DICT_CAP; value++) {
            String suffix = String.format("-%02d", value);
            entries.add(new ObjectEntry(KeyBytes.ofUtf8("key" + suffix), 1L, 0L,
                    null, "storage" + suffix, null, false, "owner" + suffix,
                    "display" + suffix, "algorithm" + suffix, "type" + suffix));
        }
        PageBlock persisted = PageBlock.deserialize(
                PageBlock.pack(entries, comparator, PageCodec.LZ4).serialize());
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("max-dictionaries" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(entries), SortMode.OBJECTS, PageCodec.LZ4);
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        MergePlanner.PipelinePlan plan = new MergePlanner(
                SortConfigs.base().withMergeBudgetBytes(64L << 20),
                SortMetrics.NO_OP, () -> -1).pipelineParallelism(1, catalog);

        for (int column = 0; column < PageBlockCodec.DICT_COLUMN_COUNT; column++) {
            assertThat(persisted.dictionariesUnsafe().size(column)).isEqualTo(PageBlock.DICT_CAP);
        }
        DecodedPageBudget budget = new DecodedPageBudget(
                plan.retainedPageBytes(), SortMetrics.NO_OP);

        assertThat(budget.reserve(persisted))
                .isEqualTo(DecodedPageBudget.retainedBytes(persisted));
    }

    @Test
    void clusteredRowsAttributeEveryRawPayloadByteExactlyOnce() {
        PageBlock page = null;
        for (int count = 2; count < 20; count++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int row = 0; row < count; row++) {
                rows.add(SortTestSupport.object(String.format("key-%02d", row)));
            }
            PageBlock candidate = PageBlock.pack(rows, comparator);
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
            segments.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), pages));
        }
        List<PageRunSegmentIo> channels = new ArrayList<>();
        for (Path segment : segments) {
            channels.add(PageRunSegmentIo.open(segment, SortMetrics.NO_OP));
        }
        SegmentHeaderCursors.Settings settings = new SegmentHeaderCursors.Settings(
                1, 1);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            PipelineFailure failure = new PipelineFailure();
            try (SegmentHeaderCursors cursors = new SegmentHeaderCursors(
                    channels, settings, SortMetrics.NO_OP, failure)) {
                assertThat(cursors.next(segmentCount - 1)).isNotNull();
            }
        });
        for (PageRunSegmentIo channel : channels) {
            channel.close();
        }
    }

    @Test
    void headerPassRejectsTruncatedFrameTiling(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("truncated" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a")),
                        List.of(SortTestSupport.object("c"))));
        PageRunSegmentIo.RoutingPage first;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            first = io.nextRoutingPage();
        }
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            ByteBuffer shortened = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(first.framedLen() - 9).flip();
            channel.write(shortened, first.offset());
        }

        assertThatThrownBy(() -> {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
                while (io.nextRoutingPage() != null) {
                    // Header pass must reject before an encoder is started.
                }
                io.checkRoutingComplete();
            }
        }).isInstanceOf(IOException.class);
    }

    @Test
    void headerPassRejectsSplicedFrameOrder(@TempDir Path root) throws IOException {
        Path target = SortTestSupport.writeIndexedPages(
                root.resolve("target" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a")),
                        List.of(SortTestSupport.object("c"))));
        Path donor = SortTestSupport.writeIndexedPages(
                root.resolve("donor" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("0"))));
        PageRunSegmentIo.RoutingPage targetSecond;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(target, SortMetrics.NO_OP)) {
            io.nextRoutingPage();
            targetSecond = io.nextRoutingPage();
        }
        ByteBuffer donorFrame;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(donor, SortMetrics.NO_OP)) {
            PageRunSegmentIo.RoutingPage page = io.nextRoutingPage();
            assertThat(page.framedLen()).isEqualTo(targetSecond.framedLen());
            donorFrame = io.readAt(page.offset(), page.framedLen());
        }
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
            channel.write(donorFrame, targetSecond.offset());
        }

        assertThatThrownBy(() -> {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(target, SortMetrics.NO_OP)) {
                io.nextRoutingPage();
                io.nextRoutingPage();
            }
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("page minKey regressed");
    }

    @Test
    void disjointSingleRowPagesUseWholePageForwarding(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = run(root,
                List.of(List.of("a", "c"), List.of("b", "d")), Long.MAX_VALUE, metrics);

        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        assertThat(metrics.pipelinePagesForwarded.sum()).isEqualTo(4);
        assertThat(metrics.pipelineClusterPages.sum()).isZero();
    }

    @Test
    void fullOverlapSingleRowPagesMergeAndNeverSplitEqualRawKeys(@TempDir Path root)
            throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = run(root,
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

        SortTransformResult result = run(root, segments, Long.MAX_VALUE, metrics);

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
        PageBlock sample = PageBlock.pack(segments.getFirst().getFirst(), comparator);
        long pageBytes = DecodedPageBudget.retainedBytes(
                PageBlock.deserialize(sample.serialize()));
        long plannedPageBytes = DecodedPageBudget.retainedPageUpperBound(
                sample.rawPayloadLength(), sample.serialize().length);
        long retainedRefs = (100L * (SegmentHeaderCursors.QUEUE_DEPTH + 2L)
                + 100L * (PartEncoders.QUEUE_DEPTH + 2L)) * PageRef.retainedBytes(5);
        long mergeBudget = PartEncoders.WRITER_HEAP_ESTIMATE_BYTES + retainedRefs
                + sample.serialize().length + 3L * plannedPageBytes - 1L;

        DecodedPageBudget eager = new DecodedPageBudget(3L * pageBytes - 1L,
                SortMetrics.NO_OP);
        eager.reserve(PageBlock.deserialize(sample.serialize()));
        eager.reserve(PageBlock.deserialize(sample.serialize()));
        assertThatThrownBy(() -> eager.reserve(PageBlock.deserialize(sample.serialize())))
                .isInstanceOf(MergeMemoryExhaustedException.class);

        SortTransformResult result = runPages(root, segments, Long.MAX_VALUE,
                metrics, SortedFileWriterFactory.DEFAULT, 10_000, mergeBudget,
                0, PageCodec.NONE, 1);

        assertThat(result.totalRows()).isEqualTo(200);
        assertThat(metrics.pipelineDecodedPageBytesPeak.get()).isLessThan(3L * pageBytes);
    }

    @Test
    void pipelineRunsCascadeBeforeFinalRouting(@TempDir Path root) throws IOException {
        List<List<String>> segments = List.of(
                List.of("a"), List.of("b"), List.of("c"), List.of("d"), List.of("e"));

        SortTransformResult result = run(root, segments, Long.MAX_VALUE, SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT, 2, 64L << 20);

        assertThat(result.cascadedPasses()).isEqualTo(2);
        assertThat(result.mergePasses()).isEqualTo(3);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void smallCorpusRunsEveryRequestedPipelineEncoder(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<List<ListEntry>>> pages = List.of(
                List.of(List.of(SortTestSupport.object("a"))));

        SortTransformResult result = runPages(root, pages, Long.MAX_VALUE, metrics,
                SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20, Long.MAX_VALUE);

        assertThat(result.finalizationParallelism()).isEqualTo(4);
        assertThat(metrics.count("SORT.pipeline_encoders_fd_clamped")).isZero();
        assertThat(metrics.count("SORT.pipeline_encoders_heap_clamped")).isZero();
    }

    @Test
    void pipelineAdmissionUsesOnlySurvivorFdsAndPipelineHeap(@TempDir Path root)
            throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("admission" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        SortConfig base = SortConfigs.base().withMinParallelStagedBytes(Long.MAX_VALUE)
                .withMergeBudgetBytes(64L << 20);
        MergePlanner fdPlanner = new MergePlanner(base, SortMetrics.NO_OP,
                () -> MergeFdBudget.FD_HEADROOM + catalog.descriptors().size() + 2);
        assertThat(fdPlanner.pipelineParallelism(4, catalog))
                .extracting(MergePlanner.PipelinePlan::encoders,
                        MergePlanner.PipelinePlan::reason)
                .containsExactly(2, MergePlanner.PipelineClampReason.FD_CLAMPED);

        long readPageBytes = catalog.maxRecordLen();
        long retainedPageBytes = DecodedPageBudget.retainedPageUpperBound(
                catalog.maxRawPayloadLength(), catalog.maxRecordLen());
        long routerRefs = 4L + catalog.totalRecords()
                * (1L + 2L * (PartEncoders.QUEUE_DEPTH + 1L));
        long routerBytes = routerRefs * PageRef.retainedBytes(catalog.maxKeyLength());
        long perEncoder = readPageBytes + retainedPageBytes
                + PartEncoders.WRITER_HEAP_ESTIMATE_BYTES;
        MergePlanner heapPlanner = new MergePlanner(
                base.withMergeBudgetBytes(routerBytes + 2 * perEncoder),
                SortMetrics.NO_OP, () -> -1);
        assertThat(heapPlanner.pipelineParallelism(4, catalog))
                .extracting(MergePlanner.PipelinePlan::encoders,
                        MergePlanner.PipelinePlan::reason)
                .containsExactly(2, MergePlanner.PipelineClampReason.HEAP_CLAMPED);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        MergePlanner refusingPlanner = new MergePlanner(
                base.withMergeBudgetBytes(1), metrics, () -> -1);
        assertThatThrownBy(() -> refusingPlanner.pipelineParallelism(1, catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum pipeline lane does not fit");
        assertThat(metrics.count("SORT.pipeline_encoder_heap_floor_exhausted")).isEqualTo(1);
    }

    @Test
    void millionPageCatalogPricesOnlyBoundedInFlightPlans(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("large-catalog" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog physical = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        PageRunSegmentDescriptor base = physical.descriptors().getFirst();
        PageRunTrailer.Trailer largeTrailer = new PageRunTrailer.Trailer(
                base.trailer().segMinKey(), base.trailer().segMaxKey(),
                base.trailer().extensionStart(), 1_000_000, 1_000_000,
                base.trailer().maxRecordLen());
        PageRunCatalog large = PageRunCatalog.fromDescriptors(List.of(
                new PageRunSegmentDescriptor(base.path(), base.fileSize(), base.trailerStart(),
                        largeTrailer, base.extension(), 64 << 10,
                        base.physicalFormat())));
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(64L << 20)
                .withMergeBudgetBytes(256L << 20);

        MergePlanner planner = new MergePlanner(config, SortMetrics.NO_OP, () -> -1);
        MergePlanner.PipelinePlan plan = planner.pipelineParallelism(4, large);

        assertThat(planner.pipelinePlanRefs(large))
                .isEqualTo(MergePlanner.MAX_PIPELINE_PLAN_REFS);
        assertThat(plan.encoders()).isEqualTo(4);
    }

    @Test
    void longKeyCatalogPricesScannerPendingQueuedAndExecutingReferences(@TempDir Path root)
            throws IOException {
        String maximumKey = "x".repeat(1_024);
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("long-key" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object(maximumKey))));
        PageRunCatalog physical = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        PageRunSegmentDescriptor base = physical.descriptors().getFirst();
        int segments = 8;
        List<PageRunSegmentDescriptor> descriptors = new ArrayList<>(segments);
        for (int index = 0; index < segments; index++) {
            PageRunTrailer.Trailer trailer = new PageRunTrailer.Trailer(
                    base.trailer().segMinKey(), base.trailer().segMaxKey(),
                    base.trailer().extensionStart(), 1_000_000, 1_000_000,
                    base.trailer().maxRecordLen());
            descriptors.add(new PageRunSegmentDescriptor(
                    root.resolve("synthetic-" + index + StagingNames.PAGE_RUN_SUFFIX),
                    base.fileSize(), base.trailerStart(), trailer, base.extension(),
                    base.maxRawPayloadLength(), base.physicalFormat()));
        }
        PageRunCatalog catalog = PageRunCatalog.fromDescriptors(descriptors);
        int refBytes = PageRef.retainedBytes(maximumKey.length());
        int admittedEncoders = 3;
        long cursorRefs = (long) segments * (SegmentHeaderCursors.QUEUE_DEPTH + 2L);
        long planRefs = (long) MergePlanner.MAX_PIPELINE_PLAN_REFS
                * (1L + admittedEncoders * (PartEncoders.QUEUE_DEPTH + 1L));
        long retainedPageBytes = DecodedPageBudget.retainedPageUpperBound(
                catalog.maxRawPayloadLength(), catalog.maxRecordLen());
        long budgetForThree = (cursorRefs + planRefs) * refBytes
                + admittedEncoders * (PartEncoders.WRITER_HEAP_ESTIMATE_BYTES
                + catalog.maxRecordLen() + retainedPageBytes);
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(1L << 40)
                .withMergeBudgetBytes(budgetForThree);

        MergePlanner.PipelinePlan plan = new MergePlanner(
                config, SortMetrics.NO_OP, () -> -1).pipelineParallelism(4, catalog);

        assertThat(catalog.maxKeyLength()).isEqualTo(maximumKey.length());
        assertThat(plan.refBytes()).isEqualTo(refBytes).isGreaterThan(200);
        assertThat(plan.encoders()).isEqualTo(admittedEncoders);
        assertThat(plan.reason()).isEqualTo(MergePlanner.PipelineClampReason.HEAP_CLAMPED);
        assertThat(plan.clusterBudgetBytes()).isEqualTo(retainedPageBytes);
    }

    @Test
    void descriptorFloorRefusesBeforeOpeningAPipelineLane(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("fd-floor" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        int softLimit = MergeFdBudget.FD_HEADROOM + catalog.descriptors().size();
        MergePlanner planner = new MergePlanner(
                SortConfigs.base().withMergeBudgetBytes(64L << 20), metrics, () -> softLimit);

        assertThatThrownBy(() -> planner.pipelineParallelism(4, catalog))
                .isInstanceOf(MergeMemoryExhaustedException.class)
                .hasMessageContaining("minimum pipeline lane does not fit descriptor budget")
                .hasMessageContaining("reason=fd_exhausted");
        assertThat(metrics.count("SORT.pipeline_encoders_fd_clamped")).isEqualTo(1);
    }

    @Test
    void unboundedByteTargetClosesPlansAtTheReferenceCap(@TempDir Path root)
            throws IOException {
        int pages = MergePlanner.MAX_PIPELINE_PLAN_REFS + 1;
        List<List<ListEntry>> segment = new ArrayList<>(pages);
        for (int page = 0; page < pages; page++) {
            segment.add(List.of(SortTestSupport.object(String.format("key-%05d", page))));
        }
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortTransformResult result = runPages(root, List.of(segment), Long.MAX_VALUE,
                metrics, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                0, PageCodec.NONE, 1);

        assertThat(result.finalFiles()).hasSize(2);
        assertThat(metrics.count("SORT.pipeline_plan_ref_capped")).isEqualTo(1);
        assertThat(keys(result.finalFiles())).hasSize(pages);
    }

    @Test
    void mixedCatalogChargesTheLegacyWholePageCeiling(@TempDir Path root)
            throws IOException {
        Path legacy = SortTestSupport.writePageRun(
                root.resolve("legacy" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(SortTestSupport.object("a"), SortTestSupport.object("b")), comparator);
        Path current = SortTestSupport.writeIndexedPages(
                root.resolve("current" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("c"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(legacy, current),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        int mergeBudgetBytes = 64 << 20;
        MergePlanner planner = new MergePlanner(
                SortConfigs.base().withMergeBudgetBytes(mergeBudgetBytes),
                SortMetrics.NO_OP, () -> -1);

        MergePlanner.PipelinePlan plan = planner.pipelineParallelism(4, catalog);

        assertThat(catalog.maxRawPayloadLength()).isPositive().isLessThan(mergeBudgetBytes);
        assertThat(plan.retainedPageBytes()).isGreaterThan(plan.legacyDecodedLimit());
        assertThat(plan.encoders()).isEqualTo(1);
        assertThat(plan.cursorDepth()).isEqualTo(SegmentHeaderCursors.QUEUE_DEPTH);
        assertThat(plan.refBytes()).isEqualTo(PageRef.retainedBytes(catalog.maxKeyLength()));
    }

    @Test
    void cascadeFanInReservesEveryPipelineOutputDescriptor(@TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("fanin" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"))));
        PageRunCatalog catalog = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        SortConfig config = SortConfigs.base()
                .withFanIn(100)
                .withMergeBudgetBytes(64L << 20)
                .withMergePerStreamBytes(1);
        MergePlanner planner = new MergePlanner(config, SortMetrics.NO_OP,
                () -> MergeFdBudget.FD_HEADROOM + 5);

        assertThat(planner.serialFanIn(catalog)).isEqualTo(5);
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

        SortTransformResult result = runPages(root, List.of(pages), targetBytes,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                0, PageCodec.LZ4, 1);

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

        SortTransformResult result = runPages(root, segmentPages, 1, SortMetrics.NO_OP,
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
        SortTransformResult result = runPages(root, pages, 1,
                SortMetrics.NO_OP, reordered, 10_000, 64L << 20,
                0, PageCodec.NONE, 2);

        List<Integer> observed = List.copyOf(closeOrder);
        assertThat(observed.indexOf(4)).isLessThan(observed.indexOf(2));
        assertThat(result.finalFiles()).hasSizeGreaterThan(1);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
    }

    @Test
    void fourSharedChannelEncodersMatchSerialOutput(@TempDir Path root) throws IOException {
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
        SortTransformResult serial = runPages(root.resolve("serial"), pages, 4_096,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                0, PageCodec.NONE, 1);
        SortTransformResult concurrent = runPages(root.resolve("concurrent"), pages, 4_096,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                0, PageCodec.NONE, 4);

        assertThat(concurrent.finalFiles()).hasSameSizeAs(serial.finalFiles());
        for (int part = 0; part < serial.finalFiles().size(); part++) {
            assertThat(Files.mismatch(serial.finalFiles().get(part),
                    concurrent.finalFiles().get(part))).isEqualTo(-1L);
        }
        assertThat(concurrent.totalRows()).isEqualTo(serial.totalRows());
    }

    @Test
    void encoderProgressUsesTheSharedRowBatchGranularity(@TempDir Path root) throws IOException {
        List<ListEntry> rows = new ArrayList<>();
        for (int row = 0; row < 2_500; row++) {
            rows.add(SortTestSupport.object(String.format("key-%04d", row)));
        }
        List<Long> progress = new java.util.concurrent.CopyOnWriteArrayList<>();

        SortTransformResult result = runPages(root, List.of(List.of(rows)), Long.MAX_VALUE,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20,
                0, PageCodec.NONE, 1, progress::add);

        assertThat(result.totalRows()).isEqualTo(2_500);
        assertThat(progress).containsExactly(1_000L, 1_000L, 500L);
    }

    @Test
    void corruptReferencedPageAbortsWithoutTemporaryOutput(@TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Files.createDirectories(root.resolve("data"));
        Path segment = SortTestSupport.writeIndexedPages(
                staging.resolve("crc" + StagingNames.PAGE_RUN_SUFFIX),
                List.of(List.of(SortTestSupport.object("a"), SortTestSupport.object("b"))));
        PageRunSegmentIo.RoutingPage page;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
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

        SortTransformResult result = run(root, keys, 2_048, SortMetrics.NO_OP, measuring);

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
    void callerInterruptCancelsHeaderCursorsRouterAndEncodersWithoutPublishing(@TempDir Path root)
            throws Exception {
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch blockWriter = new CountDownLatch(1);
        SortedFileWriterFactory blocking = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    @Override
                    public void write(ListEntry entry) throws IOException {
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
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofPlatform().start(() -> {
            try {
                run(root, List.of(keys), Long.MAX_VALUE, SortMetrics.NO_OP, blocking);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        try {
            assertThat(writerStarted.await(60, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            assertThat(caller.join(Duration.ofSeconds(60))).isTrue();
        } finally {
            blockWriter.countDown();
        }
        assertThat(failure.get()).isInstanceOf(IOException.class)
                .hasMessageContaining("sort merge interrupted");
        assertNoPublishedOrTemporaryFiles(root);
    }

    private SortTransformResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics) throws IOException {
        return run(root, segmentKeys, finalFileBytes, metrics, SortedFileWriterFactory.DEFAULT);
    }

    private SortTransformResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory)
            throws IOException {
        return run(root, segmentKeys, finalFileBytes, metrics, writerFactory, 10_000, 64L << 20);
    }

    private SortTransformResult run(Path root, List<List<String>> segmentKeys,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes) throws IOException {
        List<List<List<ListEntry>>> segmentPages = segmentKeys.stream()
                .map(keys -> keys.stream()
                        .map(key -> List.<ListEntry>of(SortTestSupport.object(key))).toList())
                .toList();
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes);
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, 0, PageCodec.NONE);
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, long minParallelStagedBytes) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, minParallelStagedBytes, PageCodec.NONE, 4);
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, long minParallelStagedBytes, PageCodec codec)
            throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, minParallelStagedBytes, codec, 4);
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, long minParallelStagedBytes, PageCodec codec,
            int encoderCount) throws IOException {
        return runPages(root, segmentPages, finalFileBytes, metrics, writerFactory, fanIn,
                mergeBudgetBytes, minParallelStagedBytes, codec, encoderCount, ignored -> { });
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, long minParallelStagedBytes, PageCodec codec,
            int encoderCount, LongConsumer progressCallback) throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = new ArrayList<>();
        for (int segment = 0; segment < segmentPages.size(); segment++) {
            segments.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX),
                    segmentPages.get(segment), SortMode.OBJECTS, codec));
        }
        SortConfig config = SortConfigs.base()
                .withFinalization(SortFinalization.PIPELINE)
                .withMergeParallelism(encoderCount)
                .withMinParallelStagedBytes(minParallelStagedBytes)
                .withFanIn(fanIn)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withFinalFileBytes(finalFileBytes);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writerFactory,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                () -> -1, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortTransform(run).transform(segments, output, staging, PublishListener.NO_OP,
                progressCallback, FinalPassListener.NO_OP);
    }

    private SortTransformResult runPaths(Path root, List<Path> segments, long finalFileBytes,
            SortMetrics metrics, SortedFileWriterFactory writerFactory, int encoderCount)
            throws IOException {
        SortConfig config = SortConfigs.base()
                .withFinalization(SortFinalization.PIPELINE)
                .withMergeParallelism(encoderCount)
                .withMergeBudgetBytes(64L << 20)
                .withFinalFileBytes(finalFileBytes);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writerFactory, MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES,
                RangeMergeTimer.NO_OP, () -> -1, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortTransform(run).transform(segments, root.resolve("data"),
                root.resolve("_staging"), PublishListener.NO_OP, ignored -> { },
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
            try (SegmentReader reader = new SegmentReader(file)) {
                while (reader.hasNext()) {
                    keys.add(reader.next().key().asString());
                }
            }
        }
        return keys;
    }

}
