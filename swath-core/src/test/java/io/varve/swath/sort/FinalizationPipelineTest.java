/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FinalizationPipelineTest {
    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void readerSlotDepthIsClampedByTheDecodedPageBudget() {
        assertThat(SegmentReaderSlots.slotDepth(8, 8L * 1024, 1024)).isEqualTo(1);
        assertThat(SegmentReaderSlots.slotDepth(8, 24L * 1024, 1024)).isEqualTo(3);
        assertThat(SegmentReaderSlots.slotDepth(8, Long.MAX_VALUE, 1024)).isEqualTo(4);
        assertThat(SegmentReaderSlots.slotDepth(0, 1, 1)).isEqualTo(1);
    }

    @Test
    void calibratedPartTargetUsesCompletedEncodedToLogicalRatio() {
        PipelinePartSizer sizer = new PipelinePartSizer(
                PipelinePartSizer.Policy.CALIBRATED_BYTES, 100, 1);

        assertThat(sizer.calibratedLogicalTarget()).isEqualTo(400);
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
    void moreReaderSlotsThanDecodePermitsCompleteAtDepthOne(@TempDir Path root) throws IOException {
        int cores = Runtime.getRuntime().availableProcessors();
        int segmentCount = Math.multiplyExact(2, cores);
        Path staging = Files.createDirectories(root.resolve("reader-staging"));
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
        PageRunCatalog catalog = PageRunCatalog.preflight(segments,
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(ignored -> { }));
        long budgetBytes = Math.multiplyExact(
                (long) segmentCount, catalog.maxRawPayloadLength());
        assertThat(SegmentReaderSlots.slotDepth(
                segmentCount, budgetBytes, catalog.maxRawPayloadLength())).isEqualTo(1);

        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            PipelineFailure failure = new PipelineFailure();
            try (SegmentReaderSlots readers = new SegmentReaderSlots(
                    catalog, budgetBytes, SortMetrics.NO_OP, failure)) {
                for (int segment = 0; segment < segmentCount; segment++) {
                    assertThat(readers.next(segment)).isNotNull();
                }
            }
        });
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
    void overlapClusterBeyondFormerPageCapCompletesWithinDecodedBudget(@TempDir Path root)
            throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<String>> segments = java.util.stream.IntStream.range(0, 65)
                .mapToObj(ignored -> List.of("same-key")).toList();

        SortTransformResult result = run(root, segments, Long.MAX_VALUE, metrics);

        assertThat(keys(result.finalFiles())).hasSize(65).containsOnly("same-key");
        assertThat(metrics.pipelineClusterPages.sum()).isEqualTo(65);
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
    void pipelineEncoderCountUsesTheSharedParallelismClamp(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<List<List<ListEntry>>> pages = List.of(
                List.of(List.of(SortTestSupport.object("a"))));

        SortTransformResult result = runPages(root, pages, Long.MAX_VALUE, metrics,
                SortedFileWriterFactory.DEFAULT, 10_000, 64L << 20, Long.MAX_VALUE);

        assertThat(result.finalizationParallelism()).isEqualTo(1);
        assertThat(metrics.count("SORT.pipeline_encoder_below_staged_floor")).isEqualTo(1);
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
    void equalKeyGroupCrossingRouterBatchBoundaryStaysInOnePart(@TempDir Path root)
            throws IOException {
        List<ListEntry> firstPage = new ArrayList<>(MergeRouter.BATCH_ROWS + 1);
        for (int row = 0; row < MergeRouter.BATCH_ROWS + 1; row++) {
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
                .hasSize(MergeRouter.BATCH_ROWS + 2).containsOnly("a");
        assertThat(keys(List.of(result.finalFiles().getLast()))).containsExactly("b");
    }

    @Test
    void assemblerUsesMergeOrdinalsWhenEncodersFinishOutOfOrder(@TempDir Path root)
            throws IOException {
        CountDownLatch secondPartClosed = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> closeOrder = new ConcurrentLinkedQueue<>();
        SortedFileWriterFactory reordered = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    private final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public void write(ListEntry entry) throws IOException {
                        if (index == 1) {
                            try {
                                if (!secondPartClosed.await(10, TimeUnit.SECONDS)) {
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
                            if (index == 2) {
                                secondPartClosed.countDown();
                            }
                        }
                    }
                };

        SortTransformResult result = run(root,
                List.of(List.of("a", "b", "c", "d", "e", "f")), 1,
                SortMetrics.NO_OP, reordered);

        List<Integer> observed = List.copyOf(closeOrder);
        assertThat(observed.indexOf(2)).isLessThan(observed.indexOf(1));
        assertThat(result.finalFiles()).hasSizeGreaterThan(1);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
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
    void callerInterruptCancelsReadersRouterAndEncodersWithoutPublishing(@TempDir Path root)
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

        assertThat(writerStarted.await(10, TimeUnit.SECONDS)).isTrue();
        caller.interrupt();
        caller.join(10_000);
        blockWriter.countDown();

        assertThat(caller.isAlive()).isFalse();
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
                mergeBudgetBytes, 0);
    }

    private SortTransformResult runPages(Path root, List<List<List<ListEntry>>> segmentPages,
            long finalFileBytes, SortMetrics metrics, SortedFileWriterFactory writerFactory,
            int fanIn, long mergeBudgetBytes, long minParallelStagedBytes) throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = new ArrayList<>();
        for (int segment = 0; segment < segmentPages.size(); segment++) {
            segments.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX),
                    segmentPages.get(segment)));
        }
        SortConfig config = SortConfigs.base()
                .withFinalization(SortFinalization.PIPELINE)
                .withMergeParallelism(3)
                .withMinParallelStagedBytes(minParallelStagedBytes)
                .withFanIn(fanIn)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withFinalFileBytes(finalFileBytes);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                metrics, writerFactory,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortTransform(run).transform(segments, output, staging, PublishListener.NO_OP,
                units -> { }, FinalPassListener.NO_OP);
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
