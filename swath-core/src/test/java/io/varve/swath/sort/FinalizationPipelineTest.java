/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = new ArrayList<>();
        for (int segment = 0; segment < segmentKeys.size(); segment++) {
            List<List<ListEntry>> pages = segmentKeys.get(segment).stream()
                    .map(key -> List.<ListEntry>of(SortTestSupport.object(key))).toList();
            segments.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), pages));
        }
        SortConfig config = SortConfigs.base()
                .withFinalization(SortFinalization.PIPELINE)
                .withMergeParallelism(3)
                .withMergeBudgetBytes(64L << 20)
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
