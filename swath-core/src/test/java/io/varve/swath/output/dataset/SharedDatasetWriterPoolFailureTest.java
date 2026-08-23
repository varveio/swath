/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.PartListener;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SharedDatasetWriterPoolFailureTest {

    @Test
    void writerFailureDrainsAndDeletesThePartialPart(@TempDir Path directory) throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.WRITE_FAIL, releaseWriter);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();
        releaseWriter.countDown();

        assertThatThrownBy(pool::close)
                .hasMessageContaining("writer failed")
                .hasRootCauseMessage("write failed");
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void finalizeFailureIsSurfacedAndDeletesTheUnpublishablePart(@TempDir Path directory)
            throws Exception {
        SharedDatasetWriterPool pool = fixture(
                directory, Mode.FINALIZE_FAIL, new CountDownLatch(0)).pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));

        assertThatThrownBy(pool::close)
                .hasMessageContaining("writer failed")
                .hasRootCauseMessage("finalize failed");
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void interruptedClosePreservesDiscardFailureAndDeletesItsOpenPart(@TempDir Path directory)
            throws Exception {
        Fixture fixture = fixture(directory, Mode.OPEN_THEN_DISCARD_FAIL, new CountDownLatch(0));
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();

        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        Thread closer = Thread.ofPlatform().start(() -> {
            Thread.currentThread().interrupt();
            try {
                pool.close();
                result.set(new AssertionError("interrupted close unexpectedly completed"));
            } catch (Throwable failure) {
                result.set(failure);
                restoredInterrupt.set(Thread.currentThread().isInterrupted());
            }
        });
        closer.join();

        Throwable closeFailure = result.get();
        assertThat(closeFailure)
                .hasMessageContaining("interrupted closing")
                .hasRootCauseInstanceOf(InterruptedException.class);
        assertThat(closeFailure.getSuppressed()).anySatisfy(laneFailure -> {
            assertThat(laneFailure).isInstanceOf(InterruptedException.class);
            assertThat(laneFailure.getSuppressed())
                    .extracting(Throwable::getMessage)
                    .contains("discard failed");
        });
        assertThat(restoredInterrupt.get()).isTrue();
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void discardCleanupFailureIsSuppressedOntoTheStoredWriterFailure(
            @TempDir Path directory) throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.WRITE_AND_DISCARD_FAIL, releaseWriter);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();
        releaseWriter.countDown();

        assertThatThrownBy(pool::close)
                .hasRootCauseMessage("write failed")
                .satisfies(failure -> assertThat(failure.getCause().getSuppressed())
                        .extracting(Throwable::getMessage)
                        .contains("discard failed"));
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void saturatedLaneReleasesBlockedProducerAsWriterAdvancesThenAbortCleansUp(
            @TempDir Path directory)
            throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.BLOCK_FIRST_WRITE, releaseWriter);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 1, 2));

        CountDownLatch producerStarted = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var blockedProducer = executor.submit(() -> {
                producerStarted.countDown();
                pool.submit(PageBatches.batch(0, 2, 2, 3));
                return null;
            });
            producerStarted.await();
            assertThat(blockedProducer.isDone()).isFalse();

            releaseWriter.countDown();
            blockedProducer.get();
            pool.abort();
        }
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void globallyBusyLanesDoNotMasqueradeAsHeadOfLineBlocking(
            @TempDir Path directory) throws Exception {
        CountDownLatch releaseWriters = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.BLOCK_EVERY_FIRST_WRITE, releaseWriters, 2, 1);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        pool.submit(PageBatches.batch(1, 0, 1, 2));
        fixture.format().writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 2, 3));
        pool.submit(PageBatches.batch(1, 1, 3, 4));

        CountDownLatch producerStarted = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var blockedProducer = executor.submit(() -> {
                producerStarted.countDown();
                pool.submit(PageBatches.batch(0, 2, 4, 5));
                return null;
            });
            producerStarted.await();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                SharedDatasetWriterPool.LaneStatistics lane = pool.laneStatistics().get(0);
                assertThat(lane.submitBlockedCount()).isEqualTo(1L);
                assertThat(lane.headOfLineBlockedCount()).isZero();
            });

            releaseWriters.countDown();
            blockedProducer.get();
            pool.abort();
        }

        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void saturatedStickyLaneRecordsHeadOfLineBlockingWhileAnotherLaneWaitsForWork(
            @TempDir Path directory) throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.BLOCK_FIRST_WRITE, releaseWriter, 2, 1);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 1, 2));
        await().atMost(Duration.ofSeconds(2))
                .until(() -> pool.laneStatistics().get(1).waitingForWork());

        CountDownLatch producerStarted = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var blockedProducer = executor.submit(() -> {
                producerStarted.countDown();
                pool.submit(PageBatches.batch(0, 2, 2, 3));
                return null;
            });
            producerStarted.await();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                List<SharedDatasetWriterPool.LaneStatistics> lanes = pool.laneStatistics();
                assertThat(lanes.get(0).submitBlockedCount()).isEqualTo(1L);
                assertThat(lanes.get(0).submitBlockedNanos()).isPositive();
                assertThat(lanes.get(0).headOfLineBlockedCount()).isEqualTo(1L);
                assertThat(lanes.get(0).headOfLineBlockedNanos()).isPositive();
                assertThat(lanes.get(1).queueDepth()).isZero();
            });

            releaseWriter.countDown();
            blockedProducer.get();
            pool.abort();
        }

        List<SharedDatasetWriterPool.LaneStatistics> lanes = pool.laneStatistics();
        assertThat(lanes.get(0).queueDepthPeak()).isEqualTo(1);
        assertThat(lanes.get(0).submitBlockedNanos()).isPositive();
        assertThat(lanes.get(0).headOfLineBlockedNanos()).isPositive();
        assertThat(lanes.get(1).submitBlockedCount()).isZero();
        assertUnpublishedAndEmpty(directory);
    }

    @Test
    void laneStatisticsAttributeSuccessfulRowsBatchesAndFinalization(
            @TempDir Path directory) throws Exception {
        SharedDatasetWriterPool pool = fixture(
                directory, Mode.PASS_THROUGH, new CountDownLatch(0), 2, 8).pool();
        pool.submit(PageBatches.batch(0, 0, 0, 3));
        pool.submit(PageBatches.batch(1, 0, 3, 8));
        pool.close();

        List<SharedDatasetWriterPool.LaneStatistics> lanes = pool.laneStatistics();
        assertThat(lanes).extracting(SharedDatasetWriterPool.LaneStatistics::rowsWritten)
                .containsExactly(3L, 5L);
        assertThat(lanes).extracting(SharedDatasetWriterPool.LaneStatistics::batchesWritten)
                .containsExactly(1L, 1L);
        assertThat(lanes).extracting(SharedDatasetWriterPool.LaneStatistics::partsFinalized)
                .containsExactly(1L, 1L);
        assertThat(lanes).extracting(SharedDatasetWriterPool.LaneStatistics::finalizeCount)
                .containsExactly(1L, 1L);
        assertThat(lanes).extracting(SharedDatasetWriterPool.LaneStatistics::queueDepth)
                .containsOnly(0);
    }

    @Test
    void interruptPromptlyCancelsSubmitBlockedOnSaturatedQueueAndCallerCanRestoreStatus(
            @TempDir Path directory) throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        Fixture fixture = fixture(directory, Mode.BLOCK_FIRST_WRITE, releaseWriter);
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 1, 2));

        CountDownLatch submitStarted = new CountDownLatch(1);
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        Thread producer = Thread.ofPlatform().start(() -> {
            submitStarted.countDown();
            try {
                pool.submit(PageBatches.batch(0, 2, 2, 3));
                result.set(new AssertionError("saturated submit unexpectedly completed"));
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
                restoredInterrupt.set(Thread.currentThread().isInterrupted());
                result.set(expected);
            } catch (Throwable failure) {
                result.set(failure);
            }
        });
        submitStarted.await();
        boolean observedBlocked = awaitState(producer, Thread.State.WAITING, Duration.ofSeconds(2));
        producer.interrupt();
        boolean promptlyReleased = producer.join(Duration.ofSeconds(2));

        releaseWriter.countDown();
        pool.abort();
        producer.join();
        assertThat(observedBlocked).as("producer reached the saturated queue wait").isTrue();
        assertThat(promptlyReleased).as("interrupt releases the saturated put promptly").isTrue();
        assertThat(result.get()).isInstanceOf(InterruptedException.class);
        assertThat(restoredInterrupt.get()).isTrue();
        SharedDatasetWriterPool.LaneStatistics lane = pool.laneStatistics().get(0);
        assertThat(lane.submitBlockedCount()).isEqualTo(1L);
        assertThat(lane.submitBlockedNanos()).isPositive();
        assertUnpublishedAndEmpty(directory);
    }

    private static boolean awaitState(Thread thread, Thread.State state, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (thread.isAlive() && thread.getState() != state && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread.getState() == state;
    }

    private static Fixture fixture(Path directory, Mode mode, CountDownLatch releaseWriter)
            throws IOException {
        return fixture(directory, mode, releaseWriter, 1, 1);
    }

    private static Fixture fixture(Path directory, Mode mode, CountDownLatch releaseWriter,
            int writers, int queueCapacity) throws IOException {
        Files.createDirectories(directory);
        BlockingFormat format = new BlockingFormat(mode, releaseWriter);
        DatasetWriterPoolConfig config = new DatasetWriterPoolConfig(
                "test", "bucket", PartListener.NONE, List.of(), 0, 0, null);
        SharedDatasetWriterPool pool = new SharedDatasetWriterPool(directory, format, "hash", writers,
                Long.MAX_VALUE, queueCapacity, config);
        return new Fixture(pool, format);
    }

    private static void assertUnpublishedAndEmpty(Path directory) throws IOException {
        DatasetLayout layout = DatasetLayout.of(directory);
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.state()).doesNotExist();
        assertThat(layout.success()).doesNotExist();
        assertThat(layout.dataParts(".test")).isEmpty();
    }

    private enum Mode {
        PASS_THROUGH, WRITE_FAIL, WRITE_AND_DISCARD_FAIL, FINALIZE_FAIL, BLOCK_FIRST_WRITE,
        BLOCK_EVERY_FIRST_WRITE, OPEN_THEN_DISCARD_FAIL
    }

    private record Fixture(SharedDatasetWriterPool pool, BlockingFormat format) {
    }

    private static final class BlockingFormat implements DatasetFormat {
        private final Mode mode;
        private final CountDownLatch releaseWriter;
        private final CountDownLatch writerEntered;

        BlockingFormat(Mode mode, CountDownLatch releaseWriter) {
            this.mode = mode;
            this.releaseWriter = releaseWriter;
            this.writerEntered = new CountDownLatch(mode == Mode.BLOCK_EVERY_FIRST_WRITE ? 2 : 1);
        }

        @Override public String partSuffix() { return ".test"; }
        @Override public String manifestFormat() { return "TEST"; }
        @Override public String manifestSchema() { return "key"; }

        @Override public DatasetPartWriter openPart(Path path) throws IOException {
            Files.createFile(path);
            return new DatasetPartWriter() {
                private long rows;

                @Override public Path path() { return path; }
                @Override public long rows() { return rows; }
                @Override public long bufferedDataSize() { return rows; }

                @Override public void write(ListEntry entry) throws IOException {
                    if (mode == Mode.WRITE_FAIL || mode == Mode.WRITE_AND_DISCARD_FAIL) {
                        writerEntered.countDown();
                        awaitRelease();
                        throw new IOException("write failed");
                    }
                    if (rows == 0 && (mode == Mode.BLOCK_EVERY_FIRST_WRITE
                            || (mode == Mode.BLOCK_FIRST_WRITE
                                    && path.getFileName().toString().startsWith("part-w0-")))) {
                        writerEntered.countDown();
                        awaitRelease();
                    }
                    rows++;
                    if (mode == Mode.OPEN_THEN_DISCARD_FAIL) {
                        writerEntered.countDown();
                    }
                }

                @Override public void close() throws IOException {
                    if (mode == Mode.FINALIZE_FAIL) {
                        throw new IOException("finalize failed");
                    }
                }

                @Override public void discard() throws IOException {
                    if (mode == Mode.WRITE_AND_DISCARD_FAIL
                            || mode == Mode.OPEN_THEN_DISCARD_FAIL) {
                        throw new IOException("discard failed");
                    }
                }

                @Override public String md5() {
                    try {
                        return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest());
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }

                @Override public long digestNanos() { return 0L; }

                private void awaitRelease() throws IOException {
                    try {
                        releaseWriter.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("writer interrupted", e);
                    }
                }
            };
        }
    }
}
