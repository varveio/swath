/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.PartListener;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
    void interruptedLaneCloseStillDiscardsItsOpenPart(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, Mode.OPEN_THEN_WAIT, new CountDownLatch(0));
        SharedDatasetWriterPool pool = fixture.pool();
        pool.submit(PageBatches.batch(0, 0, 0, 1));
        fixture.format().writerEntered.await();

        pool.interruptLanesForTest();
        assertThatThrownBy(pool::close)
                .hasMessageContaining("writer failed")
                .hasRootCauseInstanceOf(InterruptedException.class);
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
        Files.createDirectories(directory);
        BlockingFormat format = new BlockingFormat(mode, releaseWriter);
        DatasetWriterPoolConfig config = new DatasetWriterPoolConfig(
                "test", "bucket", PartListener.NONE, List.of(), 0, 0, null);
        SharedDatasetWriterPool pool = new SharedDatasetWriterPool(directory, format, "hash", 1,
                Long.MAX_VALUE, 1, config);
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
        WRITE_FAIL, WRITE_AND_DISCARD_FAIL, FINALIZE_FAIL, BLOCK_FIRST_WRITE, OPEN_THEN_WAIT
    }

    private record Fixture(SharedDatasetWriterPool pool, BlockingFormat format) {
    }

    private static final class BlockingFormat implements DatasetFormat {
        private final Mode mode;
        private final CountDownLatch releaseWriter;
        private final CountDownLatch writerEntered = new CountDownLatch(1);

        BlockingFormat(Mode mode, CountDownLatch releaseWriter) {
            this.mode = mode;
            this.releaseWriter = releaseWriter;
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
                    if (mode == Mode.BLOCK_FIRST_WRITE && rows == 0) {
                        writerEntered.countDown();
                        awaitRelease();
                    }
                    rows++;
                    if (mode == Mode.OPEN_THEN_WAIT) {
                        writerEntered.countDown();
                    }
                }

                @Override public void close() throws IOException {
                    if (mode == Mode.FINALIZE_FAIL) {
                        throw new IOException("finalize failed");
                    }
                }

                @Override public void discard() throws IOException {
                    if (mode == Mode.WRITE_AND_DISCARD_FAIL) {
                        throw new IOException("discard failed");
                    }
                }

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
