/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.model.ListEntry;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link LaneRouting}: with two lanes of one slot each and lane 0's writer wedged on its first
 * row, a third page from the same node has nowhere to go under sticky routing (head-of-line:
 * lane 1 idles) but goes straight to lane 1 under spill routing.
 */
class SharedDatasetWriterPoolRoutingTest {

    @Test
    void spillRoutingSendsAFullStickyLanesPageToTheIdleLane(@TempDir Path directory)
            throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        WedgeLaneZeroFormat format = new WedgeLaneZeroFormat(releaseWriter);
        SharedDatasetWriterPool pool = pool(directory, format, LaneRouting.SPILL);

        pool.submit(PageBatches.batch(0, 0, 0, 1));   // lane 0 dequeues it and wedges in write()
        format.writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 1, 2));   // both queues empty: the sticky lane 0 slot
        pool.submit(PageBatches.batch(0, 2, 2, 3));   // lane 0 full, lane 1 empty: routed to lane 1

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(pool.laneStatistics().get(1).batchesWritten())
                        .as("the third page was written by the idle lane, not queued behind the wedge")
                        .isEqualTo(1L));
        assertThat(pool.laneStatistics().get(0).submitBlockedCount())
                .as("no submit ever blocked on the full sticky lane")
                .isZero();

        releaseWriter.countDown();
        pool.close();
        assertThat(format.writeCount()).isEqualTo(3);
        assertThat(pool.laneStatistics().get(0).batchesWritten()).isEqualTo(2L);
    }

    @Test
    void stickyRoutingKeepsANodesPagesOnItsLaneEvenWhileAnotherLaneIdles(@TempDir Path directory)
            throws Exception {
        CountDownLatch releaseWriter = new CountDownLatch(1);
        WedgeLaneZeroFormat format = new WedgeLaneZeroFormat(releaseWriter);
        SharedDatasetWriterPool pool = pool(directory, format, LaneRouting.STICKY);

        pool.submit(PageBatches.batch(0, 0, 0, 1));
        format.writerEntered.await();
        pool.submit(PageBatches.batch(0, 1, 1, 2));
        Thread submitter = Thread.ofPlatform().start(() -> {
            try {
                pool.submit(PageBatches.batch(0, 2, 2, 3));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(pool.laneStatistics().get(0).headOfLineBlockedCount())
                        .as("the third page waits on lane 0 while lane 1 is idle")
                        .isEqualTo(1L));
        assertThat(pool.laneStatistics().get(1).batchesWritten()).isZero();

        releaseWriter.countDown();
        submitter.join(Duration.ofSeconds(2));
        pool.close();
        assertThat(format.writeCount()).isEqualTo(3);
        assertThat(pool.laneStatistics().get(0).batchesWritten()).isEqualTo(3L);
    }

    private static SharedDatasetWriterPool pool(Path directory, DatasetFormat format,
            LaneRouting routing) throws IOException {
        Files.createDirectories(directory);
        DatasetWriterPoolConfig config = new DatasetWriterPoolConfig(
                "test", "bucket", PartListener.NONE, List.of(), 0, 0, null, routing);
        return new SharedDatasetWriterPool(directory, format, "hash", 2, Long.MAX_VALUE, 1, config);
    }

    /** Lane 0's part writer blocks on its first row until released; lane 1 writes through. */
    private static final class WedgeLaneZeroFormat implements DatasetFormat {
        private final CountDownLatch releaseWriter;
        final CountDownLatch writerEntered = new CountDownLatch(1);
        private final AtomicInteger writes = new AtomicInteger();

        WedgeLaneZeroFormat(CountDownLatch releaseWriter) {
            this.releaseWriter = releaseWriter;
        }

        @Override public String partSuffix() { return ".test"; }
        @Override public String manifestFormat() { return "TEST"; }
        @Override public String manifestSchema() { return "key"; }

        @Override public DatasetPartWriter openPart(Path path) throws IOException {
            Files.createFile(path);
            boolean laneZero = path.getFileName().toString().startsWith("part-w0-");
            return new DatasetPartWriter() {
                private long rows;

                @Override public Path path() { return path; }
                @Override public long rows() { return rows; }
                @Override public long bufferedDataSize() { return rows; }

                @Override public void write(ListEntry entry) throws IOException {
                    writes.incrementAndGet();
                    if (rows == 0 && laneZero) {
                        writerEntered.countDown();
                        try {
                            releaseWriter.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("writer interrupted", e);
                        }
                    }
                    rows++;
                }

                @Override public void close() { }
                @Override public void discard() { }

                @Override public String md5() {
                    try {
                        return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest());
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }

                @Override public long digestNanos() { return 0L; }
            };
        }

        int writeCount() {
            return writes.get();
        }
    }
}
