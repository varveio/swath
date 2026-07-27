/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.runtime.RunContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Parquet writer pool: rotation-trigger attribution
 * ({@code swath.parquet.rotation{trigger}}), finalize/discard outcome counters
 * ({@code swath.parquet.parts{outcome}}), footer-fsync latency
 * ({@code swath.parquet.finalize.latency}), and the lanes' own encode/write span
 * ({@code swath.parquet.write.latency}). Uses the package-private
 * clock+metrics test seam so the time trigger is deterministic.
 */
class ParquetWriterPoolMetricsTest {

    /** Warm parquet-mr/Hadoop classloading off the timed critical section (see ParquetRotationCadenceTest). */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void sizeTriggerRotationRecordsCounterAndFinalizeLatency(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, 64 * 1024, 8,
                ParquetWriterPoolConfig.DEFAULT.withMetrics(ctx.metrics()));

        for (int p = 0; p < 20; p++) {
            pool.submit(batch(0, p, p * 1000, p * 1000 + 1000));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> pool.committedPartCount() > 1);
        pool.close();

        MeterRegistry registry = ctx.meterRegistry();
        assertThat(registry.get("swath.parquet.rotation").tag("trigger", "size").counter().count())
                .isGreaterThan(0.0);
        assertThat(registry.get("swath.parquet.parts").tag("outcome", "finalized").counter().count())
                .isEqualTo((double) pool.committedPartCount());
        assertThat(registry.get("swath.parquet.finalize.latency").timer().count())
                .isEqualTo(pool.committedPartCount());
    }

    @Test
    void maxRowsRotationRecordsRowsReason(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(5L).withMetrics(ctx.metrics()));

        pool.submit(batch(0, 0, 0, 10));   // one batch, 10 rows >= maxRows(5) → rotates inline
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);
        pool.close();

        assertThat(ctx.meterRegistry().get("swath.parquet.rotation").tag("trigger", "rows").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void timeTriggerRotationRecordsTimeReason(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        AtomicLong clock = new AtomicLong(0);
        long intervalNanos = Duration.ofSeconds(10).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos).withMetrics(ctx.metrics()),
                clock::get);

        pool.submit(batch(0, 0, 0, 10));   // opens part-w0-00000 (partOpenedAtNanos captured at clock=0)
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));
        Thread.sleep(50);   // let writeBatch()'s rotationReason() check (still reading clock=0) finish too
        clock.addAndGet(intervalNanos + 1);
        pool.submit(batch(0, 1, 10, 11));   // next write on the lane evaluates the time trigger
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);
        pool.close();

        assertThat(ctx.meterRegistry().get("swath.parquet.rotation").tag("trigger", "time").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void abortDiscardsRecordsDiscardedOutcome(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withMetrics(ctx.metrics()));
        pool.submit(batch(0, 0, 0, 1000));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));   // the lane has opened a part

        pool.abort();

        assertThat(ctx.meterRegistry().get("swath.parquet.parts").tag("outcome", "discarded").counter().count())
                .isEqualTo(1.0);
        assertThat(ctx.meterRegistry().find("swath.parquet.parts").tag("outcome", "finalized").counter()).isNull();
    }

    @Test
    void finalizeFailureRecordsFinalizeFailedAndNeitherFinalizedNorDiscarded(@TempDir Path dir) throws Exception {
        // Force PartWriter.close()'s footer-fsync FileChannel.open(..., WRITE) to fail by
        // revoking the already-created part file's permissions out from under it -- the writer's
        // own already-open output-stream handle is unaffected (POSIX permission checks apply only
        // at open() time, not to already-open descriptors), so the write itself still lands; only
        // the SECOND, explicit open for fsync (the one finalizeCurrent's IOException catch guards)
        // fails.
        //
        // A ROW-COUNT trigger -- not the clock-driven time trigger this test used to arm the fault
        // with -- makes the fault window deterministic. Whether rotation fires is then a pure
        // function of how many rows THIS lane thread has itself written from the ordered queue;
        // nothing the test thread does (revoking permissions, submitting the next batch) can move
        // that number. The old clock-based design raced: under full-suite load, the lane thread
        // could still be evaluating the FIRST (under-threshold) batch's rotationReason() by the
        // time the test thread had already bumped the injected clock and revoked permissions -- so
        // the FIRST batch's own rotation check (now reading the already-bumped clock) could fire
        // the fault before the SECOND, intended batch ever ran, recording the failure into the
        // pool's shared state before the test's own second submit() call -- which then threw the
        // already-recorded failure straight out of `pool.submit()` (via `checkFailure()`) instead
        // of surfacing where this test asserts it (#37).
        RunContext ctx = RunContext.create();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(5L).withMetrics(ctx.metrics()));
        Path partPath = DatasetLayout.of(dir).dataFile("part-w0-00000.parquet");

        pool.submit(batch(0, 0, 0, 3));   // opens part-w0-00000; 3 rows < maxRows(5) -> never rotates on its own
        await().atMost(Duration.ofSeconds(5)).until(() -> Files.exists(partPath));
        Files.setPosixFilePermissions(partPath, Set.of());   // revoke access for any NEW open() of this file
        pool.submit(batch(0, 1, 3, 6));   // cumulative rows=6 >= maxRows(5) -> rotationReason() fires ROWS -> finalizeCurrent() -> close() fails

        try {
            MeterRegistry registry = ctx.meterRegistry();
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> registry.find("swath.parquet.parts").tag("outcome", "finalize_failed").counter() != null
                            && registry.get("swath.parquet.parts").tag("outcome", "finalize_failed").counter().count() >= 1.0);

            assertThat(registry.get("swath.parquet.parts").tag("outcome", "finalize_failed").counter().count())
                    .isEqualTo(1.0);
            assertThat(registry.find("swath.parquet.parts").tag("outcome", "finalized").counter()).isNull();
            assertThat(registry.find("swath.parquet.parts").tag("outcome", "discarded").counter()).isNull();
        } finally {
            pool.abort();   // the pool is already in a failed state -- abort() (not close()) shuts it down cleanly
        }
    }

    /**
     * The lanes' own encode/write work is a client-service-cost span in its own right
     * ({@code swath.parquet.write.latency} → {@code client_cost[].parquet_write}). Without it a
     * Parquet run's measured client cost is the pool DISPATCH alone — what the consumer stage's
     * {@code emit} span sees, a rounding error next to the encode the lanes then do — so summed
     * spans cannot account for the process CPU a Parquet run actually burns.
     */
    @Test
    void laneEncodeWorkIsRecordedAsItsOwnClientCostSpan(@TempDir Path dir) throws Exception {
        RunContext ctx = RunContext.create();
        // One lane, no rotation trigger reachable: exactly one lane-work stretch per batch, plus
        // the drain-time finalize close() runs on the lane thread.
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withMetrics(ctx.metrics()));

        for (int p = 0; p < 3; p++) {
            pool.submit(batch(0, p, p * 1000, p * 1000 + 1000));
        }
        pool.close();

        MeterRegistry registry = ctx.meterRegistry();
        Timer laneWork = registry.get("swath.parquet.write.latency").timer();
        assertThat(laneWork.count()).as("one per batch written, plus the drain-time finalize").isEqualTo(4L);
        assertThat(laneWork.totalTime(TimeUnit.NANOSECONDS))
                .as("the encode/write the pool actually did, not the dispatch that handed it over")
                .isPositive();
        // Nested, not additive: a finalize happens INSIDE the lane-work stretch that triggered it,
        // so the footer-fsync timer can never exceed the lane-work total.
        assertThat(laneWork.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(
                registry.get("swath.parquet.finalize.latency").timer().totalTime(TimeUnit.NANOSECONDS));
        assertThat(ctx.metrics().summary(Duration.ofSeconds(1), "work_stealing", 0L, 0L).clientCost())
                .as("readable from the run summary's client_cost[], where the accounting is done")
                .extracting(RunSummary.ClientCostSpan::span)
                .contains(RunMetrics.CLIENT_COST_SPAN_PARQUET_WRITE);
    }

    @Test
    void noMetricsAttached_poolIsNullSafe(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, 64 * 1024, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(5L));
        pool.submit(batch(0, 0, 0, 10));
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);
        pool.close();
    }
}
