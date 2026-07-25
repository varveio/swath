/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.model.PageBatch;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rotation-trigger mechanism: a lane rotates its open part on <b>any</b> of
 * {size, row count, time-open}, reusing {@link ParquetWriterPool}'s existing
 * finalize/rotate path — only the OR condition is new, and that is what
 * these tests pin down. An injected {@link LongSupplier} clock (the
 * package-private constructor seam) makes the time trigger deterministic,
 * no real sleeping.
 */
class ParquetRotationCadenceTest {

    private static PageBatch emptyBatch(long nodeId, long seq) {
        return new PageBatch(nodeId, seq, List.of());
    }

    /**
     * Constructing the first {@link PartWriter} in the JVM is slow (parquet-mr/Hadoop
     * classloading + ZSTD codec init, ~100ms+) — slow enough that a test racing a "part
     * opened" signal against that construction can observe the file on disk before the
     * lane has finished recording {@code partOpenedAtNanos}. Warm the classes here, off
     * the timed critical section, so the later per-test opens are effectively instant and
     * the clock-ordering in the tests below is deterministic.
     */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void timeIntervalRotatesTheOpenNonEmptyPartOnceElapsed(@TempDir Path dir) throws Exception {
        AtomicLong clock = new AtomicLong(0);
        long intervalNanos = Duration.ofSeconds(10).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos), clock::get);

        pool.submit(batch(0, 0, 0, 10));   // opens part-w0-00000 (partOpenedAtNanos captured at clock=0)
        // Wait for the lane to have actually opened+written the part before advancing the clock, so
        // partOpenedAtNanos is pinned at 0 (warmed up above, so this settles near-instantly).
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));
        Thread.sleep(50);   // let writeBatch()'s shouldRotate() check (still reading clock=0) finish too

        clock.addAndGet(intervalNanos + 1);               // interval elapsed, but nothing observes it yet
        assertThat(pool.committedPartCount()).isZero();   // no background poke — still 0 until a write occurs

        pool.submit(batch(0, 1, 10, 11));                 // next write on the lane evaluates the trigger
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);

        pool.submit(batch(0, 2, 11, 12));                 // rotation opened a fresh part — this lands in it
        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(2);   // part #0 (time-rotated) + the fresh part, closed
    }

    @Test
    void maxRowsRotatesTheOpenPartWithinASingleBatch(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(5L));

        pool.submit(batch(0, 0, 0, 10));   // one batch, 10 rows >= maxRows(5) → rotates inline
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);

        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(1);   // nothing left open after the rotated part
    }

    @Test
    void idleLaneWithZeroRowsNeverProducesAnEmptyPartFromTheTimeTrigger(@TempDir Path dir) throws Exception {
        AtomicLong clock = new AtomicLong(0);
        long intervalNanos = Duration.ofSeconds(10).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos), clock::get);

        // Every batch is empty (a page with nothing to write, e.g. fully filtered) — the lane's
        // open part (once opened) always has 0 rows. Advance the clock well past the interval
        // between each one; if the empty-part guard were missing this would storm-finalize.
        for (int i = 0; i < 5; i++) {
            pool.submit(emptyBatch(0, i));
            Thread.sleep(50);   // let the lane drain this one before the next
            clock.addAndGet(intervalNanos * 2);
        }
        Thread.sleep(200);   // final settle
        assertThat(pool.committedPartCount()).isZero();   // no empty parts, despite repeated elapsed time

        pool.close();   // clean close still finalizes the (empty) open part — unrelated to this trigger
        assertThat(pool.committedPartCount()).isEqualTo(1);
    }

    @Test
    void sizeTriggerStillRotatesWithBothNewTriggersDisabled(@TempDir Path dir) throws Exception {
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, 64 * 1024, 8,
                ParquetWriterPoolConfig.DEFAULT);

        for (int p = 0; p < 20; p++) {
            pool.submit(batch(0, p, p * 1000, p * 1000 + 1000));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> pool.committedPartCount() > 1);

        pool.close();
        assertThat(pool.committedPartCount()).isGreaterThan(1);
    }
}
