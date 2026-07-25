/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exact-boundary coverage for {@code ParquetWriterPool#shouldRotate}'s two cadence
 * triggers, guarding the inclusive {@code >=} semantics that {@link
 * ParquetRotationCadenceTest}'s sibling tests don't pin down precisely:
 *
 * <ul>
 *   <li>{@code maxRowsRotatesTheOpenPartWithinASingleBatch} submits all 10 rows in
 *       ONE batch against {@code maxRows=5} — {@code shouldRotate()} is only evaluated
 *       once, AFTER all 10 rows are already written, so that test would pass identically
 *       whether the trigger were {@code rows >= maxRows} (correct) or the off-by-one
 *       {@code rows > maxRows} (wrong, since 10 &gt; 5 either way).</li>
 *   <li>{@code timeIntervalRotatesTheOpenNonEmptyPartOnceElapsed} advances the clock to
 *       {@code intervalNanos + 1}, one nanosecond past the boundary, so it likewise can't
 *       distinguish {@code >=} from {@code >}.</li>
 * </ul>
 *
 * <p>These tests submit rows/clock advances ONE AT A TIME so {@code shouldRotate()} is
 * evaluated exactly at {@code threshold - 1} (must not fire) and then exactly at {@code
 * threshold} (must fire), and additionally verify the finalized part's actual row count
 * to prove no overshoot happened either.
 */
class RotationTriggerBoundaryTest {

    /** Same rationale as the sibling cadence tests: warm up parquet-mr/Hadoop classloading
     * off the timed critical section below. */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void rowCountRotatesInclusivelyAtExactlyMaxRowsNotBefore(@TempDir Path dir) throws Exception {
        long maxRows = 5;
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationMaxRows(maxRows));

        // One row per submit ⇒ shouldRotate() is evaluated after every single row.
        for (int i = 0; i < maxRows - 1; i++) {
            pool.submit(batch(0, i, i, i + 1));
        }
        Thread.sleep(150);   // settle: give the lane every chance to have wrongly rotated already
        assertThat(pool.committedPartCount())
                .as("rows == maxRows-1 (%d) must NOT rotate", maxRows - 1).isZero();

        pool.submit(batch(0, maxRows - 1, (int) maxRows - 1, (int) maxRows));   // the maxRows-th row
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);

        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(1);
        List<String> keys = ParquetReads.keys(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet"));
        assertThat(keys).as("the rotated part holds exactly maxRows rows — the inclusive boundary, not an overshoot")
                .hasSize((int) maxRows);
    }

    @Test
    void intervalRotatesInclusivelyAtExactlyTheIntervalNotBefore(@TempDir Path dir) throws Exception {
        AtomicLong clock = new AtomicLong(0);
        long intervalNanos = Duration.ofSeconds(10).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos), clock::get);

        pool.submit(batch(0, 0, 0, 1));   // opens the part at clock=0 (partOpenedAtNanos pinned at 0)
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));
        Thread.sleep(150);   // let writeBatch()'s inline shouldRotate() check (still reading clock=0) finish

        clock.set(intervalNanos - 1);   // one nanosecond short of the interval
        pool.submit(batch(0, 1, 1, 2));   // next write evaluates the trigger at elapsed == interval-1
        Thread.sleep(150);
        assertThat(pool.committedPartCount())
                .as("elapsed == intervalNanos-1 must NOT rotate").isZero();

        clock.set(intervalNanos);   // exactly at the interval
        pool.submit(batch(0, 2, 2, 3));   // next write evaluates the trigger at elapsed == interval
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.committedPartCount() == 1);

        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(1);
        List<String> keys = ParquetReads.keys(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet"));
        assertThat(keys).as("exactly the 3 rows written up to and including the interval boundary")
                .hasSize(3);
    }
}
