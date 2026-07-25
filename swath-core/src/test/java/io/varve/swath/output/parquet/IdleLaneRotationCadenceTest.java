/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Idle-lane rotation cadence: a lane that stops
 * receiving batches must still finalize a stale, non-empty open part on the
 * time cadence — the check-on-write path alone ({@link ParquetRotationCadenceTest})
 * never re-evaluates a lane with nothing new to write. {@link ParquetWriterPool}
 * fixes this by having the lane's own consumer thread wake on a poll timeout
 * (instead of blocking forever on {@code queue.take()}) and re-run the same
 * rotation check with no new concurrency. These tests exercise that wake-up
 * mechanism directly, without ever calling {@link ParquetWriterPool#close()} —
 * close() finalizing everything would mask the idle-path bug entirely.
 */
class IdleLaneRotationCadenceTest {

    /**
     * Same rationale as {@link ParquetRotationCadenceTest#warmupParquetWriterClasses}: the
     * first {@link PartWriter} construction in the JVM (parquet-mr/Hadoop classloading + ZSTD
     * codec init) is slow enough to blow past a short test interval on its own, which would
     * make the idle-wake assertions below pass for the wrong reason (or flake). Warm it up
     * here, off each test's timed critical section, with the interval trigger disabled.
     */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void idleNonEmptyLaneFinalizesOnTheTimeCadenceWithoutAFurtherBatchOrClose(@TempDir Path dir) throws Exception {
        long intervalNanos = Duration.ofMillis(200).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos));

        pool.submit(batch(0, 0, 0, 10));   // opens part-w0-00000, then the lane goes idle: no more batches
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));

        // No further submit(), no close() — the ONLY thing that can finalize this part is the
        // lane's own idle wake-up. Bounded poll-until-condition (never a fixed sleep-then-assert).
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> pool.committedPartCount() == 1);
        assertThat(pool.committedBytes()).isGreaterThan(0);

        // The lane is still alive and correct after an idle-triggered rotation: a fresh batch
        // opens a new part, which the final close() finalizes as usual.
        pool.submit(batch(0, 1, 10, 11));
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00001.parquet")));
        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(2);
    }

    @Test
    void idleEmptyLaneNeverProducesAPartFromTheTimeoutWakeup(@TempDir Path dir) throws Exception {
        long intervalNanos = Duration.ofMillis(100).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos));

        // Never submit anything: the lane repeatedly times out on an EMPTY lane
        // (lane.current == null) — the timeout wake-up must not fabricate a part.
        Thread.sleep(intervalNanos / 1_000_000 * 5);   // several idle wake-ups' worth of real time
        assertThat(pool.committedPartCount()).isZero();

        pool.close();   // clean close of a genuinely-empty lane: still nothing to finalize
        assertThat(pool.committedPartCount()).isZero();
    }
}
