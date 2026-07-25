/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pool-boundary defense-in-depth for the spin-storm defect: even though {@code ListCommand}
 * now rejects a too-small {@code --part-rotation-interval} at the CLI ({@code
 * MIN_PART_ROTATION_INTERVAL}), {@link
 * ParquetWriterPool}'s constructor is a public API — a caller (or a misconstructed {@code
 * ParquetSpec}) could still hand it an arbitrarily small positive {@code
 * rotationIntervalNanos}, which, unclamped, would make the idle-lane {@code
 * lane.queue.poll(rotationIntervalNanos, NANOSECONDS)} time out and re-loop immediately with
 * no work — a CPU wakeup storm. {@code ROTATION_POLL_FLOOR_NANOS} clamps the poll WAIT (never
 * the staleness decision in {@code shouldRotate()}, which still compares elapsed time against
 * the true, unclamped interval) to a 50 ms floor.
 *
 * <p>This test drives that clamp deterministically instead of racing on wall-clock timing or
 * counting wakeups (which would be flaky): the injected clock returns {@code 0} for the two
 * clock reads that happen synchronously inside {@code submit()}'s {@code writeBatch()} (the
 * part's open timestamp, then the immediate post-write staleness check — both see elapsed
 * {@code 0}, so neither rotates the 1 ns interval used here, which is the literal pathological
 * input, {@code PT0.000000001S}), then jumps far past that interval for every clock read after
 * that. The only remaining path that can observe the jump is the lane's
 * idle-timeout re-check, so the wall-clock time from {@code submit()} to that finalize IS the
 * lane's poll WAIT. Asserting a lower bound on it (well under the 50 ms floor, to leave CI
 * jitter headroom, but far above anything a spinning 1 ns poll could ever produce) proves the
 * floor is applied without a flaky exact-timing or exact-wakeup-count assertion, per the
 * "prefer a robust assertion over a flaky one" guidance for this unit.
 */
class RotationPollFloorTest {

    /** Comfortably below the 50 ms floor, but unreachable by a spinning near-zero poll (sub-ms). */
    private static final long LOWER_BOUND_MS = 30L;

    /** Same rationale as the sibling rotation-cadence tests: warm up parquet-mr/Hadoop classloading
     * off the timed critical section below, so it can't pad the measured wait either way. */
    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void idleTimeoutFinalizeNeverFiresFasterThanTheAntiSpinFloor(@TempDir Path dir) throws Exception {
        long pathologicalIntervalNanos = 1L;   // PT0.000000001S from the defect report
        long farFutureNanos = Duration.ofSeconds(1).toNanos();
        AtomicLong reads = new AtomicLong(0);
        // Read #1: openPart()'s partOpenedAtNanos. Read #2: writeBatch()'s inline staleness
        // check (still elapsed 0 -> no rotate). Read #3+: only reachable from the idle-timeout
        // re-check, once no more batches are coming.
        LongSupplier clock = () -> reads.getAndIncrement() < 2 ? 0L : farFutureNanos;

        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(pathologicalIntervalNanos), clock);

        long startNanos = System.nanoTime();
        pool.submit(batch(0, 0, 0, 10));   // opens the part; inline check sees elapsed 0, does not rotate
        await().atMost(Duration.ofSeconds(5))
                .until(() -> pool.committedPartCount() == 1);   // only the idle-timeout path can get here
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertThat(elapsedMs)
                .as("idle-timeout finalize fired after %dms; the anti-spin floor should have held the "
                        + "lane's poll wait near 50ms despite a 1ns configured interval", elapsedMs)
                .isGreaterThanOrEqualTo(LOWER_BOUND_MS);

        pool.close();
        assertThat(pool.committedPartCount()).isEqualTo(1);   // nothing left open; correctness unaffected
    }
}
