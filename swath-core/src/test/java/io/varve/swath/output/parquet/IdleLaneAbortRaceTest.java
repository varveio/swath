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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Idle-lane + concurrent {@link ParquetWriterPool#abort()}: what
 * happens when {@code abort()} races an idle lane holding an open, non-empty part whose
 * rotation interval has ALREADY elapsed — i.e. the part is "due" for an idle-timeout
 * finalize, but the lane's own thread has not yet woken from its {@code poll()} wait
 * and re-run {@code shouldRotate()}.
 *
 * <p><b>The contract</b> (class javadoc on {@link ParquetWriterPool}, I6):
 * "on {@code abort()} ... each lane discards its open part ... so a resumed run re-lists
 * those rows exactly once — only naturally rotated, finalized parts survive." Read
 * literally, "naturally rotated" means {@code finalizeCurrent()} actually ran to
 * completion on the lane's own thread BEFORE {@code aborting} was observed — merely being
 * ELIGIBLE (the interval has elapsed) does not count, because {@code runLane}'s idle-wait
 * branch only fires {@code shouldRotate()} on a poll <b>timeout</b>; a concurrent {@code
 * abort()} delivers the POISON sentinel first, which the lane observes as a normal (non-
 * timeout) queue item and takes straight to the discard path in the {@code finally} block,
 * never re-checking staleness. This test proves that is exactly what happens: no
 * corruption, no hang, and the eligible-but-unchecked part is discarded, not raced in as a
 * late finalize. It is deterministic (an injected clock, no real waiting) because {@code
 * abort()} enqueues its POISON and returns almost immediately, always far ahead of the
 * lane's real-time poll wait (set to seconds here specifically so it can never fire on its
 * own during the test).
 */
class IdleLaneAbortRaceTest {

    @BeforeEach
    void warmupParquetWriterClasses(@TempDir Path warmupDir) throws Exception {
        ParquetPoolTestSupport.warmupParquetWriterClasses(warmupDir);
    }

    @Test
    void abortDiscardsAnIdleOpenPartEvenIfItsIntervalHasAlreadyElapsed(@TempDir Path dir) throws Exception {
        AtomicLong clock = new AtomicLong(0);
        // Long real-time interval: the lane's poll() wait is derived from this value
        // literally in nanoseconds, so as long as abort() below runs well under 5 real
        // seconds (trivially true), it always wins the race against the lane's own timeout.
        long intervalNanos = Duration.ofSeconds(5).toNanos();
        var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "hash", 1, Long.MAX_VALUE, 8,
                ParquetWriterPoolConfig.DEFAULT.withRotationIntervalNanos(intervalNanos), clock::get);

        pool.submit(batch(0, 0, 0, 10));   // opens a non-empty part at clock=0; the lane then goes idle
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Files.exists(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet")));
        Thread.sleep(150);   // let the inline post-write shouldRotate() check (still reading clock=0) settle

        // The part is now ELIGIBLE for an idle-timeout finalize (interval elapsed on the
        // injected clock) — but the lane's own thread hasn't woken up to re-check yet.
        clock.addAndGet(intervalNanos + 1);

        pool.abort();   // must complete promptly (no deadlock), racing ahead of any idle re-check

        assertThat(pool.committedPartCount())
                .as("the eligible-but-not-yet-rechecked part must NOT be finalized by abort() — only a "
                        + "completed finalizeCurrent() counts as 'naturally rotated' per the pool's contract")
                .isZero();
        assertThat(DatasetLayout.of(dir).dataFile("part-w0-00000.parquet"))
                .as("the open part is discarded on abort, not left as an orphaned partial file")
                .doesNotExist();
        assertThat(DatasetLayout.of(dir).manifest())
                .as("no manifest is written blessing a failed run with nothing durable")
                .doesNotExist();
    }
}
