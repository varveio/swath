/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortLane}'s liveness and memory-corridor guarantees.
 *
 * <p>{@link SortLane#admit} must never hang forever: a bounded-timeout poll re-checks the
 * encoder's failure state and an in-flight {@link SortLane#abort()} while backpressured, instead of
 * blindly blocking on a full handoff queue with nothing left to wake it once the encoder dies.
 *
 * <p>Live sealed buffers (fill + off-thread) must never exceed {@code buffers()} — enforced by
 * releasing the off-thread capacity permit only after a segment's encode fully completes, not
 * merely once it is dequeued.
 */
final class SortLaneBackpressureTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    private static SortConfig buffers2Gate1() {
        // buffers=2 ⇒ offThreadCapacity=1: one buffer filling, at most ONE off-thread (queued or
        // encoding) at a time. segment-entries=1 ⇒ every single admitted entry seals its own buffer.
        return SortConfigs.base().withSegmentEntries(1);
    }

    @Test
    void encoderFailureWhileBackpressured_failsPromptlyInsteadOfHanging(@TempDir Path tmp) throws Exception {
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        CountDownLatch buffer1EncodingStarted = new CountDownLatch(1);
        CountDownLatch releaseBuffer1 = new CountDownLatch(1);

        SegmentSink failingSink = result -> {
            throw new IOException("injected: disk full");
        };

        SortLane lane = new SortLane(buffers2Gate1(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, staging, "seg-m1a", failingSink);
        lane.beforeEncodeHookForTesting = () -> {
            buffer1EncodingStarted.countDown();
            await(releaseBuffer1);
        };

        // Buffer 1 acquires the lane's only off-thread slot; the encoder immediately pauses in the hook.
        lane.admit(1L, page("a"));
        assertThat(buffer1EncodingStarted.await(5, TimeUnit.SECONDS)).as("encoder reached buffer 1").isTrue();

        // Buffer 2's seal now genuinely blocks: buffer 1 still holds the only off-thread slot.
        ExecutorService bg = Executors.newSingleThreadExecutor();
        try {
            Future<Exception> admit2 = bg.submit(() -> {
                try {
                    lane.admit(1L, page("b"));
                    return null;
                } catch (Exception e) {
                    return e;
                }
            });
            Thread.sleep(200);   // let the background thread genuinely enter the blocked acquire loop
            assertThat(admit2.isDone()).as("buffer 2's admit is backpressured, not yet returned").isFalse();

            // Release buffer 1: its (real) segment write completes, then the injected sink kills the
            // encoder. Under the OLD blind handoff.put(), buffer 2's thread would now hang forever —
            // nothing ever wakes a blocked queue.put() once the sole consumer has died.
            releaseBuffer1.countDown();

            Exception observed = assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> admit2.get(5, TimeUnit.SECONDS));
            assertThat(observed).as("the backpressured admit must fail, not hang, once the encoder dies")
                    .isNotNull();
            assertThat(observed.getMessage() + " / " + observed.getCause())
                    .contains("disk full");
        } finally {
            bg.shutdownNow();
        }
    }

    @Test
    void abortUnblocksAConcurrentlyBlockedSeal(@TempDir Path tmp) throws Exception {
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        CountDownLatch buffer1EncodingStarted = new CountDownLatch(1);
        CountDownLatch releaseBuffer1 = new CountDownLatch(1);   // deliberately never released by the test

        SortLane lane = new SortLane(buffers2Gate1(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, staging, "seg-m1b", SegmentSink.NONE);
        lane.beforeEncodeHookForTesting = () -> {
            buffer1EncodingStarted.countDown();
            await(releaseBuffer1);
        };

        lane.admit(1L, page("a"));
        assertThat(buffer1EncodingStarted.await(5, TimeUnit.SECONDS)).isTrue();

        ExecutorService bg = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> admit2 = bg.submit(() -> {
                try {
                    lane.admit(1L, page("b"));
                    return true;    // dropped cleanly by abort(), no exception
                } catch (Exception e) {
                    return false;   // also acceptable: unblocked via a thrown failure
                }
            });
            Thread.sleep(200);
            assertThat(admit2.isDone()).as("buffer 2 genuinely backpressured before abort()").isFalse();

            // abort() must unblock the concurrently-blocked seal — it must not require the
            // (never-releasing) encoder to finish on its own.
            assertTimeoutPreemptively(Duration.ofSeconds(5), lane::abort);
            Boolean returned = assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> admit2.get(5, TimeUnit.SECONDS));
            assertThat(returned).as("admit(buffer 2) returned instead of hanging after a concurrent abort()")
                    .isNotNull();
        } finally {
            bg.shutdownNow();
        }
    }

    @Test
    void liveOffThreadBuffersNeverExceedBuffersMinusOne(@TempDir Path tmp) throws Exception {
        // buffers=3 ⇒ offThreadCapacity=2: fill(1) + off-thread(<=2) <= 3 total live.
        SortConfig config = SortConfigs.base().withSegmentEntries(1).withBuffers(3);
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        List<SegmentResult> finalized = Collections.synchronizedList(new ArrayList<>());

        SortLane lane = new SortLane(config, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, staging, "seg-m2", finalized::add);
        // A small artificial per-segment delay so the drain thread (sealing one buffer per admit,
        // gate=1) races ahead of the encoder and genuinely overlaps — the scenario the live-buffer bound must handle.
        lane.beforeEncodeHookForTesting = () -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        for (String k : new String[]{"a", "b", "c", "d", "e", "f"}) {
            lane.admit(1L, page(k));
        }
        lane.close();

        assertThat(finalized).hasSize(6);
        assertThat(lane.offThreadCapacity()).isEqualTo(2);
        assertThat(lane.maxObservedOffThreadBuffers())
                .as("live off-thread sealed buffers must never exceed buffers()-1 ("
                        + "fill + off-thread <= buffers())")
                .isLessThanOrEqualTo(2);
    }

    /**
     * The peak in-flight staging-bytes/handoff-queue-depth/off-thread-buffer-count gauges must
     * reflect the MAX concurrently-live state, not merely the latest single reading — i.e. while
     * buffer 1 is off-thread (sealed, unfinalized, still "live") and buffer 2 is concurrently
     * filling, the staging-bytes peak must be at least the SUM of both buffers' bytes, not just
     * buffer 2's alone.
     */
    @Test
    void peakGaugesReflectConcurrentBufferingNotJustTheLatestReading(@TempDir Path tmp) throws Exception {
        Path staging = Files.createDirectories(tmp.resolve("_staging"));
        CountDownLatch buffer1EncodingStarted = new CountDownLatch(1);
        CountDownLatch releaseBuffer1 = new CountDownLatch(1);

        AtomicLong peakBytesObserved = new AtomicLong();
        AtomicInteger peakQueueDepthObserved = new AtomicInteger();
        AtomicInteger peakOffThreadObserved = new AtomicInteger();

        SortLaneMeters meters = new SortLaneMeters() {
            @Override
            public void stagingBytesLive(long liveBytes) {
                peakBytesObserved.updateAndGet(prev -> Math.max(prev, liveBytes));
            }

            @Override
            public void handoffQueueDepth(int depth) {
                peakQueueDepthObserved.updateAndGet(prev -> Math.max(prev, depth));
            }

            @Override
            public void offThreadBuffersLive(int live) {
                peakOffThreadObserved.updateAndGet(prev -> Math.max(prev, live));
            }
        };

        SortLane lane = new SortLane(buffers2Gate1(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                meters, staging, "seg-peak", SegmentSink.NONE);
        lane.beforeEncodeHookForTesting = () -> {
            buffer1EncodingStarted.countDown();
            await(releaseBuffer1);
        };

        // Buffer 1: admitted, sealed, handed off; its encode immediately pauses in the hook, holding
        // the lane's only off-thread slot (its bytes stay "live" — not yet finalized).
        lane.admit(1L, page("a"));
        long peakAfterBufferOneAlone = peakBytesObserved.get();
        assertThat(peakAfterBufferOneAlone).as("buffer 1's bytes must be observed at least once").isPositive();
        assertThat(buffer1EncodingStarted.await(5, TimeUnit.SECONDS)).as("encoder reached buffer 1").isTrue();

        // Buffer 2's admit accumulates its own bytes (observable) BEFORE its seal blocks on the
        // (still-held-by-buffer-1) off-thread permit — mirrors the backpressure scenario above.
        ExecutorService bg = Executors.newSingleThreadExecutor();
        try {
            Future<?> admit2 = bg.submit(() -> {
                try {
                    lane.admit(1L, page("bb"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(200);   // let buffer 2's admit() accumulate its bytes before it blocks on seal()
            assertThat(admit2.isDone()).as("buffer 2's admit is backpressured, not yet returned").isFalse();

            assertThat(peakBytesObserved.get())
                    .as("peak staging bytes must reflect BOTH buffer 1 (still off-thread, unfinalized) "
                            + "and buffer 2 (filling) simultaneously live, not just the latest single buffer")
                    .isGreaterThan(peakAfterBufferOneAlone);

            releaseBuffer1.countDown();
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> admit2.get(5, TimeUnit.SECONDS));
            lane.close();
        } finally {
            bg.shutdownNow();
        }

        assertThat(peakOffThreadObserved.get())
                .as("off-thread capacity is 1 (buffers=2) — peak concurrently-live off-thread buffers "
                        + "must never exceed it")
                .isEqualTo(1);
        assertThat(peakQueueDepthObserved.get())
                .as("the off-thread semaphore gates entry to the handoff queue — its peak depth must "
                        + "never exceed the off-thread capacity either")
                .isLessThanOrEqualTo(lane.offThreadCapacity());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<ListEntry> page(String key) {
        return List.of(SortTestSupport.object(key));
    }
}
