/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.sort.stage.SortLane;

/**
 * First-class-meter seam for the {@link SortLane}. Keeps {@code io.varve.swath.sort} free of a
 * Micrometer dependency: the pipeline injects an adapter over the live {@code RunMetrics},
 * and every unit test that constructs a lane without metrics uses {@link #NO_OP}. Null-safe by
 * construction — the default methods do nothing, so the lane's hot path never branches on {@code
 * null}.
 *
 * <p>Maps to the {@code swath.sort.*} first-class meters: {@code entriesAccepted} → {@code
 * swath.sort.entries}; {@code segmentFinalized} → {@code swath.sort.segments.written} + {@code
 * swath.sort.segment.bytes} (+ the {@code page_runs_per_buffer} classification signal); {@code
 * backpressureWaited} → {@code swath.sort.backpressure.wait} (the accepted trade: the listing
 * thread blocks handing off to a busy encoder rather than growing memory unbounded).
 *
 * <p><b>In-flight staging legibility (instrumentation only — no admit/seal/backpressure
 * behavior change).</b> {@code stagingBytesLive}/{@code handoffQueueDepth}/{@code
 * offThreadBuffersLive} are "live" (instantaneous) readings reported at each observation point;
 * the pipeline's {@code RunMetrics} adapter folds each into a CAS-max high-water mark and exposes
 * it as a {@code swath.sort.*.peak} gauge (mirrors {@code RunMetrics.incrementInFlight}'s existing
 * peak idiom). Read the three peaks together to tell bounded (linear-in-{@code T}) memory from an
 * unbounded leak — see {@code docs/metrics-and-observability.md} for the full read; {@code
 * handoff.queue.depth.peak} is additionally bounded above by {@code buffers()-1} since the
 * off-thread semaphore already gates entry to that queue, so a peak above it is itself a bug
 * signal.
 */
public interface SortLaneMeters {

    /** Null object: records nothing. */
    SortLaneMeters NO_OP = new SortLaneMeters() {
    };

    /** Entries admitted into the fill buffer this page ({@code swath.sort.entries}). */
    default void entriesAccepted(long entries) {
    }

    /**
     * A sealed buffer was encoded + finalized as a staging segment ({@code swath.sort.segments.written}
     * / {@code swath.sort.segment.bytes}); {@code pageRuns} is how many per-node page runs the buffer
     * held (the {@code page_runs_per_buffer} classification signal).
     */
    default void segmentFinalized(long bytes, int pageRuns) {
    }

    /** Nanoseconds the listing (drain) thread waited to hand a sealed buffer to the encoder. */
    default void backpressureWaited(long nanos) {
    }

    /**
     * Total live (admitted-but-not-yet-durable) staging-buffer bytes across the lane, right
     * after an admit accumulated more into the currently-filling buffer — includes the fill buffer
     * plus any sealed buffer(s) still queued/encoding off-thread. Feeds {@code
     * swath.sort.staging.bytes.peak}.
     */
    default void stagingBytesLive(long liveBytes) {
    }

    /**
     * The {@link SortLane} handoff queue's depth immediately after a seal handed a buffer
     * off. Feeds {@code swath.sort.handoff.queue.depth.peak}.
     */
    default void handoffQueueDepth(int depth) {
    }

    /**
     * Concurrently-live off-thread (queued + actively encoding) sealed-buffer count, relative
     * to the configured {@code buffers()-1} bound. Feeds {@code swath.sort.off_thread.buffers.peak}.
     */
    default void offThreadBuffersLive(int live) {
    }
}
