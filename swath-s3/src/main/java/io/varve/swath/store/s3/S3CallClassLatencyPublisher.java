/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.time.Duration;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

/**
 * Bridges the AWS SDK's per-attempt {@link HttpMetric#CONCURRENCY_ACQUIRE_DURATION} (the
 * Apache {@code ApacheHttpClient} connection-pool checkout wait) and {@link
 * CoreMetric#TIME_TO_FIRST_BYTE} (request start through first response byte) into a per-thread
 * {@link PhaseCapture} that {@link S3PageFetcher} reads back <b>synchronously</b> right after its
 * {@code listObjectsV2} call returns.
 *
 * <p><b>Why this is safe (no race).</b> {@code S3PoolMetricsLocalStackIT} already proves — against a
 * REAL sync {@code ApacheHttpClient} call, not a hand-built fixture — that {@link
 * S3PoolMetricPublisher#publish} has already run by the time a synchronous {@code
 * s3.listObjectsV2(...)} call returns to the caller (its gauge is asserted non-{@code NaN}
 * immediately after, with no wait). This publisher relies on that SAME synchronous-publish
 * guarantee: {@link #begin()} is called on the calling (worker/thief/seed) thread immediately before
 * the SDK call, {@link #publish} fires on that same thread before the call returns, and {@link
 * #end()} clears the thread-local — {@code S3PageFetcher} calls it from an OUTER {@code finally} that
 * wraps the whole SDK-call attempt (not just the typed-exception catch arm), so it fires exactly once
 * on every exit path, including a non-{@code SdkException} {@code RuntimeException}/{@code Error}
 * escaping the call (e.g. from an interceptor). Each worker/thief/seed thread issues its own SDK
 * calls strictly sequentially (never split across threads), so the {@link ThreadLocal} always
 * correlates {@link #publish} back to the request that thread is CURRENTLY making, never a
 * concurrent sibling's.
 *
 * <p><b>Not thief-exclusive.</b> Despite "worker/thief" above, the seed step ({@code
 * SeedStep}) issues its own {@code delimiter=/} structure probes through this SAME fetcher, on its own
 * thread, before any thief exists — this publisher observes and captures those calls identically (it
 * has no notion of call class at all, only raw SDK metrics); {@code S3PageFetcher#callClass}
 * downstream classifies them the same as a thief structure probe (see its javadoc).
 *
 * <p><b>Attached ALONGSIDE, never instead of, {@link S3PoolMetricPublisher}.</b> The AWS SDK invokes
 * every registered client-level {@link MetricPublisher} for a given attempt — attaching this one via
 * a SECOND {@code overrideBuilder.addMetricPublisher(...)} call (see {@code S3ClientFactory}) does
 * not disturb the existing {@code swath.s3.pool.*} gauge feed. (Multiple client-level publishers are
 * unambiguous; the "publishers replace, don't add" behavior the SDK documents is specific to a
 * PER-REQUEST override list vs the client-level one, which this class deliberately avoids using —
 * a request-level override was considered and rejected for exactly that risk.)
 *
 * <p><b>Best-effort.</b> Either metric may be absent on a given attempt (the SDK reports what its
 * HTTP client/pipeline actually measured for that call) — {@link PhaseCapture} exposes {@code -1} for
 * an unobserved phase, the same "unavailable, don't fabricate a zero" sentinel the rest of {@code
 * RunMetrics} uses.
 */
public final class S3CallClassLatencyPublisher implements MetricPublisher {

    /** Mutable per-request capture handed back to {@link S3PageFetcher}; {@code -1} = not observed. */
    public static final class PhaseCapture {
        private volatile long connectAcquireNanos = -1L;
        private volatile long timeToFirstByteNanos = -1L;

        /** The Apache pool connection-checkout wait, in nanos; {@code -1} if the SDK didn't report it. */
        public long connectAcquireNanos() {
            return connectAcquireNanos;
        }

        /** Request start through first response byte, in nanos; {@code -1} if the SDK didn't report it. */
        public long timeToFirstByteNanos() {
            return timeToFirstByteNanos;
        }
    }

    private static final ThreadLocal<PhaseCapture> CURRENT = new ThreadLocal<>();

    /**
     * Begin capturing for the calling thread's in-flight request. MUST be paired with {@link #end()}
     * in a {@code finally} block.
     *
     * @return the capture to read back after the SDK call returns (success or exception)
     */
    public static PhaseCapture begin() {
        PhaseCapture capture = new PhaseCapture();
        CURRENT.set(capture);
        return capture;
    }

    /** Clears the calling thread's in-flight capture; MUST be called in a {@code finally} after {@link #begin()}. */
    public static void end() {
        CURRENT.remove();
    }

    @Override
    public void publish(MetricCollection metricCollection) {
        PhaseCapture capture = CURRENT.get();
        if (capture == null) {
            return;   // no in-flight capture on this thread (e.g. a call issued off the tracked path)
        }
        MetricCollections.walk(metricCollection, c -> apply(c, capture));
    }

    private void apply(MetricCollection collection, PhaseCapture capture) {
        Duration acquire = MetricCollections.last(collection, HttpMetric.CONCURRENCY_ACQUIRE_DURATION);
        if (acquire != null) {
            capture.connectAcquireNanos = acquire.toNanos();
        }
        Duration ttfb = MetricCollections.last(collection, CoreMetric.TIME_TO_FIRST_BYTE);
        if (ttfb != null) {
            capture.timeToFirstByteNanos = ttfb.toNanos();
        }
    }

    @Override
    public void close() {
        // No resources to release -- capture state lives in the ThreadLocal, cleared by end().
    }
}
