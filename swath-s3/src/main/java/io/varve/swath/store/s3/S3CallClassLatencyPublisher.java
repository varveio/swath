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
 * Apache {@code ApacheHttpClient} connection-pool checkout wait), {@link
 * CoreMetric#TIME_TO_FIRST_BYTE} (request start through first response byte) and the {@link
 * CoreMetric#TIME_TO_LAST_BYTE}-minus-{@code TIME_TO_FIRST_BYTE} response-handling window (see
 * {@link PhaseCapture#sdkUnmarshalNanos()}) into a per-thread {@link PhaseCapture} that {@link
 * S3PageFetcher} reads back <b>synchronously</b> right after its {@code listObjectsV2} call returns.
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
 * <p><b>Best-effort.</b> Any of the three metrics may be absent on a given attempt (the SDK reports
 * what its HTTP client/pipeline actually measured for that call) — {@link PhaseCapture} exposes
 * {@code -1} for an unobserved phase, the same "unavailable, don't fabricate a zero" sentinel the
 * rest of {@code RunMetrics} uses.
 */
public final class S3CallClassLatencyPublisher implements MetricPublisher {

    /** Mutable per-request capture handed back to {@link S3PageFetcher}; {@code -1} = not observed. */
    public static final class PhaseCapture {
        private volatile long connectAcquireNanos = -1L;
        private volatile long timeToFirstByteNanos = -1L;
        private volatile long sdkUnmarshalNanos = -1L;

        /** The Apache pool connection-checkout wait, in nanos; {@code -1} if the SDK didn't report it. */
        public long connectAcquireNanos() {
            return connectAcquireNanos;
        }

        /** Request start through first response byte, in nanos; {@code -1} if the SDK didn't report it. */
        public long timeToFirstByteNanos() {
            return timeToFirstByteNanos;
        }

        /**
         * The SDK's response-handling window — first response byte through the SDK's protocol
         * response handler returning — in nanos; {@code -1} if it could not be derived.
         *
         * <p><b>Derived, not read.</b> {@link CoreMetric#UNMARSHALLING_DURATION} would be the exact
         * boundary, but this SDK version does <b>not</b> report it for S3 {@code ListObjectsV2} at
         * all (proven empirically; the metric never appears in the published collection tree — see
         * {@code S3SdkUnmarshalPhaseLocalStackIT}). What it does report is {@link
         * CoreMetric#TIME_TO_LAST_BYTE}, and on the SYNC path that stamp is taken in the SDK's
         * {@code HandleResponseStage} <b>after</b> the response handler has returned — so {@code
         * TIME_TO_LAST_BYTE - TIME_TO_FIRST_BYTE} is the response-handling window: draining the
         * remaining response body off the socket (the handler parses straight from the live stream)
         * plus the XML parse and POJO construction. Not pure client CPU, and not a true unmarshal
         * span — a close upper bound on one.
         *
         * <p>It stops before the SDK's response-INTERCEPTOR chain (S3's always-on {@code
         * encoding-type=url} percent-decode, which rebuilds the response object), so that part
         * stays inside the total-minus-TTFB residual.
         *
         * <p>Derived per {@link MetricCollection}, so both stamps always come from the SAME attempt;
         * a negative difference (two stamps that disagree) is rejected rather than clamped, leaving
         * the unobserved sentinel.
         */
        public long sdkUnmarshalNanos() {
            return sdkUnmarshalNanos;
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
        Duration ttlb = MetricCollections.last(collection, CoreMetric.TIME_TO_LAST_BYTE);
        if (ttfb != null && ttlb != null) {
            // Both stamps read off THIS collection, so they are always the same attempt's -- see
            // PhaseCapture#sdkUnmarshalNanos for why the window is derived rather than read.
            long windowNanos = ttlb.toNanos() - ttfb.toNanos();
            if (windowNanos >= 0L) {
                capture.sdkUnmarshalNanos = windowNanos;
            }
        }
    }

    @Override
    public void close() {
        // No resources to release -- capture state lives in the ThreadLocal, cleared by end().
    }
}
