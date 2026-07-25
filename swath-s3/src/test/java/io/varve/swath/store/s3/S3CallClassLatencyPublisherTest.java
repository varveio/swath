/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;

/**
 * {@link S3CallClassLatencyPublisher} extracts {@link HttpMetric#CONCURRENCY_ACQUIRE_DURATION}
 * and {@link CoreMetric#TIME_TO_FIRST_BYTE} into the thread-local {@link
 * S3CallClassLatencyPublisher.PhaseCapture} started by {@link S3CallClassLatencyPublisher#begin()}.
 *
 * <p>Same hand-built {@link MetricCollector} tree idiom as {@code S3PoolMetricPublisherTest} — the
 * SDK only populates these fields on a real HTTP-client attempt, so these tests assert the
 * extraction logic directly, independent of a real request.
 */
class S3CallClassLatencyPublisherTest {

    @Test
    void beginWithNoPublishLeavesCaptureUnobserved() {
        S3CallClassLatencyPublisher.PhaseCapture capture = S3CallClassLatencyPublisher.begin();
        try {
            assertThat(capture.connectAcquireNanos()).isEqualTo(-1L);
            assertThat(capture.timeToFirstByteNanos()).isEqualTo(-1L);
        } finally {
            S3CallClassLatencyPublisher.end();
        }
    }

    @Test
    void publishFillsTheCaptureStartedOnThisThread() {
        S3CallClassLatencyPublisher.PhaseCapture capture = S3CallClassLatencyPublisher.begin();
        try {
            MetricCollector root = MetricCollector.create("ApiCall");
            MetricCollector attempt = root.createChild("ApiCallAttempt");
            attempt.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(12));
            attempt.reportMetric(CoreMetric.TIME_TO_FIRST_BYTE, Duration.ofMillis(45));

            new S3CallClassLatencyPublisher().publish(root.collect());

            assertThat(capture.connectAcquireNanos()).isEqualTo(Duration.ofMillis(12).toNanos());
            assertThat(capture.timeToFirstByteNanos()).isEqualTo(Duration.ofMillis(45).toNanos());
        } finally {
            S3CallClassLatencyPublisher.end();
        }
    }

    @Test
    void extractsFromNestedGrandchild() {
        S3CallClassLatencyPublisher.PhaseCapture capture = S3CallClassLatencyPublisher.begin();
        try {
            MetricCollector root = MetricCollector.create("ApiCall");
            MetricCollector attempt = root.createChild("ApiCallAttempt");
            MetricCollector httpClient = attempt.createChild("HttpClient");
            httpClient.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(3));

            new S3CallClassLatencyPublisher().publish(root.collect());

            assertThat(capture.connectAcquireNanos()).isEqualTo(Duration.ofMillis(3).toNanos());
            // TIME_TO_FIRST_BYTE never reported anywhere -- stays the -1 unobserved sentinel.
            assertThat(capture.timeToFirstByteNanos()).isEqualTo(-1L);
        } finally {
            S3CallClassLatencyPublisher.end();
        }
    }

    @Test
    void publishWithNoBeginIsANoOp() {
        // No begin() on this thread -- publish must not throw or leak into another thread's capture.
        MetricCollector root = MetricCollector.create("ApiCall");
        root.createChild("ApiCallAttempt").reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(9));
        new S3CallClassLatencyPublisher().publish(root.collect());
    }

    @Test
    void endClearsTheCaptureForANewBegin() {
        S3CallClassLatencyPublisher.PhaseCapture first = S3CallClassLatencyPublisher.begin();
        MetricCollector root = MetricCollector.create("ApiCall");
        root.createChild("ApiCallAttempt").reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(1));
        new S3CallClassLatencyPublisher().publish(root.collect());
        assertThat(first.connectAcquireNanos()).isEqualTo(Duration.ofMillis(1).toNanos());
        S3CallClassLatencyPublisher.end();

        // A publish() firing after end() (e.g. a stray async callback) must not mutate the stale capture.
        MetricCollector root2 = MetricCollector.create("ApiCall");
        root2.createChild("ApiCallAttempt").reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(99));
        new S3CallClassLatencyPublisher().publish(root2.collect());
        assertThat(first.connectAcquireNanos())
                .as("the FIRST capture is untouched once end() cleared the thread-local")
                .isEqualTo(Duration.ofMillis(1).toNanos());
    }

    @Test
    void closeIsANoOp() {
        new S3CallClassLatencyPublisher().close();
    }
}
