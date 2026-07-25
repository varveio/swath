/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;

/**
 * §3.8: {@link S3PoolMetricPublisher} extracts the SDK's {@link HttpMetric} connection-pool
 * fields (whichever collection level they land at) into {@link RunMetrics}'s {@code
 * swath.s3.pool.*} gauges.
 *
 * <p>The SDK only populates these {@code HttpMetric} values on a real HTTP-client attempt — a
 * unit-constructed {@link MetricCollector} tree never has them filled in "for free". These tests
 * therefore hand-build the collection the SDK would produce (a top-level {@code ApiCall}
 * collector with an attempt-level child reporting the pool metrics) and assert the publisher's
 * extraction logic directly, independent of a real request.
 */
class S3PoolMetricPublisherTest {

    private static RunMetrics newMetrics() {
        return new RunMetrics(new SimpleMeterRegistry());
    }

    private static double gaugeValue(RunMetrics metrics, String name) {
        Gauge gauge = metrics.registry().find(name).gauge();
        assertThat(gauge).as("gauge %s registered", name).isNotNull();
        return gauge.value();
    }

    @Test
    void extractsAllFourMetricsFromAttemptLevelChild() {
        RunMetrics metrics = newMetrics();
        MetricCollector root = MetricCollector.create("ApiCall");
        MetricCollector attempt = root.createChild("ApiCallAttempt");
        attempt.reportMetric(HttpMetric.LEASED_CONCURRENCY, 12);
        attempt.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 68);
        attempt.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 3);
        attempt.reportMetric(HttpMetric.MAX_CONCURRENCY, 80);

        new S3PoolMetricPublisher(metrics).publish(root.collect());

        assertThat(gaugeValue(metrics, "swath.s3.pool.leased")).isEqualTo(12.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available")).isEqualTo(68.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition")).isEqualTo(3.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.max")).isEqualTo(80.0);
    }

    @Test
    void extractsFromNestedGrandchild() {
        RunMetrics metrics = newMetrics();
        MetricCollector root = MetricCollector.create("ApiCall");
        MetricCollector attempt = root.createChild("ApiCallAttempt");
        MetricCollector httpClient = attempt.createChild("HttpClient");
        httpClient.reportMetric(HttpMetric.LEASED_CONCURRENCY, 5);
        httpClient.reportMetric(HttpMetric.MAX_CONCURRENCY, 80);

        new S3PoolMetricPublisher(metrics).publish(root.collect());

        assertThat(gaugeValue(metrics, "swath.s3.pool.leased")).isEqualTo(5.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.max")).isEqualTo(80.0);
        // Never observed at any level — stays unobserved (NaN -- emitted at OTLP export, dropped downstream, not omitted by Micrometer).
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition")).isNaN();
    }

    @Test
    void partialMetricSetLeavesOthersUnobserved() {
        RunMetrics metrics = newMetrics();
        MetricCollector root = MetricCollector.create("ApiCall");
        MetricCollector attempt = root.createChild("ApiCallAttempt");
        attempt.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 7);

        new S3PoolMetricPublisher(metrics).publish(root.collect());

        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition")).isEqualTo(7.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.leased")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.max")).isNaN();
    }

    @Test
    void noMetricsAnywhereLeavesAllGaugesUnobserved() {
        RunMetrics metrics = newMetrics();
        MetricCollector root = MetricCollector.create("ApiCall");
        root.createChild("ApiCallAttempt");

        new S3PoolMetricPublisher(metrics).publish(root.collect());

        assertThat(gaugeValue(metrics, "swath.s3.pool.leased")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition")).isNaN();
        assertThat(gaugeValue(metrics, "swath.s3.pool.max")).isNaN();
    }

    @Test
    void closeIsANoOp() {
        RunMetrics metrics = newMetrics();
        new S3PoolMetricPublisher(metrics).close();
    }

    /**
     * The SDK reports one attempt-level {@link MetricCollector} PER retry attempt —
     * multiple sibling children under the same {@code ApiCall} root, not multiple values reported on
     * ONE collection (that shape is already covered by {@code extractsAllFourMetricsFromAttemptLevelChild}
     * via {@code metricValues} returning a list). {@link S3PoolMetricPublisher#apply} walks {@code
     * collection.children()} in order and calls {@link RunMetrics#updateS3Pool} once per sibling that
     * reports a value, so the LAST attempt sibling visited — i.e. the most recent retry — wins on each
     * field, never the first attempt and never some field-by-field blend across attempts.
     *
     * <p>Values are deliberately non-monotonic across the three attempts (neither ascending nor
     * descending, and the LAST attempt's value is neither the min nor the max of the three): with a
     * monotonic sequence (e.g. an ascending 50/80/99), "last wins" is indistinguishable from "max
     * wins" — a regression that switched the extraction to pick the largest reported value instead of
     * the last-reported one would still pass. {@code MAX_CONCURRENCY = {50, 99, 80}} (last=80, which
     * is neither the max 99 nor the min 50) and {@code LEASED_CONCURRENCY = {10, 33, 20}} (last=20,
     * neither the max 33 nor the min 10) rule out both first-wins and max/min-wins.
     */
    @Test
    void multipleAttemptSiblingsLastAttemptWinsOverEarlierRetries() {
        RunMetrics metrics = newMetrics();
        MetricCollector root = MetricCollector.create("ApiCall");

        MetricCollector attempt1 = root.createChild("ApiCallAttempt");
        attempt1.reportMetric(HttpMetric.MAX_CONCURRENCY, 50);
        attempt1.reportMetric(HttpMetric.LEASED_CONCURRENCY, 10);
        attempt1.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 4);
        attempt1.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 40);

        MetricCollector attempt2 = root.createChild("ApiCallAttempt");
        attempt2.reportMetric(HttpMetric.MAX_CONCURRENCY, 99);
        attempt2.reportMetric(HttpMetric.LEASED_CONCURRENCY, 33);
        attempt2.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 9);
        attempt2.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 66);

        MetricCollector attempt3 = root.createChild("ApiCallAttempt");
        attempt3.reportMetric(HttpMetric.MAX_CONCURRENCY, 80);
        attempt3.reportMetric(HttpMetric.LEASED_CONCURRENCY, 20);
        attempt3.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 6);
        attempt3.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 55);

        new S3PoolMetricPublisher(metrics).publish(root.collect());

        assertThat(gaugeValue(metrics, "swath.s3.pool.max"))
                .as("last retry attempt's MAX_CONCURRENCY (80) wins -- not the first (50), and not the "
                        + "max-of-three (99), which rules out both first-wins and max-wins regressions")
                .isEqualTo(80.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.leased"))
                .as("last retry attempt's LEASED_CONCURRENCY (20) wins -- not the first (10), and not "
                        + "the max-of-three (33), which rules out both first-wins and max-wins regressions")
                .isEqualTo(20.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition"))
                .as("last retry attempt's PENDING_CONCURRENCY_ACQUIRES (6) wins -- not the first (4), and "
                        + "not the max-of-three (9), which rules out both first-wins and max-wins regressions")
                .isEqualTo(6.0);
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available"))
                .as("last retry attempt's AVAILABLE_CONCURRENCY (55) wins -- not the first (40), and not "
                        + "the max-of-three (66), which rules out both first-wins and max-wins regressions")
                .isEqualTo(55.0);
    }
}
