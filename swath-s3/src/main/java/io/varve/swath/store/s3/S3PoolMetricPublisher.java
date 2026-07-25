/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.RunMetrics;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

/**
 * §3.8: bridges the AWS SDK's per-attempt {@code ApacheHttpClient} connection-pool metrics
 * ({@link HttpMetric#LEASED_CONCURRENCY}, {@link HttpMetric#AVAILABLE_CONCURRENCY}, {@link
 * HttpMetric#PENDING_CONCURRENCY_ACQUIRES}, {@link HttpMetric#MAX_CONCURRENCY}) into {@link
 * RunMetrics}'s {@code swath.s3.pool.*} gauges — the "acquisition-starved vs self-throttle"
 * discriminator: {@code pending_acquisition > 0} is live acquisition-starvation;
 * {@code leased ≪ max} while in-flight is low is AIMD self-throttle.
 *
 * <p>These metrics live on the <b>attempt-level child</b> of the per-request {@link
 * MetricCollection}, not the top level — {@link #publish} therefore walks the collection AND
 * every descendant (recursively) rather than reading only the root. Whole-client counts (the
 * Apache pool is shared across requests), not per-connection.
 *
 * <p>Kept in {@code swath-s3}, not {@code swath-core}, so {@code RunMetrics} stays free of any
 * AWS SDK import (the SDK-free-boundary invariant); this class holds the only reference to both
 * {@link RunMetrics} and the SDK metrics types.
 */
public final class S3PoolMetricPublisher implements MetricPublisher {

    private final RunMetrics metrics;

    public S3PoolMetricPublisher(RunMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void publish(MetricCollection metricCollection) {
        MetricCollections.walk(metricCollection, this::apply);
    }

    private void apply(MetricCollection collection) {
        Integer leased = MetricCollections.last(collection, HttpMetric.LEASED_CONCURRENCY);
        Integer idleAvailable = MetricCollections.last(collection, HttpMetric.AVAILABLE_CONCURRENCY);
        Integer pending = MetricCollections.last(collection, HttpMetric.PENDING_CONCURRENCY_ACQUIRES);
        Integer max = MetricCollections.last(collection, HttpMetric.MAX_CONCURRENCY);
        if (leased != null || idleAvailable != null || pending != null || max != null) {
            metrics.updateS3Pool(leased, idleAvailable, pending, max);
        }
    }

    @Override
    public void close() {
        // No resources to release — the gauge state lives in RunMetrics, which outlives this publisher.
    }
}
