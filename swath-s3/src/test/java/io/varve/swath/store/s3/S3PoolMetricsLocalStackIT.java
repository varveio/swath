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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * {@code swath.s3.pool.*} (§3.8) is fed entirely by {@link S3PoolMetricPublisher}
 * observing the AWS SDK's {@code HttpMetric.*} on the sync {@code ApacheHttpClient}'s connection
 * pool ({@link S3PoolMetricPublisherTest} proves the extraction logic against a HAND-BUILT {@code
 * MetricCollection} tree — never against a real HTTP client attempt). This is the most load-bearing
 * gap of the five: nothing previously proved the sync {@code ApacheHttpClient} in THIS SDK version
 * actually emits {@code HttpMetric.MAX_CONCURRENCY} (or any of the other three) on a real request —
 * if it silently stopped doing so (an SDK upgrade, a config change), every {@code swath.s3.pool.*}
 * gauge would stay permanently NaN/unobserved in production with the unit tests alone still green.
 *
 * <p>Builds the client via the PRODUCTION wiring ({@link S3ClientFactory#create(S3Config,
 * RunMetrics)}), issues one real {@code ListObjectsV2} against LocalStack, then asserts {@code
 * swath.s3.pool.max} is a finite, positive number — proof {@link S3PoolMetricPublisher} actually
 * received {@code HttpMetric.MAX_CONCURRENCY} from a genuine SDK attempt.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class S3PoolMetricsLocalStackIT {

    private static final String BUCKET = "s3-pool-metrics-it";

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private S3Client s3;

    @AfterEach
    void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    @Timeout(60)
    void realListObjectsV2PopulatesTheS3PoolMaxGaugeFromTheSyncApacheHttpClient() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Config config = LocalStackSupport.config(LOCALSTACK, 8);
        s3 = S3ClientFactory.create(config, metrics);

        LocalStackSupport.createBucket(s3, BUCKET);
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key("k1").build(), RequestBody.fromBytes(new byte[]{1}));

        s3.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).maxKeys(10).build());

        Gauge maxGauge = metrics.registry().find("swath.s3.pool.max").gauge();
        assertThat(maxGauge).as("swath.s3.pool.max gauge registered").isNotNull();
        double max = maxGauge.value();
        assertThat(max)
                .as("S3PoolMetricPublisher received HttpMetric.MAX_CONCURRENCY from a real SDK attempt "
                        + "(sync ApacheHttpClient) -- a permanently-unobserved gauge would read NaN here")
                .isNotNaN();
        assertThat(max).as("the Apache pool's max concurrency is a genuine positive size").isGreaterThan(0.0);
    }
}
