/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The S3 client is built with {@code maxConnections = T + 16}. The pool must
 * exceed T or it silently caps concurrency.
 */
class S3ClientFactoryTest {

    @Test
    void maxConnectionsIsTargetPlusHeadroom() {
        assertThat(S3ClientFactory.CONNECTION_HEADROOM).isEqualTo(16);
        assertThat(S3ClientFactory.maxConnectionsFor(64)).isEqualTo(80);
        assertThat(S3ClientFactory.maxConnectionsFor(1)).isEqualTo(17);
        assertThat(S3ClientFactory.maxConnectionsFor(256)).isEqualTo(272);
    }

    @Test
    void buildsAClientWithoutNetwork() {
        S3Config config = testConfig(Region.US_EAST_1);
        try (S3Client client = S3ClientFactory.create(config)) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    void explicitRegionIsAppliedToClientBuilder() {
        S3Config config = testConfig(Region.AP_NORTHEAST_1);
        try (S3Client client = S3ClientFactory.create(config)) {
            assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.AP_NORTHEAST_1);
        }
    }

    /**
     * The built client must actually CARRY the shipped timeout/retry wiring — {@code
     * apiCallTimeout=60s}, {@code apiCallAttemptTimeout=10s}, {@code maxAttempts=1} — read back
     * off {@code serviceClientConfiguration().overrideConfiguration()}, not just that the client
     * builds or that the constants equal themselves: dropping {@code .apiCallTimeout(...)} or the
     * {@code maxAttempts} wiring would otherwise pass silently.
     */
    @Test
    void builtClientCarriesTheShippedTimeoutAndRetryWiring() {
        S3Config config = testConfig(Region.US_EAST_1);
        try (S3Client client = S3ClientFactory.create(config)) {
            ClientOverrideConfiguration override =
                    client.serviceClientConfiguration().overrideConfiguration();
            assertThat(override.apiCallTimeout())
                    .as("overall apiCallTimeout — the PRIMARY liveness guarantee")
                    .contains(Duration.ofSeconds(60));
            assertThat(override.apiCallAttemptTimeout())
                    .as("per-attempt timeout — a stalled read is abandoned at 10s, not the old 30s")
                    .contains(Duration.ofSeconds(10));
            assertThat(override.retryStrategy())
                    .as("SDK retry is disabled (maxAttempts=1): swath's own loop is the sole retrier")
                    .hasValueSatisfying(rs -> assertThat(rs.maxAttempts()).isEqualTo(1));
        }
    }

    /**
     * §3.8: the {@code RunMetrics} overload builds a client with an {@link S3PoolMetricPublisher}
     * attached to the client's override configuration — asserted directly (not just {@code client !=
     * null}, which would pass even if {@code addMetricPublisher} were never called) via the built
     * client's {@code serviceClientConfiguration().overrideConfiguration()
     * .metricPublishers()}, which the SDK carries through from the builder unchanged.
     *
     * <p>A SECOND client-level publisher, {@link S3CallClassLatencyPublisher}, is attached
     * ALONGSIDE the pool publisher (never instead of it — multiple client-level publishers are all
     * invoked by the SDK, unlike a per-request override list) so the per-call-class latency-phase
     * decomposition can observe {@code HttpMetric.CONCURRENCY_ACQUIRE_DURATION}/{@code
     * CoreMetric.TIME_TO_FIRST_BYTE} without disturbing this existing pool-gauge feed.
     */
    @Test
    void metricsOverloadAttachesPoolMetricPublisherWithoutNetwork() {
        S3Config config = testConfig(Region.US_EAST_1);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (S3Client client = S3ClientFactory.create(config, metrics)) {
            List<MetricPublisher> publishers =
                    client.serviceClientConfiguration().overrideConfiguration().metricPublishers();
            assertThat(publishers).hasSize(2);
            assertThat(publishers).anyMatch(S3PoolMetricPublisher.class::isInstance);
            assertThat(publishers).anyMatch(S3CallClassLatencyPublisher.class::isInstance);
        }
    }

    /**
     * {@code metrics == null} must behave exactly like {@link S3ClientFactory#create(S3Config)}: no
     * publisher attached at all — not just "no crash", verified via the same
     * reachable-override-configuration assertion as the strengthened test above.
     */
    @Test
    void metricsOverloadWithNullMetricsBuildsClientWithoutPublisher() {
        S3Config config = testConfig(Region.US_EAST_1);
        try (S3Client client = S3ClientFactory.create(config, null)) {
            List<MetricPublisher> publishers =
                    client.serviceClientConfiguration().overrideConfiguration().metricPublishers();
            assertThat(publishers).isEmpty();
        }
    }

    /**
     * The {@code RunMetrics} overload of {@link S3ClientFactory#httpClient(int, RunMetrics)}
     * must build a client without touching the network — installing {@link
     * S3HandshakeCountingSocketFactory} must not itself dial out or throw. {@code metrics == null}
     * must keep building the exact same client {@link S3ClientFactory#httpClient(int)} always has.
     */
    @Test
    void httpClientMetricsOverloadBuildsWithoutNetworkBothWithAndWithoutMetrics() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        try (var withMetrics = S3ClientFactory.httpClient(64, metrics);
             var withoutMetrics = S3ClientFactory.httpClient(64, null)) {
            assertThat(withMetrics).isNotNull();
            assertThat(withoutMetrics).isNotNull();
        }
    }

    /**
     * A {@code bearerTokenSupplier}-configured {@link S3Config} must still build a client without
     * touching the network — {@link S3ClientFactory#create} installs {@link BearerTokenAuthScheme}
     * via {@code putAuthScheme}, which is a builder-time registration, not a connection.
     */
    @Test
    void bearerTokenSupplierConfigBuildsAClientWithoutNetwork() {
        S3Config config = new S3Config(
                Region.US_EAST_1,
                URI.create("https://storage.googleapis.com"),
                true,
                64,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("unused", "unused")),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT,
                () -> "test-token");
        try (S3Client client = S3ClientFactory.create(config)) {
            assertThat(client).isNotNull();
        }
    }

    private static S3Config testConfig(Region region) {
        return new S3Config(
                region,
                URI.create("http://localhost:4566"),
                true,
                64,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
    }
}
