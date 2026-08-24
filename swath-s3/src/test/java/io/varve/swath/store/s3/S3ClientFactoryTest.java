/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
    void realApacheRequestPrependsSwathUserAgentToSdkMarkers() throws Exception {
        AtomicReference<String> userAgent = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try {
            server.createContext("/", exchange -> respondToListRequest(exchange, userAgent));
            server.start();

            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            try (S3Client client = S3ClientFactory.create(testConfig(Region.US_EAST_1, endpoint))) {
                client.listObjectsV2(ListObjectsV2Request.builder().bucket("bucket").build());
            }
        } finally {
            server.stop(0);
        }

        assertThat(userAgent.get()).startsWith("swath/development aws-sdk-java/2.31.78 ");
        assertThat(userAgent.get().split("swath/", -1)).hasSize(2);
        assertThat(userAgent.get()).contains("api/S3#2.31.78");
    }

    @Test
    void streamingListParserPreservesTheCompleteSdkModelAndDecoderOrdering() throws Exception {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Name>bucket-name</Name>
                  <Prefix>folder%2Braw%2525%2F</Prefix>
                  <Delimiter>%2F</Delimiter>
                  <MaxKeys>1000</MaxKeys>
                  <KeyCount>2</KeyCount>
                  <EncodingType>url</EncodingType>
                  <ContinuationToken>opaque-current-token</ContinuationToken>
                  <NextContinuationToken>opaque-next-token</NextContinuationToken>
                  <StartAfter>%25before%2B</StartAfter>
                  <IsTruncated>true</IsTruncated>
                  <Contents>
                    <Key>folder%2Braw%2525%2Fkey%20one</Key>
                    <LastModified>2026-08-24T12:34:56.123Z</LastModified>
                    <ETag>&quot;etag-value&quot;</ETag>
                    <ChecksumAlgorithm>CRC32</ChecksumAlgorithm>
                    <ChecksumAlgorithm>SHA256</ChecksumAlgorithm>
                    <ChecksumType>COMPOSITE</ChecksumType>
                    <Size>123456789</Size>
                    <StorageClass>GLACIER</StorageClass>
                    <Owner><ID>owner-id</ID><DisplayName>owner name</DisplayName></Owner>
                    <RestoreStatus>
                      <IsRestoreInProgress>false</IsRestoreInProgress>
                      <RestoreExpiryDate>2026-09-01T00:00:00Z</RestoreExpiryDate>
                    </RestoreStatus>
                    <FutureObjectField><Nested>ignored</Nested></FutureObjectField>
                  </Contents>
                  <CommonPrefixes><Prefix>folder%2Braw%2525%2Fsub%2F</Prefix></CommonPrefixes>
                  <FutureResponseField><Nested>ignored</Nested></FutureResponseField>
                </ListBucketResult>
                """;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        try {
            server.createContext("/", exchange -> respond(exchange, 200, xml, "x-amz-request-charged", "requester"));
            server.start();

            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            try (S3Client client = S3ClientFactory.create(testConfig(Region.US_EAST_1, endpoint), metrics)) {
                ListObjectsV2Response response = client.listObjectsV2(
                        ListObjectsV2Request.builder().bucket("bucket").maxKeys(1000).build());

                assertThat(response.sdkHttpResponse().statusCode()).isEqualTo(200);
                assertThat(response.requestChargedAsString()).isEqualTo("requester");
                assertThat(response.name()).isEqualTo("bucket-name");
                assertThat(response.prefix()).isEqualTo("folder+raw%25/");
                assertThat(response.delimiter()).isEqualTo("/");
                assertThat(response.maxKeys()).isEqualTo(1000);
                assertThat(response.keyCount()).isEqualTo(2);
                assertThat(response.encodingTypeAsString()).isEqualTo("url");
                assertThat(response.continuationToken()).isEqualTo("opaque-current-token");
                assertThat(response.nextContinuationToken()).isEqualTo("opaque-next-token");
                assertThat(response.startAfter()).isEqualTo("%before+");
                assertThat(response.isTruncated()).isTrue();
                assertThat(response.commonPrefixes()).singleElement().satisfies(prefix ->
                        assertThat(prefix.prefix()).isEqualTo("folder+raw%25/sub/"));
                assertThat(response.contents()).singleElement().satisfies(object -> {
                    assertThat(object.key()).isEqualTo("folder+raw%25/key one");
                    assertThat(object.lastModified()).isEqualTo(Instant.parse("2026-08-24T12:34:56.123Z"));
                    assertThat(object.eTag()).isEqualTo("\"etag-value\"");
                    assertThat(object.checksumAlgorithmAsStrings()).containsExactly("CRC32", "SHA256");
                    assertThat(object.checksumTypeAsString()).isEqualTo("COMPOSITE");
                    assertThat(object.size()).isEqualTo(123456789L);
                    assertThat(object.storageClassAsString()).isEqualTo("GLACIER");
                    assertThat(object.owner().id()).isEqualTo("owner-id");
                    assertThat(object.owner().displayName()).isEqualTo("owner name");
                    assertThat(object.restoreStatus().isRestoreInProgress()).isFalse();
                    assertThat(object.restoreStatus().restoreExpiryDate())
                            .isEqualTo(Instant.parse("2026-09-01T00:00:00Z"));
                });
            }
        } finally {
            server.stop(0);
            registry.close();
        }

        assertThat(registry.get("swath.steal_reason")
                        .tags("outcome", "S3_RESPONSE", "reason", "streaming_xml")
                        .counter()
                        .count())
                .isEqualTo(1.0);
    }

    @Test
    void streamingListParserLeavesErrorBodiesOnTheSdkErrorPath() throws Exception {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error>
                  <Code>SlowDown</Code>
                  <Message>Please reduce your request rate.</Message>
                  <RequestId>request-id</RequestId>
                </Error>
                """;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        try {
            server.createContext("/", exchange -> respond(exchange, 503, xml, "x-amz-request-id", "request-id"));
            server.start();

            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            try (S3Client client = S3ClientFactory.create(testConfig(Region.US_EAST_1, endpoint), metrics)) {
                assertThatExceptionOfType(S3Exception.class)
                        .isThrownBy(() -> client.listObjectsV2(
                                ListObjectsV2Request.builder().bucket("bucket").build()))
                        .satisfies(error -> {
                            assertThat(error.statusCode()).isEqualTo(503);
                            assertThat(error.awsErrorDetails().errorCode()).isEqualTo("SlowDown");
                            assertThat(error.awsErrorDetails().errorMessage())
                                    .isEqualTo("Please reduce your request rate.");
                        });
            }

            assertThat(registry.find("swath.steal_reason")
                            .tags("outcome", "S3_RESPONSE", "reason", "streaming_xml")
                            .counters())
                    .isEmpty();
        } finally {
            server.stop(0);
            registry.close();
        }
    }

    @Test
    void streamingListParserReturnsAnHttp200ErrorDocumentToTheSdkErrorHandler() throws Exception {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/" future="retained">
                  <Code>InternalError</Code>
                  <Message>An internal error occurred.</Message>
                  <RequestId>request-id</RequestId>
                  <HostId>host-id</HostId>
                  <FutureErrorField><Nested>retained</Nested></FutureErrorField>
                </Error>
                """;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        try {
            server.createContext("/", exchange -> respond(exchange, 200, xml, "x-amz-request-id", "request-id"));
            server.start();

            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            try (S3Client client = S3ClientFactory.create(testConfig(Region.US_EAST_1, endpoint), metrics)) {
                assertThatExceptionOfType(S3Exception.class)
                        .isThrownBy(() -> client.listObjectsV2(
                                ListObjectsV2Request.builder().bucket("bucket").build()))
                        .satisfies(error -> {
                            assertThat(error.statusCode()).isEqualTo(200);
                            assertThat(error.awsErrorDetails().errorCode()).isEqualTo("InternalError");
                            assertThat(error.awsErrorDetails().errorMessage())
                                    .isEqualTo("An internal error occurred.");
                        });
            }

            assertThat(registry.get("swath.steal_reason")
                            .tags("outcome", "S3_RESPONSE", "reason", "sdk_error_in_success")
                            .counter()
                            .count())
                    .isEqualTo(1.0);
            assertThat(registry.find("swath.steal_reason")
                            .tags("outcome", "S3_RESPONSE", "reason", "streaming_xml")
                            .counters())
                    .isEmpty();
        } finally {
            server.stop(0);
            registry.close();
        }
    }

    @Test
    void streamingListParserFailsClosedOnMalformedSuccessfulXml() throws Exception {
        String xml = "<ListBucketResult><Contents><Key>unterminated";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        try {
            server.createContext("/", exchange -> respond(exchange, 200, xml, "x-amz-request-id", "request-id"));
            server.start();

            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            try (S3Client client = S3ClientFactory.create(testConfig(Region.US_EAST_1, endpoint), metrics)) {
                assertThatExceptionOfType(SdkClientException.class)
                        .isThrownBy(() -> client.listObjectsV2(
                                ListObjectsV2Request.builder().bucket("bucket").build()))
                        .withMessageContaining("Unable to stream ListObjectsV2 XML response");
            }

            assertThat(registry.find("swath.steal_reason")
                            .tags("outcome", "S3_RESPONSE", "reason", "streaming_xml")
                            .counters())
                    .isEmpty();
        } finally {
            server.stop(0);
            registry.close();
        }
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
        return testConfig(region, URI.create("http://localhost:4566"));
    }

    private static S3Config testConfig(Region region, URI endpoint) {
        return new S3Config(
                region,
                endpoint,
                true,
                64,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
    }

    private static void respondToListRequest(HttpExchange exchange, AtomicReference<String> userAgent)
            throws IOException {
        userAgent.set(exchange.getRequestHeaders().getFirst("User-Agent"));
        byte[] response = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Name>bucket</Name>
                  <MaxKeys>1000</MaxKeys>
                  <IsTruncated>false</IsTruncated>
                </ListBucketResult>
                """.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(response);
        }
    }

    private static void respond(
            HttpExchange exchange, int status, String responseBody, String headerName, String headerValue)
            throws IOException {
        byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.getResponseHeaders().set(headerName, headerValue);
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(response);
        }
    }
}
