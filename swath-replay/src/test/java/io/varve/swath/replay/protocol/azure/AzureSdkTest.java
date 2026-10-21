/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.core.util.ClientOptions;
import com.azure.core.util.Header;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpServer;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServeConfig;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.NativeSdkFixtures;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.replay.testkit.RecordingListingProxy;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Official Azure Java 12.35.1 SDK with explicit 2026-06-06 service version. */
class AzureSdkTest {
    @Test
    void officialSdkConsumesNativeFiveThousandPageOnSortedFixture(@TempDir Path dir) throws Exception {
        var objects = new io.varve.swath.model.ObjectEntry[5001];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = ObjectEntries.key(String.format("k%05d", i)).size(i + 1).isLatest(true).build();
        }
        Path fixture = NativeSdkFixtures.sorted(dir, objects);
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.SORTED,
                2, 8, Set.of(Protocol.AZURE), "replay", 32L * 1024 * 1024, 16 * 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()))) {
                var client = new BlobServiceClientBuilder()
                        .endpoint("http://127.0.0.1:" + proxy.port() + "/replay")
                        .serviceVersion(BlobServiceVersion.V2026_06_06)
                        .httpClient(new JdkHttpClientBuilder().build())
                        .buildClient().getBlobContainerClient("bucket");
                var pages = client.listBlobs(new ListBlobsOptions().setMaxResultsPerPage(5000),
                        Duration.ofSeconds(30)).iterableByPage().iterator();
                assertThat(pages.hasNext()).isTrue();
                var firstPage = pages.next().getValue();
                assertThat(firstPage).hasSize(5000);
                assertThat(firstPage.getFirst().getName()).isEqualTo("k00000");
                assertThat(firstPage.getLast().getName()).isEqualTo("k04999");
                assertThat(pages.hasNext()).isTrue();
                var lastPage = pages.next().getValue();
                assertThat(lastPage).hasSize(1);
                assertThat(lastPage.getFirst().getName()).isEqualTo("k05000");
                assertThat(pages.hasNext()).isFalse();
                assertThat(proxy.exchanges()).hasSize(2).allSatisfy(exchange ->
                        assertThat(exchange.pathAndQuery()).contains("maxresults=5000"));
                assertThat(proxy.exchanges().get(1).pathAndQuery()).contains("marker=");
                assertThat(proxy.exchanges()).allSatisfy(exchange ->
                        assertThat(exchange.forwardedSemanticHeadersMatch()).isTrue());
            }
        }
    }

    @Test
    void officialSdkDecodesEncodedNamesOnSortedFixture(@TempDir Path dir) throws Exception {
        Path fixture = NativeSdkFixtures.sorted(dir,
                ObjectEntries.key("a/1").size(1).isLatest(true).build(),
                ObjectEntries.key("a/2").size(2).isLatest(true).build(),
                ObjectEntries.key("a/sub/1").size(3).isLatest(true).build(),
                ObjectEntries.key("x\uFFFE/y").size(4).isLatest(true).build(),
                ObjectEntries.key("é").size(5).isLatest(true).build(),
                ObjectEntries.key("\uE000").size(6).isLatest(true).build(),
                ObjectEntries.key("😀").size(7).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.SORTED,
                2, 8, Set.of(Protocol.AZURE), "replay", 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()))) {
                var client = new BlobServiceClientBuilder()
                        .endpoint("http://127.0.0.1:" + proxy.port() + "/replay")
                        .serviceVersion(BlobServiceVersion.V2026_06_06)
                        .httpClient(new JdkHttpClientBuilder().build())
                        .buildClient().getBlobContainerClient("bucket");
                List<String> names = new ArrayList<>();
                client.listBlobs(new ListBlobsOptions().setMaxResultsPerPage(1), Duration.ofSeconds(10))
                        .forEach(item -> names.add(item.getName()));
                assertThat(names).containsExactly("a/1", "a/2", "a/sub/1", "x\uFFFE/y", "é", "\uE000", "😀");
                assertThat(proxy.exchanges()).anySatisfy(exchange ->
                        assertThat(new String(exchange.responseBody(), java.nio.charset.StandardCharsets.UTF_8))
                                .contains("<Name Encoded=\"true\">"));
                int beforeNegative = proxy.exchanges().size();
                Throwable rejected = org.assertj.core.api.Assertions.catchThrowable(() ->
                        client.listBlobsByHierarchy("/", new ListBlobsOptions().setStartFrom("a/2"),
                                Duration.ofSeconds(10)).iterator().hasNext());
                assertThat(rejected).isNotNull();
                assertThat(rejected).isInstanceOf(BlobStorageException.class);
                BlobStorageException serviceError = (BlobStorageException) rejected;
                assertThat(serviceError.getStatusCode()).isEqualTo(400);
                assertThat(serviceError.getErrorCode()).isEqualTo(BlobErrorCode.UNSUPPORTED_QUERY_PARAMETER);
                assertThat(proxy.exchanges()).hasSize(beforeNegative + 1);
                assertThat(proxy.exchanges().getLast().responseHeader("x-ms-error-code"))
                        .isEqualTo("UnsupportedQueryParameter");
            }
        }
    }

    @Test
    void officialFlatAndHierarchyPaginatorsUseLocalAccountPath(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("part.parquet");
        ParquetFixtures.write(fixture,
                ObjectEntries.key("a/1").size(1).isLatest(true).build(),
                ObjectEntries.key("a/2").size(2).isLatest(true).build(),
                ObjectEntries.key("b").size(3).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.AZURE), "replay", 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()))) {
            var client = new BlobServiceClientBuilder()
                    .endpoint("http://127.0.0.1:" + proxy.port() + "/replay")
                    .serviceVersion(BlobServiceVersion.V2026_06_06)
                    .httpClient(new JdkHttpClientBuilder().build())
                    .clientOptions(new ClientOptions().setHeaders(
                            List.of(new Header("x-ms-client-request-id", "sdk-probe-1"))))
                    .buildClient().getBlobContainerClient("bucket");
            List<String> flat = new ArrayList<>();
            client.listBlobs(new ListBlobsOptions().setMaxResultsPerPage(1), Duration.ofSeconds(10))
                    .forEach(item -> {
                        flat.add(item.getName());
                        assertThat(item.getProperties().getContentLength()).isPositive();
                        assertThat(item.getProperties().getLastModified()).isNotNull();
                        assertThat(item.getProperties().getBlobType()).isNotNull();
                    });
            assertThat(flat).containsExactly("a/1", "a/2", "b");
            assertThat(proxy.exchanges()).hasSize(3);
            assertThat(proxy.exchanges().get(1).pathAndQuery()).contains("marker=");

            List<String> hierarchy = new ArrayList<>();
            client.listBlobsByHierarchy("/", new ListBlobsOptions().setMaxResultsPerPage(1),
                    Duration.ofSeconds(10)).forEach(item -> hierarchy.add(item.getName()));
            assertThat(hierarchy).containsExactly("a/", "b");
            assertThat(proxy.exchanges()).hasSize(5);
            assertThat(proxy.exchanges().get(4).pathAndQuery()).contains("marker=");

            List<String> from = new ArrayList<>();
            client.listBlobs(new ListBlobsOptions().setStartFrom("a/2").setMaxResultsPerPage(1),
                    Duration.ofSeconds(10)).forEach(item -> from.add(item.getName()));
            assertThat(from).containsExactly("a/2", "b");
            assertThat(proxy.exchanges()).hasSize(7);
            assertThat(proxy.exchanges()).isNotEmpty().allSatisfy(exchange -> {
                assertThat(exchange.pathAndQuery()).startsWith("/replay/bucket?")
                        .contains("restype=container");
                assertThat(exchange.requestHeader("x-ms-version")).isEqualTo("2026-06-06");
                assertThat(exchange.requestHeader("user-agent"))
                        .contains("azsdk-java-azure-storage-blob/12.35.1");
                assertThat(exchange.requestHeader("x-ms-client-request-id")).isEqualTo("sdk-probe-1");
                assertThat(exchange.responseHeader("x-ms-client-request-id")).isEqualTo("sdk-probe-1");
                assertThat(exchange.responseHeader("date")).matches(
                        "[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT");
                assertThat(exchange.forwardedSemanticHeadersMatch()).isTrue();
            });
            assertThat(proxy.exchanges().stream().map(RecordingListingProxy.Exchange::pathAndQuery))
                    .anySatisfy(path ->
                    assertThat(path).contains("delimiter=/"));
            assertThat(proxy.exchanges().stream().map(RecordingListingProxy.Exchange::pathAndQuery))
                    .anySatisfy(path ->
                    assertThat(path).contains("startFrom=a/2"));
            assertThat(proxy.exchanges().stream().map(RecordingListingProxy.Exchange::pathAndQuery))
                    .anySatisfy(path ->
                    assertThat(path).contains("marker="));
            }
        }
    }

    @Test
    void retryDisabledSdkFailureMakesOneAttempt() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        endpoint.createContext("/", exchange -> {
            attempts.incrementAndGet();
            byte[] body = "<?xml version=\"1.0\"?><Error><Code>ServerBusy</Code><Message>busy</Message></Error>"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.getResponseHeaders().set("x-ms-error-code", "ServerBusy");
            exchange.sendResponseHeaders(503, body.length);
            try (var out = exchange.getResponseBody()) { out.write(body); }
        });
        endpoint.start();
        try {
            var client = new BlobServiceClientBuilder()
                    .endpoint("http://127.0.0.1:" + endpoint.getAddress().getPort() + "/replay")
                    .serviceVersion(BlobServiceVersion.V2026_06_06)
                    .httpClient(new JdkHttpClientBuilder().build())
                    .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, 30, 1L, 1L, null))
                    .buildClient().getBlobContainerClient("bucket");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.listBlobs().iterator().hasNext())
                    .isInstanceOf(BlobStorageException.class)
                    .satisfies(error -> {
                        BlobStorageException sdk = (BlobStorageException) error;
                        assertThat(sdk.getStatusCode()).isEqualTo(503);
                        assertThat(sdk.getErrorCode()).isEqualTo(BlobErrorCode.SERVER_BUSY);
                    });
            assertThat(attempts).hasValue(1);
        } finally {
            endpoint.stop(0);
        }
    }

    @Test
    void officialSdkDecodesReplayFixtureFaultWithAndWithoutRetries(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("bad.parquet");
        ParquetFixtures.write(fixture, ObjectEntries.key("bad").size(-1).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.AZURE), "replay", 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()))) {
                var oneAttempt = new BlobServiceClientBuilder()
                        .endpoint("http://127.0.0.1:" + proxy.port() + "/replay")
                        .serviceVersion(BlobServiceVersion.V2026_06_06)
                        .httpClient(new JdkHttpClientBuilder().build())
                        .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, 30, 1L, 1L, null))
                        .buildClient().getBlobContainerClient("bucket");
                org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        oneAttempt.listBlobs().iterator().hasNext())
                        .isInstanceOf(BlobStorageException.class)
                        .satisfies(error -> {
                            BlobStorageException sdk = (BlobStorageException) error;
                            assertThat(sdk.getStatusCode()).isEqualTo(500);
                            assertThat(sdk.getErrorCode()).isEqualTo(BlobErrorCode.INTERNAL_ERROR);
                        });
                assertThat(proxy.exchanges()).hasSize(1);
                assertThat(proxy.exchanges().getFirst().status()).isEqualTo(500);
                assertThat(proxy.exchanges().getFirst().responseHeader("x-ms-error-code"))
                        .isEqualTo("InternalError");

                var normalRetry = new BlobServiceClientBuilder()
                        .endpoint("http://127.0.0.1:" + proxy.port() + "/replay")
                        .serviceVersion(BlobServiceVersion.V2026_06_06)
                        .httpClient(new JdkHttpClientBuilder().build())
                        .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 3, 30, 1L, 1L, null))
                        .buildClient().getBlobContainerClient("bucket");
                org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        normalRetry.listBlobs().iterator().hasNext())
                        .isInstanceOf(BlobStorageException.class)
                        .satisfies(error -> {
                            BlobStorageException sdk = (BlobStorageException) error;
                            assertThat(sdk.getStatusCode()).isEqualTo(500);
                            assertThat(sdk.getErrorCode()).isEqualTo(BlobErrorCode.INTERNAL_ERROR);
                        });
                assertThat(proxy.exchanges()).hasSize(4);
            }
        }
    }

}
