/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpServer;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServeConfig;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Official Azure Java 12.35.1 SDK with explicit 2026-06-06 service version. */
class AzureSdkTest {
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
            List<WireRequest> requests = new CopyOnWriteArrayList<>();
            HttpServer proxy = proxy(server.port(), requests);
            try {
            var client = new BlobServiceClientBuilder()
                    .endpoint("http://127.0.0.1:" + proxy.getAddress().getPort() + "/replay")
                    .serviceVersion(BlobServiceVersion.V2026_06_06)
                    .httpClient(new JdkHttpClientBuilder().build())
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

            List<String> hierarchy = new ArrayList<>();
            client.listBlobsByHierarchy("/", new ListBlobsOptions().setMaxResultsPerPage(1),
                    Duration.ofSeconds(10)).forEach(item -> hierarchy.add(item.getName()));
            assertThat(hierarchy).containsExactly("a/", "b");

            List<String> from = new ArrayList<>();
            client.listBlobs(new ListBlobsOptions().setStartFrom("a/2").setMaxResultsPerPage(1),
                    Duration.ofSeconds(10)).forEach(item -> from.add(item.getName()));
            assertThat(from).containsExactly("a/2", "b");
            assertThat(requests).isNotEmpty().allSatisfy(request -> {
                assertThat(request.path()).startsWith("/replay/bucket?").contains("restype=container");
                assertThat(request.version()).isEqualTo("2026-06-06");
            });
            assertThat(requests.stream().map(WireRequest::path)).anySatisfy(path ->
                    assertThat(path).contains("delimiter=/"));
            assertThat(requests.stream().map(WireRequest::path)).anySatisfy(path ->
                    assertThat(path).contains("startFrom=a/2"));
            assertThat(requests.stream().map(WireRequest::path)).anySatisfy(path ->
                    assertThat(path).contains("marker="));
            } finally {
                proxy.stop(0);
            }
        }
    }

    private static HttpServer proxy(int replayPort, List<WireRequest> requests) throws Exception {
        HttpServer proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        HttpClient forward = HttpClient.newHttpClient();
        proxy.createContext("/", exchange -> {
            requests.add(new WireRequest(exchange.getRequestURI().toString(),
                    exchange.getRequestHeaders().getFirst("x-ms-version")));
            URI target = URI.create("http://127.0.0.1:" + replayPort + exchange.getRequestURI());
            try {
                HttpRequest.Builder request = HttpRequest.newBuilder(target).GET();
                String version = exchange.getRequestHeaders().getFirst("x-ms-version");
                if (version != null) request.header("x-ms-version", version);
                String accept = exchange.getRequestHeaders().getFirst("accept");
                if (accept != null) request.header("accept", accept);
                var response = forward.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
                response.headers().firstValue("content-type").ifPresent(value ->
                        exchange.getResponseHeaders().set("Content-Type", value));
                response.headers().firstValue("x-ms-version").ifPresent(value ->
                        exchange.getResponseHeaders().set("x-ms-version", value));
                response.headers().firstValue("x-ms-request-id").ifPresent(value ->
                        exchange.getResponseHeaders().set("x-ms-request-id", value));
                byte[] body = response.body();
                exchange.sendResponseHeaders(response.statusCode(), body.length);
                try (var output = exchange.getResponseBody()) { output.write(body); }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(e);
            }
        });
        proxy.start();
        return proxy;
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
                    .isInstanceOf(BlobStorageException.class);
            assertThat(attempts).hasValue(1);
        } finally {
            endpoint.stop(0);
        }
    }

    private record WireRequest(String path, String version) { }
}
