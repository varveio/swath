/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Official 2.73.0 HTTP/JSON client; iterateAll owns opaque-token pagination. */
class GcsSdkTest {
    @Test
    void officialHttpClientUsesHierarchyOffsetsAndUnicodeOnSortedFixture(@TempDir Path dir) throws Exception {
        Path fixture = NativeSdkFixtures.sorted(dir,
                ObjectEntries.key("a/1").size(1).isLatest(true).build(),
                ObjectEntries.key("a/2").size(2).isLatest(true).build(),
                ObjectEntries.key("a/sub/1").size(3).isLatest(true).build(),
                ObjectEntries.key("a/sub/2").size(4).isLatest(true).build(),
                ObjectEntries.key("b").size(5).isLatest(true).build(),
                ObjectEntries.key("é").size(6).isLatest(true).build(),
                ObjectEntries.key("\uE000").size(7).isLatest(true).build(),
                ObjectEntries.key("😀").size(8).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.SORTED,
                2, 8, Set.of(Protocol.GCS), null, 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()));
                 Storage storage = StorageOptions.http().setProjectId("replay-test")
                         .setCredentials(NoCredentials.getInstance())
                         .setHost("http://127.0.0.1:" + proxy.port()).build().getService()) {
                List<String> bounded = new ArrayList<>();
                for (Blob blob : storage.list("bucket", Storage.BlobListOption.pageSize(1),
                        Storage.BlobListOption.prefix("a/"), Storage.BlobListOption.startOffset("a/2"),
                        Storage.BlobListOption.endOffset("b")).iterateAll()) {
                    bounded.add(blob.getName());
                }
                assertThat(bounded).containsExactly("a/2", "a/sub/1", "a/sub/2");
                assertThat(proxy.exchanges()).hasSize(3);
                assertThat(proxy.exchanges().get(1).pathAndQuery()).contains("pageToken=");
                List<String> hierarchy = new ArrayList<>();
                for (Blob blob : storage.list("bucket", Storage.BlobListOption.pageSize(1),
                        Storage.BlobListOption.prefix("a/"),
                        Storage.BlobListOption.currentDirectory()).iterateAll()) {
                    hierarchy.add(blob.getName());
                }
                assertThat(hierarchy).containsExactly("a/1", "a/2", "a/sub/");
                assertThat(proxy.exchanges()).hasSize(6);
                assertThat(proxy.exchanges().get(4).pathAndQuery()).contains("pageToken=");
                List<String> ordered = new ArrayList<>();
                for (Blob blob : storage.list("bucket", Storage.BlobListOption.pageSize(1)).iterateAll()) {
                    ordered.add(blob.getName());
                }
                assertThat(ordered).containsExactly("a/1", "a/2", "a/sub/1", "a/sub/2",
                        "b", "é", "\uE000", "😀");
                assertThat(proxy.exchanges()).hasSize(14);
                assertThat(proxy.exchanges().get(7).pathAndQuery()).contains("pageToken=");
                assertThat(proxy.exchanges().stream().map(RecordingListingProxy.Exchange::pathAndQuery))
                        .anySatisfy(path -> assertThat(path).contains("startOffset=a/2")
                                .contains("endOffset=b"));
                assertThat(proxy.exchanges().stream().map(RecordingListingProxy.Exchange::pathAndQuery))
                        .anySatisfy(path -> assertThat(path).contains("delimiter=/"));
                assertThat(proxy.exchanges()).allSatisfy(exchange -> {
                    assertThat(List.of(exchange.requestHeader("user-agent").split("\\s+")))
                            .contains("gcloud-java/2.73.0");
                    assertThat(List.of(exchange.requestHeader("x-goog-api-client").split("\\s+")))
                            .contains("gdcl/2.7.2");
                    assertThat(exchange.forwardedSemanticHeadersMatch()).isTrue();
                    assertThat(exchange.status()).isEqualTo(200);
                });
                assertSdkRunCanBeSanitized(dir, proxy);
            }
        }
    }

    @Test
    void officialHttpClientAutoPaginatesUnmodifiedEndpointOverride(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("part.parquet");
        ParquetFixtures.write(fixture,
                ObjectEntries.key("a").size(1).isLatest(true).build(),
                ObjectEntries.key("b").size(2).isLatest(true).build(),
                ObjectEntries.key("c").size(3).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.GCS), null, 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()));
                 Storage storage = StorageOptions.http().setProjectId("replay-test")
                    .setCredentials(NoCredentials.getInstance())
                    .setHost("http://127.0.0.1:" + proxy.port())
                    .build().getService()) {
                List<String> names = new ArrayList<>();
                for (Blob blob : storage.list("bucket", Storage.BlobListOption.pageSize(1)).iterateAll()) {
                    names.add(blob.getName());
                    assertThat(blob.getSize()).isPositive();
                    assertThat(blob.getUpdateTimeOffsetDateTime()).isNotNull();
                    assertThat(blob.getGeneration()).isEqualTo(1L);
                    assertThat(blob.getContentType()).isEqualTo("application/octet-stream");
                }
                assertThat(names).containsExactly("a", "b", "c");
                assertThat(proxy.exchanges()).hasSize(3);
                assertThat(proxy.exchanges()).allSatisfy(exchange -> {
                    assertThat(exchange.pathAndQuery()).startsWith("/storage/v1/b/bucket/o?")
                            .contains("projection=full").doesNotContain("fields=");
                    assertThat(List.of(exchange.requestHeader("user-agent").split("\\s+")))
                            .contains("gcloud-java/2.73.0");
                    assertThat(List.of(exchange.requestHeader("x-goog-api-client").split("\\s+")))
                            .contains("gdcl/2.7.2");
                    assertThat(exchange.forwardedSemanticHeadersMatch()).isTrue();
                    assertThat(exchange.status()).isEqualTo(200);
                });
            }
        }
    }

    @Test
    void retryDisabledSdkFailureMakesOneRequest() throws Exception {
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer proxy = proxy(0, requests, true);
        try (Storage storage = StorageOptions.http().setProjectId("replay-test")
                .setCredentials(NoCredentials.getInstance())
                .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(1).build())
                .setHost("http://127.0.0.1:" + proxy.getAddress().getPort())
                .build().getService()) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> storage.list("bucket"))
                    .isInstanceOf(StorageException.class)
                    .satisfies(error -> {
                        StorageException sdk = (StorageException) error;
                        assertThat(sdk.getCode()).isEqualTo(503);
                    });
            assertThat(requests).hasSize(1);
        } finally {
            proxy.stop(0);
        }
    }

    @Test
    void officialSdkDecodesReplayFixtureFaultAndRecordsNormalRetries(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("bad.parquet");
        ParquetFixtures.write(fixture, ObjectEntries.key("bad").size(-1).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.GCS), null, 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            try (RecordingListingProxy proxy = new RecordingListingProxy(
                    URI.create("http://127.0.0.1:" + server.port()))) {
                try (Storage oneAttempt = StorageOptions.http().setProjectId("replay-test")
                        .setCredentials(NoCredentials.getInstance())
                        .setRetrySettings(retries(1))
                        .setHost("http://127.0.0.1:" + proxy.port()).build().getService()) {
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> oneAttempt.list("bucket"))
                            .isInstanceOf(StorageException.class)
                            .satisfies(error -> {
                                StorageException sdk = (StorageException) error;
                                assertThat(sdk.getCode()).isEqualTo(500);
                                assertThat(sdk.getReason()).isEqualTo("internalError");
                            });
                }
                assertThat(proxy.exchanges()).hasSize(1);
                assertThat(proxy.exchanges().getFirst().status()).isEqualTo(500);

                try (Storage normalRetry = StorageOptions.http().setProjectId("replay-test")
                        .setCredentials(NoCredentials.getInstance())
                        .setRetrySettings(retries(3))
                        .setHost("http://127.0.0.1:" + proxy.port()).build().getService()) {
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> normalRetry.list("bucket"))
                            .isInstanceOf(StorageException.class)
                            .satisfies(error -> {
                                StorageException sdk = (StorageException) error;
                                assertThat(sdk.getCode()).isEqualTo(500);
                                assertThat(sdk.getReason()).isEqualTo("internalError");
                            });
                }
                assertThat(proxy.exchanges()).hasSize(4);
            }
        }
    }

    private static RetrySettings retries(int attempts) {
        return RetrySettings.newBuilder().setMaxAttempts(attempts)
                .setInitialRetryDelayDuration(Duration.ofMillis(1))
                .setMaxRetryDelayDuration(Duration.ofMillis(1))
                .setRetryDelayMultiplier(1.0)
                .setTotalTimeoutDuration(Duration.ofSeconds(5)).build();
    }

    private static void assertSdkRunCanBeSanitized(Path dir, RecordingListingProxy proxy) throws Exception {
        ObjectMapper json = new ObjectMapper();
        Path manifest = dir.resolve("sdk-manifest.json");
        Files.writeString(manifest, """
                {"provider":"gcs","namespace_mode":"flat-ubla","region":"local-test",
                 "api_version":"json-v1","sdk_product":"gcloud-java","sdk_version":"2.73.0",
                 "capture_date":"2026-09-25",
                 "objects":[{"name":"a/1","size":1,"sha256":"%s"}]}
                """.formatted("a".repeat(64)));
        List<Map<String, Object>> steps = new ArrayList<>();
        for (RecordingListingProxy.Exchange exchange : proxy.exchanges().subList(0, 3)) {
            Map<String, Object> request = Map.of("method", exchange.method(),
                    "url", "http://127.0.0.1:" + proxy.port() + exchange.pathAndQuery(),
                    "headers", captureHeaders(exchange.requestHeaders()));
            Map<String, Object> response = Map.of("status", exchange.status(),
                    "headers", captureHeaders(exchange.responseHeaders()),
                    "body_base64", Base64.getEncoder().encodeToString(exchange.responseBody()));
            steps.add(Map.of("phase", "walk", "exchange", Map.of(
                    "captured_at", "2026-09-25T10:00:00Z", "request", request,
                    "response", response)));
        }
        Path raw = dir.resolve("sdk-run-raw.json");
        Path safe = dir.resolve("sdk-run-safe.json");
        Path reportFile = dir.resolve("sdk-sanitize.log");
        Files.writeString(raw, json.writeValueAsString(Map.of("steps", steps)));
        Path script = Path.of("..", "scripts", "provider-conformance", "evidence.py").toAbsolutePath();
        Process process = new ProcessBuilder("python3", script.toString(), "sanitize",
                "--kind", "sdk_run", "--input", raw.toString(), "--output", safe.toString(),
                "--provider", "gcs", "--bucket", "bucket", "--manifest", manifest.toString(),
                "--probe-id", "gcs-09").redirectErrorStream(true)
                .redirectOutput(reportFile.toFile()).start();
        if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
            throw new AssertionError("provider capture sanitizer timed out");
        }
        assertThat(process.exitValue()).as(Files.readString(reportFile)).isZero();
        var sanitized = json.readTree(safe.toFile());
        assertThat(sanitized.path("schema_version").asText()).isEqualTo("provider-capture-run-v1");
        assertThat(sanitized.path("steps").size()).isEqualTo(3);
    }

    private static List<Map<String, String>> captureHeaders(Map<String, List<String>> headers) {
        List<Map<String, String>> result = new ArrayList<>();
        headers.forEach((name, values) -> values.forEach(value -> result.add(
                Map.of("name", name, "value", value))));
        return result;
    }

    private static HttpServer proxy(int replayPort, List<String> requests, boolean fail) throws Exception {
        HttpServer proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        HttpClient forward = HttpClient.newHttpClient();
        proxy.createContext("/", exchange -> {
            requests.add(exchange.getRequestURI().toString());
            byte[] body;
            int status;
            String contentType;
            if (fail) {
                status = 503;
                contentType = "application/json";
                body = "{\"error\":{\"code\":503,\"message\":\"busy\"}}"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            } else {
                URI target = URI.create("http://127.0.0.1:" + replayPort + exchange.getRequestURI());
                try {
                    var response = forward.send(HttpRequest.newBuilder(target).GET().build(),
                            HttpResponse.BodyHandlers.ofByteArray());
                    status = response.statusCode();
                    contentType = response.headers().firstValue("content-type").orElse("application/json");
                    body = response.body();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException(e);
                }
            }
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(status, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        proxy.start();
        return proxy;
    }
}
