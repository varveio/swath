/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Official 2.73.0 HTTP/JSON client; iterateAll owns opaque-token pagination. */
class GcsSdkTest {
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
            List<String> requests = new CopyOnWriteArrayList<>();
            HttpServer proxy = proxy(server.port(), requests, false);
            try {
            try (Storage storage = StorageOptions.http().setProjectId("replay-test")
                    .setCredentials(NoCredentials.getInstance())
                    .setHost("http://127.0.0.1:" + proxy.getAddress().getPort())
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
            }
            assertThat(requests).hasSize(3);
            assertThat(requests).allSatisfy(path -> {
                assertThat(path).startsWith("/storage/v1/b/bucket/o?");
                assertThat(path).contains("projection=full").doesNotContain("fields=");
            });
            } finally {
                proxy.stop(0);
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
                    .isInstanceOf(StorageException.class);
            assertThat(requests).hasSize(1);
        } finally {
            proxy.stop(0);
        }
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
