/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayAllProtocolsTest {
    @TempDir Path temp;

    @Test
    void neutralSeekAndDelimiterObservationsMatchEachNativeResponse() throws Exception {
        Path fixture = temp.resolve("shapes.parquet");
        try (var writer = ParquetFixtures.open(fixture)) {
            writer.write(ObjectEntries.bare("a/1"));
            writer.write(ObjectEntries.bare("a/2"));
            writer.write(ObjectEntries.bare("b"));
        }
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 16, Set.of(Protocol.S3, Protocol.GCS, Protocol.AZURE), "replay",
                2L * 1024 * 1024, 64 * 1024, Duration.ofSeconds(10), Duration.ofSeconds(30),
                Duration.ofSeconds(30), null, (request, result) -> Duration.ZERO);
        try (ReplayServer server = ReplayServer.open(config);
             HttpClient client = HttpClient.newHttpClient()) {
            server.start();
            String[][] routes = {
                    {"s3", "/bucket?list-type=2&max-keys=1", "/bucket?list-type=2&delimiter=%2F"},
                    {"gcs", "/storage/v1/b/bucket/o?maxResults=1",
                            "/storage/v1/b/bucket/o?delimiter=%2F"},
                    {"azure", "/replay/bucket?restype=container&comp=list&maxresults=1",
                            "/replay/bucket?restype=container&comp=list&delimiter=%2F"}
            };
            for (String[] route : routes) {
                for (int i = 1; i <= 2; i++) {
                    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                            "http://127.0.0.1:" + server.port() + route[i]));
                    if (route[0].equals("azure")) request.header("x-ms-version", "2026-06-06");
                    assertThat(client.send(request.build(), HttpResponse.BodyHandlers.ofString()).statusCode())
                            .as(route[0] + " route " + i).isEqualTo(200);
                }
                assertThat(server.metrics().registry().get("swath.replay.protocol.objects")
                        .tags("protocol", route[0], "shape", "seek").counter().count()).isEqualTo(1);
                assertThat(server.metrics().registry().get("swath.replay.protocol.objects")
                        .tags("protocol", route[0], "shape", "delimiter").counter().count()).isEqualTo(1);
                assertThat(server.metrics().registry().get("swath.replay.protocol.prefixes")
                        .tags("protocol", route[0], "shape", "delimiter").counter().count()).isEqualTo(1);
            }
            assertThat(server.metrics().registry().find("swath.replay.response.percent.encoding.path")
                    .tag("protocol", "azure").counter()).isNull();
            assertThat(server.metrics().registry().find("swath.replay.response.percent.encoding.path")
                    .tag("protocol", "gcs").counter()).isNull();
        }
    }

    @Test
    void allEnabledRoutesShareOneOpenedFixtureAndMakeConcurrentProgress() throws Exception {
        Path fixture = temp.resolve("part.parquet");
        try (var writer = ParquetFixtures.open(fixture)) {
            writer.write(ObjectEntries.bare("a"));
        }
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 16, Set.of(Protocol.S3, Protocol.GCS, Protocol.AZURE), "replay",
                2L * 1024 * 1024, 64 * 1024, Duration.ofSeconds(10), Duration.ofSeconds(30),
                Duration.ofSeconds(30), null, (request, result) -> Duration.ZERO);
        try (ReplayServer server = ReplayServer.open(config);
             HttpClient client = HttpClient.newHttpClient()) {
            server.start();
            assertThat(server.servingMetadata().protocols()).containsExactly("azure", "gcs", "s3");
            assertThat(server.servingMetadata().fixtureIdentity()).isNotEqualTo("unknown");
            assertThat(server.servingMetadata().orderingProfile())
                    .isEqualTo(ServingMetadata.ORDERING_PROFILE);
            List<String> paths = List.of("/bucket?list-type=2", "/storage/v1/b/bucket/o",
                    "/replay/bucket?restype=container&comp=list");
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                long[] encodedBytes = new long[3];
                List<Callable<HttpResponse<String>>> calls = IntStream.range(0, 27)
                        .mapToObj(i -> (Callable<HttpResponse<String>>) () -> {
                            int protocol = i % 3;
                            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + server.port() + paths.get(protocol)));
                            if (protocol == 2) request.header("x-ms-version", "2026-06-06");
                            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
                        }).toList();
                int responseIndex = 0;
                for (var future : workers.invokeAll(calls)) {
                    HttpResponse<String> response = future.get();
                    assertThat(response.statusCode()).isEqualTo(200);
                    encodedBytes[responseIndex++ % 3] += response.body()
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                }
                String[] protocols = {"s3", "gcs", "azure"};
                for (int i = 0; i < protocols.length; i++) {
                    String protocol = protocols[i];
                    assertThat(server.metrics().registry().get("swath.replay.protocol.http.requests")
                            .tags("protocol", protocol, "status_class", "2xx")
                            .counter().count()).isEqualTo(9);
                    assertThat(server.metrics().registry().get("swath.replay.protocol.objects")
                            .tags("protocol", protocol, "shape", "page")
                            .counter().count()).isEqualTo(9);
                    assertThat(server.metrics().registry().get("swath.replay.protocol.prefixes")
                            .tags("protocol", protocol, "shape", "page")
                            .counter().count()).isZero();
                    assertThat(server.metrics().registry().get("swath.replay.protocol.encoded.bytes")
                            .tags("protocol", protocol, "shape", "page")
                            .counter().count()).isEqualTo(encodedBytes[i]);
                }
            }
            long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (server.servingMetadata().activeResponses() != 0 && System.nanoTime() < drainDeadline) {
                Thread.sleep(5);
            }
            assertThat(server.servingMetadata().activeResponses()).isZero();
            assertThat(server.servingMetadata().chargedResponseBytes()).isZero();
        }
    }
}
