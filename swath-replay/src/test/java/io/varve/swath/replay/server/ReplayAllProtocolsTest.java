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
            List<String> paths = List.of("/bucket?list-type=2", "/storage/v1/b/bucket/o",
                    "/replay/bucket?restype=container&comp=list");
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<HttpResponse<String>>> calls = IntStream.range(0, 27)
                        .mapToObj(i -> (Callable<HttpResponse<String>>) () -> {
                            int protocol = i % 3;
                            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + server.port() + paths.get(protocol)));
                            if (protocol == 2) request.header("x-ms-version", "2026-06-06");
                            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
                        }).toList();
                for (var future : workers.invokeAll(calls)) {
                    assertThat(future.get().statusCode()).isEqualTo(200);
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
