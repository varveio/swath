/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.testkit.HttpProbe;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import org.junit.jupiter.api.Test;

class MetricsEndpointTest {

    @Test
    void servesTheRegistryAsJsonOnItsOwnPort() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");
                String body = scrape(endpoint, "/metrics");

                assertThat(body).contains("\"schema_version\":1");
                assertThat(body).contains("\"serving_mode\":\"sorted\"");
                assertThat(body).contains("\"swath.replay.http.requests\"");
                assertThat(body).contains("\"swath.replay.fixture.list.latency\"");
                // The timer fields a headroom check reads, present and typed as numbers.
                assertThat(body).containsPattern("\"name\":\"swath.replay.page.read.latency\".*?\"p99_ms\":");
            }
        }
    }

    @Test
    void scrapingDoesNotPerturbWhatItMeasures() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "duckdb", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");
                long afterOneList = server.metrics().snapshot().httpRequests();
                scrape(endpoint, "/metrics");
                scrape(endpoint, "/metrics");

                // The whole reason the endpoint has its own connector: a harness may poll it
                // through a measured window without becoming part of the measurement.
                assertThat(server.metrics().snapshot().httpRequests()).isEqualTo(afterOneList);
            }
        }
    }

    @Test
    void answersHealthzAndRefusesAnythingElse() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                assertThat(scrape(endpoint, "/healthz")).isEqualTo("ok\n");
                assertThat(status(endpoint, "/bucket?list-type=2")).isEqualTo(404);
            }
        }
    }

    private static ReplayServer server() {
        return new ReplayServer("127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(
                new S3ResultEntry.ObjectResult(new ListedObject(
                        "a/1".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        1, 0, "etag", "STANDARD", null, null, null, null))),
                false, null));
    }

    private static String scrape(MetricsEndpoint endpoint, String path) throws Exception {
        return send(endpoint, path).body();
    }

    private static int status(MetricsEndpoint endpoint, String path) throws Exception {
        return send(endpoint, path).statusCode();
    }

    private static HttpResponse<String> send(MetricsEndpoint endpoint, String path) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + endpoint.port() + path)).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }
}
