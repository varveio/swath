/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServeConfig;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GcsWireTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void nativeRouteWalksOwnTokensAndMapsClientErrors(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("part.parquet");
        ParquetFixtures.write(fixture,
                ObjectEntries.key("a").size(1).isLatest(true).build(),
                ObjectEntries.key("b").size(2).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.GCS), null, 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();
            URI base = URI.create("http://127.0.0.1:" + server.port() + "/storage/v1/b/bucket/o");
            var first = get(client, URI.create(base + "?maxResults=1"));
            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(first.headers().firstValue("content-type").orElse("")).contains("application/json");
            var firstJson = JSON.readTree(first.body());
            assertThat(firstJson.path("items").get(0).path("name").asText()).isEqualTo("a");
            String token = firstJson.path("nextPageToken").asText();
            assertThat(token).startsWith("gcs1.");
            var second = get(client, URI.create(base + "?maxResults=2&pageToken=" + token));
            assertThat(second.statusCode()).isEqualTo(200);
            var secondJson = JSON.readTree(second.body());
            assertThat(secondJson.path("items").get(0).path("name").asText()).isEqualTo("b");
            assertThat(secondJson.has("nextPageToken")).isFalse();

            var malformed = get(client, URI.create(base + "?maxResults=0"));
            assertThat(malformed.statusCode()).isEqualTo(400);
            assertThat(JSON.readTree(malformed.body()).path("error").path("errors").get(0)
                    .path("reason").asText()).isEqualTo("invalid");
            var wrongBucket = get(client, URI.create("http://127.0.0.1:" + server.port()
                    + "/storage/v1/b/other/o"));
            assertThat(wrongBucket.statusCode()).isEqualTo(404);
        }
    }

    private static HttpResponse<String> get(HttpClient client, URI uri) throws Exception {
        return client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
