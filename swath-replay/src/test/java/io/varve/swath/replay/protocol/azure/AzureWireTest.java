/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AzureWireTest {
    @Test
    void httpDatePadsSingleDigitDay() {
        assertThat(AzureHandler.httpDate(Instant.parse("2026-10-01T00:00:00Z")))
                .isEqualTo("Thu, 01 Oct 2026 00:00:00 GMT");
    }

    @Test
    void directRestVersionAndErrorEnvelopes(@TempDir Path dir) throws Exception {
        Path fixture = dir.resolve("part.parquet");
        ParquetFixtures.write(fixture, ObjectEntries.key("a").size(1).isLatest(true).build());
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", 0, "bucket", ServingMode.DUCKDB,
                2, 8, Set.of(Protocol.AZURE), "replay", 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, null);
        try (ReplayServer server = ReplayServer.open(config)) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port() + "/replay/bucket?restype=container&comp=list";
            for (String version : Set.of("2026-06-06", "2026-10-06")) {
                HttpResponse<String> response = get(client, base, version);
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(response.headers().firstValue("x-ms-version")).contains(version);
                assertThat(response.body()).contains("ServiceEndpoint=\"http://127.0.0.1:"
                        + server.port() + "/replay/\"");
                assertThat(response.body()).contains("<NextMarker></NextMarker>");
            }
            HttpResponse<String> unsupported = get(client, base, "2026-04-06");
            assertThat(unsupported.statusCode()).isEqualTo(400);
            assertThat(unsupported.headers().firstValue("x-ms-error-code")).contains("InvalidHeaderValue");
            assertThat(unsupported.body()).contains("<Code>InvalidHeaderValue</Code>");

            HttpResponse<String> duplicateVersion = client.send(HttpRequest.newBuilder(URI.create(base))
                    .header("x-ms-version", "2026-06-06")
                    .header("x-ms-version", "2026-10-06")
                    .GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(duplicateVersion.statusCode()).isEqualTo(400);
            assertThat(duplicateVersion.headers().firstValue("x-ms-error-code"))
                    .contains("InvalidHeaderValue");
            assertThat(duplicateVersion.headers().firstValue("x-ms-version")).isEmpty();

            HttpResponse<String> intersection = get(client, base + "&startFrom=a&delimiter=%2F", "2026-06-06");
            assertThat(intersection.statusCode()).isEqualTo(400);
            assertThat(intersection.headers().firstValue("x-ms-error-code"))
                    .contains("UnsupportedQueryParameter");
            assertThat(intersection.headers().firstValue("x-swath-replay-error"))
                    .contains("startfrom_delimiter_unmeasured");
        }
    }

    private static HttpResponse<String> get(HttpClient client, String url, String version) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).header("x-ms-version", version).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
