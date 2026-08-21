/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayConformanceComparatorTest {

    @Test
    void comparesHarListObjectsResponseAgainstReplayServerAndNormalizesOpaqueToken(@TempDir Path dir)
            throws Exception {
        Path parquet = writeParquet(dir);
        String pathAndQuery = "/bucket?list-type=2&prefix=a%2F&max-keys=1&encoding-type=url";
        String expectedXml = expectedXml(parquet, pathAndQuery);
        expectedXml = expectedXml.replaceAll(
                "<NextContinuationToken>[^<]+</NextContinuationToken>",
                "<NextContinuationToken>REAL_S3_OPAQUE_TOKEN</NextContinuationToken>");

        Path har = dir.resolve("s3.har");
        Files.writeString(har, har("http://127.0.0.1:19090" + pathAndQuery, expectedXml),
                StandardCharsets.UTF_8);

        var summary = ReplayConformanceComparator.compare(new ReplayConformanceComparator.Options(
                har, parquet, "bucket", "127.0.0.1", dir.resolve("mismatch"), 0, 0, Duration.ofSeconds(10), 4, 2));

        assertThat(summary.exitCode()).isZero();
        assertThat(summary.compared()).isEqualTo(1);
        assertThat(summary.failures()).isEmpty();
        assertThat(summary.report()).contains("swath_replay_metrics").contains("parquet_queries=");
    }

    @Test
    void reportsXmlMismatchAndWritesMismatchFiles(@TempDir Path dir) throws Exception {
        Path parquet = writeParquet(dir);
        String pathAndQuery = "/bucket?list-type=2&prefix=a%2F&max-keys=1000&encoding-type=url";
        String expectedXml = expectedXml(parquet, pathAndQuery);
        expectedXml = expectedXml.replace("<KeyCount>2</KeyCount>", "<KeyCount>99</KeyCount>");

        Path har = dir.resolve("s3.har");
        Files.writeString(har, har("http://127.0.0.1:19090" + pathAndQuery, expectedXml),
                StandardCharsets.UTF_8);

        Path mismatch = dir.resolve("mismatch");
        var summary = ReplayConformanceComparator.compare(new ReplayConformanceComparator.Options(
                har, parquet, "bucket", "127.0.0.1", mismatch, 0, 0, Duration.ofSeconds(10), 2, 2));

        assertThat(summary.exitCode()).isEqualTo(1);
        assertThat(summary.failures()).hasSize(1);
        assertThat(mismatch.resolve("00000.expected.xml")).exists();
        assertThat(mismatch.resolve("00000.actual.xml")).exists();
        assertThat(Files.readString(mismatch.resolve("00000.request.txt"))).contains("XML differs");
    }

    @Test
    void samplesComparableRequestsAcrossHar(@TempDir Path dir) throws Exception {
        Path parquet = writeParquet(dir);
        String first = expectedXml(parquet, "/bucket?list-type=2&prefix=a%2F&max-keys=1&encoding-type=url");
        String second = expectedXml(parquet, "/bucket?list-type=2&prefix=a%2F&start-after=a%2F1.txt&max-keys=1&encoding-type=url");
        Path har = dir.resolve("s3.har");
        Files.writeString(har, harEntries(
                entry("http://127.0.0.1:19090/bucket?list-type=2&prefix=a%2F&max-keys=1&encoding-type=url", first),
                entry("http://127.0.0.1:19090/bucket?list-type=2&prefix=a%2F&start-after=a%2F1.txt&max-keys=1&encoding-type=url", second),
                entry("http://127.0.0.1:19090/bucket?list-type=2&prefix=a%2F&max-keys=1&encoding-type=url", first)));

        var summary = ReplayConformanceComparator.compare(new ReplayConformanceComparator.Options(
                har, parquet, "bucket", "127.0.0.1", dir.resolve("mismatch"), 0, 2,
                Duration.ofSeconds(10), 2, 2));

        assertThat(summary.exitCode()).isZero();
        assertThat(summary.eligible()).isEqualTo(3);
        assertThat(summary.compared()).isEqualTo(2);
        assertThat(summary.skipped()).isEqualTo(1);
    }

    private static Path writeParquet(Path dir) throws Exception {
        Path parquet = dir.resolve("part-00000.parquet");
        ParquetFixtures.write(parquet, object("a/1.txt", 10), object("a/2.txt", 20));
        return parquet;
    }

    private static String expectedXml(Path parquet, String pathAndQuery) throws Exception {
        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", parquet)) {
            server.start();
            return HttpProbe.response(server, pathAndQuery).body();
        }
    }

    private static String har(String url, String body) {
        return harEntries(entry(url, body));
    }

    private static String entry(String url, String body) {
        return """
                {
                  "request": {
                    "method": "GET",
                    "url": %s
                  },
                  "response": {
                    "status": 200,
                    "headers": [
                      {"name": "Content-Type", "value": "application/xml"}
                    ],
                    "content": {
                      "mimeType": "application/xml",
                      "encoding": "base64",
                      "text": "%s"
                    }
                  }
                }
                """.formatted(json(url),
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static String harEntries(String... entries) {
        return """
                {
                  "log": {
                    "version": "1.2",
                    "entries": [
                      %s
                    ]
                  }
                }
                """.formatted(String.join(",\n", entries));
    }

    private static ObjectEntry object(String key, long size) {
        return ObjectEntries.key(key).size(size).lastModifiedEpochMicros(1_767_225_600_000_000L)
                .etag("etag-" + size).storageClass("STANDARD").isLatest(true).build();
    }

    private static String json(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
