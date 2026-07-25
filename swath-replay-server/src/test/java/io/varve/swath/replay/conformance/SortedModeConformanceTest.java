/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.server.ReplayServer;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The in-JVM HAR conformance comparator re-run against a <b>stamped sorted fixture</b>. The
 * expected XML is produced by the DuckDB role-1 oracle over the unsorted capture; the comparator
 * then serves the SAME logical objects from the sorted role-2 fixture (via {@code --serving-mode
 * auto} → sorted) and must match byte-for-byte through the real HTTP surface. The HAR/Docker smoke
 * test against a real bucket is a separate check, not exercised here.
 */
class SortedModeConformanceTest {

    private static final List<String> QUERIES = List.of(
            "/bucket?list-type=2&max-keys=1000&encoding-type=url",
            "/bucket?list-type=2&max-keys=1000&encoding-type=url&fetch-owner=true",
            "/bucket?list-type=2&prefix=a%2F&max-keys=1000&encoding-type=url",
            "/bucket?list-type=2&delimiter=%2F&max-keys=1000&encoding-type=url",
            "/bucket?list-type=2&prefix=a%2F&delimiter=%2F&max-keys=1000&encoding-type=url",
            "/bucket?list-type=2&start-after=a%2F1.txt&max-keys=1000&encoding-type=url&fetch-owner=true");

    @Test
    void sortedFixtureConformsToTheDuckDbOracleThroughTheHarComparator(@TempDir Path dir) throws Exception {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeCapture(capture.resolve("part-0.parquet"));

        Path sortedFixture = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, sortedFixture);

        // Guard: auto must genuinely resolve this fixture to the SORTED path (not silently fall back),
        // so the conformance run below actually exercises role 2.
        try (ReplayServer sortedServer = new ReplayServer(
                "127.0.0.1", 0, "bucket", sortedFixture, 0, ServingMode.AUTO)) {
            sortedServer.start();
            assertThat(sortedServer.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
        }

        // Expected XML = the DuckDB oracle over the UNSORTED capture; HAR it, then compare the sorted
        // fixture against it through the comparator (which serves it via auto → sorted).
        List<String> harEntries = new ArrayList<>();
        try (ReplayServer oracle = new ReplayServer(
                "127.0.0.1", 0, "bucket", capture, 0, ServingMode.DUCKDB)) {
            oracle.start();
            assertThat(oracle.resolvedServingMode()).isEqualTo(ServingMode.DUCKDB);
            HttpClient client = HttpClient.newHttpClient();
            for (String query : QUERIES) {
                harEntries.add(entry("http://127.0.0.1:19090" + query, HttpProbe.body(oracle, client, query)));
            }
        }
        Path har = dir.resolve("s3.har");
        Files.writeString(har, harLog(harEntries), StandardCharsets.UTF_8);

        var summary = ReplayConformanceComparator.compare(new ReplayConformanceComparator.Options(
                har, sortedFixture, "bucket", "127.0.0.1", dir.resolve("mismatch"), 0, 0,
                Duration.ofSeconds(20), 4, 2));

        assertThat(summary.compared()).isEqualTo(QUERIES.size());
        assertThat(summary.failures()).isEmpty();
        assertThat(summary.exitCode()).isZero();
    }

    private static void writeCapture(Path part) throws Exception {
        // Unsorted arrival order + a delimiter'd hierarchy; owner populated so fetch-owner matters.
        ParquetFixtures.write(part,
                object("b/1.txt"), object("a/2.txt"), object("a/dir/deep.txt"), object("a/1.txt"),
                object("c-top"), object("a/3.txt"), object("b/2.txt"));
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.key(key).size(11L).lastModifiedEpochMicros(1_767_225_600_000_000L)
                .etag("etag-" + key).storageClass("STANDARD").isLatest(true)
                .owner("owner-1", "Alice").checksum("CRC32", "FULL_OBJECT").build();
    }

    private static String entry(String url, String body) {
        return """
                {
                  "request": { "method": "GET", "url": %s },
                  "response": {
                    "status": 200,
                    "headers": [ {"name": "Content-Type", "value": "application/xml"} ],
                    "content": { "mimeType": "application/xml", "encoding": "base64", "text": "%s" }
                  }
                }
                """.formatted(json(url), Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static String harLog(List<String> entries) {
        return """
                { "log": { "version": "1.2", "entries": [ %s ] } }
                """.formatted(String.join(",\n", entries));
    }

    private static String json(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
