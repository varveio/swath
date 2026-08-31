/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * A mini-differential (the full adversarial matrix is SortedServingFullDifferentialTest):
 * serve one stamped sorted file two ways — sorted role-2 vs DuckDB role-1 — and walk every page over
 * HTTP with identical requests, asserting byte-identical XML page-for-page in both the flat and the
 * delimiter (seek-to-successor) paths.
 */
@Isolated // One test changes java.util.TimeZone's JVM-global default.
class SortedServingDifferentialTest {

    @Test
    void sortedAndDuckDbServeByteIdenticalPagesFlatAndDelimited(@TempDir Path dir) throws Exception {
        Path sorted = sortedFixture(dir);

        try (ReplayServer sortedServer = new ReplayServer(
                "127.0.0.1", 0, "bucket", sorted, 2, ServingMode.SORTED);
             ReplayServer duckServer = new ReplayServer(
                     "127.0.0.1", 0, "bucket", sorted, 2, ServingMode.DUCKDB)) {
            sortedServer.start();
            duckServer.start();

            assertThat(sortedServer.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
            assertThat(duckServer.resolvedServingMode()).isEqualTo(ServingMode.DUCKDB);

            // Flat listing, small pages force many continuation hops.
            assertThat(walk(sortedServer, null, 25)).isEqualTo(walk(duckServer, null, 25));
            // Delimiter listing exercises the seek-to-successor hybrid + CommonPrefixes rollup.
            assertThat(walk(sortedServer, "/", 4)).isEqualTo(walk(duckServer, "/", 4));
        }
    }

    /**
     * A non-UTC default zone used to make the DuckDB JDBC timestamp conversion disagree with the
     * native sorted readers. Keep this in-process (rather than relying on a CI runner's zone) so a
     * UTC-only runner cannot mask that regression.
    */
    @Test
    void sortedAndDuckDbServeTheTrueUtcTimestampOutsideUtcDefaultZone(@TempDir Path dir) throws Exception {
        TimeZone original = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        try {
            Path sorted = sortedFixture(dir);

            try (ReplayServer sortedServer = new ReplayServer(
                    "127.0.0.1", 0, "bucket", sorted, 2, ServingMode.SORTED);
                 ReplayServer duckServer = new ReplayServer(
                         "127.0.0.1", 0, "bucket", sorted, 2, ServingMode.DUCKDB)) {
                sortedServer.start();
                duckServer.start();

                List<String> sortedFlat = walk(sortedServer, null, 25);
                List<String> duckFlat = walk(duckServer, null, 25);
                assertThat(sortedFlat).isEqualTo(duckFlat);
                // ObjectEntries.withOwner fixes this fixture's mtime at 1_700_000_000_000_000 µs.
                assertThat(HttpProbe.extractTag(sortedFlat.getFirst(), "LastModified"))
                        .isEqualTo("2023-11-14T22:13:20.000Z");

                assertThat(walk(sortedServer, "/", 4)).isEqualTo(walk(duckServer, "/", 4));
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    private static List<String> walk(ReplayServer server, String delimiter, int maxKeys) throws Exception {
        List<String> pages = new ArrayList<>();
        String token = null;
        for (int guard = 0; guard < 10_000; guard++) {
            StringBuilder path = new StringBuilder("/bucket?list-type=2&encoding-type=url&fetch-owner=true&max-keys=")
                    .append(maxKeys);
            if (delimiter != null) {
                path.append("&delimiter=").append(enc(delimiter));
            }
            if (token != null) {
                path.append("&continuation-token=").append(enc(token));
            }
            String body = HttpProbe.body(server, path.toString());
            pages.add(body);
            if (!"true".equals(HttpProbe.extractTag(body, "IsTruncated"))) {
                return pages;
            }
            token = HttpProbe.extractTag(body, "NextContinuationToken");
            assertThat(token).as("truncated page must carry a continuation token").isNotNull();
        }
        throw new AssertionError("listing did not terminate");
    }

    private static Path sortedFixture(Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (int d = 0; d < 6; d++) {
                for (int i = 0; i < 40; i++) {
                    writer.write(object(String.format("dir%d/obj-%03d", d, i)));
                }
            }
            writer.write(object("root-a"));
            writer.write(object("root-b"));
        }
        Path out = Files.createDirectories(dir.resolve("sorted"));
        SortedDatasetResult result = new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, out);
        return result.finalFiles().getFirst();
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.withOwner(key, "etag-" + key);
    }

    // Note: this is java.net.URLEncoder's application/x-www-form-urlencoded encoding (space→'+',
    // a different reserved-char set than the raw %XX-every-byte encoder in the *FullDifferentialTest
    // siblings) — genuinely divergent from HttpProbe.percentEncode, so it stays local rather than
    // being silently unified onto the other encoder.
    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
