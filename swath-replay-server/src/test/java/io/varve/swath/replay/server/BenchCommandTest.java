/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortTransformResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * {@code bench} / {@link TokenWalkBenchmark}: the token-walk benchmark driver against a
 * tiny sorted fixture, in both {@code sorted} and {@code duckdb} serving modes. Exercises the
 * measurement engine directly (numbers are sane, page/key counts agree across modes) and the {@code
 * bench} CLI subcommand end-to-end (human table on stdout, JSON report on request).
 */
class BenchCommandTest {

    private static final int KEY_COUNT = 320;
    private static final int MAX_KEYS = 40;

    @Test
    void reportsSaneAndAgreeingNumbersAcrossServingModes(@TempDir Path dir) throws Exception {
        Path fixture = sortedFixture(dir);
        var options = new TokenWalkBenchmark.Options(
                fixture, "bucket", "127.0.0.1", List.of(ServingMode.SORTED, ServingMode.DUCKDB),
                MAX_KEYS, null, null, 2);

        TokenWalkBenchmark.Report report = TokenWalkBenchmark.run(options);

        assertThat(report.modes()).hasSize(2);
        TokenWalkBenchmark.ModeReport sorted = report.modes().get(0);
        TokenWalkBenchmark.ModeReport duckdb = report.modes().get(1);

        assertThat(sorted.mode()).isEqualTo("sorted");
        assertThat(sorted.resolvedMode()).isEqualTo("sorted");
        assertThat(duckdb.mode()).isEqualTo("duckdb");
        assertThat(duckdb.resolvedMode()).isEqualTo("duckdb");

        // Same fixture walked the same way: page/key counts must agree between serving modes.
        assertThat(sorted.keys()).isEqualTo(KEY_COUNT);
        assertThat(duckdb.keys()).isEqualTo(KEY_COUNT);
        assertThat(sorted.pages()).isEqualTo(duckdb.pages());
        int expectedPages = (KEY_COUNT + MAX_KEYS - 1) / MAX_KEYS;
        assertThat(sorted.pages()).isEqualTo(expectedPages);

        for (TokenWalkBenchmark.ModeReport m : report.modes()) {
            assertThat(m.startupMs()).isGreaterThanOrEqualTo(0.0);
            assertThat(m.wallMs()).isGreaterThan(0.0);
            assertThat(m.pagesPerSec()).isGreaterThan(0.0);
            assertThat(m.keysPerSec()).isGreaterThan(0.0);
            assertThat(m.clientMsPerPageAvg()).isGreaterThanOrEqualTo(0.0);
            assertThat(m.clientMsPerPageP50()).isGreaterThanOrEqualTo(0.0);
            assertThat(m.clientMsPerPageP99()).isGreaterThanOrEqualTo(m.clientMsPerPageP50());
            // One store-level page.read per HTTP page for a flat, non-delimited listing.
            assertThat(m.serverPageRead().count()).isEqualTo(m.pages());
            assertThat(m.serverFixtureList().count()).isEqualTo(m.pages());
            assertThat(m.serverPageRead().avgMs()).isGreaterThanOrEqualTo(0.0);
        }

        assertThat(report.ratios()).hasSize(1);
        TokenWalkBenchmark.RatioEntry ratio = report.ratios().get(0);
        assertThat(ratio.baseMode()).isEqualTo("sorted");
        assertThat(ratio.compareMode()).isEqualTo("duckdb");
        assertThat(ratio.wallMsRatio()).isGreaterThan(0.0);

        String table = report.humanTable();
        assertThat(table).contains("mode=sorted").contains("mode=duckdb").contains("ratio duckdb/sorted");
    }

    @Test
    void cliWritesAHumanTableAndAJsonReportWithAgreeingPageCounts(@TempDir Path dir) throws IOException {
        Path fixture = sortedFixture(dir);
        Path jsonOut = dir.resolve("bench-report.json");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        int exit;
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
            exit = new CommandLine(new BenchCommand())
                    .setCaseInsensitiveEnumValuesAllowed(true)
                    .execute(
                    "--fixture", fixture.toString(),
                    "--bucket", "bucket",
                    "--modes", "sorted,duckdb",
                    "--max-keys", Integer.toString(MAX_KEYS),
                    "--json", jsonOut.toString());
        } finally {
            System.setOut(original);
        }

        assertThat(exit).isEqualTo(0);
        String stdout = out.toString(StandardCharsets.UTF_8);
        assertThat(stdout).contains("mode=sorted").contains("mode=duckdb").contains("bench_json_written");

        assertThat(Files.exists(jsonOut)).isTrue();
        String json = Files.readString(jsonOut, StandardCharsets.UTF_8);
        assertBalancedJson(json);
        assertThat(json).contains("\"bucket\":\"bucket\"");

        int sortedPages = extractPages(json, "sorted");
        int duckdbPages = extractPages(json, "duckdb");
        assertThat(sortedPages).isEqualTo(duckdbPages).isGreaterThan(0);
        assertThat(json).contains("\"ratios\":[{");
    }

    // --- helpers ---

    private static int extractPages(String json, String mode) {
        Matcher matcher = Pattern.compile("\"mode\":\"" + mode + "\".*?\"pages\":(\\d+)", Pattern.DOTALL)
                .matcher(json);
        assertThat(matcher.find()).as("no JSON block found for mode=%s", mode).isTrue();
        return Integer.parseInt(matcher.group(1));
    }

    /** Rudimentary structural well-formedness check (brace/bracket balance outside quoted strings) —
     *  proves the hand-rolled JSON writer emitted something a real parser could read, without pulling
     *  in a JSON library just for this test. */
    private static void assertBalancedJson(String json) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> inString = true;
                case '{', '[' -> depth++;
                case '}', ']' -> depth--;
                default -> { }
            }
            assertThat(depth).as("JSON nesting went negative at index %d", i).isGreaterThanOrEqualTo(0);
        }
        assertThat(inString).as("unterminated JSON string").isFalse();
        assertThat(depth).as("unbalanced JSON braces/brackets").isEqualTo(0);
    }

    private static Path sortedFixture(Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (int i = 0; i < KEY_COUNT; i++) {
                writer.write(object(String.format("key-%05d", i)));
            }
        }
        Path out = Files.createDirectories(dir.resolve("sorted"));
        SortTransformResult result = new CaptureSorter(SortConfig.fromSystemProperties()).sort(capture, out);
        return result.finalFiles().getFirst();
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.withOwner(key, "etag");
    }
}
