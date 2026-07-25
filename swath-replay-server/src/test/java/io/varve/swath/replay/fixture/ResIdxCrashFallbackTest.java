/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ReplayServingFactory;
import io.varve.swath.replay.server.ServingMode;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RES-IDX-1 (critical case): the {@code sort-fixture} atomicity / crash-fallback contract. It guards the
 * serving side against a <b>partially-built</b> sorted fixture — the invariant that a final
 * stamped file never appears before the sort is complete, and that {@code --serving-mode auto}
 * declines a directory that only carries a crash leftover (a {@code *.tmp}), falling back to
 * DuckDB and recording the metric, then serves sorted once a real build completes.
 */
class ResIdxCrashFallbackTest {

    /**
     * The atomicity contract itself: a sort that aborts mid-transform (here a genuine abort — the
     * §0.5 duplicate-key fail-fast) leaves <b>no</b> final {@code part-*.parquet}; only a
     * {@code *.tmp} and/or partial staging may remain. A subsequent clean sort into the same output
     * directory then publishes a complete stamped file. This is exactly why {@code auto} can trust
     * "a final file exists" to mean "the sort finished".
     */
    @Test
    void anAbortedSortLeavesNoFinalFileAndACleanReRunPublishesOne(@TempDir Path dir) throws IOException {
        Path duplicateCapture = Files.createDirectories(dir.resolve("dup-capture"));
        writeUnsortedPart(duplicateCapture.resolve("part-0.parquet"), "a", "b", "b");   // adjacent-equal 'b'
        Path outputDir = Files.createDirectories(dir.resolve("out"));

        assertThatThrownBy(() -> new CaptureSorter(config()).sort(duplicateCapture, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class);

        // No final file appeared before completeness — the whole point of the tmp-then-rename publish.
        assertThat(finalSortedFiles(outputDir)).isEmpty();

        // A clean re-run into the same directory sweeps any stale tmp/staging and publishes.
        Path cleanCapture = Files.createDirectories(dir.resolve("clean-capture"));
        writeUnsortedPart(cleanCapture.resolve("part-0.parquet"), "c", "a", "b");
        var result = new CaptureSorter(config()).sort(cleanCapture, outputDir);
        assertThat(finalSortedFiles(outputDir)).isNotEmpty();
        assertThat(result.finalFiles()).allSatisfy(f -> assertThat(Files.exists(f)).isTrue());
    }

    /**
     * A directory carrying a crash-leftover {@code *.tmp} but no completed stamped file is served via
     * DuckDB by {@code auto}, with the {@code serving.fallback\{reason=no_stamp\}} metric recorded —
     * the crash tmp is never mistaken for a servable sorted file (it isn't a {@code *.parquet}). A
     * properly-built sorted fixture then resolves to the sorted path with no fallback.
     */
    @Test
    void autoDeclinesACrashTmpDirectoryWithAMetricThenServesSortedAfterARealBuild(@TempDir Path dir)
            throws Exception {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "delta", "alpha", "charlie", "bravo");

        // The state a killed in-place sort leaves: the raw capture the operator pointed at, plus an
        // abandoned part-*.parquet.tmp. No completed stamped file exists.
        Path crashDir = Files.createDirectories(dir.resolve("crash"));
        writeUnsortedPart(crashDir.resolve("part-0.parquet"), "delta", "alpha", "charlie", "bravo");
        Files.writeString(crashDir.resolve("part-00001.parquet.tmp"), "partial-crash-bytes");

        ReplayServingFactory.Result declined = ReplayServingFactory.open(crashDir, ServingMode.AUTO, 2);
        try {
            assertThat(declined.resolvedMode()).isEqualTo(ServingMode.DUCKDB);
            assertThat(declined.metrics().registry()
                    .find("swath.replay.serving.fallback").tag("reason", "no_stamp").counter().count())
                    .isEqualTo(1.0);
            // DuckDB fallback still serves the raw capture's objects.
            assertThat(listKeys(declined)).containsExactly("alpha", "bravo", "charlie", "delta");
        } finally {
            declined.fixture().close();
        }

        // A real sort-fixture build into a clean output directory: auto now serves sorted, no fallback.
        Path sortedOut = Files.createDirectories(dir.resolve("part-out"));
        new CaptureSorter(config()).sort(capture, sortedOut);

        ReplayServingFactory.Result sorted = ReplayServingFactory.open(sortedOut, ServingMode.AUTO, 2);
        try {
            assertThat(sorted.resolvedMode()).isEqualTo(ServingMode.SORTED);
            assertThat(sorted.metrics().registry().find("swath.replay.serving.fallback").counter()).isNull();
            assertThat(listKeys(sorted)).containsExactly("alpha", "bravo", "charlie", "delta");
        } finally {
            sorted.fixture().close();
        }
    }

    // --- helpers --------------------------------------------------------------

    private static SortConfig config() {
        return SortConfig.fromSystemProperties();
    }

    private static List<Path> finalSortedFiles(Path outputDir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputDir, "part-*.parquet")) {
            for (Path p : stream) {
                files.add(p);
            }
        }
        return files;
    }

    private static List<String> listKeys(ReplayServingFactory.Result result) {
        S3ListResult page = result.fixture().list(
                new S3ListRequest("bucket", null, null, null, null, 1000, false, false));
        List<String> keys = new ArrayList<>();
        for (S3ResultEntry entry : page.entries()) {
            if (entry instanceof S3ResultEntry.ObjectResult object) {
                keys.add(new String(object.object().key(), StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    private static void writeUnsortedPart(Path path, String... keys) throws IOException {
        try (var writer = ParquetFixtures.open(path)) {
            for (String k : keys) {
                writer.write(ObjectEntries.key(k).etag("etag").storageClass("STANDARD").isLatest(true).build());
            }
        }
    }
}
