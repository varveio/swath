/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortStamp;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * The {@code sort-fixture} CLI subcommand end-to-end: real invocation through picocli, atomic
 * publish, the printed summary, and the duplicate-key/versioned-capture fail-fast exit codes
 * (§0.5/§0.6). The sort/detection logic itself is covered by {@link CaptureSorterTest}
 * (root) and {@link SortedFixturesTest}; this class only proves the subcommand wiring.
 */
class SortFixtureCommandTest {

    @Test
    void sortsACaptureDirectoryAndPrintsASummary(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "c", "a", "b");
        Path outputDir = Files.createDirectories(dir.resolve("out"));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FixtureMetrics metrics = new FixtureMetrics(registry);
        SortFixtureCommand command = new SortFixtureCommand(metrics);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        int exit;
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
            exit = runViaCli(command, "--capture", capture.toString(), "--output", outputDir.toString());
        } finally {
            System.setOut(original);
        }

        assertThat(exit).isEqualTo(0);
        Path output = outputDir.resolve("part-00000.parquet");
        assertThat(Files.exists(output)).isTrue();
        assertThat(SortStamp.read(output)).isPresent();

        assertThat(registry.find("swath.replay.sortfixture.build.latency").timer().count()).isEqualTo(1);
        assertThat(registry.find("swath.replay.sortfixture.output.bytes").summary().totalAmount())
                .isGreaterThan(0.0);

        // The printed summary carries segments/merge_passes, and the real SORT.segment_flushed
        // engagement counter is recorded via FixtureMetrics#recordStealReason.
        String summary = out.toString(StandardCharsets.UTF_8);
        assertThat(summary).contains("arm=SORT_FIXTURE", "segments=", "merge_passes=", "cascaded_passes=");
        assertThat(registry.find("swath.replay.sort.steal_reason")
                .tag("outcome", "SORT").tag("reason", "segment_flushed").counter().count())
                .isGreaterThan(0.0);
        assertThat(registry.find("swath.replay.sort.progress").counter().count()).isGreaterThan(0.0);
    }

    @Test
    void duplicateKeyCaptureExitsNonZeroWithAClearMessage(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "a", "b", "b");
        Path outputDir = Files.createDirectories(dir.resolve("out"));

        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream original = System.err;
        int exit;
        try {
            System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
            exit = runViaCli(new SortFixtureCommand(new FixtureMetrics()),
                    "--capture", capture.toString(), "--output", outputDir.toString());
        } finally {
            System.setErr(original);
        }

        assertThat(exit).isNotEqualTo(0);
        assertThat(err.toString(StandardCharsets.UTF_8)).contains("sort-fixture").contains("'b'");
    }

    @Test
    void versionedCaptureExitsNonZeroWithAClearMessage(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        Path part = capture.resolve("part-0.parquet");
        try (var writer = ParquetFixtures.open(part)) {
            writer.write(object("a"));
            writer.write(ObjectEntries.key("m").versionId("v1").isLatest(true).build());
        }
        Path outputDir = Files.createDirectories(dir.resolve("out"));

        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream original = System.err;
        int exit;
        try {
            System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
            exit = runViaCli(new SortFixtureCommand(new FixtureMetrics()),
                    "--capture", capture.toString(), "--output", outputDir.toString());
        } finally {
            System.setErr(original);
        }

        assertThat(exit).isNotEqualTo(0);
        assertThat(err.toString(StandardCharsets.UTF_8)).contains("sort-fixture").contains("'m'");
    }

    @Test
    void crashLeavesOnlyATmpFileWhichIsCleanedOnTheNextRun(@TempDir Path dir) throws IOException {
        // Simulate a crashed prior sort-fixture attempt: a stale final .tmp in outputDir, and a
        // stale partial segment in this engine's own fixed staging dir.
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "a", "b");
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path staleTmp = Files.createFile(outputDir.resolve("part-00000.parquet.tmp"));
        Files.createDirectories(outputDir.resolve(CaptureSorter.STAGING_DIR_NAME));
        Files.createFile(outputDir.resolve(CaptureSorter.STAGING_DIR_NAME).resolve("fixture-0.parquet"));

        int exit = runViaCli(new SortFixtureCommand(new FixtureMetrics()),
                "--capture", capture.toString(), "--output", outputDir.toString());

        assertThat(exit).isEqualTo(0);
        assertThat(Files.exists(staleTmp)).isFalse();
        assertThat(Files.exists(outputDir.resolve(CaptureSorter.STAGING_DIR_NAME))).isFalse();
        assertThat(SortStamp.read(outputDir.resolve("part-00000.parquet"))).isPresent();
    }

    // --- helpers ---

    private static int runViaCli(SortFixtureCommand command, String... args) {
        return new CommandLine(command).execute(args);
    }

    private static void writeUnsortedPart(Path path, String... keys) throws IOException {
        try (var writer = ParquetFixtures.open(path)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.bare(key);
    }
}
