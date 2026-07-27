/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link StoreThroughputBenchTest#openOrSkip}, unit-tested directly rather than through the
 * {@code @Tag("perf")} class's own multi-second windows: not perf-tagged, so it runs in the ordinary
 * {@code test} task. Regression coverage for a real defect — before this seam existed, a forced
 * backend that declined its fixture (ARENA over its configured byte budget) threw
 * {@link IllegalArgumentException} straight out of the bench's per-backend loop, aborting every
 * later backend's measurement on that fixture rather than just skipping the one that declined.
 */
class StoreThroughputBenchDeclineHandlingTest {

    @TempDir
    private Path dir;

    @Test
    void aDeclinedBackendIsSkippedAndReportedRatherThanThrowing() throws IOException {
        Path fixture = sortedFixture();
        // Room for a fraction of one key: guaranteed to decline under ARENA regardless of what this
        // fixture's keys happen to be.
        SimStoreConfig tooTight = new SimStoreConfig(1, 1L << 20);

        String output = captureStdout(() -> {
            Optional<SimStoreFactory.Result> opened = StoreThroughputBenchTest
                    .openOrSkip(fixture, "sustained", "test", SimStoreBackend.ARENA, tooTight);
            assertThat(opened).isEmpty();
        });

        assertThat(output).contains("store_bench phase=sustained fixture=test backend=ARENA skipped=over-budget");
    }

    @Test
    void anAcceptedBackendOpensNormally() throws IOException {
        Path fixture = sortedFixture();
        SimStoreConfig generous = new SimStoreConfig(1L << 20, 1L << 20);

        Optional<SimStoreFactory.Result> opened = StoreThroughputBenchTest
                .openOrSkip(fixture, "sustained", "test", SimStoreBackend.STREAMING, generous);

        assertThat(opened).isPresent();
        opened.get().store().close();
    }

    /** A small, real, stamped multi-row-group fixture — sorted-eligible, so STREAMING accepts it. */
    private Path sortedFixture() throws IOException {
        Path capture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (int i = 0; i < 50; i++) {
                writer.write(ObjectEntries.withOwner(utf8("key/" + i), "etag-" + i));
            }
        }
        Path out = Files.createDirectory(dir.resolve("out"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, out);
        return out;
    }

    private static String captureStdout(ThrowingRunnable action) throws IOException {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            action.run();
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws IOException;
    }
}
