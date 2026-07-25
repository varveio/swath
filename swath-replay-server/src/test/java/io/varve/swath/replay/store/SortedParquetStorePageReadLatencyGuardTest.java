/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A small-scale, CI-fast page-read-latency sanity guard, not a corridor-scale reproduction: tens of
 * thousands of rows never touches enough real Parquet decode volume to reproduce the real corridor
 * miss. Its only job is to catch a GROSS regression in the store's connection/query path — a broken
 * pool that opens a fresh connection per request, or an accidental {@code threads=1} pragma, either
 * of which multiplies per-page cost several-fold over the baseline.
 */
@Tag("perf")
class SortedParquetStorePageReadLatencyGuardTest {

    private static final int ROWS = 30_000;
    private static final int PAGES_TO_SAMPLE = 20;

    @Test
    void perPageLatencyStaysSaneAcrossManyRowGroups(@TempDir Path dir) throws IOException {
        List<String> keys = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            keys.add(String.format("bucket-data/prefix-%06d/object-%06d.dat", i, i));
        }
        Path capture = Files.createDirectories(dir.resolve("capture"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String key : keys) {
                writer.write(object(key));
            }
        }
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        // Small row groups so the multi-row-group routing path is genuinely exercised (matching
        // production's row-group-granularity concern), without the multi-minute build a
        // multi-million-row fixture would need for a routine test-suite member.
        SortConfig manySmallGroups =
                SortConfigs.base().withFinalRowGroupBytes(32L << 10).withMergeBudgetBytes(64L << 20);
        new CaptureSorter(manySmallGroups).sort(capture, outputDir);
        List<Path> files = SortedFixtures.resolveFiles(outputDir);
        IndexLoadResult loaded = SortedFixtures.loadIndex(files, new FixtureMetrics());
        List<IndexEntry> index = ((IndexLoadResult.Loaded) loaded).entries();
        assertThat(index).hasSizeGreaterThan(1);

        ReplayMetrics metrics = new ReplayMetrics();
        try (SortedParquetStore store = new SortedParquetStore(files, index, metrics, 1)) {
            ByteKey from = null;
            boolean inclusive = true;
            List<Double> pageMs = new ArrayList<>();
            for (int page = 0; page < PAGES_TO_SAMPLE; page++) {
                long t0 = System.nanoTime();
                var rows = store.rows(from, inclusive, null, 1000, Projection.WITH_OWNER);
                pageMs.add((System.nanoTime() - t0) / 1_000_000.0);
                assertThat(rows).isNotEmpty();
                from = ByteKey.copyOf(rows.get(rows.size() - 1).key());
                inclusive = false;
            }
            double avgMs = pageMs.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            System.out.printf("SortedParquetStore page-read sanity guard: pages=%d avg_ms=%.3f%n",
                    pageMs.size(), avgMs);
            // Loose bound: catches the connection/query-path regression described in the class
            // javadoc, not a corridor-scale reproduction.
            assertThat(avgMs).isLessThan(200.0);
        }
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.key(key).size(4096L).etag("etag").storageClass("STANDARD").isLatest(true)
                .owner("owner-id", "owner-display").checksum("CRC32", "FULL_OBJECT").build();
    }
}
