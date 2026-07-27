/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SimStoreFactoryTest {

    private static final List<String> KEYS = List.of("alpha", "beta", "gamma");

    private static final SimStoreConfig GENEROUS = new SimStoreConfig(1L << 20, 1L << 20);

    /** Room in the ARENA budget for two 8-byte keys — far less than the fixture below needs. */
    private static final SimStoreConfig TINY = new SimStoreConfig(KeyArena.encodedBytes(16, 2), 1L << 20);

    /** Mirrors {@code WindowedListingStore}'s own (package-private) property name. */
    private static final String PREFETCH_ENABLED_PROPERTY = "swath.replay.prefetch.enabled";

    @Test
    void forcedArenaServesStubbedMetadataAndRecordsTheBackend(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture(dir), SimStoreBackend.ARENA, GENEROUS);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            assertThat(store.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst().etag()).isNull();
        }
        assertThat(backendCount(result, SimStoreBackend.ARENA)).isEqualTo(1);
    }

    @Test
    void forcedParquetServesFullMetadataAndRecordsTheBackend(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture(dir), SimStoreBackend.PARQUET, GENEROUS);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.PARQUET);
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            assertThat(store.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst().etag())
                    .isEqualTo("etag-alpha");
        }
        assertThat(backendCount(result, SimStoreBackend.PARQUET)).isEqualTo(1);
    }

    @Test
    void forcedArenaFailsFastWhenTheFixtureExceedsTheBudget(@TempDir Path dir) throws IOException {
        Path fixture = fixture(dir);

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.ARENA, TINY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY);
    }

    @Test
    void forcedWindowedServesFullMetadataAndRecordsTheBackend(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(sortedFixture(dir), SimStoreBackend.WINDOWED, GENEROUS);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.WINDOWED);
            assertThat(store).isInstanceOf(WindowedListingStore.class);
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            assertThat(store.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst().etag())
                    .isEqualTo("etag-alpha");
        }
        assertThat(backendCount(result, SimStoreBackend.WINDOWED)).isEqualTo(1);
    }

    @Test
    void forcedStreamingServesStubbedMetadataAndRecordsTheBackend(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(sortedFixture(dir), SimStoreBackend.STREAMING, GENEROUS);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.STREAMING);
            assertThat(store).isInstanceOf(StreamingListingStore.class);
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            // The sim-mode projection, same as the arena tier: keys are ground truth, metadata is not.
            assertThat(store.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst().etag()).isNull();
        }
        assertThat(backendCount(result, SimStoreBackend.STREAMING)).isEqualTo(1);
    }

    @Test
    void forcedStreamingFailsFastWhenTheFixtureIsNotSortedEligible(@TempDir Path dir) throws IOException {
        Path fixture = fixture(dir);   // unsorted, unstamped

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.STREAMING, GENEROUS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted-eligible");
    }

    @Test
    void forcedWindowedFailsFastWhenTheFixtureIsNotSortedEligible(@TempDir Path dir) throws IOException {
        Path fixture = fixture(dir);   // unsorted, unstamped

        assertThatThrownBy(() -> SimStoreFactory.open(fixture, SimStoreBackend.WINDOWED, GENEROUS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted-eligible");
    }

    @Test
    void autoResolvesToStreamingWhenTheArenaDeclinesAndTheFixtureIsSortedEligible(@TempDir Path dir)
            throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(sortedFixture(dir), SimStoreBackend.AUTO, TINY);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.STREAMING);
            assertThat(store).isInstanceOf(StreamingListingStore.class);
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
        }
        assertThat(backendCount(result, SimStoreBackend.STREAMING)).isEqualTo(1);
        assertThat(registry(result).find(SimStoreMetrics.ARENA_DECLINE_METRIC)
                .tag("reason", SimStoreMetrics.DECLINE_OVER_BUDGET).counter().count()).isEqualTo(1);
        assertThat(registry(result).find(SimStoreMetrics.STREAMING_DECLINE_METRIC).counter()).isNull();
    }

    /**
     * The windowed tier is forced-only (see {@link SimStoreBackend#WINDOWED}): the automatic ladder
     * runs arena → streaming → Parquet and must never land on it, on any fixture — including the
     * sorted-eligible one where it would previously have been chosen.
     */
    @Test
    void autoNeverResolvesToTheWindowedTier(@TempDir Path dir) throws IOException {
        for (Path fixture : List.of(fixture(mkdir(dir, "unsorted")), sortedFixture(mkdir(dir, "sorted")))) {
            for (SimStoreConfig config : List.of(GENEROUS, TINY)) {
                SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.AUTO, config);
                try (var ignored = result.store()) {
                    assertThat(result.resolvedBackend()).isNotEqualTo(SimStoreBackend.WINDOWED);
                }
            }
        }
    }

    @Test
    void autoResolvesToTheArenaWhenTheFixtureFits(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture(dir), SimStoreBackend.AUTO, GENEROUS);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
            assertThat(store).isInstanceOf(ArenaListingStore.class);
        }
        assertThat(backendCount(result, SimStoreBackend.ARENA)).isEqualTo(1);
        assertThat(registry(result).find(SimStoreMetrics.ARENA_DECLINE_METRIC).counter()).isNull();
    }

    @Test
    void autoFallsBackToParquetAndRecordsWhyWhenTheFixtureDoesNotFit(@TempDir Path dir) throws IOException {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture(dir), SimStoreBackend.AUTO, TINY);

        try (var store = result.store()) {
            assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.PARQUET);
            // The fallback store is live, not a closed leftover of the abandoned arena load.
            assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
        }
        assertThat(backendCount(result, SimStoreBackend.PARQUET)).isEqualTo(1);
        assertThat(registry(result).find(SimStoreMetrics.ARENA_DECLINE_METRIC)
                .tag("reason", SimStoreMetrics.DECLINE_OVER_BUDGET).counter().count()).isEqualTo(1);
    }

    /**
     * Each store here reuses a replay-side {@link ReplayMetrics}, and the {@code serving_mode} tag
     * it carries must name the store that ACTUALLY reads the fixture -- {@code sorted} for the two
     * tiers that read the fixture's own sorted layout (windowed wraps {@link SortedParquetStore},
     * streaming decodes its row groups directly; neither goes through
     * {@link io.varve.swath.replay.store.DuckDbListingStore}), {@code duckdb} for
     * arena/parquet/the AUTO cases that don't resolve to one of those -- not a single tag copied
     * across every backend regardless of what actually served.
     */
    @Test
    void eachBackendTagsItsMetricsWithTheStoreThatActuallyServes(@TempDir Path dir) throws IOException {
        assertServingMode(SimStoreFactory.open(fixture(mkdir(dir, "parquet")), SimStoreBackend.PARQUET, GENEROUS),
                ReplayMetrics.SERVING_MODE_DUCKDB);
        assertServingMode(SimStoreFactory.open(fixture(mkdir(dir, "arena")), SimStoreBackend.ARENA, GENEROUS),
                ReplayMetrics.SERVING_MODE_DUCKDB);
        assertServingMode(SimStoreFactory.open(sortedFixture(mkdir(dir, "windowed")), SimStoreBackend.WINDOWED, GENEROUS),
                ReplayMetrics.SERVING_MODE_SORTED);
        assertServingMode(SimStoreFactory.open(sortedFixture(mkdir(dir, "streaming")), SimStoreBackend.STREAMING, GENEROUS),
                ReplayMetrics.SERVING_MODE_SORTED);
        assertServingMode(SimStoreFactory.open(fixture(mkdir(dir, "auto-arena")), SimStoreBackend.AUTO, GENEROUS),
                ReplayMetrics.SERVING_MODE_DUCKDB);
        assertServingMode(SimStoreFactory.open(sortedFixture(mkdir(dir, "auto-streaming")), SimStoreBackend.AUTO, TINY),
                ReplayMetrics.SERVING_MODE_SORTED);
        assertServingMode(SimStoreFactory.open(fixture(mkdir(dir, "auto-parquet")), SimStoreBackend.AUTO, TINY),
                ReplayMetrics.SERVING_MODE_DUCKDB);
    }

    @Test
    void windowedHonoursPrefetchDisabledAndServesTheBareSortedStore(@TempDir Path dir) throws IOException {
        String previous = System.getProperty(PREFETCH_ENABLED_PROPERTY);
        System.setProperty(PREFETCH_ENABLED_PROPERTY, "false");
        try {
            SimStoreFactory.Result result = SimStoreFactory.open(sortedFixture(dir), SimStoreBackend.WINDOWED, GENEROUS);
            try (var store = result.store()) {
                assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.WINDOWED);
                // Bare SortedParquetStore, not the WindowedListingStore decorator -- mirroring
                // ReplayServingFactory#sorted's own bare-store path when prefetch is disabled.
                assertThat(store).isInstanceOf(SortedParquetStore.class);
                assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            }
        } finally {
            restoreProperty(PREFETCH_ENABLED_PROPERTY, previous);
        }
    }

    @Test
    void theConfiglessEntryPointReadsTheThresholdFromTheSystemProperty(@TempDir Path dir) throws IOException {
        // The forced-ARENA failure message tells an operator to raise this property, so the
        // property must actually drive a resolution rather than being a dead accessor.
        Path fixture = fixture(dir);
        System.setProperty(SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY,
                String.valueOf(TINY.arenaMaxEncodedBytes()));
        try {
            SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.AUTO);
            try (var store = result.store()) {
                assertThat(result.resolvedBackend()).isEqualTo(SimStoreBackend.PARQUET);
                assertThat(keys(store.rows(null, true, null, 10, Projection.WITH_OWNER))).isEqualTo(KEYS);
            }
        } finally {
            System.clearProperty(SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY);
        }

        // Without the property the default budget applies and the same fixture fits the arena.
        SimStoreFactory.Result withDefaults = SimStoreFactory.open(fixture, SimStoreBackend.AUTO);
        try (var store = withDefaults.store()) {
            assertThat(withDefaults.resolvedBackend()).isEqualTo(SimStoreBackend.ARENA);
            assertThat(store).isInstanceOf(ArenaListingStore.class);
        }
    }

    /**
     * The streaming tier's residency budget is the other half of the config-less entry point, and its
     * own over-budget failure tells an operator to raise this property — so the property has to
     * actually reach the store rather than being a dead accessor. One byte cannot hold a row group.
     */
    @Test
    void theConfiglessEntryPointReadsTheStreamingResidencyBudgetFromItsSystemProperty(@TempDir Path dir)
            throws IOException {
        Path fixture = sortedFixture(dir);
        System.setProperty(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY, "1");
        try {
            SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.STREAMING);
            try (var store = result.store()) {
                assertThatThrownBy(() -> store.rows(null, true, null, 1, Projection.KEYS_ONLY))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY);
            }
        } finally {
            System.clearProperty(SimStoreConfig.STREAMING_MAX_RESIDENT_BYTES_PROPERTY);
        }
    }

    private static Path fixture(Path dir) throws IOException {
        Path capture = dir.resolve("part-0.parquet");
        try (var writer = ParquetFixtures.open(capture)) {
            for (String key : KEYS) {
                writer.write(ObjectEntries.withOwner(key, "etag-" + key));
            }
        }
        return capture;
    }

    /** As {@link #fixture}, but run through the production sorter so the output is sorted-eligible
     *  (a stamped, single-row-group, {@code mode=objects} directory the windowed tier can serve). */
    private static Path sortedFixture(Path dir) throws IOException {
        Path capture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String key : KEYS) {
                writer.write(ObjectEntries.withOwner(key, "etag-" + key));
            }
        }
        Path out = Files.createDirectory(dir.resolve("out"));
        new CaptureSorter(SortConfigs.base()).sort(capture, out);
        return out;
    }

    private static Path mkdir(Path parent, String name) throws IOException {
        return Files.createDirectory(parent.resolve(name));
    }

    private static void assertServingMode(SimStoreFactory.Result result, String expected) {
        try (var store = result.store()) {
            assertThat(result.metrics().servingMode()).as("resolved %s", result.resolvedBackend())
                    .isEqualTo(expected);
        }
    }

    private static void restoreProperty(String name, String previous) {
        if (previous == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previous);
        }
    }

    private static MeterRegistry registry(SimStoreFactory.Result result) {
        return result.metrics().registry();
    }

    private static double backendCount(SimStoreFactory.Result result, SimStoreBackend backend) {
        return registry(result).find(SimStoreMetrics.BACKEND_METRIC)
                .tag("backend", SimStoreMetrics.tagValue(backend)).counter().count();
    }

    private static List<String> keys(List<ListedObject> rows) {
        return rows.stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList();
    }
}
