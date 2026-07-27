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
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SimStoreFactoryTest {

    private static final List<String> KEYS = List.of("alpha", "beta", "gamma");

    private static final SimStoreConfig GENEROUS = new SimStoreConfig(1L << 20);

    /** Room for two 8-byte keys — far less than the fixture below needs. */
    private static final SimStoreConfig TINY = new SimStoreConfig(KeyArena.encodedBytes(16, 2));

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

    private static Path fixture(Path dir) throws IOException {
        Path capture = dir.resolve("part-0.parquet");
        try (var writer = ParquetFixtures.open(capture)) {
            for (String key : KEYS) {
                writer.write(ObjectEntries.withOwner(key, "etag-" + key));
            }
        }
        return capture;
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
