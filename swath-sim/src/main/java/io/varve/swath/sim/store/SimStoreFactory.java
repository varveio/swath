/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.ListingStore;
import java.nio.file.Path;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a {@link SimStoreBackend} choice into a concrete store over a fixture, the simulator's
 * counterpart to the replay server's {@code ReplayServingFactory}. One shared {@link MeterRegistry}
 * carries both the tier-selection signals ({@link SimStoreMetrics}) and the per-read meters the
 * reused replay stores publish ({@link ReplayMetrics}), so a run is self-describing from one
 * registry; reach it through {@code result.metrics().registry()}.
 *
 * <ul>
 *   <li>{@link SimStoreBackend#PARQUET} — the replay module's Parquet-backed store, unchanged:
 *       full metadata, and the reference the arena is differentially tested against.</li>
 *   <li>{@link SimStoreBackend#ARENA} — require the keys-only arena; a fixture whose keys exceed
 *       the configured budget fails fast rather than silently serving a slower tier.</li>
 *   <li>{@link SimStoreBackend#AUTO} — arena when it fits, else the Parquet store, recording
 *       {@code swath.sim.store.arena.decline\{reason\}} and logging why.</li>
 * </ul>
 *
 * <p><b>Fixtures are local paths.</b> {@code fixturePath} is a swath Parquet capture file or a
 * directory of them, on this machine. Nothing here resolves a remote object-store location:
 * fetching a fixture to local disk is the caller's job, deliberately outside this seam.
 */
public final class SimStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(SimStoreFactory.class);

    private SimStoreFactory() {
    }

    /** The resolved store, the backend actually chosen, and the metrics that back it. */
    public record Result(ListingStore store, SimStoreBackend resolvedBackend, ReplayMetrics metrics) {
    }

    /**
     * Opens {@code fixturePath} under the requested {@code backend}.
     *
     * @throws IllegalArgumentException under {@link SimStoreBackend#ARENA} when the fixture's keys
     *                                  do not fit {@link SimStoreConfig#arenaMaxEncodedBytes()}
     */
    public static Result open(Path fixturePath, SimStoreBackend backend, SimStoreConfig config) {
        MeterRegistry registry = new SimpleMeterRegistry();
        SimStoreMetrics simMetrics = new SimStoreMetrics(registry);
        // Every backend reads the fixture through the Parquet-backed store -- the arena tiers only
        // differ in whether they keep it -- so the replay-side serving tag describes that store
        // truthfully in all three cases. Which backend SERVES the simulation is a separate signal,
        // swath.sim.store.backend, because the two genuinely differ under ARENA and AUTO.
        ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);

        return switch (backend) {
            case PARQUET -> resolved(parquetStore(fixturePath, metrics), SimStoreBackend.PARQUET,
                    metrics, simMetrics);
            case ARENA -> {
                ArenaListingStore arena;
                try (ListingStore source = parquetStore(fixturePath, metrics)) {
                    arena = loadArena(source, config).orElseThrow(() -> new IllegalArgumentException(
                            "backend " + SimStoreBackend.ARENA + " requires a fixture whose encoded keys fit in "
                                    + config.arenaMaxEncodedBytes() + " bytes (raise "
                                    + SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY + ", or use "
                                    + SimStoreBackend.PARQUET + "): " + fixturePath));
                }
                yield resolved(arena, SimStoreBackend.ARENA, metrics, simMetrics);
            }
            case AUTO -> {
                ListingStore source = parquetStore(fixturePath, metrics);
                Optional<ArenaListingStore> arena;
                try {
                    arena = loadArena(source, config);
                } catch (RuntimeException e) {
                    source.close();
                    throw e;
                }
                if (arena.isPresent()) {
                    source.close();
                    yield resolved(arena.get(), SimStoreBackend.ARENA, metrics, simMetrics);
                }
                simMetrics.recordArenaDecline(SimStoreMetrics.DECLINE_OVER_BUDGET);
                log.info("sim_store auto declined the arena tier (encoded keys exceed {} bytes) "
                        + "— falling back to {} for {}", config.arenaMaxEncodedBytes(),
                        SimStoreBackend.PARQUET, fixturePath);
                yield resolved(source, SimStoreBackend.PARQUET, metrics, simMetrics);
            }
        };
    }

    private static Result resolved(ListingStore store, SimStoreBackend backend, ReplayMetrics metrics,
                                   SimStoreMetrics simMetrics) {
        simMetrics.recordBackend(backend);
        return new Result(store, backend, metrics);
    }

    private static Optional<ArenaListingStore> loadArena(ListingStore source, SimStoreConfig config) {
        return ArenaListingStore.loadWithin(source, config.arenaMaxEncodedBytes());
    }

    private static ListingStore parquetStore(Path fixturePath, ReplayMetrics metrics) {
        return new DuckDbListingStore(fixturePath, metrics, DuckDbListingStore.defaultConnectionCount());
    }
}
