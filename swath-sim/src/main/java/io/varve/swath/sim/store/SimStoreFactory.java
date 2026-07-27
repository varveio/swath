/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedEligibility;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
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
 *   <li>{@link SimStoreBackend#WINDOWED} — require the windowed row-group prefetch over the
 *       replay module's sorted-Parquet store; a fixture that is not sorted-eligible ({@link
 *       SortedEligibility}) fails fast rather than silently serving a slower tier.</li>
 *   <li>{@link SimStoreBackend#AUTO} — arena when it fits, else windowed when the fixture is
 *       sorted-eligible, else the Parquet store, recording
 *       {@code swath.sim.store.arena.decline\{reason\}} / {@code swath.sim.store.windowed.decline\{reason\}}
 *       and logging why.</li>
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
     * Opens {@code fixturePath} under the requested {@code backend}, configured from the
     * {@code swath.sim.*} system properties — the entry point a runner uses, so an operator can
     * retune the tier threshold without a code change (the same idiom as the replay server's
     * {@code swath.replay.prefetch.*}). Tests that pin a threshold pass an explicit config instead.
     */
    public static Result open(Path fixturePath, SimStoreBackend backend) {
        return open(fixturePath, backend, SimStoreConfig.fromSystemProperties());
    }

    /**
     * Opens {@code fixturePath} under the requested {@code backend} and an explicit {@code config}.
     *
     * @throws IllegalArgumentException under {@link SimStoreBackend#ARENA} when the fixture's keys
     *                                  do not fit {@link SimStoreConfig#arenaMaxEncodedBytes()}, or
     *                                  under {@link SimStoreBackend#WINDOWED} when the fixture is
     *                                  not sorted-eligible
     */
    public static Result open(Path fixturePath, SimStoreBackend backend, SimStoreConfig config) {
        MeterRegistry registry = new SimpleMeterRegistry();
        SimStoreMetrics simMetrics = new SimStoreMetrics(registry);
        // The same registry the sorted-eligibility derive pass records into (swath.replay.index.*)
        // -- one shared registry describes the whole resolution, exactly as each branch's own
        // ReplayMetrics does.
        FixtureMetrics fixtureMetrics = new FixtureMetrics(registry);

        return switch (backend) {
            case PARQUET -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
                yield resolved(parquetStore(fixturePath, metrics), SimStoreBackend.PARQUET, metrics, simMetrics);
            }
            case ARENA -> {
                // SERVING_MODE_DUCKDB, and that is not a mislabel: the arena reads the fixture through
                // the DuckDB-over-Parquet store to stream keys out, and only then decides whether to
                // keep it, so the tag describes the store that actually touched the file, truthfully.
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
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
            case WINDOWED -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                List<Path> files = resolveFiles(fixturePath);
                // recordFallbackOnFailure=false: a forced backend that declines hard-fails, so this
                // is not a "fallback" any more than ARENA's own forced decline above is.
                SortedEligibility.Result eligibility = SortedEligibility.decide(files, fixtureMetrics, false);
                if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
                    yield resolved(windowedStore(files, eligible.index(), metrics), SimStoreBackend.WINDOWED,
                            metrics, simMetrics);
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                throw new IllegalArgumentException(
                        "backend " + SimStoreBackend.WINDOWED + " requires a sorted-eligible fixture ("
                                + reason + "), use " + SimStoreBackend.PARQUET + " instead: " + fixturePath);
            }
            case AUTO -> {
                ReplayMetrics duckdbMetrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
                ListingStore source = parquetStore(fixturePath, duckdbMetrics);
                Optional<ArenaListingStore> arena;
                try {
                    arena = loadArena(source, config);
                } catch (RuntimeException e) {
                    source.close();
                    throw e;
                }
                if (arena.isPresent()) {
                    source.close();
                    yield resolved(arena.get(), SimStoreBackend.ARENA, duckdbMetrics, simMetrics);
                }
                simMetrics.recordArenaDecline(SimStoreMetrics.DECLINE_OVER_BUDGET);
                log.info("sim_store auto declined the arena tier (encoded keys exceed {} bytes) "
                        + "— trying the windowed tier next for {}", config.arenaMaxEncodedBytes(), fixturePath);

                // source is still open here (the abandoned DuckDB pool, kept as the PARQUET fallback
                // in case windowed also declines) -- a resolveFiles/decide failure must close it too,
                // exactly like the arena step just above, or it leaks.
                List<Path> files;
                SortedEligibility.Result eligibility;
                try {
                    files = resolveFiles(fixturePath);
                    eligibility = SortedEligibility.decide(files, fixtureMetrics, true);
                } catch (RuntimeException e) {
                    source.close();
                    throw e;
                }
                if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
                    source.close();
                    ReplayMetrics sortedMetrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                    yield resolved(windowedStore(files, eligible.index(), sortedMetrics), SimStoreBackend.WINDOWED,
                            sortedMetrics, simMetrics);
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                simMetrics.recordWindowedDecline(reason);
                log.info("sim_store auto declined the windowed tier (reason={}) — falling back to {} for {}",
                        reason, SimStoreBackend.PARQUET, fixturePath);
                yield resolved(source, SimStoreBackend.PARQUET, duckdbMetrics, simMetrics);
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

    /**
     * The windowed tier: {@link WindowedListingStore} wrapping the replay module's sorted-Parquet
     * store as-is, in process — no JDBC/HTTP round trip, just the decorator's sequential-window
     * prefetch over an already-local reader. {@code window-rows}/{@code max-windows}/{@code enabled}
     * come from the same {@code swath.replay.prefetch.*} properties the replay server's
     * sorted-serving path reads, rather than a second sim-only knob for the same tuning — including
     * {@code enabled}: an operator who has turned prefetch off for the replay server must see the
     * identical store served bare here too, mirroring {@code ReplayServingFactory#sorted}.
     */
    private static ListingStore windowedStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics) {
        // Read the prefetch config before opening any connection, so a malformed
        // swath.replay.prefetch.* property can't leak a freshly-opened DuckDB pool.
        WindowedListingStore.Config prefetch = WindowedListingStore.Config.fromSystemProperties();
        int connections = SortedParquetStore.defaultConnectionCount();
        if (!prefetch.enabled()) {
            log.info("sim_store windowed prefetch DISABLED (bare store) for {}", files);
            return new SortedParquetStore(files, index, metrics, connections);
        }
        // recordPageReadLatency=false: the wrapper owns the outer per-page timer, same reasoning as
        // the replay server's own sorted-serving wiring (SortedParquetStore's javadoc).
        SortedParquetStore backing = new SortedParquetStore(files, index, metrics, connections, false);
        log.info("sim_store windowed prefetch ENABLED (window_rows={} max_windows={}) for {}",
                prefetch.windowRows(), prefetch.maxWindows(), files);
        return new WindowedListingStore(backing, metrics, prefetch.windowRows(), prefetch.maxWindows());
    }

    private static List<Path> resolveFiles(Path fixturePath) {
        try {
            return SortedFixtures.resolveFiles(fixturePath);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve fixture files for " + fixturePath, e);
        }
    }
}
