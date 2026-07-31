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
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens a local capture fixture with a shared registry for selection and replay-read metrics.
 * The caller closes the {@link Result#store() returned store}; this factory closes temporary sources,
 * including exception paths. Forced backends either open their requested store or fail: ARENA on
 * budget, and STREAMING or WINDOWED with a typed eligibility exclusion. {@link SimStoreBackend#AUTO}
 * resolves ARENA, then eligible STREAMING, then PARQUET; WINDOWED is forced-only.
 *
 * <p>ARENA and STREAMING are keys-only ({@link SimModeRows}); PARQUET and WINDOWED retain metadata.
 * Both sorted backends validate row-group first keys. STREAMING also validates every faulted row
 * group; WINDOWED validates within-group rows only on delimiter skip-scans, so ordinary index-routed
 * ranges can silently omit misplaced keys. ARENA and PARQUET query-sort instead. Within-group
 * STREAMING disorder is typed and counted before rethrow.
 */
public final class SimStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(SimStoreFactory.class);

    private SimStoreFactory() {
    }

    /**
     * A store, its resolved backend, and its metrics. {@code keyCount} is the fixture total from the
     * arena or eligible routing index, not a run result; PARQUET leaves it empty because finding it
     * would require an extra scan.
     */
    public record Result(ListingStore store, SimStoreBackend resolvedBackend, ReplayMetrics metrics,
                         OptionalLong keyCount) {
    }

    /**
     * Opens {@code fixturePath} under the requested {@code backend}, configured from
     * {@code swath.sim.*} system properties.
     */
    public static Result open(Path fixturePath, SimStoreBackend backend) {
        return open(fixturePath, backend, SimStoreConfig.fromSystemProperties());
    }

    /**
     * Opens {@code fixturePath} with {@code config}.
     *
     * @throws IllegalArgumentException   forced ARENA cannot fit the fixture
     * @throws IneligibleFixtureException forced STREAMING or WINDOWED cannot use the fixture
     */
    public static Result open(Path fixturePath, SimStoreBackend backend, SimStoreConfig config) {
        MeterRegistry registry = new SimpleMeterRegistry();
        SimStoreMetrics simMetrics = new SimStoreMetrics(registry);
        FixtureMetrics fixtureMetrics = new FixtureMetrics(registry);

        return switch (backend) {
            case PARQUET -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
                yield resolved(parquetStore(fixturePath, metrics), SimStoreBackend.PARQUET, metrics, simMetrics,
                        OptionalLong.empty());
            }
            case ARENA -> {
                // Arena loading reads its source; the resulting arena owns the served keys.
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
                ArenaListingStore arena;
                try (ListingStore source = parquetStore(fixturePath, metrics)) {
                    arena = loadArena(source, config, fixturePath).orElseThrow(() -> new IllegalArgumentException(
                            "backend " + SimStoreBackend.ARENA + " requires a fixture whose encoded keys fit in "
                                    + config.arenaMaxEncodedBytes() + " bytes (raise "
                                    + SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY + ", or use "
                                    + SimStoreBackend.PARQUET + "): " + fixturePath));
                }
                yield resolved(arena, SimStoreBackend.ARENA, metrics, simMetrics,
                        OptionalLong.of(arena.keyCount()));
            }
            case STREAMING -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                List<Path> files = resolveFiles(fixturePath);
                List<IndexEntry> index = requireSortedIndex(files, fixtureMetrics, SimStoreBackend.STREAMING);
                yield resolved(new StreamingListingStore(index, simMetrics, config.streamingMaxResidentBytes()),
                        SimStoreBackend.STREAMING, metrics, simMetrics, keyCount(index));
            }
            case WINDOWED -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                List<Path> files = resolveFiles(fixturePath);
                List<IndexEntry> index = requireSortedIndex(files, fixtureMetrics, SimStoreBackend.WINDOWED);
                yield resolved(windowedStore(files, index, metrics), SimStoreBackend.WINDOWED,
                        metrics, simMetrics, keyCount(index));
            }
            case AUTO -> {
                ReplayMetrics duckdbMetrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
                ListingStore source = parquetStore(fixturePath, duckdbMetrics);
                Optional<ArenaListingStore> arena;
                try {
                    arena = loadArena(source, config, fixturePath);
                } catch (RuntimeException e) {
                    source.close();
                    throw e;
                }
                if (arena.isPresent()) {
                    source.close();
                    yield resolved(arena.get(), SimStoreBackend.ARENA, duckdbMetrics, simMetrics,
                            OptionalLong.of(arena.get().keyCount()));
                }
                simMetrics.recordArenaDecline(SimStoreMetrics.DECLINE_OVER_BUDGET);
                log.info("sim_store auto declined the arena tier (encoded keys exceed {} bytes) "
                        + "— trying the streaming tier next for {}", config.arenaMaxEncodedBytes(), fixturePath);

                // Retain DuckDB for PARQUET fallback, closing it on every other path including failures.
                SortedEligibility.Result eligibility;
                try {
                    eligibility = SortedEligibility.decide(resolveFiles(fixturePath), fixtureMetrics, true);
                } catch (RuntimeException e) {
                    source.close();
                    throw e;
                }
                if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
                    source.close();
                    ReplayMetrics sortedMetrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                    yield resolved(
                            new StreamingListingStore(eligible.index(), simMetrics, config.streamingMaxResidentBytes()),
                            SimStoreBackend.STREAMING, sortedMetrics, simMetrics, keyCount(eligible.index()));
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                simMetrics.recordStreamingDecline(reason);
                log.info("sim_store auto declined the streaming tier (reason={}) — falling back to {} for {}",
                        reason, SimStoreBackend.PARQUET, fixturePath);
                yield resolved(source, SimStoreBackend.PARQUET, duckdbMetrics, simMetrics, OptionalLong.empty());
            }
        };
    }

    private static Result resolved(ListingStore store, SimStoreBackend backend, ReplayMetrics metrics,
                                   SimStoreMetrics simMetrics, OptionalLong keyCount) {
        simMetrics.recordBackend(backend);
        return new Result(store, backend, metrics, keyCount);
    }

    /** The eligible index's fixture total; see {@link Result#keyCount()}. */
    private static OptionalLong keyCount(List<IndexEntry> index) {
        return OptionalLong.of(index.stream().mapToLong(IndexEntry::rowCount).sum());
    }

    /** Adds fixture context to an arena rejection. */
    private static Optional<ArenaListingStore> loadArena(ListingStore source, SimStoreConfig config,
                                                         Path fixturePath) {
        try {
            return ArenaListingStore.loadWithin(source, config.arenaMaxEncodedBytes());
        } catch (IllegalArgumentException rejected) {
            throw new IllegalArgumentException("fixture " + fixturePath + " cannot be served by the "
                    + SimStoreBackend.ARENA + " tier: " + rejected.getMessage(), rejected);
        }
    }

    private static ListingStore parquetStore(Path fixturePath, ReplayMetrics metrics) {
        return new DuckDbListingStore(fixturePath, metrics, DuckDbListingStore.defaultConnectionCount());
    }

    /** Returns a forced sorted backend's index or a typed eligibility exclusion. */
    private static List<IndexEntry> requireSortedIndex(List<Path> files, FixtureMetrics fixtureMetrics,
                                                       SimStoreBackend backend) {
        SortedEligibility.Result eligibility = SortedEligibility.decide(files, fixtureMetrics, false);
        if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
            return eligible.index();
        }
        String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
        throw new IneligibleFixtureException(backend, reason, files);
    }

    /**
     * Builds the forced windowed tier using the replay server's prefetch configuration; disabled
     * prefetch returns the bare sorted store. The wrapper owns page-read latency measurement.
     */
    private static ListingStore windowedStore(List<Path> files, List<IndexEntry> index, ReplayMetrics metrics) {
        // Parse before opening a pool, so malformed properties cannot leak one.
        WindowedListingStore.Config prefetch = WindowedListingStore.Config.fromSystemProperties();
        int connections = SortedParquetStore.defaultConnectionCount();
        if (!prefetch.enabled()) {
            log.info("sim_store windowed prefetch DISABLED (bare store) for {}", files);
            return new SortedParquetStore(files, index, metrics, connections);
        }
        // The wrapper records the outer page latency.
        SortedParquetStore backing = new SortedParquetStore(files, index, metrics, connections, false);
        try {
            log.info("sim_store windowed prefetch ENABLED (window_rows={} max_windows={}) for {}",
                    prefetch.windowRows(), prefetch.maxWindows(), files);
            return new WindowedListingStore(backing, metrics, prefetch.windowRows(), prefetch.maxWindows());
        } catch (RuntimeException e) {
            backing.close();
            throw e;
        }
    }

    private static List<Path> resolveFiles(Path fixturePath) {
        try {
            return SortedFixtures.resolveFiles(fixturePath);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve fixture files for " + fixturePath, e);
        }
    }
}
