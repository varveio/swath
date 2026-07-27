/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedEligibility;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a {@code --serving-mode} choice into a concrete fixture (pager + store) at server startup
 * (§3). A single shared {@link MeterRegistry} carries both the index/fallback meters
 * ({@link FixtureMetrics}) and the request meters ({@link ReplayMetrics}), so a run is
 * self-describing from one registry.
 *
 * <ul>
 *   <li>{@code duckdb} — always the role-1 DuckDB store (materialised); the oracle, any capture.</li>
 *   <li>{@code sorted} — require a stamped, objects-mode, strictly-sorted fixture; otherwise fail
 *       fast with a clear message (the server does not start).</li>
 *   <li>{@code auto} — serve sorted when eligible; otherwise fall back to DuckDB, recording
 *       {@code serving.fallback\{reason\}} and logging why.</li>
 * </ul>
 */
public final class ReplayServingFactory {

    private static final Logger log = LoggerFactory.getLogger(ReplayServingFactory.class);

    private ReplayServingFactory() {
    }

    /** The resolved fixture plus the concrete path chosen and the metrics that back it. */
    public record Result(ListingFixture fixture, ServingMode resolvedMode, ReplayMetrics metrics,
                         int maxConcurrentReads) {
    }

    /**
     * Opens {@code fixturePath} under the requested {@code mode}. {@code parquetConnections <= 0}
     * uses the CPU-bounded default for the chosen store (also the request-concurrency bound).
     *
     * @throws IllegalArgumentException in {@code sorted} mode when the fixture is not sorted-eligible
     */
    public static Result open(Path fixturePath, ServingMode mode, int parquetConnections) {
        MeterRegistry registry = new SimpleMeterRegistry();
        FixtureMetrics fixtureMetrics = new FixtureMetrics(registry);
        List<Path> files = resolveFiles(fixturePath);

        return switch (mode) {
            case DUCKDB -> duckDb(fixturePath, parquetConnections, registry);
            case SORTED -> {
                // recordFallbackOnFailure=false: sorted mode never falls back, it hard-fails — a
                // decline here isn't a "fallback" and must not bump that counter.
                SortedEligibility.Result eligibility = SortedEligibility.decide(files, fixtureMetrics, false);
                if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
                    yield sorted(files, eligible.index(), parquetConnections, registry);
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                throw new IllegalArgumentException(
                        "--serving-mode sorted requires a stamped, objects-mode, strictly-sorted, "
                                + "pure-OBJECT fixture (" + reason + "): " + fixturePath);
            }
            case AUTO -> {
                SortedEligibility.Result eligibility = SortedEligibility.decide(files, fixtureMetrics, true);
                if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
                    yield sorted(files, eligible.index(), parquetConnections, registry);
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                // SANITY_FAILED/MIXED_ROW_TYPES are recorded inside SortedFixtures.loadIndex; record
                // the other reasons (which never reached loadIndex) here so every auto decline lands
                // exactly one counter, never zero and never two.
                if (!SortedEligibility.RECORDED_BY_LOAD_INDEX.contains(reason)) {
                    fixtureMetrics.recordFallback(reason);
                }
                log.info("replay_serving auto declined sorted serving (reason={}) — falling back to DuckDB for {}",
                        reason, fixturePath);
                yield duckDb(fixturePath, parquetConnections, registry);
            }
        };
    }

    private static Result duckDb(Path fixturePath, int parquetConnections, MeterRegistry registry) {
        int connections = parquetConnections > 0 ? parquetConnections : DuckDbListingStore.defaultConnectionCount();
        ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_DUCKDB);
        DuckDbListingStore store = new DuckDbListingStore(fixturePath, metrics, connections);
        ListObjectsV2Pager pager = new ListObjectsV2Pager(store, metrics);
        return new Result(pager, ServingMode.DUCKDB, metrics, connections);
    }

    private static Result sorted(List<Path> files, List<IndexEntry> index, int parquetConnections,
                                 MeterRegistry registry) {
        int connections = parquetConnections > 0 ? parquetConnections : SortedParquetStore.defaultConnectionCount();
        ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
        WindowedListingStore.Config prefetch = WindowedListingStore.Config.fromSystemProperties();
        ListingStore store;
        if (prefetch.enabled()) {
            // Suppress the delegate's own page.read.latency: the wrapper owns the outer per-page timer
            // (a hit costs sub-ms, a miss pays a window fill measured separately by prefetch.window.fill)
            // so the corridor metric stays the honest amortized per-page cost.
            SortedParquetStore backing = new SortedParquetStore(files, index, metrics, connections, false);
            store = new WindowedListingStore(backing, metrics, prefetch.windowRows(), prefetch.maxWindows());
            log.info("replay_serving sorted prefetch ENABLED (window_rows={} max_windows={}) for {}",
                    prefetch.windowRows(), prefetch.maxWindows(), files);
        } else {
            store = new SortedParquetStore(files, index, metrics, connections);
            log.info("replay_serving sorted prefetch DISABLED (bare store) for {}", files);
        }
        ListObjectsV2Pager pager = new ListObjectsV2Pager(store, metrics);
        return new Result(pager, ServingMode.SORTED, metrics, connections);
    }

    private static List<Path> resolveFiles(Path fixturePath) {
        try {
            return SortedFixtures.resolveFiles(fixturePath);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve fixture files for " + fixturePath, e);
        }
    }
}
