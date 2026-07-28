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
 *   <li>{@link SimStoreBackend#STREAMING} — require the keys-only decode-once streaming tier; a
 *       fixture that is not sorted-eligible ({@link SortedEligibility}) fails fast rather than
 *       silently serving a slower tier.</li>
 *   <li>{@link SimStoreBackend#WINDOWED} — require the windowed row-group prefetch over the
 *       replay module's sorted-Parquet store; same eligibility, same hard failure. Forced-only:
 *       {@link SimStoreBackend#AUTO} never resolves here (see the constant's own note).</li>
 *   <li>{@link SimStoreBackend#AUTO} — arena when it fits, else streaming when the fixture is
 *       sorted-eligible, else the Parquet store, recording
 *       {@code swath.sim.store.arena.decline\{reason\}} / {@code swath.sim.store.streaming.decline\{reason\}}
 *       and logging why.</li>
 * </ul>
 *
 * <p><b>Fixtures are local paths.</b> {@code fixturePath} is a swath Parquet capture file or a
 * directory of them, on this machine. Nothing here resolves a remote object-store location:
 * fetching a fixture to local disk is the caller's job, deliberately outside this seam.
 *
 * <h2>What an unsorted fixture does here, tier by tier</h2>
 * A fixture that is not in strictly ascending key order is a corrupt input for a simulator: the sim's
 * whole premise is that its store answers the ranges a real ordered store would. The check is inline
 * in the loops each tier already runs — never a separate validation pass — and it does not reach every
 * tier equally, which is stated here rather than left to be discovered:
 * <ul>
 *   <li>{@link SimStoreBackend#STREAMING} — <b>guarded</b>. Row-group first keys are proved ascending
 *       at index derive ({@link SortedFixtures#loadIndex}, the fixture is otherwise ineligible), and
 *       every key of every row group the run actually faults in is proved on the way into its
 *       {@link KeyBlock}. The first violation fails the read, naming file, row group and row.</li>
 *   <li>{@link SimStoreBackend#WINDOWED} — <b>partly guarded</b>. Same index-derive gate, and the
 *       {@code delimiter=/} skip-scan proves the ascent of every row it steps over
 *       ({@code SortedRowGroupReader.KeyCursor}). Its plain range reads, though, go through a bounded
 *       DuckDB query that sorts what it returns, so disorder inside a row group cannot be seen there —
 *       it shows up as a short page, not as an out-of-order one.</li>
 *   <li>{@link SimStoreBackend#ARENA} — <b>duplicates guarded, disorder normalised</b>. The arena is
 *       loaded <em>through</em> the Parquet store below, whose reads are {@code ORDER BY key}: keys
 *       therefore arrive sorted whatever the file holds, and the arena's ascending check can only
 *       fire on a duplicate (which survives sorting). The keys it then serves are the fixture's, in
 *       order.</li>
 *   <li>{@link SimStoreBackend#PARQUET} — <b>not guarded, and deliberately</b>. That store exists to
 *       serve arbitrary captures, sorted or not, and it re-sorts at query time; proving the file's
 *       physical order would mean a second scan of it, which is precisely what an inline check is
 *       not. A fixture that reaches this tier is served in key order regardless.</li>
 * </ul>
 * {@link SimStoreBackend#AUTO} therefore hard-fails an unsorted fixture whenever it lands on the
 * streaming tier — which is where a fixture large enough for a real sweep lands by construction. The
 * failure is a typed {@link io.varve.swath.sort.RowGroupOrderException} and is counted
 * ({@link SimStoreMetrics#SEGMENT_REFUSED_METRIC}) before it is rethrown, so a sweep classifies the
 * excluded fixture from the reason and the metrics rather than from a message.
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
     *                                  under {@link SimStoreBackend#STREAMING} /
     *                                  {@link SimStoreBackend#WINDOWED} when the fixture is not
     *                                  sorted-eligible
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
                    arena = loadArena(source, config, fixturePath).orElseThrow(() -> new IllegalArgumentException(
                            "backend " + SimStoreBackend.ARENA + " requires a fixture whose encoded keys fit in "
                                    + config.arenaMaxEncodedBytes() + " bytes (raise "
                                    + SimStoreConfig.ARENA_MAX_ENCODED_BYTES_PROPERTY + ", or use "
                                    + SimStoreBackend.PARQUET + "): " + fixturePath));
                }
                yield resolved(arena, SimStoreBackend.ARENA, metrics, simMetrics);
            }
            case STREAMING -> {
                // SERVING_MODE_SORTED: like the windowed tier below, this one reads the fixture's own
                // sorted layout natively (row group by row group), never a materialized DuckDB table.
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                List<Path> files = resolveFiles(fixturePath);
                List<IndexEntry> index = requireSortedIndex(files, fixtureMetrics, SimStoreBackend.STREAMING);
                yield resolved(new StreamingListingStore(index, simMetrics, config.streamingMaxResidentBytes()),
                        SimStoreBackend.STREAMING, metrics, simMetrics);
            }
            case WINDOWED -> {
                ReplayMetrics metrics = new ReplayMetrics(registry, ReplayMetrics.SERVING_MODE_SORTED);
                List<Path> files = resolveFiles(fixturePath);
                List<IndexEntry> index = requireSortedIndex(files, fixtureMetrics, SimStoreBackend.WINDOWED);
                yield resolved(windowedStore(files, index, metrics), SimStoreBackend.WINDOWED,
                        metrics, simMetrics);
            }
            // AUTO is a fixture-tier router, not an engine algorithm path: AGENTS.md's "instrument
            // every new algorithm path" doctrine (recordStealReason) is scoped to the LISTING
            // ENGINE's own split/steal/pivot/seed/backoff decisions, whose post-hoc keyspace-shape
            // classification that counter family exists for. Choosing a simulator fixture's backing
            // store is a different concern with its own engagement counters
            // (SimStoreMetrics#recordArenaDecline / #recordStreamingDecline, both tagged with why),
            // not an omission of the engine's.
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
                    yield resolved(arena.get(), SimStoreBackend.ARENA, duckdbMetrics, simMetrics);
                }
                simMetrics.recordArenaDecline(SimStoreMetrics.DECLINE_OVER_BUDGET);
                log.info("sim_store auto declined the arena tier (encoded keys exceed {} bytes) "
                        + "— trying the streaming tier next for {}", config.arenaMaxEncodedBytes(), fixturePath);

                // source is still open here (the abandoned DuckDB pool, kept as the PARQUET fallback
                // in case streaming also declines) -- a resolveFiles/decide failure must close it too,
                // exactly like the arena step just above, or it leaks.
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
                            SimStoreBackend.STREAMING, sortedMetrics, simMetrics);
                }
                String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
                simMetrics.recordStreamingDecline(reason);
                log.info("sim_store auto declined the streaming tier (reason={}) — falling back to {} for {}",
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

    /**
     * The arena load, with the fixture named on the way out of a rejection. The arena rejects a key
     * that is over-long or not strictly above its predecessor, and its message names the offending key
     * and its row ordinal — but it is built over a {@link ListingStore} and cannot know which fixture
     * that store is reading, which is exactly what a sweep over a corpus needs told.
     */
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

    /**
     * The derived routing index of a sorted-eligible {@code files}, for the two forced backends that
     * require one. {@code recordFallbackOnFailure=false}: a forced backend that declines hard-fails,
     * so this is not a "fallback" any more than {@link SimStoreBackend#ARENA}'s forced decline is.
     */
    private static List<IndexEntry> requireSortedIndex(List<Path> files, FixtureMetrics fixtureMetrics,
                                                       SimStoreBackend backend) {
        SortedEligibility.Result eligibility = SortedEligibility.decide(files, fixtureMetrics, false);
        if (eligibility instanceof SortedEligibility.Result.Eligible eligible) {
            return eligible.index();
        }
        String reason = ((SortedEligibility.Result.Ineligible) eligibility).reason();
        throw new IllegalArgumentException("backend " + backend + " requires a sorted-eligible fixture ("
                + reason + "), use " + SimStoreBackend.PARQUET + " instead: " + files);
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
