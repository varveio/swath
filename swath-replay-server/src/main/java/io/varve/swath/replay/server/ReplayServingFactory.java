/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortStamp;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

    /**
     * Reasons {@link SortedFixtures#loadIndex} already records {@code serving.fallback} for itself
     * (when asked to) — {@code auto}'s post-decision recording below must not repeat these, or a
     * decline would double-count the counter.
     */
    private static final Set<String> RECORDED_BY_LOAD_INDEX =
            Set.of(SortedFixtures.SANITY_FAILED, SortedFixtures.MIXED_ROW_TYPES);

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
                Eligibility eligibility = decideSorted(files, fixtureMetrics, false);
                if (eligibility instanceof Eligibility.Eligible eligible) {
                    yield sorted(files, eligible.index(), parquetConnections, registry);
                }
                String reason = ((Eligibility.Ineligible) eligibility).reason();
                throw new IllegalArgumentException(
                        "--serving-mode sorted requires a stamped, objects-mode, strictly-sorted, "
                                + "pure-OBJECT fixture (" + reason + "): " + fixturePath);
            }
            case AUTO -> {
                Eligibility eligibility = decideSorted(files, fixtureMetrics, true);
                if (eligibility instanceof Eligibility.Eligible eligible) {
                    yield sorted(files, eligible.index(), parquetConnections, registry);
                }
                String reason = ((Eligibility.Ineligible) eligibility).reason();
                // SANITY_FAILED/MIXED_ROW_TYPES are recorded inside SortedFixtures.loadIndex; record
                // the other reasons (which never reached loadIndex) here so every auto decline lands
                // exactly one counter, never zero and never two.
                if (!RECORDED_BY_LOAD_INDEX.contains(reason)) {
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

    /**
     * Decides sorted-serving eligibility. Every resolved file — not just the first (a multi-file
     * directory with a stamped first file and a bad later file must not serve sorted and silently
     * omit that later file's keys from routing) — must carry a recognized sortedness stamp (§0.9),
     * be {@code objects} mode (§0.6), and a {@code format_version} this reader knows. The resolved
     * set must then prove multi-file completeness ({@link #multiFileCompletenessViolation}) before
     * the index is even attempted, and finally load a globally strictly-ascending, provably
     * pure-{@code OBJECT} index (§0.5 / the mixed-row-type fix).
     *
     * @param recordFallbackOnFailure forwarded to {@link SortedFixtures#loadIndex} — {@code false}
     *                                for {@code --serving-mode sorted} (a hard fail is not a
     *                                "fallback")
     */
    private static Eligibility decideSorted(List<Path> files, FixtureMetrics fixtureMetrics,
                                            boolean recordFallbackOnFailure) {
        if (files.isEmpty()) {
            return new Eligibility.Ineligible(SortedFixtures.NO_STAMP);
        }
        List<SortStamp> stamps = new ArrayList<>(files.size());
        for (Path file : files) {
            Optional<SortStamp> stamp = detect(file);
            if (stamp.isEmpty()) {
                log.info("replay_serving: {} carries no recognized sortedness stamp", file);
                return new Eligibility.Ineligible(SortedFixtures.NO_STAMP);
            }
            if (stamp.get().mode() != SortMode.OBJECTS) {
                log.info("replay_serving: {} is stamped mode={}, not objects", file, stamp.get().mode());
                return new Eligibility.Ineligible(SortedFixtures.UNSUPPORTED_MODE);
            }
            if (!stamp.get().isKnownFormatVersion()) {
                log.info("replay_serving: {} is stamped with an unrecognized format_version={}",
                        file, stamp.get().formatVersion());
                return new Eligibility.Ineligible(SortedFixtures.UNKNOWN_FORMAT_VERSION);
            }
            stamps.add(stamp.get());
        }
        String violation = multiFileCompletenessViolation(files, stamps);
        if (violation != null) {
            log.info("replay_serving: multi-file completeness check failed for {} — {}", files, violation);
            return new Eligibility.Ineligible(SortedFixtures.INCOMPLETE_MULTIFILE);
        }
        IndexLoadResult loaded = loadIndex(files, fixtureMetrics, recordFallbackOnFailure);
        return switch (loaded) {
            case IndexLoadResult.Loaded l -> new Eligibility.Eligible(l.entries());
            case IndexLoadResult.SanityFailed f -> new Eligibility.Ineligible(f.reason());
        };
    }

    /**
     * Proves the resolved file set is a COMPLETE multi-file (or single-file) sorted
     * output, self-describingly from each file's own stamp — never a sidecar. For {@code N} files
     * actually present, their {@code file_index} values must be exactly {@code 1..N} (contiguous, no
     * gaps, no duplicates — a gap or an out-of-range index means a file is missing or extra), and
     * exactly one file — the one at index {@code N} — must carry {@code file_final=true}. This is
     * exactly what a crash between renaming files {@code 1..k} and {@code k+1} of an {@code N}-file
     * publish leaves behind: the observed set is {@code 1..k} (contiguous — so the gap alone doesn't
     * catch it), but NONE of them is stamped final (the true final file, index {@code N > k}, was
     * never renamed into place) — so the missing-final check catches it. A stray/duplicated file
     * reusing an index, or a file whose index exceeds the observed count, fails the contiguity check
     * instead. Returns {@code null} when the set is complete; otherwise a detail string for logs.
     */
    static String multiFileCompletenessViolation(List<Path> files, List<SortStamp> stamps) {
        int n = files.size();
        boolean[] seen = new boolean[n + 1];   // 1-based; seen[0] unused
        int finalCount = 0;
        int finalIndex = -1;
        for (int i = 0; i < n; i++) {
            SortStamp stamp = stamps.get(i);
            int index = stamp.fileIndex();
            if (index < 1 || index > n || seen[index]) {
                return "file " + files.get(i) + " reports file_index=" + index
                        + ", which is not a valid position among " + n + " file(s) actually present "
                        + "(indices must be exactly 1.." + n + ", contiguous, no duplicates)";
            }
            seen[index] = true;
            if (stamp.fileFinal()) {
                finalCount++;
                finalIndex = index;
            }
        }
        if (finalCount != 1) {
            return "expected exactly one file stamped file_final=true across " + n
                    + " file(s), found " + finalCount + " — a crash mid-multi-file-publish leaves the true "
                    + "final file unrenamed, so this set is an incomplete prefix of the true output";
        }
        if (finalIndex != n) {
            return "the file_final=true flag is on file_index=" + finalIndex + ", expected the max index " + n;
        }
        return null;
    }

    private static List<Path> resolveFiles(Path fixturePath) {
        try {
            return SortedFixtures.resolveFiles(fixturePath);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve fixture files for " + fixturePath, e);
        }
    }

    private static Optional<SortStamp> detect(Path file) {
        try {
            return SortedFixtures.detect(file);
        } catch (IOException e) {
            // An unreadable/foreign footer is simply "not stamped" for routing purposes.
            return Optional.empty();
        }
    }

    private static IndexLoadResult loadIndex(List<Path> files, FixtureMetrics fixtureMetrics,
                                             boolean recordFallbackOnFailure) {
        try {
            return SortedFixtures.loadIndex(files, fixtureMetrics, recordFallbackOnFailure);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to derive the sorted-fixture index", e);
        }
    }

    private sealed interface Eligibility {
        record Eligible(List<IndexEntry> index) implements Eligibility {
        }

        record Ineligible(String reason) implements Eligibility {
        }
    }
}
