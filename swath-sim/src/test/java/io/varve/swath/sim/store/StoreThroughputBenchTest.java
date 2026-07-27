/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sustained store-call throughput per {@link SimStoreBackend}: how many {@code rows()} calls a
 * tier sustains per second, and at what per-call cost, under one sequential stream walking a real
 * fixture through the shared {@link ListObjectsV2Pager} seam — the same access shape one
 * work-stealing worker's page-by-page walk has. This is a sanity check against the simulated-run
 * budget (a run issues on the order of 150K store calls), not a per-commit regression gate: there
 * is no fixed pass/fail threshold here, because which tier a fixture's size should resolve to is an
 * operational judgement call informed by this report, not asserted by it.
 *
 * <p>Opt-in only ({@code @Tag("perf")} — excluded from the ordinary {@code test} task, included
 * only under {@code -Ponly Perf}/{@code -Pperf}) and further gated on a real fixture actually being
 * present locally: this module never bundles or downloads a corpus fixture (see the README's
 * "Fixtures" section), so a run with neither property set below is skipped, not failed. Point
 * {@link #FIXTURE_PROPERTY} at a multi-million-key sorted, stamped fixture and (optionally)
 * {@link #GIANT_FIXTURE_PROPERTY} at a much larger one — the arena tier is skipped for the latter,
 * outside its design envelope by construction (an arena sized to hold it would dwarf a sane heap).
 */
@Tag("perf")
class StoreThroughputBenchTest {

    /** System property naming a local fixture path in the tens-of-millions-of-keys range. */
    static final String FIXTURE_PROPERTY = "swath.sim.bench.fixture";

    /** System property naming a local fixture path well beyond the arena tier's design envelope. */
    static final String GIANT_FIXTURE_PROPERTY = "swath.sim.bench.giant-fixture";

    private static final String BUCKET = "bucket";
    private static final int PAGE_SIZE = 1000;
    private static final Duration WARMUP_DURATION = Duration.ofSeconds(1);
    private static final Duration MEASURED_DURATION = Duration.ofSeconds(5);

    /** Comfortably covers a multi-million-key fixture's encoded keys without sizing to the giant. */
    private static final SimStoreConfig BENCH_CONFIG = new SimStoreConfig(2L << 30);

    @Test
    void reportsSustainedThroughputForEachConfiguredFixture() {
        boolean ranAny = benchFixture(System.getProperty(FIXTURE_PROPERTY), "configured", true);
        ranAny |= benchFixture(System.getProperty(GIANT_FIXTURE_PROPERTY), "giant", false);
        assumeTrue(ranAny, "neither -D" + FIXTURE_PROPERTY + " nor -D" + GIANT_FIXTURE_PROPERTY
                + " is set; nothing to bench");
    }

    /** @return false without benching anything when {@code pathProperty} is unset. */
    private static boolean benchFixture(String pathProperty, String label, boolean includeArena) {
        if (pathProperty == null || pathProperty.isBlank()) {
            return false;
        }
        Path fixture = Path.of(pathProperty);
        assertThat(Files.exists(fixture)).as("%s fixture at %s", label, fixture).isTrue();
        List<SimStoreBackend> backends = includeArena
                ? List.of(SimStoreBackend.ARENA, SimStoreBackend.WINDOWED, SimStoreBackend.PARQUET)
                : List.of(SimStoreBackend.WINDOWED, SimStoreBackend.PARQUET);
        for (SimStoreBackend backend : backends) {
            SimStoreFactory.Result result = SimStoreFactory.open(fixture, backend, BENCH_CONFIG);
            try (ListingStore store = result.store()) {
                BenchResult bench = benchOne(store, result.metrics());
                System.out.printf(Locale.ROOT,
                        "store_bench fixture=%s backend=%s calls=%d elapsed_ms=%d calls_per_sec=%.1f us_per_call=%.2f%n",
                        label, backend, bench.calls(), bench.elapsed().toMillis(),
                        bench.callsPerSecond(), bench.microsPerCall());
            }
        }
        return true;
    }

    private static BenchResult benchOne(ListingStore store, ReplayMetrics metrics) {
        ListObjectsV2Pager pager = new ListObjectsV2Pager(store, metrics);
        walk(pager, WARMUP_DURATION);   // discarded: lets connection pools/caches settle
        return walk(pager, MEASURED_DURATION);
    }

    /**
     * Walks {@code pager} sequentially for {@code duration}, wall-clock. A fixture small enough to
     * exhaust before {@code duration} elapses wraps back to the start rather than stopping — a
     * sustained-rate measurement over a fixed window, not a one-shot full listing.
     */
    private static BenchResult walk(ListObjectsV2Pager pager, Duration duration) {
        String token = null;
        long calls = 0;
        Instant start = Instant.now();
        Instant deadline = start.plus(duration);
        while (Instant.now().isBefore(deadline)) {
            S3ListResult result = pager.list(new S3ListRequest(BUCKET, null, null, null, token, PAGE_SIZE,
                    false, false));
            calls++;
            token = result.truncated() ? result.nextContinuationToken() : null;
        }
        return new BenchResult(calls, Duration.between(start, Instant.now()));
    }

    private record BenchResult(long calls, Duration elapsed) {

        double callsPerSecond() {
            return calls / (elapsed.toNanos() / 1_000_000_000.0);
        }

        double microsPerCall() {
            return (elapsed.toNanos() / 1_000.0) / calls;
        }
    }
}
