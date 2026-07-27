/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sustained store-call throughput per {@link SimStoreBackend}: how many {@code rows()} calls a
 * tier sustains per second, and at what per-call cost, under one sequential stream walking a real
 * fixture through the shared {@link ListObjectsV2Pager} seam — the same access shape one
 * work-stealing worker's page-by-page walk has. This is a sanity check against the simulated-run
 * budget (a run issues on the order of {@value #RUN_CALLS} store calls), not a per-commit regression
 * gate: there is no fixed pass/fail threshold here, because which tier a fixture's size should
 * resolve to is an operational judgement call informed by this report, not asserted by it.
 *
 * <p>Three measurements, in the order a reader wants them:
 * <ol>
 *   <li><b>Sustained rate</b> per tier per fixture — a fixed wall-clock window, warm.</li>
 *   <li><b>A whole simulated run, cold</b> — exactly {@value #RUN_CALLS} calls from a
 *       freshly-opened store, so every segment decode the walk needs is inside the reported total.
 *       This is the number the giant tier is judged on; the sustained rate above deliberately is
 *       not, because a warm window hides the decode.</li>
 *   <li><b>A mid-keyspace cursor start</b> — the steal/split shape: one read at a key halfway
 *       through the fixture, against a store that has never touched it. A tier that scanned from the
 *       file head to serve it would show up here and nowhere else.</li>
 * </ol>
 *
 * <p>Opt-in only ({@code @Tag("perf")} — excluded from the ordinary {@code test} task, included
 * only under {@code -PonlyPerf}/{@code -Pperf}) and further gated on a real fixture actually being
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

    /** Store calls one simulated run issues — the budget every tier is ultimately measured against. */
    private static final int RUN_CALLS = 150_000;

    /**
     * Tiers slow enough that a whole {@value #RUN_CALLS}-call run would take days are reported on the
     * sustained-rate window only; running one to completion is not a measurement, it is a hang.
     */
    private static final List<SimStoreBackend> RUN_CAPABLE =
            List.of(SimStoreBackend.ARENA, SimStoreBackend.STREAMING);

    /**
     * A third of the perf tier's heap ({@code maxHeapSize = "2g"} whenever {@code @Tag("perf")}
     * actually runs — see {@code swath.java-conventions}), not a fixed 2 GiB: budgeting the WHOLE
     * heap to the arena would leave too little JVM/JDBC headroom for the arena to OOM before its
     * own budget check gets the chance to decline gracefully. Still comfortably covers a
     * multi-million-key fixture's encoded keys without sizing to the giant.
     *
     * <p>The streaming residency budget is a flat 1 GiB and does <b>not</b> come out of that heap:
     * decoded segments are off-heap, so the two budgets do not compete.
     */
    private static final SimStoreConfig BENCH_CONFIG =
            new SimStoreConfig(Runtime.getRuntime().maxMemory() / 3, 1L << 30);

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
                ? List.of(SimStoreBackend.ARENA, SimStoreBackend.STREAMING, SimStoreBackend.WINDOWED,
                        SimStoreBackend.PARQUET)
                : List.of(SimStoreBackend.STREAMING, SimStoreBackend.WINDOWED, SimStoreBackend.PARQUET);

        for (SimStoreBackend backend : backends) {
            sustainedRate(fixture, label, backend);
        }
        for (SimStoreBackend backend : backends) {
            if (RUN_CAPABLE.contains(backend)) {
                wholeRun(fixture, label, backend);
            }
        }
        midKeyspaceStart(fixture, label);
        return true;
    }

    /** Measurement 1: warm sustained rate over a fixed wall-clock window. */
    private static void sustainedRate(Path fixture, String label, SimStoreBackend backend) {
        Instant openedAt = Instant.now();
        SimStoreFactory.Result result = SimStoreFactory.open(fixture, backend, BENCH_CONFIG);
        Duration open = Duration.between(openedAt, Instant.now());
        try (ListingStore store = result.store()) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(store, result.metrics());
            walk(pager, WARMUP_DURATION, Integer.MAX_VALUE);   // discarded: lets pools/segments settle
            BenchResult bench = walk(pager, MEASURED_DURATION, Integer.MAX_VALUE);
            System.out.printf(Locale.ROOT,
                    "store_bench phase=sustained fixture=%s backend=%s open_ms=%d calls=%d elapsed_ms=%d "
                            + "calls_per_sec=%.1f us_per_call=%.2f heap_used_mb=%.1f%s%n",
                    label, backend, open.toMillis(), bench.calls(), bench.elapsed().toMillis(),
                    bench.callsPerSecond(), bench.microsPerCall(), heapUsedMb(),
                    segmentReport(result, store));
        }
    }

    /**
     * Measurement 2: one whole simulated run against a COLD store. Nothing is warmed and nothing is
     * discarded — every segment decode the walk triggers is inside the elapsed time reported here.
     */
    private static void wholeRun(Path fixture, String label, SimStoreBackend backend) {
        Instant openedAt = Instant.now();
        SimStoreFactory.Result result = SimStoreFactory.open(fixture, backend, BENCH_CONFIG);
        Duration open = Duration.between(openedAt, Instant.now());
        try (ListingStore store = result.store()) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(store, result.metrics());
            BenchResult bench = walk(pager, Duration.ofDays(1), RUN_CALLS);
            System.out.printf(Locale.ROOT,
                    "store_bench phase=run fixture=%s backend=%s open_ms=%d calls=%d elapsed_ms=%d "
                            + "calls_per_sec=%.1f us_per_call=%.2f heap_used_mb=%.1f%s%n",
                    label, backend, open.toMillis(), bench.calls(), bench.elapsed().toMillis(),
                    bench.callsPerSecond(), bench.microsPerCall(), heapUsedMb(),
                    segmentReport(result, store));
        }
    }

    /**
     * Measurement 3: the steal case. One range read at the first key of the fixture's middle row
     * group, issued against a store that has served nothing — so the elapsed time is the whole cost
     * of starting a cursor there, and the decoded row count says how much of the fixture that cost
     * covered (one row group, if the seek is index-driven; everything before it, if it is not).
     */
    private static void midKeyspaceStart(Path fixture, String label) {
        List<IndexEntry> index = index(fixture);
        if (index.size() < 3) {
            return;
        }
        ByteKey midKey = index.get(index.size() / 2).firstKey();
        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.STREAMING, BENCH_CONFIG);
        try (ListingStore store = result.store()) {
            Instant started = Instant.now();
            int rows = store.rows(midKey, true, null, PAGE_SIZE, Projection.KEYS_ONLY).size();
            Duration elapsed = Duration.between(started, Instant.now());
            System.out.printf(Locale.ROOT,
                    "store_bench phase=mid_start fixture=%s backend=%s row_groups=%d landing_group=%d "
                            + "rows=%d elapsed_us=%d%s%n",
                    label, SimStoreBackend.STREAMING, index.size(), index.size() / 2, rows,
                    elapsed.toNanos() / 1_000, segmentReport(result, store));
        }
    }

    /**
     * Walks {@code pager} sequentially until {@code duration} elapses or {@code maxCalls} calls have
     * been issued, whichever comes first. A fixture small enough to exhaust mid-window wraps back to
     * the start rather than stopping — a sustained-rate measurement over a fixed window, not a
     * truncated one-shot listing.
     */
    private static BenchResult walk(ListObjectsV2Pager pager, Duration duration, int maxCalls) {
        String token = null;
        long calls = 0;
        Instant start = Instant.now();
        Instant deadline = start.plus(duration);
        while (calls < maxCalls && Instant.now().isBefore(deadline)) {
            S3ListResult result = pager.list(new S3ListRequest(BUCKET, null, null, null, token, PAGE_SIZE,
                    false, false));
            calls++;
            token = result.truncated() ? result.nextContinuationToken() : null;
        }
        return new BenchResult(calls, Duration.between(start, Instant.now()));
    }

    /** The streaming tier's own counters, appended to a report line; empty for every other tier. */
    private static String segmentReport(SimStoreFactory.Result result, ListingStore store) {
        if (!(store instanceof StreamingListingStore streaming)) {
            return "";
        }
        MeterRegistry registry = result.metrics().registry();
        Timer decode = registry.find(SimStoreMetrics.SEGMENT_DECODE_METRIC).timer();
        return String.format(Locale.ROOT,
                " segment_hits=%.0f segment_faults_forward=%.0f segment_faults_seek=%.0f"
                        + " decoded_rows=%.0f decode_total_ms=%.0f offheap_peak_mb=%.1f",
                counter(registry, SimStoreMetrics.SEGMENT_HIT_METRIC),
                faults(registry, SimStoreMetrics.FAULT_FORWARD),
                faults(registry, SimStoreMetrics.FAULT_SEEK),
                counter(registry, SimStoreMetrics.SEGMENT_DECODE_ROWS_METRIC),
                decode == null ? 0.0 : decode.totalTime(TimeUnit.MILLISECONDS),
                streaming.peakResidentBytes() / (double) (1 << 20));
    }

    private static double counter(MeterRegistry registry, String name) {
        var counter = registry.find(name).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static double faults(MeterRegistry registry, String kind) {
        var counter = registry.find(SimStoreMetrics.SEGMENT_FAULT_METRIC).tag("kind", kind).counter();
        return counter == null ? 0.0 : counter.count();
    }

    /** Live heap after a collection — the on-heap half of "what did this tier cost to hold". */
    private static double heapUsedMb() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (double) (1 << 20);
    }

    private static List<IndexEntry> index(Path fixture) {
        try {
            IndexLoadResult loaded = SortedFixtures.loadIndex(
                    SortedFixtures.resolveFiles(fixture), new FixtureMetrics());
            return loaded instanceof IndexLoadResult.Loaded l ? l.entries() : List.of();
        } catch (IOException e) {
            throw new IllegalStateException("failed to derive the bench fixture's row-group index", e);
        }
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
