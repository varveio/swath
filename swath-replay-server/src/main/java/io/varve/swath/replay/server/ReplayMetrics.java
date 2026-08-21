/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micrometer-backed replay-server counters/timers, constructed once and passed down into
 * {@code .protocol} ({@code ListObjectsV2Pager}) and {@code .store} ({@code DuckDbListingStore}).
 * Those packages depending on this concrete class is an accepted, narrow, one-directional edge
 * (a leaf class with no dependency back on protocol/store) — not the protocol↔server import cycle
 * this package split was meant to remove. If this class ever grows server-only concerns, extract a
 * narrow metrics-sink interface for {@code .protocol}/{@code .store} to depend on instead; not
 * needed while it stays this small.
 */
public final class ReplayMetrics {

    private static final Logger log = LoggerFactory.getLogger(ReplayMetrics.class);

    /** The default {@code serving.path} tag: the DuckDB role-1 path. */
    public static final String SERVING_MODE_DUCKDB = "duckdb";

    /** The sorted-Parquet role-2 serving path tag. */
    public static final String SERVING_MODE_SORTED = "sorted";

    /** {@code swath.replay.delimiter.path} tag: the store answered the whole rollup in one set-oriented pass. */
    public static final String DELIMITER_PATH_ROLLUP = "rollup";

    /** {@code swath.replay.delimiter.path} tag: the store declined and the pager walked ranges instead. */
    public static final String DELIMITER_PATH_WALK = "walk";

    private final MeterRegistry registry;
    private final String servingMode;
    private final Counter httpRequests;
    private final Counter httpErrors;
    private final Timer httpRequestLatency;
    private final Timer fixtureListLatency;
    private final Map<ShapeLatency.Shape, Timer> shapedRequestLatency;
    private final Counter servingPath;
    private final Timer pageReadLatency;
    private final Timer parquetQueryLatency;
    private final Counter parquetQueryErrors;
    private final DistributionSummary parquetQueryRows;
    private final Counter prefetchWindowHit;
    private final DistributionSummary prefetchFillRows;
    private final Timer prefetchWindowFill;
    private final Counter delimiterSkipScanRowGroupOpens;
    private final AtomicLong parquetQueriesInFlight = new AtomicLong();
    private final long startedNanos = System.nanoTime();

    public ReplayMetrics() {
        this(new SimpleMeterRegistry());
    }

    public ReplayMetrics(MeterRegistry registry) {
        this(registry, SERVING_MODE_DUCKDB);
    }

    /**
     * @param servingMode the {@code swath.replay.serving.path\{mode\}} tag value for this fixture —
     *                    {@link #SERVING_MODE_SORTED} for the role-2 sorted-Parquet store,
     *                    {@link #SERVING_MODE_DUCKDB} (the default) for the role-1 DuckDB store. The
     *                    server wiring, which resolves the concrete path, chooses it once at
     *                    construction; every {@code list} request then bumps that one counter.
     */
    public ReplayMetrics(MeterRegistry registry, String servingMode) {
        this.registry = registry;
        this.servingMode = servingMode;
        httpRequests = Counter.builder("swath.replay.http.requests").register(registry);
        httpErrors = Counter.builder("swath.replay.http.errors").register(registry);
        httpRequestLatency = Timer.builder("swath.replay.http.request.latency")
                .publishPercentiles(0.5, 0.99).register(registry);
        fixtureListLatency = Timer.builder("swath.replay.fixture.list.latency")
                .publishPercentiles(0.5, 0.99).register(registry);
        shapedRequestLatency = new EnumMap<>(ShapeLatency.Shape.class);
        for (ShapeLatency.Shape shape : ShapeLatency.Shape.values()) {
            shapedRequestLatency.put(shape, Timer.builder("swath.replay.request.latency")
                    .tag("shape", shape.name().toLowerCase(Locale.ROOT))
                    .publishPercentiles(0.5, 0.99)
                    .register(registry));
        }
        servingPath = Counter.builder("swath.replay.serving.path").tag("mode", servingMode).register(registry);
        pageReadLatency = Timer.builder("swath.replay.page.read.latency")
                .publishPercentiles(0.5, 0.99).register(registry);
        parquetQueryLatency = Timer.builder("swath.replay.parquet.query.latency")
                .publishPercentiles(0.5, 0.99).register(registry);
        parquetQueryErrors = Counter.builder("swath.replay.parquet.query.errors").register(registry);
        parquetQueryRows = DistributionSummary.builder("swath.replay.parquet.query.rows").register(registry);
        prefetchWindowHit = Counter.builder("swath.replay.prefetch.window.hit").register(registry);
        prefetchFillRows = DistributionSummary.builder("swath.replay.prefetch.fill.rows").register(registry);
        prefetchWindowFill = Timer.builder("swath.replay.prefetch.window.fill")
                .publishPercentiles(0.5, 0.99).register(registry);
        delimiterSkipScanRowGroupOpens = Counter.builder("swath.replay.delimiter.skipscan.row_group_opens")
                .register(registry);
        Gauge
                .builder("swath.replay.parquet.queries.in_flight", parquetQueriesInFlight, AtomicLong::get)
                .register(registry);
    }

    /** The resolved serving path this metrics instance tags requests with. */
    public String servingMode() {
        return servingMode;
    }

    public MeterRegistry registry() {
        return registry;
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void recordHttpRequest(Timer.Sample sample, int status) {
        httpRequests.increment();
        if (status >= 500) {
            httpErrors.increment();
        }
        sample.stop(httpRequestLatency);
    }

    public void recordFixtureList(Timer.Sample sample) {
        servingPath.increment();
        sample.stop(fixtureListLatency);
    }

    /**
     * Starts the store-level page-read timer. Callers must borrow their connection <b>first</b> and
     * only then start this timer, so {@code swath.replay.page.read.latency} measures the read
     * itself and never the connection-pool wait — keeping the decode cost it reports distinct from
     * pool contention.
     */
    public Timer.Sample startPageReadTimer() {
        return startTimer();
    }

    /** Records one store-level range read (see {@link #startPageReadTimer()}). */
    public void recordPageRead(Timer.Sample sample) {
        sample.stop(pageReadLatency);
    }

    public void recordParquetQuery(Timer.Sample sample, int rows, boolean success) {
        parquetQueriesInFlight.decrementAndGet();
        parquetQueryRows.record(rows);
        if (!success) {
            parquetQueryErrors.increment();
        }
        sample.stop(parquetQueryLatency);
    }

    public Timer.Sample startParquetQueryTimer() {
        parquetQueriesInFlight.incrementAndGet();
        return startTimer();
    }

    /** Records one sequential-window prefetch cache hit (served from a buffered window, no delegate read). */
    public void recordPrefetchHit() {
        prefetchWindowHit.increment();
    }

    /**
     * Records one sequential-window prefetch cache miss (a delegate window fill was required), tagged
     * with why the fill was sized as it was: {@code continuation} (this position was registered as the
     * tail of a page already served, so a paginating client is walking forward and the fill ramps
     * toward {@code window-rows}) or {@code cold} (a probe or a seek into unvisited keyspace, whose
     * fill stays at the caller's own limit). The split is what tells post-hoc analysis whether the
     * ramp is engaging on a given client's request mix.
     */
    public void recordPrefetchMiss(String reason) {
        Counter.builder("swath.replay.prefetch.window.miss")
                .tag("reason", reason).register(registry).increment();
    }

    /**
     * Records the server's own cost of serving one request, tagged with the request's
     * {@link ShapeLatency.Shape shape} — {@code worker_page}, {@code pivot_probe} or
     * {@code structure_probe}, the same three the latency injector keys on.
     *
     * <p>The shapes cost wildly different amounts to serve and are issued in wildly different
     * proportions by different clients: a work-stealing scan probes constantly, a sequential pager
     * only ever asks for worker pages. An untagged average over that mixture describes no client in
     * particular, and moves when the client's mixture moves rather than when the server does — which
     * is exactly the wrong behavior for a number whose job is to prove the server was not the
     * bottleneck.
     *
     * <p>Timed around the fixture read and <b>excluding</b> any injected delay: this measures what
     * the server costs, not what it was told to pretend to cost.
     */
    public void recordShapedRequest(Timer.Sample sample, ShapeLatency.Shape shape) {
        sample.stop(shapedRequestLatency.get(shape));
    }


    /**
     * Records one request the server could not serve within its injected latency profile, tagged
     * with the request's shape, plus how far past the profile it went.
     *
     * <p>This is the fairness gate for any benchmark run against an injected profile. While the
     * server is faster than the backend it imitates, every client observes exactly the profile and
     * the server is invisible. Once it is slower, the client observes the server instead — and it
     * does so unevenly, because server cost depends on the access pattern the client happens to
     * have. A run whose overruns are a negligible fraction of its requests measured what it meant
     * to; a run full of them measured the harness. Only a counter can tell those apart after the
     * fact, and the server that could have said so is usually gone by then.
     */
    public void recordInjectionOverrun(ShapeLatency.Shape shape, long overshootNanos) {
        String tag = shape.name().toLowerCase(Locale.ROOT);
        Counter.builder("swath.replay.inject.overrun")
                .tag("shape", tag).register(registry).increment();
        DistributionSummary.builder("swath.replay.inject.overrun.ms")
                .tag("shape", tag).register(registry)
                .record(overshootNanos / 1_000_000.0);
    }

    /**
     * Records one request-time refusal of a fixture the sorted path cannot serve, tagged with the
     * typed reason ({@code row_group_disorder}: a row group's own rows are not in ascending order,
     * which eligibility cannot see because it proves the ascent of row-group <em>first</em> keys
     * only). Its own counter and not a {@code serving.fallback} reason, because it is not a fallback
     * — the fixture already passed eligibility, no other path can take over mid-request, and the
     * request fails. A sweep classifying an excluded capture reads this, never the error body.
     */
    public void recordServingRefused(String reason) {
        Counter.builder("swath.replay.serving.refused")
                .tag("reason", reason).register(registry).increment();
    }

    /**
     * Records which path served one {@code delimiter=/} listing request — {@link #DELIMITER_PATH_ROLLUP}
     * (the store answered the whole rollup natively) or {@link #DELIMITER_PATH_WALK} (the store declined
     * and the pager fell back to a seek per common prefix). Silent-until-someone-times-it was the whole
     * complaint that motivated this counter: a stalled delimiter panel is now attributable to the
     * fallback path from the metrics alone, without a wall-clock reproduction.
     */
    public void recordDelimiterPath(String path) {
        Counter.builder("swath.replay.delimiter.path").tag("path", path).register(registry).increment();
    }

    /**
     * Records one row-group open inside {@code SortedParquetStore}'s delimiter skip-scan (a group the
     * zero-I/O whole-group shortcut couldn't resolve without decoding). A test asserting this stays
     * bounded by the number of rolled-up prefixes — not by the number of keys under them — is the cost
     * regression guard for the skip-scan itself.
     */
    public void recordDelimiterSkipScanRowGroupOpen() {
        delimiterSkipScanRowGroupOpens.increment();
    }

    /** Records how many rows one window fill asked the delegate for (proves the ramp engages). */
    public void recordPrefetchFillRows(int rows) {
        prefetchFillRows.record(rows);
    }

    /** Starts the timer around a prefetch window fill (the delegate read on a miss). */
    public Timer.Sample startPrefetchFillTimer() {
        return startTimer();
    }

    /** Records the duration of one prefetch window fill (see {@link #startPrefetchFillTimer()}). */
    public void recordPrefetchFill(Timer.Sample sample) {
        sample.stop(prefetchWindowFill);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos),
                (long) httpRequests.count(),
                httpRequestLatency.count(),
                httpRequestLatency.totalTime(TimeUnit.MILLISECONDS),
                httpRequestLatency.max(TimeUnit.MILLISECONDS),
                fixtureListLatency.count(),
                fixtureListLatency.totalTime(TimeUnit.MILLISECONDS),
                fixtureListLatency.max(TimeUnit.MILLISECONDS),
                parquetQueryLatency.count(),
                parquetQueryLatency.totalTime(TimeUnit.MILLISECONDS),
                parquetQueryLatency.max(TimeUnit.MILLISECONDS),
                (long) parquetQueryRows.totalAmount(),
                parquetQueryRows.max(),
                (long) parquetQueryErrors.count());
    }

    public void logSummary() {
        String line = summaryLine();
        if (!line.isBlank()) {
            log.info(line);
        }
    }

    public String summaryLine() {
        Snapshot s = snapshot();
        if (s.httpRequests() == 0 && s.parquetQueries() == 0) {
            return "";
        }
        return ("replay_server_metrics wall_ms=%d http_requests=%d http_request_ms_sum=%s http_request_ms_avg=%s "
                + "http_request_ms_max=%s fixture_lists=%d fixture_list_ms_sum=%s fixture_list_ms_avg=%s "
                + "fixture_list_ms_max=%s parquet_queries=%d parquet_query_ms_sum=%s "
                + "parquet_query_ms_avg=%s parquet_query_ms_max=%s parquet_rows=%d parquet_rows_per_query_avg=%s "
                + "parquet_rows_per_query_max=%s parquet_query_errors=%d")
                .formatted(
                        s.wallMs(),
                        s.httpRequests(),
                        format(s.httpRequestMsTotal()), format(avg(s.httpRequestMsTotal(), s.httpRequestTimerCount())),
                        format(s.httpRequestMsMax()),
                        s.fixtureLists(), format(s.fixtureListMsTotal()),
                        format(avg(s.fixtureListMsTotal(), s.fixtureLists())),
                        format(s.fixtureListMsMax()),
                        s.parquetQueries(), format(s.parquetQueryMsTotal()),
                        format(avg(s.parquetQueryMsTotal(), s.parquetQueries())), format(s.parquetQueryMsMax()),
                        s.parquetRows(), format(avg(s.parquetRows(), s.parquetQueries())),
                        format(s.parquetRowsPerQueryMax()), s.parquetQueryErrors());
    }

    private static double avg(double total, long count) {
        return count == 0 ? 0.0 : total / count;
    }

    private static String format(double value) {
        return "%.3f".formatted(value);
    }

    public record Snapshot(long wallMs, long httpRequests, long httpRequestTimerCount, double httpRequestMsTotal,
                           double httpRequestMsMax, long fixtureLists, double fixtureListMsTotal,
                           double fixtureListMsMax, long parquetQueries, double parquetQueryMsTotal,
                           double parquetQueryMsMax, long parquetRows, double parquetRowsPerQueryMax,
                           long parquetQueryErrors) {
    }
}
