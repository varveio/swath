/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public final class ReplayServer implements AutoCloseable {

    private final Server server;
    private final AutoCloseable ownedFixture;
    private final ReplayMetrics metrics;
    private final ServingMode resolvedMode;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ReplayServer(String host, int port, String bucket, Path fixture) {
        // DUCKDB, not the CLI's `sorted` default: this constructor's callers are the in-process
        // ones — the conformance comparator serving raw HAR captures, and tests over arbitrary
        // fixtures — for which "works on any capture" is the point. A served benchmark goes through
        // the CLI, where the operator states the mode.
        this(host, port, bucket, fixture, 0, ServingMode.DUCKDB);
    }

    public ReplayServer(String host, int port, String bucket, Path fixture, int parquetConnections,
                                 ServingMode mode) {
        this(host, port, bucket, fixture, parquetConnections, mode, (req, result) -> Duration.ZERO);
    }

    /** As above, with an explicit concurrently-served-request ceiling (see the latency overload). */
    public ReplayServer(String host, int port, String bucket, Path fixture, int parquetConnections,
                        ServingMode mode, int maxConcurrentRequests) {
        this(host, port, bucket, fixture, parquetConnections, mode, (req, result) -> Duration.ZERO,
                maxConcurrentRequests);
    }

    /**
     * As above, with per-request latency injection on the production serving path — the {@code
     * serve} command's {@code --inject-latency} wiring. The hook sees the request and the result it
     * produced (so a delay can be fanout-proportional, e.g. {@link ShapeLatency}); it runs after the
     * fixture read, outside the read-concurrency permit, on the Jetty handler thread.
     */
    public ReplayServer(String host, int port, String bucket, Path fixture, int parquetConnections,
                        ServingMode mode, BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        this(host, port, bucket, fixture, parquetConnections, mode, latency, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    /**
     * As above, with an explicit ceiling on concurrently served requests.
     *
     * <p>It has to be explicit because injected latency is a <b>blocking</b> sleep held on the
     * serving thread, so an in-flight request occupies one for the whole profile. Jetty's default
     * pool stops at 200 threads; a client fanning out wider than that has its excess requests queued
     * in the connector, and the wait lands on the client as latency from a server that is, by CPU,
     * asleep. A client that fans out wider than the pool — and some cannot be told not to — would
     * then be measured against a different backend than a narrower one.
     */
    public ReplayServer(String host, int port, String bucket, Path fixture, int parquetConnections,
                        ServingMode mode, BiFunction<S3ListRequest, S3ListResult, Duration> latency,
                        int maxConcurrentRequests) {
        this(host, port, bucket, ReplayServingFactory.open(fixture, mode, parquetConnections), latency,
                maxConcurrentRequests);
    }

    /**
     * The default ceiling on concurrently served requests, set comfortably above the widest fan-out a
     * listing client is likely to drive: a request parked in an injected sleep holds its thread for
     * the whole profile, and a queued one is indistinguishable, to the client, from a slow server.
     */
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 512;

    private ReplayServer(String host, int port, String bucket, ReplayServingFactory.Result fixture,
                         BiFunction<S3ListRequest, S3ListResult, Duration> latency,
                         int maxConcurrentRequests) {
        this(host, port, bucket, fixture.fixture(), fixture.fixture(), fixture.metrics(),
                fixture.maxConcurrentReads(), fixture.resolvedMode(), latency, maxConcurrentRequests);
    }

    // Package-private (not private) so tests can exercise close()'s idempotency/suppression
    // contract and the concurrency bound directly, without going through a real DuckDB fixture.
    ReplayServer(String host, int port, String bucket, ListingFixture fixture) {
        this(host, port, bucket, fixture, ReplayServerFixtureConfig.DEFAULT);
    }

    /**
     * The direct-injection seam: wire a server around an already-constructed {@code fixture} with
     * its optional {@link ReplayServerFixtureConfig} clump (owned fixture, concurrency bound,
     * per-request latency injection). Always resolves to {@link ServingMode#DUCKDB} and installs a
     * fresh {@link ReplayMetrics}; production wiring goes through {@link ReplayServingFactory}
     * instead. Latency injection reuses the {@code io.varve.swath.testkit.LatencyModel} profiles the
     * {@code swath-core} testFixtures define, via a test-side adapter (this module's main code does
     * not depend on testFixtures).
     */
    ReplayServer(String host, int port, String bucket, ListingFixture fixture,
                          ReplayServerFixtureConfig config) {
        this(host, port, bucket, fixture, config.ownedFixture(), new ReplayMetrics(),
                config.maxConcurrentReads(), ServingMode.DUCKDB, config.latency());
    }

    private ReplayServer(String host, int port, String bucket, ListingFixture fixture,
                          AutoCloseable ownedFixture, ReplayMetrics metrics, int maxConcurrentReads,
                          ServingMode resolvedMode, BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        this(host, port, bucket, fixture, ownedFixture, metrics, maxConcurrentReads, resolvedMode, latency,
                DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    private ReplayServer(String host, int port, String bucket, ListingFixture fixture,
                          AutoCloseable ownedFixture, ReplayMetrics metrics, int maxConcurrentReads,
                          ServingMode resolvedMode, BiFunction<S3ListRequest, S3ListResult, Duration> latency,
                          int maxConcurrentRequests) {
        QueuedThreadPool pool = new QueuedThreadPool(Math.max(8, maxConcurrentRequests));
        pool.setName("replay-serve");
        this.server = new Server(pool);
        this.ownedFixture = ownedFixture;
        this.metrics = metrics;
        this.resolvedMode = resolvedMode;
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(new ReplayHandler(bucket, fixture, metrics, maxConcurrentReads, latency));
    }

    /** The concrete serving path this server resolved to ({@link ServingMode#SORTED} or {@link ServingMode#DUCKDB}). */
    public ServingMode resolvedServingMode() {
        return resolvedMode;
    }

    public void start() throws Exception {
        server.start();
    }

    public void join() throws InterruptedException {
        server.join();
    }

    public int port() {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    ReplayMetrics metrics() {
        return metrics;
    }

    public String metricsSummary() {
        return metrics.summaryLine();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException failure = null;
        try {
            server.stop();
        } catch (Exception e) {
            failure = new IllegalStateException("failed to stop S3 listing replay server", e);
        }
        if (ownedFixture != null) {
            try {
                ownedFixture.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = new IllegalStateException("failed to close S3 listing replay fixture", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        metrics.logSummary();
        if (failure != null) {
            throw failure;
        }
    }
}
