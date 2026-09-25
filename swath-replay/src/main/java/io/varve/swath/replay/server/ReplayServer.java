/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.fixture.FixtureIdentity;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.azure.AzureHandler;
import io.varve.swath.replay.protocol.gcs.GcsHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReplayServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplayServer.class);
    private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(10);

    private final Server server;
    private final AutoCloseable ownedFixture;
    private final ListingRequestRunner runner;
    private final ReplayMetrics metrics;
    private final ServingMode resolvedMode;
    private final int resolvedParquetConnections;
    private final Duration stopTimeout;
    private final ServeConfig serveConfig;
    private final String fixtureIdentity;
    private final int readPermitLimit;
    private final int maxConcurrentRequests;
    private final String bucket;
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
        this(host, port, bucket, openValidated(fixture, mode, parquetConnections, maxConcurrentRequests), latency,
                maxConcurrentRequests);
    }

    private static ReplayServingFactory.Result openValidated(Path fixture, ServingMode mode,
                                                               int parquetConnections,
                                                               int maxConcurrentRequests) {
        validateMaxConcurrentRequests(maxConcurrentRequests);
        return ReplayServingFactory.open(fixture, mode, parquetConnections);
    }

    static void validateMaxConcurrentRequests(int maxConcurrentRequests) {
        if (maxConcurrentRequests < 8) {
            throw new IllegalArgumentException("maxConcurrentRequests must be at least 8");
        }
    }

    /**
     * The default ceiling on concurrently served requests, set comfortably above the widest fan-out a
     * listing client is likely to drive: a request parked in an injected sleep holds its thread for
     * the whole profile, and a queued one is indistinguishable, to the client, from a slow server.
     */
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 512;

    /** Opens one fixture owner and routes all selected protocols through its shared resources. */
    public static ReplayServer open(ServeConfig config) {
        String identity;
        try {
            identity = FixtureIdentity.of(config.fixture());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to identify replay fixture", e);
        }
        ReplayServingFactory.Result opened = ReplayServingFactory.open(config.fixture(),
                config.servingMode(), config.parquetConnections());
        try {
            return new ReplayServer(config, opened, identity);
        } catch (RuntimeException | Error e) {
            try {
                opened.close();
            } catch (RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private ReplayServer(ServeConfig config, ReplayServingFactory.Result opened, String identity) {
        this.ownedFixture = opened;
        this.serveConfig = config;
        this.fixtureIdentity = identity;
        this.readPermitLimit = opened.requestAdmissionLimit();
        this.maxConcurrentRequests = config.maxConcurrentRequests();
        this.bucket = config.bucket();
        this.metrics = opened.metrics();
        this.resolvedMode = opened.resolvedMode();
        this.resolvedParquetConnections = opened.parquetConnections();
        this.stopTimeout = config.stopTimeout();
        QueuedThreadPool pool = new QueuedThreadPool(config.maxConcurrentRequests());
        pool.setName("replay-serve");
        this.server = new Server(pool);
        ServerConnector connector;
        if (config.protocols().size() == 1 && config.protocols().contains(Protocol.S3)) {
            connector = new ServerConnector(server);
        } else {
            HttpConfiguration http = new HttpConfiguration();
            http.setRequestHeaderSize(64 * 1024);
            connector = new ServerConnector(server, new HttpConnectionFactory(http));
        }
        connector.setHost(config.host());
        connector.setPort(config.port());
        connector.setIdleTimeout(config.idleTimeout().toMillis());
        server.addConnector(connector);
        Map<Protocol, ListingProtocolHandler> handlers = new EnumMap<>(Protocol.class);
        if (config.protocols().contains(Protocol.S3)) {
            handlers.put(Protocol.S3, new S3ListingProtocolHandler(config.bucket(), opened.fixture(), metrics,
                    config.s3Latency()));
        }
        if (config.protocols().contains(Protocol.GCS)) {
            handlers.put(Protocol.GCS, new GcsHandler(config.bucket(), opened.store(), identity, metrics));
        }
        if (config.protocols().contains(Protocol.AZURE)) {
            String host = config.advertisedHost() == null ? config.host() : config.advertisedHost();
            String authorityHost = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
            handlers.put(Protocol.AZURE, new AzureHandler(config.azureAccount(), config.bucket(),
                    () -> "http://%s:%d/%s/".formatted(authorityHost, connector.getLocalPort(),
                    config.azureAccount()), opened.store(), identity, metrics));
        }
        this.runner = new ListingRequestRunner(metrics, opened.requestAdmissionLimit(),
                config.maxResponses(), config.responseBufferBudget(), config.maxResponseBytes(),
                config.writeTimeout());
        server.setHandler(new ReplayHandler(handlers, config.azureAccount(), runner));
    }

    // Package-private so the wiring test can pass deliberately different backing-pool and outer
    // admission limits and prove this boundary does not accidentally collapse them again.
    ReplayServer(String host, int port, String bucket, ReplayServingFactory.Result fixture,
                 BiFunction<S3ListRequest, S3ListResult, Duration> latency,
                 int maxConcurrentRequests) {
        this(host, port, bucket, fixture.fixture(), fixture, fixture.metrics(),
                fixture.parquetConnections(), fixture.requestAdmissionLimit(), fixture.resolvedMode(), latency,
                maxConcurrentRequests, DEFAULT_STOP_TIMEOUT);
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
                config.maxConcurrentReads(), config.maxConcurrentReads(), ServingMode.DUCKDB, config.latency(),
                DEFAULT_MAX_CONCURRENT_REQUESTS, config.stopTimeout());
    }

    private ReplayServer(String host, int port, String bucket, ListingFixture fixture,
                          AutoCloseable ownedFixture, ReplayMetrics metrics, int parquetConnections,
                          int requestAdmissionLimit,
                          ServingMode resolvedMode, BiFunction<S3ListRequest, S3ListResult, Duration> latency,
                          int maxConcurrentRequests, Duration stopTimeout) {
        validateMaxConcurrentRequests(maxConcurrentRequests);
        QueuedThreadPool pool = new QueuedThreadPool(maxConcurrentRequests);
        pool.setName("replay-serve");
        this.server = new Server(pool);
        this.ownedFixture = ownedFixture;
        this.serveConfig = null;
        this.fixtureIdentity = "unknown";
        this.readPermitLimit = requestAdmissionLimit;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.bucket = bucket;
        this.metrics = metrics;
        this.resolvedMode = resolvedMode;
        this.resolvedParquetConnections = parquetConnections;
        this.stopTimeout = stopTimeout;
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        ReplayHandler handler = new ReplayHandler(bucket, fixture, metrics, requestAdmissionLimit, latency,
                maxConcurrentRequests);
        this.runner = handler.runner();
        server.setHandler(handler);
    }

    /** The concrete serving path this server resolved to ({@link ServingMode#SORTED} or {@link ServingMode#DUCKDB}). */
    public ServingMode resolvedServingMode() {
        return resolvedMode;
    }

    /**
     * The pooled-reader count this server actually opened with — the requested one, or the resolved
     * mode's own default when none was requested.
     *
     * <p>Exposed because the two differ, and only the server knows which it got: the default belongs
     * to the store the mode resolves to, and that resolution happens here rather than in the caller.
     * A caller that wants to report the value it is serving under has to ask, not assume.
     */
    public int resolvedParquetConnections() {
        return resolvedParquetConnections;
    }

    public void start() throws Exception {
        try {
            server.start();
        } catch (Exception | Error startFailure) {
            try {
                close();
            } catch (RuntimeException cleanup) {
                startFailure.addSuppressed(cleanup);
            }
            throw startFailure;
        }
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

    ServingMetadata servingMetadata() {
        if (serveConfig == null) {
            return new ServingMetadata(java.util.List.of("s3"), bucket, null,
                    fixtureIdentity, resolvedMode.toString(), "fixture", maxConcurrentRequests,
                    runner.maxResponses(), readPermitLimit, runner.responseBufferBudget(),
                    runner.maxResponseBytes(), runner.chargedBytes(), runner.peakChargedBytes(),
                    runner.activeResponses(), stopTimeout.toMillis(),
                    ((ServerConnector) server.getConnectors()[0]).getIdleTimeout(),
                    runner.writeTimeoutMs(), "off", "default",
                    java.util.Map.of("s3", "list-objects-v2"));
        }
        var protocols = serveConfig.protocols().stream().map(protocol ->
                protocol.name().toLowerCase(java.util.Locale.ROOT)).sorted().toList();
        var profiles = new java.util.TreeMap<String, String>();
        for (String protocol : protocols) {
            profiles.put(protocol, switch (protocol) {
                case "s3" -> "list-objects-v2";
                case "gcs" -> "json-v1";
                case "azure" -> "blob-list-flat-2026-06-06,2026-10-06";
                default -> throw new IllegalStateException("unknown replay protocol " + protocol);
            });
        }
        return new ServingMetadata(protocols, serveConfig.bucket(), serveConfig.azureAccount(),
                fixtureIdentity, resolvedMode.toString(), "fixture-plus-synthetic",
                serveConfig.maxConcurrentRequests(), serveConfig.maxResponses(), readPermitLimit,
                serveConfig.responseBufferBudget(), serveConfig.maxResponseBytes(),
                runner.chargedBytes(), runner.peakChargedBytes(), runner.activeResponses(),
                serveConfig.stopTimeout().toMillis(), serveConfig.idleTimeout().toMillis(),
                serveConfig.writeTimeout().toMillis(),
                serveConfig.s3Latency() instanceof ShapeLatency ? "s3-shape" : "off", "default",
                profiles);
    }

    public String metricsSummary() {
        return metrics.summaryLine();
    }

    @Override
    public void close() {
        closeAt(System.nanoTime() + stopTimeout.toNanos(),
                java.util.concurrent.CompletableFuture.completedFuture(null));
    }

    void closeAt(long deadline) {
        closeAt(deadline, java.util.concurrent.CompletableFuture.completedFuture(null));
    }

    void beginShutdownAt(long deadline) {
        runner.beginShutdown(deadline);
    }

    void closeAt(long deadline, java.util.concurrent.CompletableFuture<Void> metricsStopped) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        runner.beginShutdown(deadline);
        AtomicReference<RuntimeException> closeFailure = new AtomicReference<>();
        RuntimeException failure = null;
        long remainingMs = Math.max(1, (deadline - System.nanoTime()) / 1_000_000L);
        server.setStopTimeout(remainingMs);
        ((QueuedThreadPool) server.getThreadPool()).setStopTimeout(remainingMs);
        // Jetty 12 resets the pool's stop timeout to at least 1 s inside Server.doStop().
        // The daemon stopper lets our one absolute deadline remain authoritative.
        CountDownLatch jettyStopped = new CountDownLatch(1);
        AtomicReference<Exception> stopError = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            try {
                server.stop();
            } catch (Exception e) {
                stopError.set(e);
                if (System.nanoTime() >= deadline) {
                    log.error("replay Jetty stop failed after shutdown deadline", e);
                }
            } finally {
                jettyStopped.countDown();
            }
        }, "replay-jetty-stop");
        stopper.setDaemon(true);
        stopper.start();
        boolean stoppedInTime;
        try {
            stoppedInTime = jettyStopped.await(Math.max(0, deadline - System.nanoTime()),
                    TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stoppedInTime = false;
        }
        if (!stoppedInTime) {
            log.error("shutdown_incomplete Jetty stop exceeded its deadline");
        } else if (stopError.get() instanceof TimeoutException) {
            log.error("shutdown_incomplete Jetty stop exceeded its deadline");
        } else if (stopError.get() != null) {
            failure = new IllegalStateException("failed to stop swath-replay", stopError.get());
        }
        runner.afterOperations(() -> {
            if (ownedFixture == null) {
                return;
            }
            try {
                if (ownedFixture instanceof ReplayServingFactory.Result result) {
                    result.closeStore();
                } else {
                    ownedFixture.close();
                }
            } catch (Exception e) {
                RuntimeException wrapped = new IllegalStateException(
                        "failed to close S3 listing replay fixture", e);
                closeFailure.set(wrapped);
                log.error("replay store cleanup failed", e);
            }
        });
        runner.afterAll(() -> metricsStopped.whenComplete((ignored, endpointFailure) -> {
            metrics.logSummary();
            try {
                if (ownedFixture instanceof ReplayServingFactory.Result result) {
                    result.closeRegistry();
                } else {
                    metrics.registry().close();
                }
            } catch (RuntimeException e) {
                closeFailure.set(e);
                log.error("replay metrics cleanup failed", e);
            } finally {
                runner.stopDeadlines();
            }
        }));
        boolean drained = runner.awaitAll(deadline);
        if (!drained) {
            log.error("shutdown_incomplete active replay work retained its resources for deferred cleanup");
        }
        RuntimeException cleanupFailure = closeFailure.get();
        if (cleanupFailure != null) {
            if (failure == null) {
                failure = cleanupFailure;
            } else {
                failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
