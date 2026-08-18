/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.MeterRegistry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * A read-only HTTP surface over a running {@code serve}'s meters: {@code GET /metrics} answers the
 * whole registry as JSON, {@code GET /healthz} answers {@code ok} once the server is listening.
 *
 * <p><b>Why an endpoint and not a report file.</b> The consumer is a benchmark harness that runs the
 * server as a sidecar and the tool under test as the foreground process. When the tool exits the
 * sidecar is <em>killed</em>, not asked to stop — GCP Batch's background runnables work exactly this
 * way — so anything written on shutdown is written by a code path that may never run. A scrape the
 * harness pulls while the run is still alive survives any death the server suffers afterwards.
 *
 * <p><b>Why a second port.</b> Scraping must not be observable in what is being measured. On its own
 * connector a scrape never enters the serving path: it takes no read permit, receives no injected
 * latency, and increments no request counter — so reading the metrics cannot change them. It also
 * keeps the scrape answerable while every serving thread is parked in an injected sleep, which is
 * precisely the moment a harness most wants an answer.
 *
 * <p>Its thread pool is deliberately tiny. This is a low-rate diagnostic surface, and taking threads
 * from the box to answer it would tax the measurement it exists to validate.
 */
final class MetricsEndpoint implements AutoCloseable {

    private static final int MAX_THREADS = 4;
    private static final int MIN_THREADS = 1;

    private final Server server;

    private MetricsEndpoint(Server server) {
        this.server = server;
    }

    /**
     * Binds and starts the endpoint.
     *
     * @param port {@code 0} binds a free port, which {@link #port()} then reports
     */
    static MetricsEndpoint start(String host, int port, MeterRegistry registry, String servingMode,
                                 long startedNanos) throws Exception {
        QueuedThreadPool pool = new QueuedThreadPool(MAX_THREADS, MIN_THREADS);
        pool.setName("replay-metrics");
        Server server = new Server(pool);
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(new MetricsHandler(registry, servingMode, startedNanos));
        server.start();
        return new MetricsEndpoint(server);
    }

    int port() {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @Override
    public void close() throws Exception {
        server.stop();
    }

    private static final class MetricsHandler extends Handler.Abstract {

        private final MeterRegistry registry;
        private final String servingMode;
        private final long startedNanos;

        private MetricsHandler(MeterRegistry registry, String servingMode, long startedNanos) {
            this.registry = registry;
            this.servingMode = servingMode;
            this.startedNanos = startedNanos;
        }

        @Override
        public boolean handle(Request request, Response response, Callback callback) {
            String path = request.getHttpURI().getPath();
            if ("/healthz".equals(path)) {
                write(response, HttpStatus.OK_200, "text/plain", "ok\n", callback);
                return true;
            }
            if (!"/metrics".equals(path)) {
                write(response, HttpStatus.NOT_FOUND_404, "text/plain",
                        "not found; this port serves /metrics and /healthz only\n", callback);
                return true;
            }
            long uptimeMillis = (System.nanoTime() - startedNanos) / 1_000_000L;
            String body = MetricsSnapshotJson.render(registry, servingMode, uptimeMillis,
                    System.currentTimeMillis());
            write(response, HttpStatus.OK_200, "application/json", body + "\n", callback);
            return true;
        }

        private static void write(Response response, int status, String contentType, String body,
                                  Callback callback) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            response.setStatus(status);
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
            response.getHeaders().put(HttpHeader.CONTENT_LENGTH, Integer.toString(bytes.length));
            response.write(true, ByteBuffer.wrap(bytes), callback);
        }
    }
}
