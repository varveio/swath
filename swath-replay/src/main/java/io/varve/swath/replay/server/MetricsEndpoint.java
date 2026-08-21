/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
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
 * A read-only HTTP surface over a running {@code serve}'s meters and runtime: {@code GET /metrics}
 * answers the whole registry as JSON, {@code GET /runtime-attestation} answers the Linux resource
 * limits visible inside the server container, and {@code GET /healthz} answers {@code ok} once the
 * server is listening.
 *
 * <p><b>Why an endpoint and not a report file.</b> Run as a sidecar, this server is typically
 * <em>killed</em> when the process it serves exits, rather than asked to stop — so anything written
 * on shutdown is written by a code path that may never run. A scrape taken while the run is alive
 * survives any death the server suffers afterwards.
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
    private final JvmGcMetrics gc;
    private final JvmHeapPressureMetrics heapPressure;

    private MetricsEndpoint(Server server, JvmGcMetrics gc, JvmHeapPressureMetrics heapPressure) {
        this.server = server;
        this.gc = gc;
        this.heapPressure = heapPressure;
    }

    /**
     * Binds what the process knows about itself: heap, GC, threads, and its own CPU.
     *
     * <p><b>Why this belongs here and not in the serving path.</b> A sidecar's consumer cannot read
     * these any other way. Its cgroup is not visible from the container beside it — {@code
     * /sys/fs/cgroup} is namespaced — so a harness wanting to know whether the server was saturated
     * has had to infer it from throughput times service time, which is an inference that has been
     * wrong in both directions. The scrape already crosses the container boundary; the numbers
     * should cross with it.
     *
     * <p>Bound at the endpoint rather than at the registry because these exist to be scraped: no
     * {@code --metrics-port}, no consumer, no reason to pay for the GC notification listeners.
     *
     * <p>{@code jvm.gc.*} and {@code jvm.memory.pressure} are the pair that says whether a pause,
     * rather than a slow read, is what pushed a request past its injected deadline — a question the
     * request timers alone cannot answer, since a pause lands inside them.
     */
    private static Binders bindProcessMeters(MeterRegistry registry) {
        new JvmMemoryMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        JvmGcMetrics gc = new JvmGcMetrics();
        gc.bindTo(registry);
        JvmHeapPressureMetrics heapPressure = new JvmHeapPressureMetrics();
        heapPressure.bindTo(registry);
        return new Binders(gc, heapPressure);
    }

    private record Binders(JvmGcMetrics gc, JvmHeapPressureMetrics heapPressure) {
    }

    /**
     * Binds and starts the endpoint.
     *
     * @param port {@code 0} binds a free port, which {@link #port()} then reports
     */
    static MetricsEndpoint start(String host, int port, MeterRegistry registry, String servingMode,
                                 long startedNanos) throws Exception {
        return start(host, port, registry, servingMode, startedNanos, RuntimeAttestation.system());
    }

    static MetricsEndpoint start(String host, int port, MeterRegistry registry, String servingMode,
                                 long startedNanos, RuntimeAttestation attestation) throws Exception {
        QueuedThreadPool pool = new QueuedThreadPool(MAX_THREADS, MIN_THREADS);
        pool.setName("replay-metrics");
        Server server = new Server(pool);
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        server.setHandler(new MetricsHandler(registry, servingMode, startedNanos, attestation));
        Binders binders = bindProcessMeters(registry);
        server.start();
        return new MetricsEndpoint(server, binders.gc(), binders.heapPressure());
    }

    int port() {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @Override
    public void close() throws Exception {
        // Both binders register GC notification listeners; closing them is what unregisters those.
        // Stop the server first so nothing is mid-scrape when the meters go away.
        try {
            server.stop();
        } finally {
            heapPressure.close();
            gc.close();
        }
    }

    private static final class MetricsHandler extends Handler.Abstract {

        private final MeterRegistry registry;
        private final String servingMode;
        private final long startedNanos;
        private final RuntimeAttestation attestation;

        private MetricsHandler(MeterRegistry registry, String servingMode, long startedNanos,
                               RuntimeAttestation attestation) {
            this.registry = registry;
            this.servingMode = servingMode;
            this.startedNanos = startedNanos;
            this.attestation = attestation;
        }

        @Override
        public boolean handle(Request request, Response response, Callback callback) {
            String path = request.getHttpURI().getPath();
            if ("/healthz".equals(path)) {
                write(response, HttpStatus.OK_200, "text/plain", "ok\n", callback);
                return true;
            }
            if ("/runtime-attestation".equals(path)) {
                write(response, HttpStatus.OK_200, "application/json", attestation.render() + "\n", callback);
                return true;
            }
            if (!"/metrics".equals(path)) {
                write(response, HttpStatus.NOT_FOUND_404, "text/plain",
                        "not found; this port serves /metrics, /runtime-attestation, and /healthz only\n",
                        callback);
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
