/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.Gauge;
import java.lang.reflect.Field;
import java.net.StandardSocketOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ConnectionStatistics;
import org.eclipse.jetty.io.SocketChannelEndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Diagnostic-only launcher with an explicitly small accepted TCP send buffer. It exercises a
 * pending write on hosts where the default kernel send buffer absorbs a whole maximum page.
 * This class is not shipped in swath-replay's installDist.
 */
public final class ResourceFaultServer {
    private ResourceFaultServer() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 15) {
            throw new IllegalArgumentException("expected fixture bucket mode port metricsPort readers "
                    + "maxRequests budget cap writeMs idleMs protocol account sendBuffer latencySpec");
        }
        Path fixture = Path.of(args[0]);
        String bucket = args[1];
        ServingMode mode = ServingMode.valueOf(args[2].toUpperCase(java.util.Locale.ROOT));
        int port = Integer.parseInt(args[3]);
        int metricsPort = Integer.parseInt(args[4]);
        int readers = Integer.parseInt(args[5]);
        int requests = Integer.parseInt(args[6]);
        long budget = Long.parseLong(args[7]);
        int cap = Integer.parseInt(args[8]);
        Duration writeTimeout = Duration.ofMillis(Long.parseLong(args[9]));
        Duration idleTimeout = Duration.ofMillis(Long.parseLong(args[10]));
        Protocol protocol = Protocol.valueOf(args[11].toUpperCase(java.util.Locale.ROOT));
        String account = protocol == Protocol.AZURE ? args[12] : null;
        int sendBuffer = Integer.parseInt(args[13]);
        if (sendBuffer != 0 && (sendBuffer < 4096 || sendBuffer > 1024 * 1024)) {
            throw new IllegalArgumentException("diagnostic accepted send buffer must be 0 or 4 KiB..1 MiB");
        }
        String latencySpec = args[14];
        var latency = latencySpec.isEmpty()
                ? (java.util.function.BiFunction<io.varve.swath.replay.protocol.S3ListRequest,
                        io.varve.swath.replay.protocol.S3ListResult, Duration>) (request, result) -> Duration.ZERO
                : ShapeLatency.parse(latencySpec, 0, 1);
        ServeConfig config = new ServeConfig(fixture, "127.0.0.1", port, bucket, mode,
                readers, requests, Set.of(protocol), account, budget, cap, Duration.ofSeconds(10),
                idleTimeout, writeTimeout, null, latency);
        ReplayServer server = ReplayServer.open(config);
        Field jettyField = ReplayServer.class.getDeclaredField("server");
        jettyField.setAccessible(true);
        Server jetty = (Server) jettyField.get(server);
        Connector connector = jetty.getConnectors()[0];
        if (!(connector instanceof ServerConnector tcp)) {
            throw new IllegalStateException("replay connector is not a ServerConnector");
        }
        if (sendBuffer != 0) tcp.setAcceptedSendBufferSize(sendBuffer);
        ConnectionStatistics connections = new ConnectionStatistics();
        tcp.addBean(connections);
        AtomicInteger sendBufferMin = new AtomicInteger(Integer.MAX_VALUE);
        AtomicInteger sendBufferMax = new AtomicInteger();
        AtomicInteger sendBufferSamples = new AtomicInteger();
        AtomicInteger sendBufferErrors = new AtomicInteger();
        tcp.addBean((Connection.Listener) new Connection.Listener() {
            @Override public void onOpened(Connection connection) {
                if (!(connection.getEndPoint() instanceof SocketChannelEndPoint socket)) {
                    sendBufferErrors.incrementAndGet();
                    return;
                }
                try {
                    int observed = socket.getChannel().getOption(StandardSocketOptions.SO_SNDBUF);
                    sendBufferMin.accumulateAndGet(observed, Math::min);
                    sendBufferMax.accumulateAndGet(observed, Math::max);
                    sendBufferSamples.incrementAndGet();
                } catch (Exception unavailable) {
                    sendBufferErrors.incrementAndGet();
                }
            }
        });
        var registry = server.metrics().registry();
        Gauge.builder("swath.replay.diagnostic.connections.accepted", connections,
                ConnectionStatistics::getConnectionsTotal).register(registry);
        Gauge.builder("swath.replay.diagnostic.connections.active", connections,
                ConnectionStatistics::getConnections).register(registry);
        Gauge.builder("swath.replay.diagnostic.connections.peak", connections,
                ConnectionStatistics::getConnectionsMax).register(registry);
        Gauge.builder("swath.replay.diagnostic.sndbuf.min.bytes", sendBufferMin,
                value -> value.get() == Integer.MAX_VALUE ? 0 : value.get()).register(registry);
        Gauge.builder("swath.replay.diagnostic.sndbuf.max.bytes", sendBufferMax,
                AtomicInteger::get).register(registry);
        Gauge.builder("swath.replay.diagnostic.sndbuf.samples", sendBufferSamples,
                AtomicInteger::get).register(registry);
        Gauge.builder("swath.replay.diagnostic.sndbuf.errors", sendBufferErrors,
                AtomicInteger::get).register(registry);
        AtomicReference<MetricsEndpoint> metrics = new AtomicReference<>();
        AtomicBoolean closed = new AtomicBoolean();
        Runnable close = () -> {
            if (!closed.compareAndSet(false, true)) return;
            try {
                MetricsEndpoint endpoint = metrics.get();
                if (endpoint != null) endpoint.close();
            } catch (Exception e) {
                System.err.println("diagnostic metrics stop failed: " + e);
            } finally {
                server.close();
            }
        };
        Thread hook = new Thread(close, "resource-fault-server-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            server.start();
            metrics.set(MetricsEndpoint.start("127.0.0.1", metricsPort,
                    server.metrics().registry(), server.resolvedServingMode().toString(),
                    System.nanoTime(), server::servingMetadata));
            System.err.printf("resource_fault_server endpoint=http://127.0.0.1:%d "
                    + "accepted_send_buffer_bytes=%d transport=%s metrics_port=%d%n",
                    server.port(), tcp.getAcceptedSendBufferSize(),
                    sendBuffer == 0 ? "instrumented_default_sndbuf" : "diagnostic_accepted_sndbuf",
                    metrics.get().port());
            server.join();
        } finally {
            close.run();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown already began.
            }
        }
    }
}
