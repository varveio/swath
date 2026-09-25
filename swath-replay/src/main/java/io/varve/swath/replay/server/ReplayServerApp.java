/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.fixture.SortFixtureCommand;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "swath-replay",
        mixinStandardHelpOptions = true,
        description = "Inspect and serve captured swath Parquet listings.",
        subcommands = {
                ReplayServerApp.ServeCommand.class,
                SortFixtureCommand.class,
                BenchCommand.class
        })
public final class ReplayServerApp implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Option(names = "--fixture", paramLabel = "PATH",
            description = "Canonical swath Parquet listing fixture file or output directory.")
    Path fixture;

    @Option(names = "--bucket", description = "Bucket name to expose.")
    String bucket;

    @Mixin
    ServeOptions serveOptions;

    public static void main(String[] args) {
        int exit = new CommandLine(new ReplayServerApp())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);
        System.exit(exit);
    }

    @Override
    public Integer call() throws Exception {
        if (fixture == null || bucket == null) {
            spec.commandLine().usage(System.err);
            return 2;
        }
        return serve(fixture, bucket, serveOptions);
    }

    private static Integer serve(Path fixture, String bucket, ServeOptions options) throws Exception {
        // Not resolved here. The default belongs to the store the serving mode resolves to, and
        // only the factory knows which that is -- sorted mode wants a small multiple of the cores
        // (a slot is a Parquet file handle plus its decoded footer), DuckDB mode wants at most four
        // (a slot is a connection owning a thread pool). Resolving in the caller picked one store's
        // default for both modes, and since the result is positive the factory's own mode-aware
        // default became unreachable: `serve --serving-mode sorted` with no flag opened four readers
        // rather than the eight-to-thirty-two the sorted store asks for. Pass the request through
        // unresolved -- <= 0 means "the mode's own default" -- and report what came back.
        ShapeLatency injected =
                ShapeLatency.parse(options.injectLatency, options.latencyJitter, options.latencyScale);
        long startedNanos = System.nanoTime();
        ServeConfig config = new ServeConfig(fixture, options.host, options.port, bucket,
                options.servingMode, options.parquetConnections, options.maxConcurrentRequests,
                parseProtocols(options.protocols), options.azureAccount, options.responseBufferBudget,
                options.maxResponseBytes, parseDuration(options.stopTimeout),
                parseDuration(options.idleTimeout), parseDuration(options.writeTimeout),
                options.advertisedHost, injected == null ? (req, result) -> Duration.ZERO : injected);
        ReplayServer server = ReplayServer.open(config);
        ShutdownOwner owner = new ShutdownOwner(server, config.stopTimeout());
        Thread hook = new Thread(() -> {
            owner.close();
            System.err.println("swath_replay_shutdown_hook complete");
        }, "swath-replay-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            server.start();
            // Opened after the fixture is served, so a reader that can reach /metrics knows the
            // index derive is already done and the numbers it reads are serving numbers.
            MetricsEndpoint metrics = options.metricsPort < 0 ? null
                    : MetricsEndpoint.start(options.host, options.metricsPort, server.metrics().registry(),
                            server.resolvedServingMode().toString(), startedNanos, server::servingMetadata);
            owner.setMetrics(metrics);
            try {
                ServingMetadata serving = server.servingMetadata();
                System.err.printf("swath_replay endpoint=http://%s:%d bucket=%s fixture=%s "
                                + "serving_mode=%s parquet_connections=%d inject_latency=%s latency_scale=%s "
                                + "metrics_endpoint=%s max_concurrent_requests=%d protocols=%s "
                                + "response_buffer_budget=%d max_response_bytes=%d max_responses=%d "
                                + "output_chunk_bytes=%d "
                                + "fixture_identity=%s ordering_profile=%s metadata_policy=%s profiles=%s "
                                + "native_endpoints=%s%n",
                        options.host, server.port(), bucket, fixture.toAbsolutePath(),
                        server.resolvedServingMode(), server.resolvedParquetConnections(),
                        injected == null ? "off" : options.injectLatency, options.latencyScale,
                        metrics == null ? "off" : metricsEndpoint(options.host, metrics.port()),
                        options.maxConcurrentRequests, String.join(",", serving.protocols()),
                        config.responseBufferBudget(), config.maxResponseBytes(), config.maxResponses(),
                        serving.outputChunkBytes(),
                        serving.fixtureIdentity(), serving.orderingProfile(), serving.metadataPolicy(),
                        serving.profiles(), endpointExamples(config, server.port()));
                server.join();
            } finally {
                owner.close();
            }
        } finally {
            owner.close();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // The shutdown hook is already running.
            }
        }
        return 0;
    }

    private static Set<Protocol> parseProtocols(String value) {
        try {
            Set<Protocol> parsed = Arrays.stream(value.split(",", -1))
                    .map(String::trim).map(String::toUpperCase).map(Protocol::valueOf)
                    .collect(Collectors.toSet());
            if (parsed.isEmpty()) {
                throw new IllegalArgumentException("empty protocol set");
            }
            return parsed;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("--protocols must be a comma-separated subset of s3,gcs,azure", e);
        }
    }

    static Duration parseDuration(String value) {
        String text = value.trim().toLowerCase(java.util.Locale.ROOT);
        try {
            if (text.startsWith("p") || text.startsWith("-p") || text.startsWith("+p")) {
                return Duration.parse(value.trim());
            }
            if (text.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            return Duration.parse(value.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid serve timeout: " + value, e);
        }
    }

    private static final class ShutdownOwner {
        private final ReplayServer server;
        private final Duration stopTimeout;
        private final AtomicBoolean closed = new AtomicBoolean();
        private MetricsEndpoint metrics;

        private ShutdownOwner(ReplayServer server, Duration stopTimeout) {
            this.server = server;
            this.stopTimeout = stopTimeout;
        }

        synchronized void setMetrics(MetricsEndpoint metrics) { this.metrics = metrics; }

        synchronized void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long deadline = System.nanoTime() + stopTimeout.toNanos();
            server.beginShutdownAt(deadline);
            try {
                if (metrics != null) {
                    metrics.closeAt(deadline);
                }
            } catch (Exception e) {
                System.err.println("failed to stop replay metrics endpoint: " + e.getMessage());
            } finally {
                server.closeAt(deadline, metrics == null
                        ? java.util.concurrent.CompletableFuture.completedFuture(null)
                        : metrics.stoppedFuture());
            }
        }
    }

    static String metricsEndpoint(String host, int port) {
        String authorityHost = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return "http://%s:%d/metrics".formatted(authorityHost, port);
    }

    private static String endpointExamples(ServeConfig config, int port) {
        String host = config.advertisedHost() == null ? config.host() : config.advertisedHost();
        String authority = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        String base = "http://" + authority + ':' + port;
        var examples = new java.util.ArrayList<String>();
        if (config.protocols().contains(Protocol.S3)) {
            examples.add("s3:" + base + '/' + config.bucket() + "?list-type=2");
        }
        if (config.protocols().contains(Protocol.GCS)) {
            examples.add("gcs:" + base + "/storage/v1/b/" + config.bucket() + "/o");
        }
        if (config.protocols().contains(Protocol.AZURE)) {
            examples.add("azure:" + base + '/' + config.azureAccount() + '/' + config.bucket()
                    + "?restype=container&comp=list");
        }
        return String.join(",", examples);
    }

    @Command(name = "serve",
            mixinStandardHelpOptions = true,
            description = "Serve a captured swath Parquet listing through selected native listing routes.")
    static final class ServeCommand implements Callable<Integer> {

        @Option(names = "--fixture", required = true, paramLabel = "PATH",
                description = "Canonical swath Parquet listing fixture file or output directory.")
        Path fixture;

        @Option(names = "--bucket", required = true, description = "Bucket name to expose.")
        String bucket;

        @Mixin
        ServeOptions serveOptions;

        @Override
        public Integer call() throws Exception {
            return serve(fixture, bucket, serveOptions);
        }
    }
}
