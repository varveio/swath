/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.fixture.SortFixtureCommand;
import java.nio.file.Path;
import java.util.concurrent.Callable;
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
        try (ReplayServer server = injected == null
                ? new ReplayServer(options.host, options.port, bucket, fixture, options.parquetConnections,
                        options.delimiterConnections, options.servingMode, options.maxConcurrentRequests)
                : new ReplayServer(options.host, options.port, bucket, fixture, options.parquetConnections,
                        options.delimiterConnections, options.servingMode, injected,
                        options.maxConcurrentRequests)) {
            server.start();
            // Opened after the fixture is served, so a reader that can reach /metrics knows the
            // index derive is already done and the numbers it reads are serving numbers.
            try (MetricsEndpoint metrics = options.metricsPort < 0 ? null
                    : MetricsEndpoint.start(options.host, options.metricsPort, server.metrics().registry(),
                            server.resolvedServingMode().toString(), startedNanos)) {
                System.err.printf("swath_replay endpoint=http://%s:%d bucket=%s fixture=%s "
                                + "serving_mode=%s parquet_connections=%d delimiter_connections=%d "
                                + "inject_latency=%s latency_scale=%s "
                                + "metrics_endpoint=%s max_concurrent_requests=%d%n",
                        options.host, server.port(), bucket, fixture.toAbsolutePath(),
                        server.resolvedServingMode(), server.resolvedParquetConnections(),
                        server.resolvedDelimiterConnections(),
                        injected == null ? "off" : options.injectLatency, options.latencyScale,
                        metrics == null ? "off" : metricsEndpoint(options.host, metrics.port()),
                        options.maxConcurrentRequests);
                server.join();
            }
        }
        return 0;
    }

    static String metricsEndpoint(String host, int port) {
        String authorityHost = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return "http://%s:%d/metrics".formatted(authorityHost, port);
    }

    @Command(name = "serve",
            mixinStandardHelpOptions = true,
            description = "Serve a captured swath Parquet listing as an S3 ListObjectsV2 endpoint.")
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
