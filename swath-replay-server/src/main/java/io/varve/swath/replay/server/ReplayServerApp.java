/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.fixture.SortFixtureCommand;
import io.varve.swath.replay.store.DuckDbListingStore;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "swath-replay-server",
        mixinStandardHelpOptions = true,
        description = "Serve captured swath Parquet listings as S3 ListObjectsV2 endpoints.",
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
        int connections = options.parquetConnections > 0
                ? options.parquetConnections : DuckDbListingStore.defaultConnectionCount();
        ShapeLatency injected = ShapeLatency.parse(options.injectLatency, options.latencyJitter);
        try (ReplayServer server = injected == null
                ? new ReplayServer(options.host, options.port, bucket, fixture, connections, options.servingMode)
                : new ReplayServer(options.host, options.port, bucket, fixture, connections, options.servingMode,
                        injected)) {
            server.start();
            System.err.printf("s3_listing_replay_server endpoint=http://%s:%d bucket=%s fixture=%s "
                            + "serving_mode=%s parquet_connections=%d inject_latency=%s%n",
                    options.host, server.port(), bucket, fixture.toAbsolutePath(),
                    server.resolvedServingMode(), connections,
                    injected == null ? "off" : options.injectLatency);
            server.join();
        }
        return 0;
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
