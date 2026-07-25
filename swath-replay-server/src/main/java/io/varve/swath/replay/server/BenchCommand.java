/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.store.DuckDbListingStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code bench}: the token-walk benchmark driver. Walks a fixture's FULL listing over
 * real loopback HTTP, once per {@code --modes} entry, and reports the §6 corridor numbers — see
 * {@link TokenWalkBenchmark} for the measurement itself. Supports comparing two (or more) serving
 * modes against the SAME fixture in one invocation, reporting a ratio of the later modes against
 * the first requested one.
 */
@Command(name = "bench",
        mixinStandardHelpOptions = true,
        description = "Walk a fixture's full listing over HTTP and report page/latency/throughput numbers.")
public final class BenchCommand implements Callable<Integer> {

    @Option(names = "--fixture", required = true, paramLabel = "PATH",
            description = "Canonical swath Parquet listing fixture file or output directory.")
    Path fixture;

    @Option(names = "--bucket", required = true, description = "Bucket name to expose.")
    String bucket;

    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Bind host for the temporary server(s).")
    String host;

    @Option(names = "--modes", defaultValue = "auto", split = ",",
            description = "Serving mode(s) to benchmark: auto, duckdb, sorted. Comma-separated to run the SAME "
                    + "fixture through more than one mode in this invocation (e.g. sorted,duckdb), reporting a "
                    + "ratio of each later mode against the first.")
    List<ServingMode> modes;

    @Option(names = "--max-keys", defaultValue = "1000", description = "max-keys per page request.")
    int maxKeys;

    @Option(names = "--prefix", description = "Optional prefix to scope the walk.")
    String prefix;

    @Option(names = "--delimiter", description = "Optional delimiter to scope the walk.")
    String delimiter;

    @Option(names = "--parquet-connections", defaultValue = "0",
            description = "DuckDB connections for concurrent Parquet replay reads; 0 uses the CPU-bounded default.")
    int parquetConnections;

    @Option(names = "--json", paramLabel = "PATH",
            description = "Optional path to also write a machine-readable JSON report; by default the report "
                    + "goes only to stdout as a human-readable table.")
    Path json;

    @Override
    public Integer call() throws Exception {
        int connections = parquetConnections > 0 ? parquetConnections : DuckDbListingStore.defaultConnectionCount();
        var options = new TokenWalkBenchmark.Options(
                fixture, bucket, host, new ArrayList<>(modes), maxKeys, prefix, delimiter, connections);
        TokenWalkBenchmark.Report report = TokenWalkBenchmark.run(options);
        System.out.print(report.humanTable());
        if (json != null) {
            Files.writeString(json, report.toJson(), StandardCharsets.UTF_8);
            System.out.printf(Locale.ROOT, "bench_json_written path=%s%n", json.toAbsolutePath());
        }
        return 0;
    }
}
