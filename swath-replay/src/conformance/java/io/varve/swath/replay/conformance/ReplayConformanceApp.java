/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "swath-replay-conformance",
        mixinStandardHelpOptions = true,
        description = "Compare recorded S3 ListObjectsV2 HAR responses with swath-replay responses.")
public final class ReplayConformanceApp implements Callable<Integer> {

    @Option(names = "--har", required = true, paramLabel = "PATH",
            description = "HAR file captured from real S3 via mitmproxy.")
    Path har;

    @Option(names = "--fixture", required = true, paramLabel = "PATH",
            description = "Canonical swath Parquet listing fixture file or output directory from the same run.")
    Path fixture;

    @Option(names = "--bucket", required = true, description = "Bucket name captured in the HAR.")
    String bucket;

    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Temporary swath-replay bind host.")
    String host;

    @Option(names = "--mismatch-dir", paramLabel = "PATH",
            description = "Optional directory for expected/actual XML files when comparisons fail.")
    Path mismatchDir;

    @Option(names = "--max-entries", defaultValue = "0",
            description = "Maximum comparable HAR entries to check; 0 checks all.")
    int maxEntries;

    @Option(names = "--sample-entries", defaultValue = "0",
            description = "Deterministically sample this many comparable HAR entries across the whole capture; 0 disables sampling.")
    int sampleEntries;

    @Option(names = "--timeout-seconds", defaultValue = "30",
            description = "Per-request timeout for replay comparisons.")
    long timeoutSeconds;

    @Option(names = "--parallelism", defaultValue = "0",
            description = "Concurrent replay requests; 0 uses the CPU-bounded default.")
    int parallelism;

    @Option(names = "--replay-parquet-connections", defaultValue = "0",
            description = "DuckDB connections in the temporary replay server; 0 uses the CPU-bounded default.")
    int replayParquetConnections;

    public static void main(String[] args) {
        int exit = new CommandLine(new ReplayConformanceApp()).execute(args);
        System.exit(exit);
    }

    @Override
    public Integer call() throws Exception {
        var options = new ReplayConformanceComparator.Options(
                har, fixture, bucket, host, mismatchDir, maxEntries, sampleEntries, Duration.ofSeconds(timeoutSeconds),
                parallelism, replayParquetConnections);
        var summary = ReplayConformanceComparator.compare(options);
        System.out.print(summary.report());
        return summary.exitCode();
    }
}
