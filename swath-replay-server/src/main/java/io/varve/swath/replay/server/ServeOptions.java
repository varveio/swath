/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import picocli.CommandLine.Option;

/**
 * The serve parameters shared verbatim by {@link ReplayServerApp}'s top-level invocation and its
 * nested {@link ReplayServerApp.ServeCommand} — {@code --fixture}/{@code --bucket} stay declared
 * separately on each command, since only {@link ReplayServerApp.ServeCommand} marks them {@code
 * required}: the top-level command must stay optional on them so the other subcommands
 * ({@code sort-fixture}, {@code bench}) can run without supplying either.
 */
final class ServeOptions {

    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Bind host.")
    String host;

    @Option(names = "--port", defaultValue = "0", description = "Bind port; 0 chooses a free port.")
    int port;

    @Option(names = "--parquet-connections", defaultValue = "0",
            description = "Concurrent fixture readers, which is also this server's read-concurrency "
                    + "bound: a request beyond it queues. Set it above the widest fan-out any client "
                    + "will drive, or the server's own cost starts varying with the client's "
                    + "concurrency. 0 uses the store's default.")
    int parquetConnections;

    @Option(names = "--serving-mode", defaultValue = "sorted",
            description = "How to serve the fixture: sorted (the default -- require a stamped, "
                    + "objects-mode, strictly-sorted fixture and fail by name otherwise), or duckdb "
                    + "(force the role-1 oracle, which serves any capture).")
    ServingMode servingMode;

    @Option(names = "--metrics-port", defaultValue = "-1",
            description = "Serve this server's own meters as JSON on a second port (GET /metrics, "
                    + "GET /healthz). Negative disables it (the default); 0 binds a free port, "
                    + "reported in the startup line. A scrape never touches the serving path, so "
                    + "reading the metrics cannot perturb them.")
    int metricsPort;

    @Option(names = "--max-concurrent-requests", defaultValue = "512",
            description = "Ceiling on requests served at once. Injected latency is a blocking sleep "
                    + "held on the serving thread, so an in-flight request occupies one for the whole "
                    + "profile; a client fanning out wider than this has its excess queued, and the "
                    + "wait reaches it as latency from a server that is, by CPU, asleep. Raise it "
                    + "above the widest fan-out any client will use.")
    int maxConcurrentRequests;

    @Option(names = "--inject-latency", paramLabel = "SPEC",
            description = "Per-request-shape fault latency: 'prod-commoncrawl' or a "
                    + "shape=delay list (worker_page|pivot_probe|structure_probe; e.g. "
                    + "'worker_page=223ms,structure_probe=223ms+55ms/cp' — the /cp term scales a "
                    + "structure probe's delay with the CommonPrefixes it returns). Off by default.")
    String injectLatency;

    @Option(names = "--latency-jitter", defaultValue = "0",
            description = "Deterministic jitter fraction in [0,1) applied to injected latency, "
                    + "keyed off the request bytes (reproducible across runs).")
    double latencyJitter;

    @Option(names = "--latency-scale", defaultValue = "1",
            description = "Divide every injected latency by this factor, for compressed-time replay "
                    + "runs (e.g. 50 walks a profile in a fiftieth of the wall clock it describes). "
                    + "Requires --inject-latency. Only the injected delay scales, not the server's "
                    + "own per-request cost, so a scaled run's absolute wall clock is not the "
                    + "unscaled run's divided. Default 1 injects the profile as written.")
    double latencyScale;
}
