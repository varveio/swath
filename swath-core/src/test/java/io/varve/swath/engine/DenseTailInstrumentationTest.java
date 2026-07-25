/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * §5 instrumentation guard for the dense-tail perf followups: the far-ahead / step-back pivot
 * MECHANISM counters and the alphabet-aware chooser engagement counters must FIRE (and be visible
 * in {@code list_run_diagnostics} steal_reasons) when the engine actually splits a bounded dense
 * range, so post-hoc analysis can tell from the metrics alone which mechanism carried a bucket.
 *
 * <p>Not a correctness guard (PROP/RES/CONC are owned elsewhere) — but each scenario still asserts
 * byte-exact once-only emission so an instrumentation change can never quietly break coverage.
 */
final class DenseTailInstrumentationTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /** A flat DENSE hex root: high-entropy 16-hex keys, no {@code /} — forces bounded far-ahead splits. */
    private static List<byte[]> hexRoot(int n, long seed) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(("%016x".formatted(r.nextLong())).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    @Test
    @Timeout(120)
    void pivotAndAlphabetEngagementCountersFireOnBoundedDenseSplits(@TempDir Path dir) throws Exception {
        // Few workers → the dense root is banded into a handful of LARGE bounded ranges (each many
        // pages at a small maxKeys), so thieves and owners repeatedly split bounded ranges — the exact
        // path the far-ahead and alphabet-aware pivots serve. Page latency exposes the
        // concurrency that drives the steals.
        List<byte[]> keyspace = hexRoot(12000, 909L);
        // NONE seed → one root range with 15 idle workers ⇒ heavy thief stealing over bounded children
        // (the far-ahead / alphabet pivot path). SHALLOW pre-bands into balanced ranges each worker just
        // drains, which is the wrong shape for exercising the thief split machinery.
        Result r = run(dir, "pivot-alpha", keyspace, 16, 100, Duration.ofMillis(2), SeedMode.NONE);

        EngineHarness.assertExactlyOnce(r.emitted, keyspace);
        Map<String, Long> reasons = r.reasons;

        // Thief splits committed → a pivot MECHANISM was recorded for each (§5).
        assertThat(sumCategory(reasons, "PIVOT")).as("a pivot-mechanism counter fired per committed split")
                .isPositive();

        // The alphabet chooser was consulted on every bounded interpolated pivot → an ALPHABET counter
        // fired (chosen when it landed on a populated observed scalar, else fallback).
        assertThat(sumCategory(reasons, "ALPHABET"))
                .as("the alphabet-aware chooser engagement counter fired on bounded splits").isPositive();
    }

    // ---- harness ---------------------------------------------------------------

    private record Result(List<byte[]> emitted, Map<String, Long> reasons) {
    }

    private static Result run(Path baseDir, String name, List<byte[]> keyspace, int workers, int maxKeys,
                              Duration pageLatency, SeedMode seedMode) throws Exception {
        Path ckptDir = baseDir.resolve(name);
        Files.createDirectories(ckptDir);

        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 && req.delimiter() == null ? pageLatency : Duration.ZERO)
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, NO_PREFIX, workers, metrics)
                    .seedSpecs(run.id(), seedMode);
            store.insertNodes(specs);
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), NO_PREFIX, ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return new Result(emitted, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    private static long sumCategory(Map<String, Long> reasons, String category) {
        long total = 0;
        for (Map.Entry<String, Long> e : reasons.entrySet()) {
            if (e.getKey().startsWith(category + ".")) {
                total += e.getValue();
            }
        }
        return total;
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], name + "-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

}
