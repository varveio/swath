/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

/**
 * Smoke test: a full {@link io.varve.swath.engine.WorkStealingScan} run over
 * {@link MockPageFetcher} wired with a {@link LatencyModels#uniformFast} model still produces a
 * byte-exact, exactly-once result. Proves the latency-injection wiring doesn't perturb engine
 * correctness; it is deliberately not the adversarial dense-tail-collapse reproduction, which is
 * covered separately.
 */
class LatencyModelEngineSmokeTest {

    @Test
    void engineRunWithUniformFastLatencyIsStillByteExact() throws Exception {
        var keyspace = Keyspaces.exactly(600);
        LatencyModel model = LatencyModels.uniformFast(11L, Duration.ofMillis(1), Duration.ofMillis(3));
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).latencyModel(model).build();

        Path dir = Files.createTempDirectory("latency-model-smoke-");
        try {
            EngineHarness.Result result = EngineHarness.run(fetcher, 4, 50, dir);
            EngineHarness.assertExactlyOnceAndTiles(result, keyspace);
        } finally {
            deleteRecursively(dir);
        }
    }

    private static void deleteRecursively(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
