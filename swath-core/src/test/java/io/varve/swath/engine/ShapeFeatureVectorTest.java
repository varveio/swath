/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.observability.RunSummary;
import io.varve.swath.testkit.EfficiencyHarness;
import io.varve.swath.testkit.EfficiencyHarness.Result;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code shape} feature-vector is a real post-hoc DISCRIMINATOR — the emitted signals
 * differ across keyspace shapes so a corpus can classify a bucket from the metrics alone
 * (docs §5). The headline gap-1 signal (per-position alphabet cardinality) cleanly separates a sparse
 * hex alphabet from a dense base58 one, with no shape-detection logic in the engine.
 */
final class ShapeFeatureVectorTest {

    private static final int WORKERS = 8;
    private static final int MAX_KEYS = 1000;
    private static final Duration LATENCY = Duration.ofMillis(1);
    private static final int N = 40_000;
    private static final long SEED = 42L;
    private static final int HEX_ALPHABET = 16;   // 0-9 A-F

    @Test
    @Timeout(120)
    void alphabetCardinalityDiscriminatesHexFromBase58(@TempDir Path dir) throws Exception {
        // Both are flat, prefix-less keyspaces, so the alphabet aggregate's relative positions map to
        // the leading characters directly: uppercase-hex uses 16 symbols, base58 uses 58.
        Result hex = run(Keyspaces.flatUpperHex(SEED, N), dir.resolve("hex"));
        Result b58 = run(Keyspaces.base58Flat(SEED, N), dir.resolve("b58"));

        RunSummary.ShapeSummary hexShape = hex.shape();
        RunSummary.ShapeSummary b58Shape = b58.shape();
        assertThat(hexShape).isNotNull();
        assertThat(b58Shape).isNotNull();

        // Sparse hex: every observed leading position holds at most the 16 hex symbols.
        for (int card : hexShape.alphabetCardinality()) {
            assertThat(card).isLessThanOrEqualTo(HEX_ALPHABET);
        }
        // Both runs observed a real alphabet, and the dense base58 one drives a strictly larger
        // observed cardinality than sparse hex — the discriminator, computed with no shape logic.
        assertThat(total(hexShape)).isPositive();
        assertThat(total(b58Shape)).isGreaterThan(total(hexShape));
    }

    private static Result run(List<byte[]> keyspace, Path ckptDir) throws Exception {
        Files.createDirectories(ckptDir);
        Result e = EfficiencyHarness.run(keyspace, WORKERS, MAX_KEYS, LATENCY, ckptDir);
        EngineHarness.assertExactlyOnce(e.emitted(), keyspace);
        return e;
    }

    private static long total(RunSummary.ShapeSummary shape) {
        long t = 0L;
        for (int card : shape.alphabetCardinality()) {
            t += card;
        }
        return t;
    }
}
