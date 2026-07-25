/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.model.PageBatch;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Shared {@link ParquetWriterPool} test scaffolding, package-private to
 * {@code io.varve.swath.output.parquet}'s own test tree — every consumer here is a swath-core
 * test class; a testFixtures home would only matter if another module's test source set needed
 * this surface, and none does within this unit's scope. {@code batch(...)} itself is promoted to
 * {@link PageBatches} in swath-core's testFixtures, since it was also duplicated byte-identically
 * in swath-cli's {@code VisibleStagingAndCrashStateContractTest}; this method just delegates so the
 * existing package-private call sites here are untouched.
 */
final class ParquetPoolTestSupport {

    private ParquetPoolTestSupport() {
    }

    /** The {@code n<nodeId>/key-<i>} batch builder shared by the writer-pool/rotation/idle-lane tests. */
    static PageBatch batch(long nodeId, long seq, int from, int to) {
        return PageBatches.batch(nodeId, seq, from, to);
    }

    /** Finalized data parts live under {@code <root>/data/}. */
    static List<Path> parts(Path dir) throws IOException {
        return DatasetLayout.of(dir).dataParts();
    }

    /**
     * Warms parquet-mr/Hadoop classloading off the timed critical section in the caller's own
     * test body — call from an {@code @BeforeEach} (kept local to each class so JUnit still runs
     * it per-class; only the body is shared).
     */
    static void warmupParquetWriterClasses(Path warmupDir) throws Exception {
        var warm = new ParquetWriterPool(warmupDir, ParquetSchema.canonical(), "warmup", 1, Long.MAX_VALUE, 4);
        warm.submit(batch(999, 0, 0, 1));
        warm.close();
    }
}
