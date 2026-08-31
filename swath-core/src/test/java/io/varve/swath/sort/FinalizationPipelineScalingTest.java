/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in real-writer scaling guard for complete-plan encoder overlap on an eight-core host. */
final class FinalizationPipelineScalingTest {
    private static final long ROWS = 2_000_000;

    @Test
    @Tag("perf")
    void fourEncodersFinishBelowSixtyPercentOfSerialWall(@TempDir Path root) throws IOException {
        Assumptions.assumeTrue(Runtime.getRuntime().availableProcessors() >= 8);
        Path master = Files.createDirectory(root.resolve("master"));
        SortBenchCorpus.Stats corpus = ParallelMergeBenchmark.buildCorpus(
                master, 8, ROWS, 4_000, 1_000);
        List<Path> sources = SortBenchCorpus.pageRunSegments(master);

        runArm(root.resolve("warmup"), sources, 1);
        TimedArm serial = runArm(root.resolve("n1"), sources, 1);
        TimedArm parallel = runArm(root.resolve("n4"), sources, 4);

        assertThat(serial.result().totalRows()).isEqualTo(corpus.rows());
        assertThat(parallel.result().totalRows()).isEqualTo(corpus.rows());
        assertThat(serial.result().finalFiles()).hasSizeGreaterThan(3);
        assertThat(parallel.result().finalFiles()).hasSizeGreaterThan(3);
        assertThat(parallel.nanos()).isLessThan((long) (serial.nanos() * 0.60));
    }

    private static TimedArm runArm(Path root, List<Path> sources, int encoders)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        List<Path> inputs = SortBenchCorpus.hardLinkCorpus(sources, staging);
        SortConfig config = SortConfigs.base()
                .withFinalization(SortFinalization.PIPELINE)
                .withMergeParallelism(encoders)
                .withMergeBudgetBytes(512L << 20)
                .withFinalFileBytes(8L << 20);
        SortRun run = new SortRun(config, new ListEntryComparator(), DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                () -> -1, StaleFinalSweep.OWN_PARTS_ONLY);
        long started = System.nanoTime();
        SortTransformResult result = new SortTransform(run).transform(
                inputs, output, staging, PublishListener.NO_OP, ignored -> { },
                FinalPassListener.NO_OP);
        return new TimedArm(result, System.nanoTime() - started);
    }

    private record TimedArm(SortTransformResult result, long nanos) {
    }
}
