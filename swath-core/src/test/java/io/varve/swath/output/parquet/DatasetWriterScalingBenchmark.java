/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.batch;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Opt-in diagnostic sweep for the production dataset-writer queue/routing path. It deliberately
 * asserts correctness only; relative throughput is machine/workload evidence, never a release
 * threshold. The zero-rotation arm isolates dispatch plus encoding, while the row-rotation arm
 * makes digest and complete-manifest rewrite costs visible.
 *
 * <p>Run with {@code ./gradlew :swath-core:test -PonlyPerf --tests
 * 'io.varve.swath.output.parquet.DatasetWriterScalingBenchmark'
 * -Dswath.bench=on}. Override rows with {@code -Dswath.bench.writer.rows=N}.
 */
@Tag("perf")
@EnabledIfSystemProperty(named = "swath.bench", matches = "on")
class DatasetWriterScalingBenchmark {

    private static final int TOTAL_ROWS = Integer.getInteger("swath.bench.writer.rows", 500_000);
    private static final int BATCH_ROWS = 1_000;
    private static final List<Integer> WRITERS = List.of(4, 8, 16);
    private static final List<Long> ROTATION_ROWS = List.of(0L, 20_000L);

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void writerScaling() throws Exception {
        Path root = Files.createTempDirectory("swath-writer-scaling-");
        System.out.printf("WRITER_BENCH_ENV rows=%d batch_rows=%d max_heap_bytes=%d processors=%d%n",
                TOTAL_ROWS, BATCH_ROWS, Runtime.getRuntime().maxMemory(),
                Runtime.getRuntime().availableProcessors());
        try {
            warmup(root.resolve("warmup"));
            for (long rotationRows : ROTATION_ROWS) {
                for (int writers : WRITERS) {
                    runArm(root.resolve("writers-" + writers + "-rotation-" + rotationRows),
                            writers, rotationRows);
                }
            }
        } finally {
            deleteRecursively(root);
        }
    }

    private static void warmup(Path dir) throws Exception {
        Files.createDirectories(dir);
        try (var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "warmup",
                1, Long.MAX_VALUE, 4)) {
            pool.submit(batch(0, 0, 0, 1_000));
        }
    }

    private static void runArm(Path dir, int writers, long rotationRows) throws Exception {
        Files.createDirectories(dir);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        var config = ParquetWriterPoolConfig.DEFAULT
                .withRotationMaxRows(rotationRows)
                .withMetrics(metrics);
        long startedAt = System.nanoTime();
        try (var pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "bench",
                writers, Long.MAX_VALUE,
                SharedDatasetWriterPool.defaultQueueCapacityPerLane(writers), config)) {
            long sequence = 0L;
            for (int from = 0; from < TOTAL_ROWS; from += BATCH_ROWS) {
                int to = Math.min(TOTAL_ROWS, from + BATCH_ROWS);
                long nodeId = sequence % writers;
                pool.submit(batch(nodeId, sequence++, from, to));
            }
        }
        long elapsedNanos = System.nanoTime() - startedAt;
        RunSummary.DatasetWriterSummary writer = metrics
                .summary(Duration.ofNanos(elapsedNanos), "writer_bench", 0L, 0L)
                .datasetWriter();
        long rows = writer.lanes().stream().mapToLong(RunSummary.DatasetWriterLane::rowsWritten).sum();
        long activeNanos = writer.lanes().stream()
                .mapToLong(RunSummary.DatasetWriterLane::activeElapsedNanos).sum();
        long parts = DatasetLayout.of(dir).dataParts().size();

        assertThat(rows).isEqualTo(TOTAL_ROWS);
        assertThat(parts).isEqualTo(writer.lanes().stream()
                .mapToLong(RunSummary.DatasetWriterLane::partsFinalized).sum());
        System.out.printf("WRITER_BENCH_RESULT writers=%d rotation_rows=%d queue_total=%d "
                        + "elapsed_ms=%.3f rows_per_sec=%.1f active_ms=%.3f submit_blocked_ms=%.3f "
                        + "hol_blocked_ms=%.3f parts=%d digest_ms=%.3f manifest_writes=%d "
                        + "manifest_ms=%.3f%n",
                writers, rotationRows, writer.totalQueueCapacity(), elapsedNanos / 1_000_000.0,
                TOTAL_ROWS * 1_000_000_000.0 / elapsedNanos, activeNanos / 1_000_000.0,
                writer.submitBlockedNanos() / 1_000_000.0,
                writer.headOfLineBlockedNanos() / 1_000_000.0, parts,
                writer.partDigestNanos() / 1_000_000.0, writer.manifestWriteCount(),
                writer.manifestWriteNanos() / 1_000_000.0);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
