/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * THROWAWAY measurement harness (NOT
 * {@link ParallelMergeBenchmark}): profiles ONLY the single-threaded ({@code R=1})
 * {@link SortTransform#transform} merge phase with JFR (an embedded {@link Recording}, "profile"
 * settings, started immediately before and stopped immediately after the {@code transform()} call
 * so the dump excludes corpus generation/staging-copy noise), to attribute merge-phase CPU across
 * Parquet decode (read), the k-way heap merge/compare, Parquet encode (write), and allocation/GC.
 *
 * <p>Corpus generation uses {@link SortBenchCorpus}'s streamed, uniquely-keyed, block-interleaved
 * generator. Same corpus shape (22M rows / 16 segments) as a
 * prior benchmark run, chosen so the JFR profiling window is known to run comfortably past the ~20s
 * stability floor.
 *
 * <p>Gated {@code -Dswath.profile=on} (never runs under {@code ./gradlew build} or the default
 * suite). {@code swath.profile.jfr} overrides the JFR output path (default: a fixed file directly
 * under {@code java.io.tmpdir}).
 *
 * <p>Run: {@code JAVA_TOOL_OPTIONS="-Dswath.profile=on -Dswath.profile.jfr=<path>" ./gradlew
 * :swath-core:test --tests 'io.varve.swath.sort.MergeCpuProfileHarness' -Pperf} — {@code -D} on the
 * {@code ./gradlew} command line does not reach the forked test-worker JVM;
 * {@code JAVA_TOOL_OPTIONS} does.
 */
@EnabledIfSystemProperty(named = "swath.profile", matches = "on")
class MergeCpuProfileHarness {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    private static final int NUM_SEGMENTS = Integer.getInteger("swath.profile.segments", 16);
    private static final long TOTAL_ROWS = Long.getLong("swath.profile.rows", 22_000_000L);
    private static final int BLOCK_ROWS = Integer.getInteger("swath.profile.blockRows", 4_000);
    private static final int TOTAL_DAYS = 1_500;
    private static final String KEY_PREFIX = "corp-data-lake-logs";
    private static final String[] STORAGE_CLASSES =
            {"STANDARD", "STANDARD_IA", "INTELLIGENT_TIERING", "GLACIER"};
    @Test
    @Timeout(value = 9, unit = TimeUnit.MINUTES)
    void profileSerialMerge() throws Exception {
        Path jfrOut = Path.of(System.getProperty("swath.profile.jfr",
                System.getProperty("java.io.tmpdir") + "/swath-merge-profile.jfr"));
        Path root = Files.createTempDirectory("swath-merge-cpu-profile-");
        System.out.println("PROFILE_ROOT " + root);
        System.out.println("PROFILE_JFR_OUT " + jfrOut);
        System.out.printf("PROFILE_HEAP max_memory_mb=%.1f available_processors=%d%n",
                Runtime.getRuntime().maxMemory() / (1024.0 * 1024.0), Runtime.getRuntime().availableProcessors());
        try {
            Path master = Files.createDirectory(root.resolve("master"));
            long t0 = System.nanoTime();
            SortBenchCorpus.Stats corpus = buildCorpus(master);
            long buildMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("PROFILE_CORPUS segments=%d rows=%d bytes=%d build_ms=%d%n",
                    corpus.segments(), corpus.rows(), corpus.bytes(), buildMs);

            Path output = Files.createDirectory(root.resolve("data"));
            Path staging = Files.createDirectory(root.resolve("_staging"));
            List<Path> stagingSegments = SortBenchCorpus.hardLinkCorpus(
                    SortBenchCorpus.pageRunSegments(master), staging);

            // Force R=1 (serial merge) regardless of ambient swath.sort.merge-parallelism.
            SortConfig config = SortConfig.fromProperties(
                    key -> "swath.sort.merge-parallelism".equals(key) ? "1" : null);
            SortMetrics metrics = SortMetrics.NO_OP;
            ConcurrentLinkedQueue<Long> rangeLatenciesNanos = new ConcurrentLinkedQueue<>();
            RangeMergeTimer timer = rangeLatenciesNanos::add;
            SortedFileWriterFactory writerFactory = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
            SortTransform transform =
                    new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP,
                            EqualKeyPolicy.ALLOW, metrics, writerFactory,
                            MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, timer,
                            SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));

            Configuration jfrConfig = Configuration.getConfiguration("profile");
            Recording recording = new Recording(jfrConfig);

            long cpuStartNanos = SortBenchCorpus.processCpuTimeNanos();
            long wallStartNanos = System.nanoTime();
            recording.start();
            SortTransformResult result;
            try {
                result = transform.transform(stagingSegments, output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);
            } finally {
                recording.stop();
            }
            long wallEndNanos = System.nanoTime();
            long cpuEndNanos = SortBenchCorpus.processCpuTimeNanos();
            recording.dump(jfrOut);
            recording.close();

            long mergeMs = (wallEndNanos - wallStartNanos) / 1_000_000;
            double avgCoresBusy = (cpuStartNanos < 0 || cpuEndNanos < 0)
                    ? -1
                    : (cpuEndNanos - cpuStartNanos) / 1e9 / ((wallEndNanos - wallStartNanos) / 1e9);
            System.out.printf("PROFILE_RESULT merge_ms=%d avg_cores_busy=%.2f merge_passes=%d "
                            + "cascaded_passes=%d fastpath=%d total_rows=%d files=%d%n",
                    mergeMs, avgCoresBusy, result.mergePasses(), result.cascadedPasses(),
                    result.fastPathEmissions(), result.totalRows(), result.finalFiles().size());
        } finally {
            SortBenchCorpus.deleteTree(root);
        }
    }

    private SortBenchCorpus.Stats buildCorpus(Path master) throws IOException {
        long rowsPerDay = Math.max(1, TOTAL_ROWS / TOTAL_DAYS);
        LocalDate base = LocalDate.of(2019, 1, 1);
        long totalRows = 0;
        long totalBytes = 0;
        for (int seg = 0; seg < NUM_SEGMENTS; seg++) {
            Path path = master.resolve(String.format("seg-%05d.pageseg", seg));
            PageRunSegmentWriter writer =
                    new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
            long rows;
            try (SortedCursor cursor =
                         SortBenchCorpus.generatedCursor(
                                 seg, NUM_SEGMENTS, BLOCK_ROWS, TOTAL_ROWS, rowsPerDay, base)) {
                rows = writer.writeIntermediate(cursor, path);
            }
            totalRows += rows;
            totalBytes += Files.size(path);
        }
        return new SortBenchCorpus.Stats(NUM_SEGMENTS, totalRows, totalBytes);
    }
}
