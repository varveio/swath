/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import com.sun.management.OperatingSystemMXBean;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
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
 * <p>Corpus generation is a trimmed copy of {@link ParallelMergeBenchmark}'s streamed,
 * uniquely-keyed, block-interleaved corpus generator (duplicated here rather than shared, since
 * that class's helpers are private and this is a disposable, single-purpose measurement class —
 * not wired into any product or shared test path). Same corpus shape (22M rows / 16 segments) as a
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
            CorpusStats corpus = buildCorpus(master);
            long buildMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("PROFILE_CORPUS segments=%d rows=%d bytes=%d build_ms=%d%n",
                    corpus.segments, corpus.rows, corpus.bytes, buildMs);

            Path output = Files.createDirectory(root.resolve("data"));
            Path staging = Files.createDirectory(root.resolve("_staging"));
            List<Path> stagingSegments = copyCorpus(master, staging);

            // Force R=1 (serial merge) regardless of ambient swath.sort.merge-parallelism.
            SortConfig config = SortConfig.fromProperties(
                    key -> "swath.sort.merge-parallelism".equals(key) ? "1" : null);
            SortMetrics metrics = SortMetrics.NO_OP;
            ConcurrentLinkedQueue<Long> rangeLatenciesNanos = new ConcurrentLinkedQueue<>();
            RangeMergeTimer timer = rangeLatenciesNanos::add;
            SortedFileWriterFactory writerFactory = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
            SortTransform transform =
                    new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP, metrics, writerFactory), false, timer);

            Configuration jfrConfig = Configuration.getConfiguration("profile");
            Recording recording = new Recording(jfrConfig);

            long cpuStartNanos = processCpuTimeNanos();
            long wallStartNanos = System.nanoTime();
            recording.start();
            SortTransformResult result;
            try {
                result = transform.transform(stagingSegments, output, staging, PublishListener.NO_OP);
            } finally {
                recording.stop();
            }
            long wallEndNanos = System.nanoTime();
            long cpuEndNanos = processCpuTimeNanos();
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
            deleteRecursively(root);
        }
    }

    private static long processCpuTimeNanos() {
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof OperatingSystemMXBean sun) {
            long cpu = sun.getProcessCpuTime();
            return cpu >= 0 ? cpu : -1;
        }
        return -1;
    }

    // =====================================================================
    // Corpus generation — trimmed copy of ParallelMergeBenchmark's generator (see class javadoc).
    // =====================================================================

    private record CorpusStats(int segments, long rows, long bytes) {
    }

    private CorpusStats buildCorpus(Path master) throws IOException {
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
                         new GeneratedCursor(seg, NUM_SEGMENTS, BLOCK_ROWS, TOTAL_ROWS, rowsPerDay, base)) {
                rows = writer.writeIntermediate(cursor, path);
            }
            totalRows += rows;
            totalBytes += Files.size(path);
        }
        return new CorpusStats(NUM_SEGMENTS, totalRows, totalBytes);
    }

    /** See {@code ParallelMergeBenchmark.GeneratedCursor}'s javadoc — identical generation scheme. */
    private static final class GeneratedCursor implements SortedCursor {
        private final int segment;
        private final int numSegments;
        private final int blockRows;
        private final long totalRows;
        private final long rowsPerDay;
        private final LocalDate base;

        private long currentBlock;
        private long currentI;
        private long blockEnd;

        GeneratedCursor(int segment, int numSegments, int blockRows, long totalRows, long rowsPerDay,
                        LocalDate base) {
            this.segment = segment;
            this.numSegments = numSegments;
            this.blockRows = blockRows;
            this.totalRows = totalRows;
            this.rowsPerDay = rowsPerDay;
            this.base = base;
            this.currentBlock = segment - (long) numSegments;
            this.currentI = 0;
            this.blockEnd = 0;
        }

        private boolean advanceIfNeeded() {
            while (currentI >= blockEnd) {
                currentBlock += numSegments;
                long start = currentBlock * (long) blockRows;
                if (start >= totalRows) {
                    return false;
                }
                currentI = start;
                blockEnd = Math.min(start + blockRows, totalRows);
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return advanceIfNeeded();
        }

        @Override
        public ListEntry next() {
            if (!advanceIfNeeded()) {
                throw new NoSuchElementException();
            }
            ListEntry e = entry(currentI, rowsPerDay, base);
            currentI++;
            return e;
        }

        @Override
        public void close() {
            // in-memory generator — nothing to release
        }
    }

    private static ObjectEntry entry(long i, long rowsPerDay, LocalDate base) {
        String k = key(i, rowsPerDay, base);
        long size = 1 + Math.floorMod(mix(i * 31 + 7), 5_000_000L);
        long day = i / rowsPerDay;
        long within = i % rowsPerDay;
        long lastModified = day * 86_400_000_000L + within * 137L;
        String etag = String.format("%016x%016x", mix(i + 999), mix(i + 7_777));
        String storageClass = STORAGE_CLASSES[(int) (i % STORAGE_CLASSES.length)];
        return new ObjectEntry(KeyBytes.ofUtf8(k), size, lastModified, etag, storageClass,
                null, false, null, null, null, null);
    }

    private static String key(long i, long rowsPerDay, LocalDate base) {
        long day = i / rowsPerDay;
        long within = i % rowsPerDay;
        LocalDate date = base.plusDays(day);
        long h1 = mix(i);
        long h2 = mix(i ^ 0x9E3779B97F4A7C15L);
        return String.format("%s/%04d/%02d/%02d/%08d-%016x%016x",
                KEY_PREFIX, date.getYear(), date.getMonthValue(), date.getDayOfMonth(), within, h1, h2);
    }

    private static long mix(long x) {
        x += 0x9E3779B97F4A7C15L;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }

    private static List<Path> copyCorpus(Path master, Path target) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(master, "*.pageseg")) {
            ds.forEach(files::add);
        }
        files.sort(Comparator.comparing(p -> p.getFileName().toString()));
        List<Path> out = new ArrayList<>();
        for (Path f : files) {
            Path dest = target.resolve(f.getFileName().toString());
            Files.copy(f, dest, StandardCopyOption.COPY_ATTRIBUTES);
            out.add(dest);
        }
        return out;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup of a profiling temp tree
                }
            });
        }
    }
}
