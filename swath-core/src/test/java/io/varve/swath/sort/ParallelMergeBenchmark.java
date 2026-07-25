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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * MEASUREMENT harness: proves whether the concurrent range-merge path
 * ({@code swath.sort.merge-parallelism > 1}, with the row-group skip) actually cuts
 * {@link SortTransform}'s merge-phase wall clock, and whether the effect is CPU-parallelizable or
 * disk-bandwidth-bound. Runs the ACTUAL production merge path — {@link SortTransform#transform}
 * with {@link SortedParquetWriterFactory} and production default segment/final row-group sizes —
 * over a large synthetic corpus of UNIQUE-keyed sorted staging segments, once per {@code R} in
 * {1, 2, 4, 8} (plus an R=1 repeat to bound run-to-run variance).
 *
 * <p>NOT part of the normal build or CI: this class only runs when {@code -Dswath.bench=on} is
 * passed (see {@link EnabledIfSystemProperty}), so {@code ./gradlew build} never executes it.
 *
 * <p>Run: {@code ./gradlew :swath-core:test --tests
 * 'io.varve.swath.sort.ParallelMergeBenchmark' -Dswath.bench=on -Pperf}
 * ({@code -Pperf} widens the forked test JVM's heap to 2&nbsp;GB — see
 * {@code build-logic/.../swath.java-conventions.gradle.kts} — since the default forked-test-worker
 * heap is far too small for a GB-scale merge corpus.)
 *
 * <p>Raw numbers, methodology, and conclusions are written up separately; this class only prints
 * {@code BENCH_*}-prefixed lines to stdout (captured by Gradle's per-test XML report even when the
 * console doesn't show standard streams) for that write-up to be built from.
 */
@EnabledIfSystemProperty(named = "swath.bench", matches = "on")
class ParallelMergeBenchmark {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    // --- Corpus knobs (system-property overridable for a fast smoke run before the full-size one). ---
    private static final int NUM_SEGMENTS = Integer.getInteger("swath.bench.segments", 64);
    private static final long TOTAL_ROWS = Long.getLong("swath.bench.rows", 12_000_000L);
    private static final int BLOCK_ROWS = Integer.getInteger("swath.bench.blockRows", 4_000);
    private static final int TOTAL_DAYS = 1_500;
    private static final String KEY_PREFIX = "corp-data-lake-logs";
    private static final String[] STORAGE_CLASSES =
            {"STANDARD", "STANDARD_IA", "INTELLIGENT_TIERING", "GLACIER"};
    // Production default segment-row-group-bytes (SortConfig#fromProperties) — small on purpose so
    // the row-group skip has real, narrow groups to prune.
    private static final long SEGMENT_ROW_GROUP_BYTES = 1L << 20;

    @Test
    @Timeout(value = 9, unit = TimeUnit.MINUTES)
    void parallelMergeScaling() throws IOException {
        Path root = Files.createTempDirectory("swath-parallel-merge-bench-");
        System.out.println("BENCH_ROOT " + root);
        System.out.printf("BENCH_HEAP max_memory_mb=%.1f available_processors=%d%n",
                Runtime.getRuntime().maxMemory() / (1024.0 * 1024.0), Runtime.getRuntime().availableProcessors());
        try {
            Path master = Files.createDirectory(root.resolve("master"));
            long t0 = System.nanoTime();
            CorpusStats corpus = buildCorpus(master);
            long buildMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("BENCH_CORPUS segments=%d rows=%d bytes=%d build_ms=%d%n",
                    corpus.segments, corpus.rows, corpus.bytes, buildMs);

            List<Integer> ranges = List.of(1, 2, 4, 8);
            Map<Integer, ArmResult> results = new LinkedHashMap<>();
            List<Path> baselineFinals = null;

            for (int r : ranges) {
                ArmResult ar = runArm(root, master, r, "r" + r);
                results.put(r, ar);
                if (r == 1) {
                    baselineFinals = ar.finalFiles;
                } else {
                    boolean exact = byteExact(baselineFinals, ar.finalFiles);
                    ar.byteExact = exact;
                    if (!exact) {
                        System.out.println("BENCH_BYTE_EXACT_FAIL r=" + r);
                        throw new AssertionError(
                                "byte-exact mismatch at R=" + r + " vs the R=1 baseline — silent data loss");
                    }
                    deleteRecursively(ar.armRoot);
                }
                System.out.println(ar.toLine());
            }

            // R=1 repeat, for run-to-run variance.
            ArmResult repeat = runArm(root, master, 1, "r1-repeat");
            boolean repeatExact = byteExact(baselineFinals, repeat.finalFiles);
            if (!repeatExact) {
                throw new AssertionError("R=1 repeat mismatched the R=1 baseline");
            }
            System.out.println(repeat.toLine());
            deleteRecursively(repeat.armRoot);

            ArmResult baseline = results.get(1);
            System.out.printf("BENCH_VARIANCE r1_first_ms=%d r1_repeat_ms=%d delta_pct=%.1f%n",
                    baseline.mergeMs, repeat.mergeMs,
                    100.0 * (repeat.mergeMs - baseline.mergeMs) / baseline.mergeMs);

            for (int r : ranges) {
                ArmResult ar = results.get(r);
                double speedup = (double) baseline.mergeMs / ar.mergeMs;
                System.out.printf("BENCH_SPEEDUP r=%d speedup=%.3f%n", r, speedup);
            }

            deleteRecursively(baseline.armRoot);
        } finally {
            deleteRecursively(root);
        }
    }

    // =====================================================================
    // Per-arm execution
    // =====================================================================

    private ArmResult runArm(Path root, Path master, int mergeParallelism, String label) throws IOException {
        Path armRoot = Files.createDirectory(root.resolve("arm-" + label));
        Path output = Files.createDirectory(armRoot.resolve("data"));
        Path staging = Files.createDirectory(armRoot.resolve("_staging"));
        List<Path> stagingSegments = copyCorpus(master, staging);

        SortConfig config = SortConfig.fromProperties(
                key -> "swath.sort.merge-parallelism".equals(key) ? String.valueOf(mergeParallelism) : null);
        ThreadSafeMetrics metrics = new ThreadSafeMetrics();
        ConcurrentLinkedQueue<Long> rangeLatenciesNanos = new ConcurrentLinkedQueue<>();
        RangeMergeTimer timer = rangeLatenciesNanos::add;
        SortedFileWriterFactory writerFactory = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
        SortTransform transform =
                new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP, metrics, writerFactory), false, timer);

        RssSampler sampler = new RssSampler();
        Thread samplerThread = new Thread(sampler, "bench-rss-sampler-" + label);
        samplerThread.setDaemon(true);

        long cpuStartNanos = processCpuTimeNanos();
        long wallStartNanos = System.nanoTime();
        samplerThread.start();
        SortTransformResult result;
        try {
            result = transform.transform(stagingSegments, output, staging, PublishListener.NO_OP);
        } finally {
            samplerThread.interrupt();
            try {
                samplerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        long wallEndNanos = System.nanoTime();
        long cpuEndNanos = processCpuTimeNanos();

        ArmResult ar = new ArmResult();
        ar.mergeParallelism = mergeParallelism;
        ar.label = label;
        ar.armRoot = armRoot;
        ar.mergeMs = (wallEndNanos - wallStartNanos) / 1_000_000;
        ar.avgCoresBusy = (cpuStartNanos < 0 || cpuEndNanos < 0)
                ? -1
                : (cpuEndNanos - cpuStartNanos) / 1e9 / ((wallEndNanos - wallStartNanos) / 1e9);
        ar.peakRssBytes = sampler.maxRssBytes;
        ar.mergePasses = result.mergePasses();
        ar.cascadedPasses = result.cascadedPasses();
        ar.fastPathEmissions = result.fastPathEmissions();
        ar.totalRows = result.totalRows();
        ar.finalFiles = result.finalFiles();
        ar.rangeParallelCount = metrics.count("SORT.merge_range_parallel");
        ar.rowgroupSkipEngagedCount = metrics.count("SORT.merge_range_rowgroup_skipped");
        ar.rangeLatenciesMs = rangeLatenciesNanos.stream().map(n -> n / 1_000_000L).collect(Collectors.toList());
        return ar;
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
    // Corpus generation — streamed, unique, monotonically-keyed rows
    // interleaved round-robin (at BLOCK_ROWS granularity) across NUM_SEGMENTS
    // sorted staging segments, so the merge does genuine cross-stream k-way
    // work (not a trivial disjoint concatenation) while each segment's
    // Parquet row groups stay NARROW (real row-group-skip opportunity).
    // =====================================================================

    private record CorpusStats(int segments, long rows, long bytes) {
    }

    private CorpusStats buildCorpus(Path master) throws IOException {
        long rowsPerDay = Math.max(1, TOTAL_ROWS / TOTAL_DAYS);
        LocalDate base = LocalDate.of(2019, 1, 1);
        long totalRows = 0;
        long totalBytes = 0;
        for (int seg = 0; seg < NUM_SEGMENTS; seg++) {
            Path path = master.resolve(String.format("seg-%05d.parquet", seg));
            SegmentWriter writer =
                    new SegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, SEGMENT_ROW_GROUP_BYTES);
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

    /**
     * Lazily generates this segment's owned rows (streaming, O(1) memory): the global row index
     * space [0, totalRows) is chopped into {@code blockRows}-sized blocks, round-robin assigned to
     * segments ({@code block % numSegments == segment}); this segment visits its owned blocks in
     * increasing block-index order and, within a block, increasing row-index order — so the emitted
     * sequence is strictly increasing in the global row index, and (see {@link #key}) the derived
     * key is a monotonic function of that index, making the cursor already internally sorted with
     * NO in-memory sort. Segments therefore interleave across the WHOLE keyspace at block
     * granularity (genuine k-way merge), while each segment's own row groups span only one
     * contiguous block's worth of keys (narrow — real row-group-skip opportunity per range).
     */
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

    /** Deterministic realistic {@link ObjectEntry} for global row index {@code i} (see {@link #key}). */
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

    /**
     * Realistic S3-style key, {@code <prefix>/YYYY/MM/DD/<within-day>-<hash>} (~70-80 bytes) —
     * STRICTLY monotonic in {@code i} (see the {@link GeneratedCursor} javadoc): fixed prefix, then
     * a fixed-width zero-padded calendar date (always non-decreasing with {@code i}), then a
     * fixed-width zero-padded within-day counter (unique + increasing for a fixed day) — the
     * trailing hash is cosmetic only, never needed to break a tie (day, within-day) already
     * bijects with {@code i}.
     */
    private static String key(long i, long rowsPerDay, LocalDate base) {
        long day = i / rowsPerDay;
        long within = i % rowsPerDay;
        LocalDate date = base.plusDays(day);
        long h1 = mix(i);
        long h2 = mix(i ^ 0x9E3779B97F4A7C15L);
        return String.format("%s/%04d/%02d/%02d/%08d-%016x%016x",
                KEY_PREFIX, date.getYear(), date.getMonthValue(), date.getDayOfMonth(), within, h1, h2);
    }

    /** splitmix64 finalizer — a fast, deterministic, well-mixed 64-bit hash of {@code x}. */
    private static long mix(long x) {
        x += 0x9E3779B97F4A7C15L;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }

    // =====================================================================
    // Staging copy, byte-exact verification, RSS sampling, misc helpers
    // =====================================================================

    private static List<Path> copyCorpus(Path master, Path target) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(master, "*.parquet")) {
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

    /** Streams both file lists position-for-position; no full materialization (scales to any size). */
    private static boolean byteExact(List<Path> expected, List<Path> actual) throws IOException {
        try (MultiFileStream a = new MultiFileStream(expected); MultiFileStream b = new MultiFileStream(actual)) {
            long row = 0;
            while (a.hasNext() && b.hasNext()) {
                ListEntry ea = a.next();
                ListEntry eb = b.next();
                if (!ea.equals(eb)) {
                    System.out.println("BENCH_MISMATCH row=" + row + " expected=" + ea + " actual=" + eb);
                    return false;
                }
                row++;
            }
            if (a.hasNext() != b.hasNext()) {
                System.out.println("BENCH_MISMATCH row_count_diverges after row=" + row);
                return false;
            }
            return true;
        }
    }

    /** Sequentially concatenates several sorted Parquet files (filename order) as one entry stream. */
    private static final class MultiFileStream implements AutoCloseable {
        private final List<Path> files;
        private int idx = -1;
        private SegmentReader current;

        MultiFileStream(List<Path> files) throws IOException {
            this.files = files;
            advance();
        }

        private void advance() throws IOException {
            while (current == null || !current.hasNext()) {
                if (current != null) {
                    current.close();
                    current = null;
                }
                idx++;
                if (idx >= files.size()) {
                    return;
                }
                current = new SegmentReader(files.get(idx));
            }
        }

        boolean hasNext() {
            return current != null && current.hasNext();
        }

        ListEntry next() throws IOException {
            ListEntry e = current.next();
            advance();
            return e;
        }

        @Override
        public void close() throws IOException {
            if (current != null) {
                current.close();
            }
        }
    }

    /** Background poller of {@code /proc/self/status VmRSS} — the run's peak current RSS. */
    private static final class RssSampler implements Runnable {
        private static final Path STATUS = Path.of("/proc/self/status");
        volatile long maxRssBytes = -1;

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                sample();
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            sample();   // one last sample to catch the tail
        }

        private void sample() {
            long rss = readRssBytes();
            if (rss > maxRssBytes) {
                maxRssBytes = rss;
            }
        }

        private static long readRssBytes() {
            if (!Files.isReadable(STATUS)) {
                return -1;
            }
            try {
                for (String line : Files.readAllLines(STATUS)) {
                    if (line.startsWith("VmRSS:")) {
                        String[] parts = line.trim().split("\\s+");
                        return Long.parseLong(parts[1]) * 1024L;
                    }
                }
            } catch (IOException | NumberFormatException e) {
                return -1;
            }
            return -1;
        }
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
                    // best-effort cleanup of a bench temp tree
                }
            });
        }
    }

    private static final class ArmResult {
        int mergeParallelism;
        String label;
        Path armRoot;
        long mergeMs;
        double avgCoresBusy;
        long peakRssBytes;
        long mergePasses;
        long cascadedPasses;
        long fastPathEmissions;
        long totalRows;
        List<Path> finalFiles;
        long rangeParallelCount;
        long rowgroupSkipEngagedCount;
        List<Long> rangeLatenciesMs;
        boolean byteExact = true;   // R=1 baseline is trivially exact against itself

        String toLine() {
            return String.format(
                    "BENCH_ROW label=%s r=%d merge_ms=%d avg_cores_busy=%.2f peak_rss_mb=%.1f "
                            + "merge_passes=%d cascaded_passes=%d fastpath=%d total_rows=%d files=%d "
                            + "range_parallel_count=%d rowgroup_skip_engaged=%d byte_exact=%s "
                            + "range_latencies_ms=%s",
                    label, mergeParallelism, mergeMs, avgCoresBusy, peakRssBytes / (1024.0 * 1024.0),
                    mergePasses, cascadedPasses, fastPathEmissions, totalRows, finalFiles.size(),
                    rangeParallelCount, rowgroupSkipEngagedCount, byteExact, rangeLatenciesMs);
        }
    }

    /** Thread-safe {@link SortMetrics} — the parallel path records from several range threads at once. */
    private static final class ThreadSafeMetrics implements SortMetrics {
        private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.computeIfAbsent(outcome + "." + reason, k -> new LongAdder()).increment();
        }

        long count(String key) {
            LongAdder a = counts.get(key);
            return a == null ? 0 : a.sum();
        }
    }
}
