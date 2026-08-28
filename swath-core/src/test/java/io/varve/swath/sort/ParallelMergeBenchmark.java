/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * MEASUREMENT harness: proves whether the concurrent range-merge path
 * ({@code swath.sort.merge-parallelism > 1}, with the live page-run skip) actually cuts
 * {@link SortTransform}'s merge-phase wall clock, and whether the effect is CPU-parallelizable or
 * disk-bandwidth-bound. Runs the ACTUAL production merge path — {@link SortTransform#transform}
 * with {@link SortedParquetWriterFactory} over live-format page-run staging segments produced through
 * the same {@link SortBuffer} seal and {@link PageRunSegmentWriter#flush} seam as listing, once per
 * requested {@code R} in {1, 2, 4, 8} (plus an R=1 repeat to bound run-to-run variance).
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
    private static final String EXTERNAL_STAGING_PROPERTY = "swath.bench.staging-dir";
    static final String ARM = "MERGE_ONLY_PAGE_RUN";

    // --- Corpus knobs (system-property overridable for a fast smoke run before the full-size one). ---
    private static final int NUM_SEGMENTS = Integer.getInteger("swath.bench.segments", 64);
    private static final long TOTAL_ROWS = Long.getLong("swath.bench.rows", 12_000_000L);
    private static final int BLOCK_ROWS = Integer.getInteger("swath.bench.blockRows", 4_000);
    private static final int PAGE_ROWS = Integer.getInteger("swath.bench.pageRows", 1_000);
    /**
     * The {@code R} values to sweep, in order. {@code R=1} must be FIRST — it is the identity baseline
     * every later arm is full-row compared against. The sweep is user-settable through
     * {@code swath.bench.ranges}, so this is enforced rather than assumed: a sweep starting anywhere
     * else would otherwise dereference a null baseline and surface as an NPE partway through a
     * half-hour run, instead of saying up front what the operator got wrong.
     */
    private static final List<Integer> RANGES = parseRanges();

    private static List<Integer> parseRanges() {
        String configured = System.getProperty("swath.bench.ranges", "1,2,4,8");
        if (configured == null || configured.isBlank()) {
            throw invalidRanges(configured, "must not be empty");
        }
        String[] tokens = configured.split(",", -1);
        List<Integer> ranges = new ArrayList<>(tokens.length);
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (token.isEmpty()) {
                throw invalidRanges(configured, "contains an empty value at position " + (i + 1));
            }
            try {
                ranges.add(Integer.parseInt(token));
            } catch (NumberFormatException e) {
                throw invalidRanges(configured,
                        "contains invalid integer '" + token + "' at position " + (i + 1));
            }
        }
        if (ranges.get(0) != 1) {
            throw invalidRanges(configured, "must start with R=1 for the identity baseline");
        }
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < ranges.size(); i++) {
            int range = ranges.get(i);
            if (range <= 0) {
                throw invalidRanges(configured, "contains nonpositive R=" + range);
            }
            if (!seen.add(range)) {
                throw invalidRanges(configured, "contains duplicate R=" + range);
            }
            if (i > 0 && range <= ranges.get(i - 1)) {
                throw invalidRanges(configured, "must be strictly increasing; R=" + range
                        + " follows R=" + ranges.get(i - 1));
            }
        }
        return List.copyOf(ranges);
    }

    private static IllegalArgumentException invalidRanges(String configured, String reason) {
        return new IllegalArgumentException(
                "invalid swath.bench.ranges='" + configured + "': " + reason);
    }
    private static final int TOTAL_DAYS = 1_500;
    // Generous: the corpus knobs above (and swath.bench.ranges) govern how long a sweep actually takes,
    // and this class never runs under the default suite — the timeout is a runaway backstop, not a budget.
    @Test
    @Timeout(value = 120, unit = TimeUnit.MINUTES)
    void parallelMergeScaling() throws IOException {
        Path root = Files.createTempDirectory("swath-parallel-merge-bench-");
        System.out.println("BENCH_ROOT " + root);
        System.out.println("BENCH_ARM arm=" + ARM + " listing_fetches=0");
        System.out.printf("BENCH_HEAP max_memory_mb=%.1f available_processors=%d%n",
                Runtime.getRuntime().maxMemory() / (1024.0 * 1024.0), Runtime.getRuntime().availableProcessors());
        try {
            Path master = externalStaging();
            if (master == null) {
                master = Files.createDirectory(root.resolve("master"));
                long t0 = System.nanoTime();
                SortBenchCorpus.Stats corpus = buildCorpus(master);
                long buildMs = (System.nanoTime() - t0) / 1_000_000;
                System.out.printf("BENCH_CORPUS arm=%s source=generated segments=%d rows=%d bytes=%d build_ms=%d%n",
                        ARM, corpus.segments(), corpus.rows(), corpus.bytes(), buildMs);
            } else {
                List<Path> inputs = requirePageRunInputs(master);
                long bytes = inputs.stream().mapToLong(ParallelMergeBenchmark::size).sum();
                System.out.printf("BENCH_CORPUS arm=%s source=external staging=%s segments=%d bytes=%d build_ms=0%n",
                        ARM, master, inputs.size(), bytes);
            }

            measureOpenReaderHeap(master);

            List<Integer> ranges = RANGES;
            Map<Integer, ArmResult> results = new LinkedHashMap<>();
            List<Path> baselineFinals = null;

            for (int r : ranges) {
                ArmResult ar = runArm(root, master, r, "r" + r);
                results.put(r, ar);
                if (r == 1) {
                    baselineFinals = ar.finalFiles;
                } else {
                    boolean exact = fullRowsEqual(baselineFinals, ar.finalFiles);
                    ar.fullRowExact = exact;
                    if (!exact) {
                        System.out.println("BENCH_FULL_ROW_EXACT_FAIL requested_r=" + r
                                + " actual_ranges=" + ar.actualRanges);
                        throw new AssertionError(
                                "full-row mismatch at requested R=" + r + " (actual ranges="
                                        + ar.actualRanges + ") vs the R=1 baseline — silent data loss");
                    }
                    SortBenchCorpus.deleteTree(ar.armRoot);
                }
                System.out.println(ar.toLine());
            }

            // R=1 repeat, for run-to-run variance.
            ArmResult repeat = runArm(root, master, 1, "r1-repeat");
            boolean repeatExact = fullRowsEqual(baselineFinals, repeat.finalFiles);
            if (!repeatExact) {
                throw new AssertionError("R=1 repeat mismatched the R=1 baseline");
            }
            System.out.println(repeat.toLine());
            SortBenchCorpus.deleteTree(repeat.armRoot);

            ArmResult baseline = results.get(1);
            System.out.printf("BENCH_VARIANCE requested_r=1 actual_ranges=1 first_elapsed_ms=%d "
                            + "repeat_elapsed_ms=%d delta_pct=%.1f%n",
                    baseline.elapsedNanos / 1_000_000, repeat.elapsedNanos / 1_000_000,
                    100.0 * (repeat.elapsedNanos - baseline.elapsedNanos) / baseline.elapsedNanos);

            for (int r : ranges) {
                ArmResult ar = results.get(r);
                if (r == 1) {
                    System.out.println("BENCH_SPEEDUP requested_r=1 actual_ranges=1 status=baseline speedup=1.000");
                } else if (ar.rangeBelowStagedFloorCount > 0
                        || ar.rangeFdLimitedCount > 0
                        || ar.rangeFdExhaustedCount > 0
                        || ar.rangeWouldCascadeCount > 0
                        || ar.rangeUnsplittableCount > 0
                        || ar.actualRanges != ar.requestedRanges) {
                    String status = ar.actualRanges <= 1 ? "not_engaged" : "clamped_or_reduced";
                    System.out.printf("BENCH_SPEEDUP requested_r=%d actual_ranges=%d status=%s "
                                    + "speedup=unavailable%n",
                            r, ar.actualRanges, status);
                } else {
                    double speedup = (double) baseline.elapsedNanos / ar.elapsedNanos;
                    System.out.printf("BENCH_SPEEDUP requested_r=%d actual_ranges=%d status=engaged "
                                    + "speedup=%.3f%n",
                            r, ar.actualRanges, speedup);
                }
            }

            SortBenchCorpus.deleteTree(baseline.armRoot);
        } finally {
            SortBenchCorpus.deleteTree(root);
        }
    }

    // =====================================================================
    // Per-arm execution
    // =====================================================================

    private ArmResult runArm(Path root, Path master, int mergeParallelism, String label) throws IOException {
        Path armRoot = Files.createDirectory(root.resolve("arm-" + label));
        Path output = Files.createDirectory(armRoot.resolve("data"));
        Path staging = Files.createDirectory(armRoot.resolve("_staging"));
        List<Path> stagingSegments = SortBenchCorpus.copyCorpus(master, staging);

        // merge-parallelism is the swept knob; every OTHER swath.sort.* property falls through to the
        // real system properties, so an arm can hold one knob fixed while another varies (e.g. pinning
        // merge-budget-bytes while -Xmx changes, to separate cascade removal from GC relief).
        SortConfig config = SortConfig.fromProperties(
                key -> "swath.sort.merge-parallelism".equals(key)
                        ? String.valueOf(mergeParallelism)
                        : System.getProperty(key));
        ThreadSafeMetrics metrics = new ThreadSafeMetrics();
        BenchRangeTimer timer = new BenchRangeTimer();
        SortedFileWriterFactory writerFactory = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
        SortTransform transform =
                new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP,
                        EqualKeyPolicy.ALLOW, metrics, writerFactory,
                        MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, timer,
                        SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));

        RssSampler sampler = new RssSampler();
        Thread samplerThread = new Thread(sampler, "bench-rss-sampler-" + label);
        samplerThread.setDaemon(true);

        // Peak HEAP is the gating metric for merge-parallelism (the R× writer/floor term the merge
        // budget does not cover), and unlike RSS it can be attributed per arm: RSS is process-wide and
        // monotone across arms in one JVM (a later R=1 arm inherits an earlier R=8 arm's peak), whereas
        // the heap pools' peak is resettable. Same measurement as production's efficiency.peak_heap_bytes
        // (ResourceMetrics#peakHeapBytes: used, summed across HEAP pools).
        System.gc();   // settle the prior arm's garbage so this arm's peak is its own
        resetHeapPeaks();
        long cpuStartNanos = SortBenchCorpus.processCpuTimeNanos();
        long wallStartNanos = System.nanoTime();
        samplerThread.start();
        SortTransformResult result;
        try {
            result = transform.transform(stagingSegments, output, staging, PublishListener.NO_OP,
                    units -> { }, FinalPassListener.NO_OP);
        } finally {
            samplerThread.interrupt();
            try {
                samplerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        long wallEndNanos = System.nanoTime();
        long cpuEndNanos = SortBenchCorpus.processCpuTimeNanos();

        ArmResult ar = new ArmResult();
        ar.requestedRanges = mergeParallelism;
        ar.label = label;
        ar.armRoot = armRoot;
        ar.elapsedNanos = wallEndNanos - wallStartNanos;
        ar.avgCoresBusy = (cpuStartNanos < 0 || cpuEndNanos < 0)
                ? -1
                : (cpuEndNanos - cpuStartNanos) / 1e9 / ((wallEndNanos - wallStartNanos) / 1e9);
        ar.peakRssBytes = sampler.maxRssBytes;
        ar.peakHeapBytes = peakHeapBytes();
        ar.mergePasses = result.mergePasses();
        ar.cascadedPasses = result.cascadedPasses();
        ar.fastPathEmissions = result.fastPathEmissions();
        ar.totalRows = result.totalRows();
        ar.finalFiles = result.finalFiles();
        ar.inputSegments = stagingSegments.size();
        ar.rangeParallelCount = metrics.count("SORT.merge_range_parallel");
        ar.actualRanges = ar.rangeParallelCount > 0 ? ar.rangeParallelCount : 1;
        ar.rangeBelowStagedFloorCount = metrics.count("SORT.merge_range_below_staged_floor");
        ar.rangeFdLimitedCount = metrics.count("SORT.merge_range_fd_limited");
        ar.rangeFdExhaustedCount = metrics.count("SORT.merge_range_fd_exhausted");
        ar.rangeWouldCascadeCount = metrics.count("SORT.merge_range_would_cascade");
        ar.rangeUnsplittableCount = metrics.count("SORT.merge_range_unsplittable");
        ar.pageSkipEngagedCount = metrics.count("SORT.merge_range_page_skipped");
        ar.sampleCappedSegments = metrics.count("SORT.merge_range_sample_capped");
        ar.pageWholeEmissions = metrics.count("SORT.page_whole_emitted");
        ar.pageOverlapKeyMerges = metrics.count("SORT.page_overlap_keymerge");
        ar.boundaryNanos = timer.boundaryNanos;
        ar.rangeLatenciesNanos = timer.rangeLatenciesNanos.stream().toList();
        return ar;
    }

    /**
     * DIRECT measurement of the per-open-stream retained heap for the live page-run format. Opens a
     * {@link PageFrontierReader} for every corpus segment (its constructor reads and CRC-verifies the
     * first framed page body), settles the heap, and reports the retained delta per open frontier.
     *
     * <p>Without this the {@code R×} heap term can only be inferred by fitting the observed
     * peak-heap slope and dividing by the segment count — which then "explains" the same slope it
     * was derived from. This makes it a measurement instead.
     */
    private static void measureOpenReaderHeap(Path master) throws IOException {
        List<Path> segments = SortBenchCorpus.pageRunSegments(master);
        long before = settledHeapBytes();
        List<PageFrontierReader> open = new ArrayList<>(segments.size());
        try {
            for (Path segment : segments) {
                PageFrontierReader reader = new PageFrontierReader(segment, SortMetrics.NO_OP);
                open.add(reader);
            }
            long after = settledHeapBytes();
            long delta = after - before;
            System.out.printf("BENCH_STREAM_HEAP staging_format=page-run open_frontiers=%d "
                            + "retained_bytes=%d per_frontier_bytes=%d per_frontier_mb=%.2f%n",
                    open.size(), delta, delta / Math.max(1, open.size()),
                    delta / (1024.0 * 1024.0) / Math.max(1, open.size()));
        } finally {
            for (PageFrontierReader reader : open) {
                reader.close();
            }
        }
    }

    /** Used heap after giving the collector a chance to settle — the live-set estimate, not float. */
    private static long settledHeapBytes() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static Path externalStaging() throws IOException {
        String configured = System.getProperty(EXTERNAL_STAGING_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return null;
        }
        Path staging = Path.of(configured).toAbsolutePath().normalize();
        requirePageRunInputs(staging);
        return staging;
    }

    static List<Path> requirePageRunInputs(Path staging) throws IOException {
        if (!Files.isDirectory(staging)) {
            throw new IllegalArgumentException("swath.bench.staging-dir must name a directory: " + staging);
        }
        List<Path> inputs = SortBenchCorpus.pageRunSegments(staging);
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("swath.bench.staging-dir contains no *.pageseg inputs: " + staging);
        }
        return inputs;
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to stat external staging input " + path, e);
        }
    }

    /** Clear every HEAP pool's recorded peak, so the next arm's peak is attributable to that arm alone. */
    private static void resetHeapPeaks() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                pool.resetPeakUsage();
            }
        }
    }

    /** Peak heap bytes since the last {@link #resetHeapPeaks()}, summed across HEAP pools. */
    private static long peakHeapBytes() {
        long total = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null) {
                total += pool.getPeakUsage().getUsed();
            }
        }
        return total;
    }

    // =====================================================================
    // Corpus generation — deterministic, unique, monotonically-keyed rows
    // interleaved round-robin (at BLOCK_ROWS granularity) across NUM_SEGMENTS
    // sorted staging segments, so the merge does genuine cross-stream k-way
    // work (not a trivial disjoint concatenation) while each live-format page
    // stays narrow (real page-skip opportunity).
    // =====================================================================

    private SortBenchCorpus.Stats buildCorpus(Path master) throws IOException {
        if (PAGE_ROWS <= 0) {
            throw new IllegalArgumentException("swath.bench.pageRows must be > 0, got " + PAGE_ROWS);
        }
        SortConfig config = SortConfig.fromSystemProperties();
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, config.segmentCodec());
        long rowsPerDay = Math.max(1, TOTAL_ROWS / TOTAL_DAYS);
        LocalDate base = LocalDate.of(2019, 1, 1);
        int totalSegments = 0;
        long totalRows = 0;
        long totalBytes = 0;
        for (int seg = 0; seg < NUM_SEGMENTS; seg++) {
            SortBuffer buffer = new SortBuffer(config, CMP);
            try (SortedCursor cursor =
                         SortBenchCorpus.generatedCursor(
                                 seg, NUM_SEGMENTS, BLOCK_ROWS, TOTAL_ROWS, rowsPerDay, base)) {
                List<ListEntry> page = new ArrayList<>(PAGE_ROWS);
                long nodeId = 0;
                while (cursor.hasNext()) {
                    page.add(cursor.next());
                    if (page.size() == PAGE_ROWS) {
                        buffer.admit(nodeId++, page);
                        page = new ArrayList<>(PAGE_ROWS);
                    }
                }
                if (!page.isEmpty()) {
                    buffer.admit(nodeId, page);
                }
            }
            if (buffer.isEmpty()) {
                continue;
            }
            Path path = master.resolve(String.format("seg-%05d.pageseg", seg));
            SegmentResult result = writer.flush(buffer.seal(SealTrigger.DRAIN), path);
            totalSegments++;
            totalRows += result.rows();
            totalBytes += result.bytes();
        }
        return new SortBenchCorpus.Stats(totalSegments, totalRows, totalBytes);
    }

    // =====================================================================
    // Staging copy, full-row identity verification, RSS sampling, misc helpers
    // =====================================================================

    /** Streams and compares decoded full rows position-for-position; this is not a file-byte check. */
    private static boolean fullRowsEqual(List<Path> expected, List<Path> actual) throws IOException {
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

    private static final class ArmResult {
        int requestedRanges;
        long actualRanges;
        String label;
        Path armRoot;
        long elapsedNanos;
        double avgCoresBusy;
        long peakRssBytes;
        long peakHeapBytes;
        int inputSegments;
        long mergePasses;
        long cascadedPasses;
        long fastPathEmissions;
        long totalRows;
        List<Path> finalFiles;
        long rangeParallelCount;
        long rangeBelowStagedFloorCount;
        long rangeFdLimitedCount;
        long rangeFdExhaustedCount;
        long rangeWouldCascadeCount;
        long rangeUnsplittableCount;
        long pageSkipEngagedCount;
        long sampleCappedSegments;
        long pageWholeEmissions;
        long pageOverlapKeyMerges;
        long boundaryNanos;
        List<Long> rangeLatenciesNanos;
        boolean fullRowExact = true;   // R=1 baseline is trivially exact against itself

        String toLine() {
            long rangeMergeSumNanos = rangeLatenciesNanos.stream().mapToLong(Long::longValue).sum();
            String boundaryMs = boundaryNanos < 0 ? "unavailable" : String.valueOf(boundaryNanos / 1_000_000);
            String rangeMergeSumMs = rangeLatenciesNanos.isEmpty()
                    ? "unavailable"
                    : String.valueOf(rangeMergeSumNanos / 1_000_000);
            List<Long> rangeLatenciesMs =
                    rangeLatenciesNanos.stream().map(n -> n / 1_000_000L).toList();
            return String.format(
                    "BENCH_ROW arm=%s label=%s staging_format=page-run requested_r=%d actual_ranges=%d "
                            + "merge_elapsed_ms=%d boundary_ms=%s range_merge_sum_ms=%s "
                            + "avg_cores_busy=%.2f peak_heap_mb=%.1f peak_rss_mb=%.1f "
                            + "rows=%d input_segments=%d output_files=%d merge_passes=%d "
                            + "cascaded_passes=%d fastpath_emissions=%d range_parallel_count=%d "
                            + "range_below_staged_floor_count=%d range_fd_limited_count=%d "
                            + "range_fd_exhausted_count=%d "
                            + "range_would_cascade_count=%d range_unsplittable_count=%d "
                            + "page_skip_engaged_ranges=%d "
                            + "sample_capped_segments=%d page_whole_emissions=%d "
                            + "page_overlap_keymerges=%d page_reads=unavailable "
                            + "read_amplification=unavailable identity_check=full-row "
                            + "full_row_exact=%s range_latencies_ms=%s",
                    ARM, label, requestedRanges, actualRanges, elapsedNanos / 1_000_000, boundaryMs,
                    rangeMergeSumMs, avgCoresBusy, peakHeapBytes / (1024.0 * 1024.0),
                    peakRssBytes / (1024.0 * 1024.0), totalRows, inputSegments, finalFiles.size(),
                    mergePasses, cascadedPasses, fastPathEmissions, rangeParallelCount,
                    rangeBelowStagedFloorCount, rangeFdLimitedCount, rangeFdExhaustedCount,
                    rangeWouldCascadeCount, rangeUnsplittableCount, pageSkipEngagedCount,
                    sampleCappedSegments, pageWholeEmissions,
                    pageOverlapKeyMerges, fullRowExact, rangeLatenciesMs);
        }
    }

    /** Captures both timer methods without manufacturing timing unavailable on the serial path. */
    private static final class BenchRangeTimer implements RangeMergeTimer {
        private final ConcurrentLinkedQueue<Long> rangeLatenciesNanos = new ConcurrentLinkedQueue<>();
        private volatile long boundaryNanos = -1;

        @Override
        public void recordRangeMerge(long nanos) {
            rangeLatenciesNanos.add(nanos);
        }

        @Override
        public void recordBoundarySampling(long nanos) {
            boundaryNanos = nanos;
        }
    }

    /** Thread-safe {@link SortMetrics} — the parallel path records from several range threads at once. */
    private static final class ThreadSafeMetrics implements SortMetrics {
        private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.computeIfAbsent(outcome + "." + reason, k -> new LongAdder()).increment();
        }

        @Override
        public void markProgress() {
        }

        @Override
        public void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        }

        long count(String key) {
            LongAdder a = counts.get(key);
            return a == null ? 0 : a.sum();
        }
    }
}
