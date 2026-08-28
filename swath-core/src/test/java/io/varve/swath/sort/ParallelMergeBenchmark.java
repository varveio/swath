/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.Manifest;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
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
 * the same {@link SortBuffer} seal and {@link PageRunSegmentWriter#flush} seam as listing. After one
 * untimed full-transform warm-up, measured arms are interleaved as serial A, ascending candidates,
 * serial B, descending candidates, serial C; speedups use medians and are invalidated when either
 * bracket exceeds the explicit variance threshold.
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
    static final String ARM = "MERGE_BENCH_PAGE_RUN";
    private static final String NO_FINGERPRINT = "not_applicable";

    // --- Corpus knobs (system-property overridable for a fast smoke run before the full-size one). ---
    private static final int NUM_SEGMENTS = Integer.getInteger("swath.bench.segments", 64);
    private static final long TOTAL_ROWS = Long.getLong("swath.bench.rows", 12_000_000L);
    private static final int BLOCK_ROWS = Integer.getInteger("swath.bench.blockRows", 4_000);
    private static final int PAGE_ROWS = Integer.getInteger("swath.bench.pageRows", 1_000);
    private static final String MAX_VARIANCE_PROPERTY = "swath.bench.max-variance-pct";
    private static final double MAX_VARIANCE_PCT = parseMaxVariancePct();
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

    private static double parseMaxVariancePct() {
        String configured = System.getProperty(MAX_VARIANCE_PROPERTY, "15.0");
        try {
            double value = Double.parseDouble(configured);
            if (Double.isFinite(value) && value > 0.0) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // The typed error below owns the diagnostic.
        }
        throw new IllegalArgumentException("invalid " + MAX_VARIANCE_PROPERTY + "='" + configured
                + "': expected a finite percentage > 0");
    }
    private static final int TOTAL_DAYS = 1_500;
    // Generous: the corpus knobs above (and swath.bench.ranges) govern how long a sweep actually takes,
    // and this class never runs under the default suite — the timeout is a runaway backstop, not a budget.
    @Test
    @Timeout(value = 120, unit = TimeUnit.MINUTES)
    void parallelMergeScaling() throws IOException {
        CorpusCatalog corpus = null;
        Path root = null;
        Throwable failure = null;
        try {
            corpus = externalStaging();
            if (corpus == null) {
                PreparedGenerated generated = prepareGenerated(
                        null, NUM_SEGMENTS, TOTAL_ROWS, BLOCK_ROWS, PAGE_ROWS);
                root = generated.root();
                corpus = generated.catalog();
            } else {
                Path output = corpus.stagingDir().getParent();
                Path tempParent = output == null ? null : output.getParent();
                if (tempParent == null) {
                    throw new IllegalArgumentException("external benchmark output has no sibling temp parent: "
                            + output);
                }
                root = Files.createTempDirectory(tempParent, "swath-parallel-merge-bench-");
            }
            BenchContext context = new BenchContext(corpus, gitSha());
            bench(context, "BENCH_ROOT", NO_FINGERPRINT, "path=" + root);
            bench(context, "BENCH_ARM", NO_FINGERPRINT, "listing_fetches=0");
            bench(context, "BENCH_HEAP", NO_FINGERPRINT,
                    String.format("max_memory_mb=%.1f available_processors=%d",
                            Runtime.getRuntime().maxMemory() / (1024.0 * 1024.0),
                            Runtime.getRuntime().availableProcessors()));
            bench(context, "BENCH_CORPUS", NO_FINGERPRINT, corpus.describe());
            bench(context, "BENCH_INPUT_ORACLE", NO_FINGERPRINT,
                    "rows=" + corpus.oracle().rows() + " trailer_entries="
                            + corpus.oracle().trailerEntries() + " trailer_records="
                            + corpus.oracle().trailerRecords() + " multiset_digest="
                            + corpus.oracle().multisetDigest());

            measureOpenReaderHeap(corpus, context);

            List<Integer> ranges = RANGES;
            List<Integer> candidates = ranges.stream().filter(range -> range != 1).toList();
            WriterFactoryProvider writerProvider =
                    config -> new SortedParquetWriterFactory(config, SortMode.OBJECTS);

            // Prime class loading, JIT compilation, Parquet writer initialization, and the RSS sampler
            // with a complete transform whose timing is deliberately discarded.
            ArmResult warmup = runArm(root, corpus, 1, "warmup-r1", writerProvider);
            bench(context, "BENCH_WARMUP", warmup.logicalOutputFingerprint,
                    "requested_r=1 actual_ranges=" + warmup.actualRanges + " rows=" + warmup.totalRows
                            + " elapsed_ignored=true");
            SortBenchCorpus.deleteTree(warmup.armRoot);

            Map<Integer, List<ArmResult>> samples = new LinkedHashMap<>();
            for (int range : ranges) {
                samples.put(range, new ArrayList<>());
            }

            // Round A: serial bracket, then candidates in ascending order.
            ArmResult baseline = runArm(root, corpus, 1, "r1-a", writerProvider);
            samples.get(1).add(baseline);
            System.out.println(baseline.toLine(context));
            for (int r : candidates) {
                ArmResult sample = runCheckedArm(root, corpus, r, "r" + r + "-a",
                        writerProvider, baseline, context);
                samples.get(r).add(sample);
                System.out.println(sample.toLine(context));
                SortBenchCorpus.deleteTree(sample.armRoot);
            }

            // Middle serial bracket.
            ArmResult middle = runCheckedArm(root, corpus, 1, "r1-b",
                    writerProvider, baseline, context);
            samples.get(1).add(middle);
            System.out.println(middle.toLine(context));
            SortBenchCorpus.deleteTree(middle.armRoot);

            // Round B reverses candidate order to counter monotone temperature/JIT drift.
            for (int i = candidates.size() - 1; i >= 0; i--) {
                int r = candidates.get(i);
                ArmResult sample = runCheckedArm(root, corpus, r, "r" + r + "-b",
                        writerProvider, baseline, context);
                samples.get(r).add(sample);
                System.out.println(sample.toLine(context));
                SortBenchCorpus.deleteTree(sample.armRoot);
            }

            // Closing serial bracket.
            ArmResult closing = runCheckedArm(root, corpus, 1, "r1-c",
                    writerProvider, baseline, context);
            samples.get(1).add(closing);
            System.out.println(closing.toLine(context));
            SortBenchCorpus.deleteTree(closing.armRoot);

            reportMeasurements(context, samples);
            SortBenchCorpus.deleteTree(baseline.armRoot);
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            IOException immutableFailure = null;
            if (corpus != null) {
                try {
                    corpus.verifyMasterUnchanged();
                } catch (IOException e) {
                    immutableFailure = e;
                }
            }
            if (root != null) {
                SortBenchCorpus.deleteTree(root);
            }
            if (immutableFailure != null) {
                if (failure != null) {
                    failure.addSuppressed(immutableFailure);
                } else {
                    throw immutableFailure;
                }
            }
        }
    }

    record PreparedGenerated(Path root, CorpusCatalog catalog) {
    }

    static PreparedGenerated prepareGenerated(Path parent, int segments, long rows,
                                              int blockRows, int pageRows) throws IOException {
        Path root = parent == null
                ? Files.createTempDirectory("swath-parallel-merge-bench-")
                : Files.createTempDirectory(parent, "swath-parallel-merge-bench-");
        boolean complete = false;
        try {
            Path master = Files.createDirectory(root.resolve("master"));
            long started = System.nanoTime();
            SortBenchCorpus.Stats stats = buildCorpus(
                    master, segments, rows, blockRows, pageRows);
            CorpusCatalog catalog = snapshotCatalog(
                    "generated", master, SortBenchCorpus.pageRunSegments(master))
                    .withBuildMillis((System.nanoTime() - started) / 1_000_000L)
                    .withGeneratedStats(stats);
            if (stats.rows() != catalog.oracle().rows() || stats.segments() != catalog.inputs().size()) {
                throw new IOException("generated corpus statistics disagree with validated input oracle");
            }
            complete = true;
            return new PreparedGenerated(root, catalog);
        } finally {
            if (!complete) {
                SortBenchCorpus.deleteTree(root);
            }
        }
    }

    // =====================================================================
    // Per-arm execution
    // =====================================================================

    static ArmResult runArm(Path root, CorpusCatalog corpus, int mergeParallelism, String label,
                            WriterFactoryProvider writerProvider) throws IOException {
        Throwable failure = null;
        try {
            return runArmBody(root, corpus, mergeParallelism, label, writerProvider);
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                corpus.verifyMasterUnchanged();
            } catch (IOException immutableFailure) {
                if (failure != null) {
                    failure.addSuppressed(immutableFailure);
                } else {
                    throw immutableFailure;
                }
            }
        }
    }

    private static ArmResult runCheckedArm(Path root, CorpusCatalog corpus, int mergeParallelism,
            String label, WriterFactoryProvider writerProvider, ArmResult baseline,
            BenchContext context) throws IOException {
        ArmResult sample = runArm(root, corpus, mergeParallelism, label, writerProvider);
        boolean exact = fullRowsEqual(baseline.finalFiles, sample.finalFiles)
                && baseline.logicalOutputFingerprint.equals(sample.logicalOutputFingerprint)
                && baseline.multisetDigest.equals(sample.multisetDigest);
        sample.fullRowExact = exact;
        if (!exact) {
            bench(context, "BENCH_FULL_ROW_EXACT_FAIL", sample.logicalOutputFingerprint,
                    "requested_r=" + mergeParallelism + " actual_ranges=" + sample.actualRanges
                            + " baseline_fingerprint=" + baseline.logicalOutputFingerprint);
            throw new AssertionError("full-row mismatch at requested R=" + mergeParallelism
                    + " (actual ranges=" + sample.actualRanges
                    + ") vs the R=1 baseline — silent data loss");
        }
        return sample;
    }

    private static void reportMeasurements(BenchContext context,
                                           Map<Integer, List<ArmResult>> samples) {
        Map<Integer, SampleStats> stats = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<ArmResult>> entry : samples.entrySet()) {
            SampleStats sampleStats = sampleStats(entry.getValue());
            stats.put(entry.getKey(), sampleStats);
            ArmResult first = entry.getValue().getFirst();
            bench(context, "BENCH_VARIANCE", first.logicalOutputFingerprint,
                    String.format("requested_r=%d samples=%d median_elapsed_ms=%d min_elapsed_ms=%d "
                                    + "max_elapsed_ms=%d spread_pct=%.1f threshold_pct=%.1f status=%s",
                            entry.getKey(), entry.getValue().size(), sampleStats.medianNanos / 1_000_000,
                            sampleStats.minNanos / 1_000_000, sampleStats.maxNanos / 1_000_000,
                            sampleStats.spreadPct, MAX_VARIANCE_PCT,
                            sampleStats.stable(MAX_VARIANCE_PCT) ? "stable" : "invalid_variance"));
        }

        SampleStats baseline = stats.get(1);
        ArmResult baselineArm = samples.get(1).getFirst();
        boolean baselineStable = baseline.stable(MAX_VARIANCE_PCT);
        bench(context, "BENCH_SPEEDUP", baselineArm.logicalOutputFingerprint,
                String.format("requested_r=1 actual_ranges=1 status=%s speedup=%s "
                                + "baseline_median_ms=%d baseline_spread_pct=%.1f",
                        baselineStable ? "baseline" : "invalid_variance",
                        baselineStable ? "1.000" : "unavailable",
                        baseline.medianNanos / 1_000_000, baseline.spreadPct));

        for (Map.Entry<Integer, List<ArmResult>> entry : samples.entrySet()) {
            int requested = entry.getKey();
            if (requested == 1) {
                continue;
            }
            List<ArmResult> arms = entry.getValue();
            ArmResult first = arms.getFirst();
            SampleStats candidate = stats.get(requested);
            ArmDisposition disposition = ArmDisposition.of(first);
            boolean consistent = arms.stream().allMatch(arm -> disposition.equals(ArmDisposition.of(arm)));
            String engagementStatus = !consistent ? "inconsistent"
                    : disposition.engaged(requested) ? "engaged"
                    : disposition.actualRanges <= 1 ? "not_engaged" : "clamped_or_reduced";
            String status;
            String speedup = "unavailable";
            if (!consistent) {
                status = "invalid_inconsistent_disposition";
            } else if (!baselineStable || !candidate.stable(MAX_VARIANCE_PCT)) {
                status = "invalid_variance";
            } else if (!disposition.engaged(requested)) {
                status = engagementStatus;
            } else {
                status = "engaged";
                speedup = String.format("%.3f", (double) baseline.medianNanos / candidate.medianNanos);
            }
            String actualRanges = consistent ? String.valueOf(disposition.actualRanges) : "mixed";
            bench(context, "BENCH_SPEEDUP", first.logicalOutputFingerprint,
                    String.format("requested_r=%d actual_ranges=%s status=%s engagement_status=%s speedup=%s "
                                    + "baseline_median_ms=%d candidate_median_ms=%d "
                                    + "baseline_spread_pct=%.1f candidate_spread_pct=%.1f",
                            requested, actualRanges, status, engagementStatus, speedup,
                            baseline.medianNanos / 1_000_000, candidate.medianNanos / 1_000_000,
                            baseline.spreadPct, candidate.spreadPct));
        }
    }

    static SampleStats sampleStats(List<ArmResult> samples) {
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("benchmark sample set must not be empty");
        }
        List<Long> elapsed = samples.stream().map(sample -> {
            if (sample.elapsedNanos <= 0) {
                throw new IllegalArgumentException("benchmark elapsed time must be positive");
            }
            return sample.elapsedNanos;
        }).sorted().toList();
        long min = elapsed.getFirst();
        long max = elapsed.getLast();
        int middle = elapsed.size() / 2;
        long median = elapsed.size() % 2 == 1
                ? elapsed.get(middle)
                : elapsed.get(middle - 1) + (elapsed.get(middle) - elapsed.get(middle - 1)) / 2;
        double spreadPct = 100.0 * (max - min) / Math.max(1.0, median);
        return new SampleStats(min, median, max, spreadPct);
    }

    record SampleStats(long minNanos, long medianNanos, long maxNanos, double spreadPct) {
        boolean stable(double thresholdPct) {
            return spreadPct <= thresholdPct;
        }
    }

    private record ArmDisposition(long actualRanges, long belowStagedFloor, long fdLimited,
                                  long fdExhausted, long wouldCascade, long unsplittable) {
        static ArmDisposition of(ArmResult arm) {
            return new ArmDisposition(arm.actualRanges, arm.rangeBelowStagedFloorCount,
                    arm.rangeFdLimitedCount, arm.rangeFdExhaustedCount,
                    arm.rangeWouldCascadeCount, arm.rangeUnsplittableCount);
        }

        boolean engaged(int requestedRanges) {
            return actualRanges == requestedRanges && belowStagedFloor == 0 && fdLimited == 0
                    && fdExhausted == 0 && wouldCascade == 0 && unsplittable == 0;
        }
    }

    private static ArmResult runArmBody(Path root, CorpusCatalog corpus, int mergeParallelism,
                                        String label, WriterFactoryProvider writerProvider) throws IOException {
        Path armRoot = Files.createDirectory(root.resolve("arm-" + label));
        Path output = Files.createDirectory(armRoot.resolve("data"));
        Path staging = Files.createDirectory(armRoot.resolve("_staging"));
        List<Path> stagingSegments = corpus.materialize(staging);

        // merge-parallelism is the swept knob; every OTHER swath.sort.* property falls through to the
        // real system properties, so an arm can hold one knob fixed while another varies (e.g. pinning
        // merge-budget-bytes while -Xmx changes, to separate cascade removal from GC relief).
        SortConfig config = SortConfig.fromProperties(
                key -> "swath.sort.merge-parallelism".equals(key)
                        ? String.valueOf(mergeParallelism)
                        : System.getProperty(key));
        ThreadSafeMetrics metrics = new ThreadSafeMetrics();
        BenchRangeTimer timer = new BenchRangeTimer();
        SortedFileWriterFactory writerFactory = writerProvider.create(config);
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
        samplerThread.start();
        long cpuStartNanos = SortBenchCorpus.processCpuTimeNanos();
        long wallStartNanos = System.nanoTime();
        long wallEndNanos;
        long cpuEndNanos;
        SortTransformResult result;
        try {
            result = transform.transform(stagingSegments, output, staging, PublishListener.NO_OP,
                    units -> { }, FinalPassListener.NO_OP);
        } finally {
            wallEndNanos = System.nanoTime();
            cpuEndNanos = SortBenchCorpus.processCpuTimeNanos();
            long samplerStopStart = System.nanoTime();
            samplerThread.interrupt();
            try {
                samplerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sampler.cleanupNanos = System.nanoTime() - samplerStopStart;
        }

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
        BenchmarkRowOracle.OutputValidation outputValidation =
                BenchmarkRowOracle.validateOutput(result.finalFiles(), corpus.oracle(), CMP);
        ar.logicalOutputFingerprint = outputValidation.orderedFingerprint();
        ar.multisetDigest = outputValidation.multisetDigest();
        if (result.totalRows() != outputValidation.rows()) {
            throw new IOException("SortTransform row count disagrees with validated output");
        }
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
        ar.proofSpoolLogicalExtentBytes = metrics.proofSpoolLogicalExtentBytes.sum();
        ar.proofSpoolPreallocationOperations = metrics.proofSpoolPreallocationOperations.sum();
        ar.proofSpoolPreallocationAttemptedBytes =
                metrics.proofSpoolPreallocationAttemptedBytes.sum();
        ar.proofSpoolMappedOperations = metrics.proofSpoolMappedOperations.sum();
        ar.proofSpoolMappedBytes = metrics.proofSpoolMappedBytes.sum();
        ar.proofSpoolServiceNanos = metrics.proofSpoolServiceNanos.sum();
        ar.boundaryNanos = timer.boundaryNanos;
        ar.rangeLatenciesNanos = timer.rangeLatenciesNanos.stream().toList();
        ar.samplerCleanupNanos = sampler.cleanupNanos;
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
    private static void measureOpenReaderHeap(CorpusCatalog corpus, BenchContext context) throws IOException {
        List<Path> segments = corpus.paths();
        long before = settledHeapBytes();
        List<PageFrontierReader> open = new ArrayList<>(segments.size());
        try {
            for (Path segment : segments) {
                PageFrontierReader reader = new PageFrontierReader(segment, SortMetrics.NO_OP);
                open.add(reader);
            }
            long after = settledHeapBytes();
            long delta = after - before;
            bench(context, "BENCH_STREAM_HEAP", NO_FINGERPRINT,
                    String.format("staging_format=page-run open_frontiers=%d retained_bytes=%d "
                                    + "per_frontier_bytes=%d per_frontier_mb=%.2f",
                            open.size(), delta, delta / Math.max(1, open.size()),
                            delta / (1024.0 * 1024.0) / Math.max(1, open.size())));
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

    static CorpusCatalog externalStaging() throws IOException {
        String configured = System.getProperty(EXTERNAL_STAGING_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return null;
        }
        Path staging = Path.of(configured).toAbsolutePath().normalize();
        if (!Files.isDirectory(staging)) {
            throw new IllegalArgumentException("swath.bench.staging-dir must name a directory: " + staging);
        }
        Path output = staging.getParent();
        if (output == null) {
            throw new IllegalArgumentException("swath.bench.staging-dir has no dataset parent: " + staging);
        }
        BenchmarkMasterSnapshot master = BenchmarkMasterSnapshot.capture(output);
        boolean complete = false;
        try {
            if (!Files.isRegularFile(output.resolve(Manifest.STATE_FILE_NAME),
                    java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("external staging requires a regular .swath-state.json: "
                        + output);
            }
            Manifest.Identity identity = Manifest.readIdentity(output)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "external staging requires the dataset .swath-state.json run identity: " + output));
            BenchmarkCheckpointCatalog.Authority authority =
                    BenchmarkCheckpointCatalog.read(output, staging, identity);
            List<BenchmarkRowOracle.SourceSegment> sources = authority.segments().stream()
                    .map(segment -> new BenchmarkRowOracle.SourceSegment(
                            segment.path(), segment.rows(), segment.bytes()))
                    .toList();
            CorpusCatalog result = snapshotCatalog("checkpoint", staging, sources,
                    authority.runId(), authority.argsHash(), master);
            complete = true;
            return result;
        } finally {
            if (!complete) {
                master.verifyUnchanged();
            }
        }
    }

    static CorpusCatalog snapshotCatalog(String source, Path staging, List<Path> inputs) throws IOException {
        List<BenchmarkRowOracle.SourceSegment> sources = new ArrayList<>();
        for (Path input : inputs) {
            try (PageRunSegmentIo io = PageRunSegmentIo.open(input, SortMetrics.NO_OP)) {
                PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
                sources.add(new BenchmarkRowOracle.SourceSegment(
                        input, trailer.totalEntries(), Files.size(input)));
            }
        }
        return snapshotCatalog(source, staging, sources, -1, "not_applicable", null);
    }

    private static CorpusCatalog snapshotCatalog(String source, Path staging,
            List<BenchmarkRowOracle.SourceSegment> sources, long runId, String argsHash,
            BenchmarkMasterSnapshot masterSnapshot) throws IOException {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException(source + " corpus contains no page-run inputs: " + staging);
        }
        List<CorpusInput> catalog = new ArrayList<>(sources.size());
        Set<Path> tracked = new HashSet<>();
        for (BenchmarkRowOracle.SourceSegment sourceSegment : sources) {
            Path normalized = sourceSegment.path().toAbsolutePath().normalize();
            if (!normalized.getParent().equals(staging)) {
                throw new IllegalArgumentException("checkpoint catalog path escapes staging directory: "
                        + sourceSegment.path());
            }
            if (!normalized.getFileName().toString().endsWith(StagingNames.PAGE_RUN_SUFFIX)) {
                throw new IllegalArgumentException("checkpoint catalog entry is not a page-run segment: "
                        + sourceSegment.path());
            }
            if (!tracked.add(normalized)) {
                throw new IllegalArgumentException("checkpoint catalog repeats staging segment: "
                        + sourceSegment.path());
            }
            catalog.add(CorpusInput.capture(normalized, sourceSegment.expectedRows(),
                    sourceSegment.expectedBytes()));
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(staging, "*" + StagingNames.PAGE_RUN_SUFFIX)) {
            for (Path entry : entries) {
                if (!tracked.contains(entry.toAbsolutePath().normalize())) {
                    throw new IllegalArgumentException("staging contains an untracked page-run segment (stale merge or"
                            + " fixture debris): " + entry);
                }
            }
        }
        catalog.sort(Comparator.comparing(input -> input.path().getFileName().toString()));
        List<BenchmarkRowOracle.SourceSegment> orderedSources = catalog.stream()
                .map(input -> new BenchmarkRowOracle.SourceSegment(
                        input.path(), input.rows(), input.size()))
                .toList();
        BenchmarkRowOracle.InputOracle oracle = BenchmarkRowOracle.readInputs(orderedSources, CMP);
        String identity = catalogIdentity(catalog, oracle);
        return new CorpusCatalog(source, staging, List.copyOf(catalog), identity, 0L, null,
                oracle, masterSnapshot, runId, argsHash);
    }

    record CorpusCatalog(String source, Path stagingDir, List<CorpusInput> inputs, String identity,
                         long buildMillis, SortBenchCorpus.Stats generatedStats,
                         BenchmarkRowOracle.InputOracle oracle,
                         BenchmarkMasterSnapshot masterSnapshot, long runId, String argsHash) {
        CorpusCatalog withBuildMillis(long buildMillis) {
            return new CorpusCatalog(source, stagingDir, inputs, identity, buildMillis,
                    generatedStats, oracle, masterSnapshot, runId, argsHash);
        }

        CorpusCatalog withGeneratedStats(SortBenchCorpus.Stats generatedStats) {
            return new CorpusCatalog(source, stagingDir, inputs, identity, buildMillis,
                    generatedStats, oracle, masterSnapshot, runId, argsHash);
        }

        List<Path> paths() {
            return inputs.stream().map(CorpusInput::path).toList();
        }

        List<Path> materialize(Path target) throws IOException {
            for (CorpusInput input : inputs) {
                input.verifyUnchanged();
            }
            return SortBenchCorpus.hardLinkCorpus(paths(), target);
        }

        void verifyMasterUnchanged() throws IOException {
            for (CorpusInput input : inputs) {
                input.verifyUnchanged();
            }
            if (masterSnapshot != null) {
                masterSnapshot.verifyUnchanged();
            }
        }

        String describe() {
            long bytes = inputs.stream().mapToLong(CorpusInput::size).sum();
            String generated = generatedStats == null
                    ? "rows=unknown"
                    : "rows=" + generatedStats.rows();
            return "staging_format=page-run segments=" + inputs.size() + " bytes=" + bytes + " "
                    + generated + " build_ms=" + buildMillis;
        }
    }

    record CorpusInput(Path path, long rows, long size, java.nio.file.attribute.FileTime modified,
                       Object fileKey, String sha256) {
        static CorpusInput capture(Path path, long rows, long expectedBytes) throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (attributes.size() != expectedBytes) {
                throw new IOException("catalog bytes changed before snapshot: " + path);
            }
            return new CorpusInput(path, rows, attributes.size(), attributes.lastModifiedTime(),
                    attributes.fileKey(), fileSha256(path));
        }

        void verifyUnchanged() throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (attributes.size() != size || !attributes.lastModifiedTime().equals(modified)
                    || !java.util.Objects.equals(attributes.fileKey(), fileKey)
                    || !fileSha256(path).equals(sha256)) {
                throw new IOException("benchmark corpus changed after catalog snapshot: " + path);
            }
        }
    }

    record BenchContext(CorpusCatalog corpus, String gitSha) {
        String tags(String fingerprint) {
            return "arm=" + ARM + " source=" + corpus.source() + " corpus_id=" + corpus.identity()
                    + " git_sha=" + gitSha + " run_id=" + corpus.runId()
                    + " args_hash=" + corpus.argsHash() + " cache_state=warm_primed"
                    + " logical_output_fingerprint=" + fingerprint;
        }
    }

    private static void bench(BenchContext context, String event, String fingerprint, String fields) {
        System.out.println(benchLine(context, event, fingerprint, fields));
    }

    static String benchLine(BenchContext context, String event, String fingerprint, String fields) {
        return event + " " + context.tags(fingerprint) + " " + fields;
    }

    private static String catalogIdentity(List<CorpusInput> inputs,
                                          BenchmarkRowOracle.InputOracle oracle) {
        MessageDigest digest = sha256();
        updateString(digest, "page-run-benchmark-corpus-v2");
        updateLong(digest, oracle.rows());
        updateLong(digest, oracle.trailerEntries());
        updateLong(digest, oracle.trailerRecords());
        updateString(digest, oracle.multisetDigest());
        for (CorpusInput input : inputs) {
            updateString(digest, input.path().getFileName().toString());
            updateLong(digest, input.rows());
            updateLong(digest, input.size());
            updateString(digest, input.sha256());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String fileSha256(Path path) throws IOException {
        MessageDigest digest = sha256();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = Files.newInputStream(path)) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is required by the JDK", e);
        }
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static void updateString(MessageDigest digest, String value) {
        if (value == null) {
            updateLong(digest, -1L);
            return;
        }
        updateBytes(digest, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void updateBytes(MessageDigest digest, byte[] bytes) {
        updateLong(digest, bytes.length);
        digest.update(bytes);
    }

    private static String gitSha() {
        String configured = System.getProperty("swath.git.sha");
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        try {
            Process git = new ProcessBuilder("git", "rev-parse", "HEAD").start();
            if (git.waitFor(2, TimeUnit.SECONDS) && git.exitValue() == 0) {
                return new String(git.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            }
            git.destroyForcibly();
        } catch (IOException e) {
            // A source archive can still run the harness; its output labels the unavailable revision.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "unknown";
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

    private static SortBenchCorpus.Stats buildCorpus(Path master, int numSegments, long totalRows,
                                                      int blockRows, int pageRows) throws IOException {
        if (pageRows <= 0) {
            throw new IllegalArgumentException("swath.bench.pageRows must be > 0, got " + pageRows);
        }
        SortConfig config = SortConfig.fromSystemProperties();
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, config.segmentCodec());
        long rowsPerDay = Math.max(1, totalRows / TOTAL_DAYS);
        LocalDate base = LocalDate.of(2019, 1, 1);
        int totalSegments = 0;
        long accumulatedRows = 0;
        long totalBytes = 0;
        for (int seg = 0; seg < numSegments; seg++) {
            SortBuffer buffer = new SortBuffer(config, CMP);
            try (SortedCursor cursor =
                         SortBenchCorpus.generatedCursor(
                                 seg, numSegments, blockRows, totalRows, rowsPerDay, base)) {
                List<ListEntry> page = new ArrayList<>(pageRows);
                long nodeId = 0;
                while (cursor.hasNext()) {
                    page.add(cursor.next());
                    if (page.size() == pageRows) {
                        buffer.admit(nodeId++, page);
                        page = new ArrayList<>(pageRows);
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
            accumulatedRows += result.rows();
            totalBytes += result.bytes();
        }
        return new SortBenchCorpus.Stats(totalSegments, accumulatedRows, totalBytes);
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
                    return false;
                }
                row++;
            }
            if (a.hasNext() != b.hasNext()) {
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
        volatile long cleanupNanos;

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

    @FunctionalInterface
    interface WriterFactoryProvider {
        SortedFileWriterFactory create(SortConfig config);
    }

    static final class ArmResult {
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
        String logicalOutputFingerprint;
        String multisetDigest;
        long samplerCleanupNanos;
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
        long proofSpoolLogicalExtentBytes;
        long proofSpoolPreallocationOperations;
        long proofSpoolPreallocationAttemptedBytes;
        long proofSpoolMappedOperations;
        long proofSpoolMappedBytes;
        long proofSpoolServiceNanos;
        long boundaryNanos;
        List<Long> rangeLatenciesNanos;
        boolean fullRowExact = true;   // R=1 baseline is trivially exact against itself

        String toLine(BenchContext context) {
            long rangeMergeSumNanos = rangeLatenciesNanos.stream().mapToLong(Long::longValue).sum();
            String boundaryMs = boundaryNanos < 0 ? "unavailable" : String.valueOf(boundaryNanos / 1_000_000);
            String rangeMergeSumMs = rangeLatenciesNanos.isEmpty()
                    ? "unavailable"
                    : String.valueOf(rangeMergeSumNanos / 1_000_000);
            List<Long> rangeLatenciesMs =
                    rangeLatenciesNanos.stream().map(n -> n / 1_000_000L).toList();
            return String.format(
                    "BENCH_ROW %s label=%s staging_format=page-run requested_r=%d actual_ranges=%d "
                            + "merge_elapsed_ms=%d boundary_ms=%s range_merge_sum_ms=%s "
                            + "avg_cores_busy=%.2f peak_heap_mb=%.1f peak_rss_mb=%.1f "
                            + "rows=%d input_segments=%d output_files=%d merge_passes=%d "
                            + "cascaded_passes=%d fastpath_emissions=%d range_parallel_count=%d "
                            + "range_below_staged_floor_count=%d range_fd_limited_count=%d "
                            + "range_fd_exhausted_count=%d "
                            + "range_would_cascade_count=%d range_unsplittable_count=%d "
                            + "page_skip_engaged_ranges=%d "
                            + "sample_capped_segments=%d page_whole_emissions=%d "
                            + "page_overlap_keymerges=%d proof_spool_logical_extent_bytes=%d "
                            + "proof_spool_preallocation_operations=%d "
                            + "proof_spool_preallocation_attempted_bytes=%d "
                            + "proof_spool_mapped_operations=%d proof_spool_mapped_bytes=%d "
                            + "proof_spool_ms=%d page_reads=unavailable "
                            + "read_amplification=unavailable identity_check=full-row "
                            + "full_row_exact=%s multiset_digest=%s sampler_cleanup_ms=%d "
                            + "range_latencies_ms=%s",
                    context.tags(logicalOutputFingerprint), label, requestedRanges, actualRanges,
                    elapsedNanos / 1_000_000, boundaryMs,
                    rangeMergeSumMs, avgCoresBusy, peakHeapBytes / (1024.0 * 1024.0),
                    peakRssBytes / (1024.0 * 1024.0), totalRows, inputSegments, finalFiles.size(),
                    mergePasses, cascadedPasses, fastPathEmissions, rangeParallelCount,
                    rangeBelowStagedFloorCount, rangeFdLimitedCount, rangeFdExhaustedCount,
                    rangeWouldCascadeCount, rangeUnsplittableCount, pageSkipEngagedCount,
                    sampleCappedSegments, pageWholeEmissions,
                    pageOverlapKeyMerges, proofSpoolLogicalExtentBytes,
                    proofSpoolPreallocationOperations, proofSpoolPreallocationAttemptedBytes,
                    proofSpoolMappedOperations, proofSpoolMappedBytes,
                    proofSpoolServiceNanos / 1_000_000L, fullRowExact, multisetDigest,
                    samplerCleanupNanos / 1_000_000, rangeLatenciesMs);
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
        private final LongAdder proofSpoolLogicalExtentBytes = new LongAdder();
        private final LongAdder proofSpoolPreallocationOperations = new LongAdder();
        private final LongAdder proofSpoolPreallocationAttemptedBytes = new LongAdder();
        private final LongAdder proofSpoolMappedOperations = new LongAdder();
        private final LongAdder proofSpoolMappedBytes = new LongAdder();
        private final LongAdder proofSpoolServiceNanos = new LongAdder();

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

        @Override
        public void recordPageAwareOverlapCluster() {
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        }

        @Override
        public void recordRangeIndexBytes(long bytes) {
        }

        @Override
        public void recordProofSpool(long logicalExtentBytes,
                                     long preallocationOperations,
                                     long preallocationAttemptedBytes,
                                     long mappedOperations,
                                     long mappedBytes,
                                     long serviceNanos) {
            proofSpoolLogicalExtentBytes.add(logicalExtentBytes);
            proofSpoolPreallocationOperations.add(preallocationOperations);
            proofSpoolPreallocationAttemptedBytes.add(preallocationAttemptedBytes);
            proofSpoolMappedOperations.add(mappedOperations);
            proofSpoolMappedBytes.add(mappedBytes);
            proofSpoolServiceNanos.add(serviceNanos);
        }

        long count(String key) {
            LongAdder a = counts.get(key);
            return a == null ? 0 : a.sum();
        }
    }
}
