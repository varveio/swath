/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.IntRange;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.slf4j.LoggerFactory;

/**
 * PROP guard for the parallel range merge over <b>page-run</b> staging — the live listing lane's
 * format, and therefore the only one that decides whether {@code swath.sort.merge-parallelism}
 * does anything on a real run.
 *
 * The suite exercises every mechanism that could lose a row: page skipping through {@link
 * RangeScopedPageFrontier}, {@link PageAwareMerger}'s decode-free page-whole fast path, the {@code
 * [lo, hi)} trim above the merge ({@link RangeFilteredCursor}), and page-run cascade intermediates.
 *
 * <p>The core property is the same and is checked against TWO independent oracles: byte-exact
 * equivalence to the serial merge of the same segments, and the exact input multiset (so two
 * symmetric bugs that both drop the same row cannot pass).
 */
class SortTransformPageRunParallelMergePropTest {

    // Contracts §7.2's documented Parquet characterization budget. This process/merge stress has
    // eight merge ranges and deliberately keeps rolled writers open, so it is not a new production
    // envelope or an attribution of this heap to final writers alone.
    private static final long PROCESS_MERGE_STRESS_HEAP_BUDGET_BYTES = 1L << 30;
    private static final long PROCESS_MERGE_PERF_WORKER_HEAP_BYTES = 2L << 30;
    private static final int MAX_GC_ATTEMPTS = 50;

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** Adversarial key shapes: heavy duplicate runs, tiny alphabets, and S3 byte-order extremes. */
    enum KeyStyle {
        DENSE_SEQUENTIAL,
        SMALL_ALPHABET,
        CLUSTERED,
        BINARY_ADVERSARIAL
    }

    private static final byte[][] BIN_POOL = {
            new byte[]{},
            new byte[]{0},
            new byte[]{0, 0},
            new byte[]{0, (byte) 0xFF},
            new byte[]{(byte) 0xFF},
            new byte[]{(byte) 0xFF, 0},
            new byte[]{(byte) 0xFF, (byte) 0xFF},
    };

    // ---------------------------------------------------------------------
    // Core property
    // ---------------------------------------------------------------------

    @Property(tries = 120)
    void pageRunParallelConcatenationIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(build(segmentCount, entryCount, style, seed), ranges,
                Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * The same property with each range forced to CASCADE (a 1-byte merge budget pins every range at
     * the {@code max(2, …)} fan-in floor). This is the arm that exercises
     * {@code writeIntermediate}'s page-run cascade output and the re-scoping of an intermediate that
     * is already range-filtered — neither of which the single-pass property reaches.
     */
    @Property(tries = 60)
    void cascadingPageRunRangesAreStillByteExactToSerial(
            @ForAll @IntRange(min = 3, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        Scenario s = build(segmentCount, entryCount, style, seed);
        Path root = Files.createTempDirectory("prange-pagerun-cascade-");
        try {
            SortTransformResult serial = run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE,
                    DuplicateHook.NO_OP, SortMetrics.NO_OP);
            // Direct, not through SortTransform: the clamp reduces R to 1 on this budget and hands
            // back to serial, so going through it would compare serial against serial and say nothing
            // about the page-run cascade branches -- which write .pageseg intermediates and are the
            // format-specific half of this path.
            CascadeRun parallel = runParallelUnclamped(s, ranges, root, "parallel", 1L);
            if (parallel == null) {
                return;   // unsplittable keyspace
            }

            assertThat(parallel.cascadedPasses())
                    .as("the ranges actually cascaded — otherwise this test proves nothing")
                    .isGreaterThan(0);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> parallelRows = readAll(parallel.parts());
            assertThat(parallel.rows()).isEqualTo(input.size());
            assertThat(parallelRows).as("exact input multiset")
                    .containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("position-for-position equal to serial")
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    /**
     * Drive {@link ParallelRangeMerge} directly, past {@link SortTransform}'s clamp, so the cascade
     * branches are genuinely reached. Cascade intermediates keep the page-run format, which is what
     * makes this worth exercising separately from the shallow single-page cases.
     */
    private CascadeRun runParallelUnclamped(Scenario s, int ranges, Path root, String name,
                                            long mergeBudgetBytes) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segs = stage(staging, s.segments());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(Long.MAX_VALUE)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withMergeParallelism(ranges);
        SortRun run = sortRun(config, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT, SortRun.PROCESS_SOFT_FD_LIMIT);
        ParallelRangeMerge merge = new ParallelRangeMerge(run);
        ParallelKickoff kickoff = parallelKickoff(segs);
        List<PageRunSegmentDescriptor> descriptors = kickoff.descriptors();
        List<byte[]> boundaries = ParallelRangeMerge.boundaries(
                descriptors, kickoff.candidates(), ranges, SortMetrics.NO_OP);
        if (boundaries == null) {
            return null;
        }
        List<ParallelRangeMerge.RangeResult> results =
                merge.run(descriptors, staging, boundaries, units -> { });

        List<Path> parts = new ArrayList<>();
        List<SortedFileWriter> writers = new ArrayList<>();
        long cascaded = 0;
        long rows = 0;
        for (ParallelRangeMerge.RangeResult rr : results) {
            parts.addAll(rr.tmpParts());
            writers.addAll(rr.writers());
            cascaded += rr.cascadedPasses();
            rows += rr.rows();
        }
        for (int i = 0; i < writers.size(); i++) {
            writers.get(i).setFileIndex(i + 1);
        }
        if (!writers.isEmpty()) {
            writers.get(writers.size() - 1).markFinal();
        }
        RolledPartWriter.closeInOrder(writers);
        return new CascadeRun(parts, rows, cascaded);
    }

    private static ParallelKickoff parallelKickoff(List<Path> paths) throws IOException {
        ParallelRangeMerge.BoundaryCandidates candidates =
                new ParallelRangeMerge.BoundaryCandidates();
        List<PageRunSegmentDescriptor> descriptors = PageRunSegmentDescriptor.readAll(paths,
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(candidates::add));
        return new ParallelKickoff(descriptors, candidates);
    }

    private static List<PageRunSegmentDescriptor> descriptorTrailers(List<Path> paths)
            throws IOException {
        return PageRunSegmentDescriptor.readAll(paths,
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.empty());
    }

    private record ParallelKickoff(List<PageRunSegmentDescriptor> descriptors,
                                   ParallelRangeMerge.BoundaryCandidates candidates) {
    }

    private record WideStressStaging(ParallelKickoff kickoff, List<byte[]> boundaries,
                                     WeakReference<Scenario> fixture) {
    }

    private record CascadeRun(List<Path> parts, long rows, long cascadedPasses) {
    }

    /**
     * Rolled output: each range also splits into several parts. Stacks intra-range file boundaries on
     * top of the inter-range ones — the whole {@code part-00000..part-(N-1)} sequence must still be the exact
     * global sort.
     */
    @Property(tries = 30)
    void rolledPageRunParallelOutputIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 5) int segmentCount,
            @ForAll @IntRange(min = 1, max = 120) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 5) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(build(segmentCount, entryCount, style, seed), ranges, 4096L,
                Long.MAX_VALUE);
    }

    @Example
    void serialPageRunRollingKeepsCrossSegmentVersionClustersKeyAtomic() throws IOException {
        Scenario scenario = versionClustersAcrossSegments();
        Path root = Files.createTempDirectory("serial-pagerun-key-atomic-roll-");
        try {
            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult serial = run(scenario, 1, root, "serial", 1L, Long.MAX_VALUE,
                    DuplicateHook.NO_OP, metrics);

            List<ListEntry> rows = readAll(serial.finalFiles());
            assertThat(metrics.count("SORT.final_roll_equal_key_deferred"))
                    .as("one bounded deferral signal per oversized key group")
                    .isEqualTo(4);
            assertThat(serial.finalFiles()).hasSize(4);
            assertKeyAtomicAndStrictlyDisjoint(serial.finalFiles());
            assertThat(rows).isSortedAccordingTo(cmp);
            assertThat(rows).containsExactlyInAnyOrderElementsOf(scenario.allEntries());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void parallelPageRunRollingKeepsCrossSegmentVersionClustersKeyAtomic() throws IOException {
        int ranges = 4;
        Scenario scenario = versionClustersAcrossSegments();
        Path root = Files.createTempDirectory("prange-pagerun-key-atomic-roll-");
        try {
            SortTransformResult serial = run(scenario, 1, root, "serial", 1L, Long.MAX_VALUE,
                    DuplicateHook.NO_OP, SortMetrics.NO_OP);
            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult parallel = run(scenario, ranges, root, "parallel", 1L,
                    Long.MAX_VALUE, DuplicateHook.NO_OP, metrics);

            List<ListEntry> serialRows = readAll(serial.finalFiles());
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());
            assertThat(metrics.count("SORT.merge_range_parallel"))
                    .as("the page-run parallel path genuinely engaged for every requested range")
                    .isEqualTo(ranges);
            assertThat(metrics.count("SORT.final_roll_equal_key_deferred"))
                    .as("one bounded deferral signal per range-local oversized key group")
                    .isEqualTo(ranges);
            assertThat(parallel.finalFiles()).hasSize(ranges);
            assertKeyAtomicAndStrictlyDisjoint(parallel.finalFiles());
            assertThat(parallelRows).isSortedAccordingTo(cmp);
            assertThat(parallelRows).containsExactlyInAnyOrderElementsOf(scenario.allEntries());
            assertThat(parallelRows).containsExactlyElementsOf(serialRows);
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    /**
     * The properties above stage fewer rows than {@code PageRunSegmentWriter}'s 1000-entry page, so
     * every segment they build holds exactly ONE page — which means they never execute
     * {@link RangeScopedPageFrontier}'s skip or tail-abandonment at all. This example stages
     * MULTI-PAGE segments and proves, from the engagement counters rather than from the output,
     * that the new path actually ran:
     *
     * <ul>
     *   <li>{@code SORT.merge_range_parallel} fires once per range — so the merge really was split,
     *       rather than silently taking the serial fallback and passing the output assertions
     *       anyway (which every other property here would do if {@code boundaries()} returned
     *       {@code null});</li>
     *   <li>{@code SORT.merge_range_page_skipped} fires — so pages really were stepped over or left
     *       unread, i.e. the skip is engaging and not quietly reading everything.</li>
     * </ul>
     *
     * Output correctness is still asserted against both oracles, so this is a superset of the
     * single-page properties, not a substitute for them.
     */
    @Example
    void multiPageSegmentsEngageTheParallelPathAndActuallySkipPages() throws IOException {
        // 6 segments x ~1400 rows: comfortably past the 1000-row page boundary, with enough
        // distinct keys that the keyspace is splittable into every requested range.
        int ranges = 4;
        Scenario s = manyDistinctKeys(6, 8400);
        Path root = Files.createTempDirectory("prange-pagerun-multipage-");
        try {
            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult serial =
                    run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE, DuplicateHook.NO_OP,
                            SortMetrics.NO_OP);
            SortTransformResult parallel =
                    run(s, ranges, root, "parallel", Long.MAX_VALUE, Long.MAX_VALUE, DuplicateHook.NO_OP,
                            metrics);

            assertThat(metrics.count("SORT.merge_range_parallel"))
                    .as("the parallel path engaged, once per range (not a silent serial fallback)")
                    .isEqualTo(ranges);
            assertThat(metrics.count("SORT.merge_range_page_skipped"))
                    .as("at least one range stepped over or left unread >=1 page")
                    .isGreaterThan(0);
            assertThat(metrics.count("SORT.merge_zone_proof_complete")).isEqualTo(1);
            assertThat(metrics.count("SORT.page_run_index_mismatch")).isZero();
            assertThat(parallel.finalFiles())
                    .as("one part per range").hasSize(ranges);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());
            assertThat(parallelRows).as("exact input multiset").containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("position-for-position equal to serial")
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    /**
     * The cascade clamp. An {@code R} the merge budget cannot carry over this many staged segments
     * must be REDUCED, not honoured: {@code perRangeFanIn} divides the budget by the range count, so
     * past the bound every range merges in several passes and the parallel merge is slower than the
     * serial one it replaced. Before the clamp this was also silent — {@code merge_range_parallel}
     * fired once per range either way, so a run pessimised by its own tuning looked like a success.
     *
     * <p>The budget here is sized to carry all the segments' streams for exactly {@code ALLOWED}
     * ranges. Against the unclamped code this test fails on both counters: 8 ranges engage and every
     * one of them cascades.
     */
    @Example
    void anOverAmbitiousRangeCountIsClampedRatherThanCascadingEveryRange() throws IOException {
        int segmentCount = 6;
        int allowed = 3;
        Scenario s = manyDistinctKeys(segmentCount, 8400);
        Path root = Files.createTempDirectory("prange-pagerun-clamp-");
        ListAppender<ILoggingEvent> appender = attachTransformLog();
        try {
            // Price an open page-run stream as the planner does: the larger of the configured
            // working-set estimate and the trailer's encoded maxRecordLen.
            Path probeDir = Files.createDirectories(root.resolve("probe"));
            long perStream = PageRunSegmentDescriptor.maxRecordLen(
                    descriptorTrailers(stage(probeDir, s.segments())));
            perStream = Math.max(perStream, SortConfigs.base().mergePerStreamBytes());
            long budget = perStream * segmentCount * allowed;

            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult serial =
                    run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE, DuplicateHook.NO_OP,
                            SortMetrics.NO_OP);
            SortTransformResult clamped =
                    run(s, 8, root, "clamped", Long.MAX_VALUE, budget, DuplicateHook.NO_OP, metrics);

            assertThat(metrics.count("SORT.merge_range_would_cascade"))
                    .as("the clamp bit, and the run said so").isEqualTo(1);
            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
                    .anyMatch(message -> message.contains("reason=would_cascade"));
            assertThat(metrics.count("SORT.merge_range_parallel"))
                    .as("ran at the affordable range count, not the 8 requested")
                    .isEqualTo(allowed);
            assertThat(clamped.cascadedPasses())
                    .as("no range cascades — which is the whole point of clamping")
                    .isZero();
            assertThat(clamped.finalFiles())
                    .as("one part per range actually used").hasSize(allowed);
            assertThat(readAll(clamped.finalFiles()))
                    .as("clamping is a performance guard, so the rows are untouched")
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            detachTransformLog(appender);
            deleteTreeBestEffort(root);
        }
    }

    /**
     * The floor of the same clamp: when not even ONE range fits the budget, the parallel path hands
     * back to the serial merge instead of paying its boundary-sampling prologue to cascade anyway.
     */
    @Example
    void aBudgetTooSmallForEvenOneRangeFallsBackToSerial() throws IOException {
        Scenario s = manyDistinctKeys(6, 8400);
        Path root = Files.createTempDirectory("prange-pagerun-clamp-floor-");
        try {
            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult serial =
                    run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE, DuplicateHook.NO_OP,
                            SortMetrics.NO_OP);
            SortTransformResult fellBack =
                    run(s, 8, root, "fallback", Long.MAX_VALUE, 1L, DuplicateHook.NO_OP, metrics);

            assertThat(metrics.count("SORT.merge_range_parallel"))
                    .as("no range ran: the parallel path declined").isZero();
            assertThat(metrics.count("SORT.merge_range_would_cascade"))
                    .as("and the budget decline is classified, not called unsplittable").isEqualTo(1);
            assertThat(metrics.count("SORT.merge_range_unsplittable")).isZero();
            assertThat(fellBack.finalFiles())
                    .as("serial output shape").hasSize(serial.finalFiles().size());
            assertThat(readAll(fellBack.finalFiles()))
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void stagedFloorAndFdDeclinesHaveExactReasonsAndTheFloorBoundaryIsInclusive()
            throws IOException {
        Scenario scenario = manyDistinctKeys(2, 40);
        Path root = Files.createTempDirectory("prange-routing-reasons-");
        ListAppender<ILoggingEvent> appender = attachTransformLog();
        try {
            Path belowOut = Files.createDirectories(root.resolve("below"));
            Path belowStaging = Files.createDirectories(belowOut.resolve("_staging"));
            List<Path> belowSegments = stage(belowStaging, scenario.segments());
            long stagedBytes = stagedBytes(belowSegments);
            CountingMetrics belowMetrics = new CountingMetrics();
            SortConfig belowConfig = SortConfigs.base()
                    .withMergeParallelism(2)
                    .withMinParallelStagedBytes(stagedBytes + 1);
            new SortTransform(sortRun(belowConfig, DuplicateHook.NO_OP, belowMetrics,
                    SortedFileWriterFactory.DEFAULT, () -> -1))
                    .transform(belowSegments, belowOut, belowStaging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(belowMetrics.count("SORT.merge_range_below_staged_floor")).isEqualTo(1);
            assertThat(belowMetrics.count("SORT.merge_range_unsplittable")).isZero();

            Path exactOut = Files.createDirectories(root.resolve("exact"));
            Path exactStaging = Files.createDirectories(exactOut.resolve("_staging"));
            List<Path> exactSegments = stage(exactStaging, scenario.segments());
            CountingMetrics exactMetrics = new CountingMetrics();
            SortConfig exactConfig = SortConfigs.base()
                    .withMergeParallelism(2)
                    .withMinParallelStagedBytes(stagedBytes(exactSegments));
            new SortTransform(sortRun(exactConfig, DuplicateHook.NO_OP, exactMetrics,
                    SortedFileWriterFactory.DEFAULT, () -> -1))
                    .transform(exactSegments, exactOut, exactStaging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(exactMetrics.count("SORT.merge_range_parallel"))
                    .as("staged bytes equal to the floor take the parallel route")
                    .isEqualTo(2);
            assertThat(exactMetrics.count("SORT.merge_range_below_staged_floor")).isZero();

            Path fdOut = Files.createDirectories(root.resolve("fd"));
            Path fdStaging = Files.createDirectories(fdOut.resolve("_staging"));
            List<Path> fdSegments = stage(fdStaging, scenario.segments());
            CountingMetrics fdMetrics = new CountingMetrics();
            SortConfig fdConfig = SortConfigs.base().withMergeParallelism(2);
            int exhaustedLimit = MergeFdBudget.FD_HEADROOM + 2;
            new SortTransform(sortRun(fdConfig, DuplicateHook.NO_OP, fdMetrics,
                    SortedFileWriterFactory.DEFAULT, () -> exhaustedLimit))
                    .transform(fdSegments, fdOut, fdStaging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(fdMetrics.count("SORT.merge_range_fd_exhausted")).isEqualTo(1);
            assertThat(fdMetrics.count("SORT.merge_range_unsplittable")).isZero();

            Path limitedOut = Files.createDirectories(root.resolve("fd-limited"));
            Path limitedStaging = Files.createDirectories(limitedOut.resolve("_staging"));
            List<Path> limitedSegments = stage(limitedStaging, scenario.segments());
            CountingMetrics limitedMetrics = new CountingMetrics();
            SortConfig limitedConfig = SortConfigs.base().withMergeParallelism(4);
            int limitedFdLimit = MergeFdBudget.FD_HEADROOM + 8;
            new SortTransform(sortRun(limitedConfig, DuplicateHook.NO_OP, limitedMetrics,
                    SortedFileWriterFactory.DEFAULT, () -> limitedFdLimit))
                    .transform(limitedSegments, limitedOut, limitedStaging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(limitedMetrics.count("SORT.merge_range_fd_limited")).isEqualTo(1);
            assertThat(limitedMetrics.count("SORT.merge_range_fd_exhausted")).isZero();
            assertThat(limitedMetrics.count("SORT.merge_range_parallel")).isEqualTo(2);

            List<String> messages = appender.list.stream()
                    .map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).anyMatch(message -> message.contains("reason=below_staged_floor"));
            assertThat(messages).anyMatch(message -> message.contains("reason=fd_exhausted"));
            assertThat(messages).anyMatch(message -> message.contains("reason=fd_limited"));
        } finally {
            detachTransformLog(appender);
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void configuredFanInThatUltimatelyForcesSerialWinsOverAPartialFdReduction()
            throws IOException {
        Scenario scenario = manyDistinctKeys(3, 60);
        Path root = Files.createTempDirectory("prange-combined-clamp-reason-");
        ListAppender<ILoggingEvent> appender = attachTransformLog();
        try {
            Path output = Files.createDirectories(root.resolve("out"));
            Path staging = Files.createDirectories(output.resolve("_staging"));
            List<Path> segments = stage(staging, scenario.segments());
            CountingMetrics metrics = new CountingMetrics();
            SortConfig config = SortConfigs.base()
                    .withFanIn(2)
                    .withMergeParallelism(4);
            // Three inputs plus one initially reserved output per range let descriptors reduce
            // R=4 to R=2. The configured fan-in of two cannot carry all three segments at any R,
            // however, and is therefore the constraint that ultimately declines parallel merge.
            int partiallyLimitedFd = MergeFdBudget.FD_HEADROOM + 8;

            new SortTransform(sortRun(config, DuplicateHook.NO_OP, metrics,
                    SortedFileWriterFactory.DEFAULT, () -> partiallyLimitedFd))
                    .transform(segments, output, staging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(metrics.count("SORT.merge_range_would_cascade")).isEqualTo(1);
            assertThat(metrics.count("SORT.merge_range_fd_exhausted")).isZero();
            assertThat(metrics.count("SORT.merge_range_fd_limited")).isZero();
            assertThat(metrics.count("SORT.merge_range_parallel")).isZero();
            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
                    .anyMatch(message -> message.contains("reason=would_cascade"));
        } finally {
            detachTransformLog(appender);
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void outputPartFdGuardFailsBeforeOverOpeningAndLeavesNoWorkersWritersOrDebris()
            throws Exception {
        Scenario scenario = manyDistinctKeys(2, 40);
        Path root = Files.createTempDirectory("prange-output-fd-guard-");
        try {
            Path staging = Files.createDirectories(root.resolve("_staging"));
            List<Path> segments = stage(staging, scenario.segments());
            TrackingWriterFactory writers = new TrackingWriterFactory(2L);
            SortConfig config = SortConfigs.base()
                    .withFinalFileBytes(1L)
                    .withMergeParallelism(2);
            // usable=6: four input readers (2 ranges x 2 segments) leave exactly two output
            // writers. Each range can open its first part; the first attempted roll must fail before
            // a third writer/descriptor is opened.
            int softLimit = MergeFdBudget.FD_HEADROOM + ParallelRangeMerge.PROOF_SPOOL_FDS + 6;
            ParallelRangeMerge merge = new ParallelRangeMerge(
                    sortRun(config, DuplicateHook.NO_OP, SortMetrics.NO_OP, writers,
                            () -> softLimit));
            ParallelKickoff kickoff = parallelKickoff(segments);
            List<PageRunSegmentDescriptor> descriptors = kickoff.descriptors();
            List<byte[]> boundaries = ParallelRangeMerge.boundaries(
                    descriptors, kickoff.candidates(), 2, SortMetrics.NO_OP);

            assertThatThrownBy(() -> merge.run(descriptors, staging, boundaries, units -> { }))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("output-part fd budget exhausted");
            assertThat(writers.opened.get()).isEqualTo(2);
            assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
            assertThat(writers.openNow.get()).as("all real file channels closed").isZero();
            assertNoOwnedDebris(staging);
            assertNoLiveWorkers(merge.workerThreadPrefix());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void outputPartFdGuardR8BudgetMathSmoke() throws Exception {
        Scenario scenario = manyDistinctKeys(8, 320);
        Path root = Files.createTempDirectory("prange-output-fd-guard-r8-");
        try {
            Path staging = Files.createDirectories(root.resolve("_staging"));
            ParallelKickoff kickoff = parallelKickoff(stage(staging, scenario.segments()));
            TrackingWriterFactory writers = new TrackingWriterFactory(1L);
            SortConfig config = SortConfigs.base()
                    .withFanIn(8)
                    .withFinalFileBytes(1L)
                    .withMergeParallelism(8);
            int ranges = 8;
            int perRangeFanIn = 8;
            int outputAllowance = 40;
            int softLimit = MergeFdBudget.FD_HEADROOM + ParallelRangeMerge.PROOF_SPOOL_FDS
                    + ranges * perRangeFanIn + outputAllowance;
            ParallelRangeMerge merge = new ParallelRangeMerge(sortRun(config, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, writers, () -> softLimit));
            List<byte[]> boundaries = ParallelRangeMerge.boundaries(kickoff.descriptors(),
                    kickoff.candidates(), ranges, SortMetrics.NO_OP);

            assertThat(boundaries).hasSize(ranges - 1);
            assertThat(merge.effectiveRanges(ranges, kickoff.descriptors()).ranges())
                    .as("the production planner admits all requested ranges")
                    .isEqualTo(ranges);
            assertThat(merge.perRangeFanIn(ranges, kickoff.descriptors())).isEqualTo(perRangeFanIn);
            assertThat(rangeRows(scenario.allEntries(), boundaries))
                    .as("page-minimum boundaries leave one row in each early range")
                    .containsExactly(1, 1, 1, 1, 1, 1, 1, 313);
            assertThat(softLimit - MergeFdBudget.FD_HEADROOM - ParallelRangeMerge.PROOF_SPOOL_FDS
                    - ranges * Math.min(perRangeFanIn, kickoff.descriptors().size()))
                    .as("open output allowance after actual input reservation")
                    .isEqualTo(outputAllowance);
            assertThatThrownBy(() -> merge.run(kickoff.descriptors(), staging, boundaries, units -> { }))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("output-part fd budget exhausted: limit=" + outputAllowance
                            + ", attempted=" + (outputAllowance + 1));
            assertThat(writers.opened.get()).isEqualTo(outputAllowance);
            assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
            assertThat(writers.openNow.get()).isZero();
            assertNoOwnedDebris(staging);
            assertNoLiveWorkers(merge.workerThreadPrefix());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    @Tag("perf")
    void processMergePerfR8WideOwnersCharacterizesHeapAndFdGuard() throws Exception {
        Path root = Files.createTempDirectory("prange-process-merge-perf-");
        try {
            Path staging = Files.createDirectories(root.resolve("_staging"));
            SortConfig config = SortConfigs.base()
                    .withFanIn(8)
                    .withFinalFileBytes(4L << 20)
                    .withMergeParallelism(8);
            int ranges = 8;
            int perRangeFanIn = 8;
            int outputAllowance = 40;
            int softLimit = MergeFdBudget.FD_HEADROOM + ParallelRangeMerge.PROOF_SPOOL_FDS
                    + ranges * perRangeFanIn + outputAllowance;
            WideStressStaging wide = stageWideWriterStress(staging, ranges);
            CountingWriterFactory writers = new CountingWriterFactory(
                    new SortedParquetWriterFactory(config, SortMode.OBJECTS));
            ParallelRangeMerge merge = new ParallelRangeMerge(sortRun(config, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, writers, () -> softLimit));

            assertThat(wide.boundaries()).hasSize(ranges - 1);
            assertThat(merge.effectiveRanges(ranges, wide.kickoff().descriptors()).ranges())
                    .as("the production planner admits all requested ranges")
                    .isEqualTo(ranges);
            assertThat(merge.perRangeFanIn(ranges, wide.kickoff().descriptors()))
                    .isEqualTo(perRangeFanIn);
            assertThat(softLimit - MergeFdBudget.FD_HEADROOM - ParallelRangeMerge.PROOF_SPOOL_FDS
                    - ranges * Math.min(perRangeFanIn, wide.kickoff().descriptors().size()))
                    .as("open output allowance after actual input reservation")
                    .isEqualTo(outputAllowance);
            awaitCollected(wide.fixture());
            long settledBaseline = settleHeapBytes();
            long testWorkerMaxHeap = Runtime.getRuntime().maxMemory();
            assertThat(testWorkerMaxHeap)
                    .as("the perf tier runs this heap characterization in its isolated 2 GiB worker")
                    .isGreaterThanOrEqualTo(PROCESS_MERGE_PERF_WORKER_HEAP_BYTES);
            HeapSampler sampler = HeapSampler.start(settledBaseline);
            long sampledPeak;
            try {
                assertThatThrownBy(() -> merge.run(wide.kickoff().descriptors(), staging,
                        wide.boundaries(), units -> { }))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("output-part fd budget exhausted: limit=" + outputAllowance
                                + ", attempted=" + (outputAllowance + 1));
            } finally {
                sampledPeak = sampler.stop();
            }
            long sampledDelta = Math.max(0L, sampledPeak - settledBaseline);
            System.out.printf("WP5_1_PROCESS_MERGE_PERF ranges=%d per_range_fan_in=%d soft_fd_limit=%d "
                            + "initial_output_allowance=%d final_file_bytes=%d "
                            + "test_worker_max_heap_bytes=%d "
                            + "settled_baseline_heap_bytes=%d sampled_peak_heap_bytes=%d "
                            + "sampled_incremental_heap_bytes=%d heap_characterization_budget_bytes=%d%n",
                    ranges, perRangeFanIn, softLimit, outputAllowance, config.finalFileBytes(),
                    testWorkerMaxHeap, settledBaseline, sampledPeak, sampledDelta,
                    PROCESS_MERGE_STRESS_HEAP_BUDGET_BYTES);
            assertThat(sampledPeak)
                    .as("sampled process heap is under the §7.2 Parquet characterization budget")
                    .isLessThan(PROCESS_MERGE_STRESS_HEAP_BUDGET_BYTES);
            assertThat(writers.opened.get()).isEqualTo(outputAllowance);
            assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
            assertThat(writers.openNow.get()).isZero();
            assertNoOwnedDebris(staging);
            assertNoLiveWorkers(merge.workerThreadPrefix());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void serialCancellationIsAnIOExceptionClosesResourcesAndPreservesInterrupt() throws IOException {
        Scenario scenario = manyDistinctKeys(2, 40);
        Path root = Files.createTempDirectory("serial-merge-cancellation-");
        SelfInterruptingWriterFactory writers = new SelfInterruptingWriterFactory();
        try {
            Path output = Files.createDirectories(root.resolve("out"));
            Path staging = Files.createDirectories(output.resolve("_staging"));
            List<Path> segments = stage(staging, scenario.segments());
            SortConfig config = SortConfigs.base().withMergeParallelism(1);
            SortTransform transform = new SortTransform(sortRun(config, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, writers, SortRun.PROCESS_SOFT_FD_LIMIT));

            assertThatThrownBy(() -> transform.transform(
                    segments, output, staging, PublishListener.NO_OP,
                    units -> { }, FinalPassListener.NO_OP))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("sort merge interrupted")
                    .hasCauseInstanceOf(MergeCancellation.Cancelled.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(writers.openNow.get()).as("serial output channel closed on cancellation").isZero();
        } finally {
            Thread.interrupted();
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void laterRangeFailureCancelsBlockedEarlierRangeInCompletionOrderAndCleans() throws Exception {
        Scenario scenario = manyDistinctKeys(2, 200);
        Path root = Files.createTempDirectory("prange-failure-quiescence-");
        try {
            Path staging = Files.createDirectories(root.resolve("_staging"));
            List<Path> segments = stage(staging, scenario.segments());
            CountDownLatch earlierRangeWriting = new CountDownLatch(1);
            TrackingWriterFactory writers =
                    new TrackingWriterFactory(earlierRangeWriting, new CountDownLatch(1));
            SortConfig config = SortConfigs.base().withMergeParallelism(2);
            ParallelRangeMerge merge = new ParallelRangeMerge(
                    sortRun(config, DuplicateHook.NO_OP, SortMetrics.NO_OP, writers, () -> -1));
            ParallelKickoff kickoff = parallelKickoff(segments);
            List<PageRunSegmentDescriptor> descriptors = kickoff.descriptors();
            List<byte[]> boundaries = ParallelRangeMerge.boundaries(
                    descriptors, kickoff.candidates(), 2, SortMetrics.NO_OP);

            assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    assertThatThrownBy(() -> merge.run(descriptors, staging, boundaries, units -> { }))
                            .isInstanceOf(IOException.class)
                            .hasMessageContaining("injected later range failure"));
            assertThat(writers.cooperativelyCancelled.get())
                    .as("blocked range returned with its interrupt set, then hit MergeCancellation")
                    .isTrue();
            assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
            assertThat(writers.openNow.get()).as("all real file channels closed").isZero();
            assertNoOwnedDebris(staging);
            assertNoLiveWorkers(merge.workerThreadPrefix());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    @Example
    void interruptWaitsForPageRunWorkersThenRestoresTheCallerFlagAndCleans() throws Exception {
        Scenario scenario = manyDistinctKeys(2, 200);
        Path root = Files.createTempDirectory("prange-interrupt-quiescence-");
        try {
            Path staging = Files.createDirectories(root.resolve("_staging"));
            List<Path> segments = stage(staging, scenario.segments());
            CountDownLatch bothWriting = new CountDownLatch(2);
            TrackingWriterFactory writers = new TrackingWriterFactory(bothWriting);
            SortConfig config = SortConfigs.base().withMergeParallelism(2);
            ParallelRangeMerge merge = new ParallelRangeMerge(
                    sortRun(config, DuplicateHook.NO_OP, SortMetrics.NO_OP, writers, () -> -1));
            ParallelKickoff kickoff = parallelKickoff(segments);
            List<PageRunSegmentDescriptor> descriptors = kickoff.descriptors();
            List<byte[]> boundaries = ParallelRangeMerge.boundaries(
                    descriptors, kickoff.candidates(), 2, SortMetrics.NO_OP);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interruptRestored = new AtomicBoolean();
            Thread caller = new Thread(() -> {
                try {
                    merge.run(descriptors, staging, boundaries, units -> { });
                } catch (Throwable t) {
                    failure.set(t);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                }
            }, "parallel-range-interrupt-caller");
            caller.start();
            assertThat(bothWriting.await(2, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            caller.join(2_000L);

            assertThat(caller.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(IOException.class);
            assertThat(interruptRestored).isTrue();
            assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
            assertThat(writers.openNow.get()).as("all real file channels closed").isZero();
            assertNoOwnedDebris(staging);
            assertNoLiveWorkers(merge.workerThreadPrefix());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    /**
     * The completeness stamp, on the LIVE staging format and with ranges that ROLL. This is the case
     * the stamp exists for and the one shallow fixtures cannot reach: several ranges each
     * emit several parts, so a range-local {@code file_index} would repeat (1,2,1,2,…) and no reader
     * could tell a complete set from a truncated one. Asserts the published set carries a contiguous
     * global {@code 1..N} in filename order with exactly one {@code file_final}, on {@code N}.
     */
    @Example
    void pageRunParallelPartsCarryAGlobalStampAcrossRollingRanges() throws IOException {
        int ranges = 3;
        Scenario s = manyDistinctKeys(6, 8400);
        Path root = Files.createTempDirectory("prange-pagerun-stamp-");
        try {
            SortConfig config = SortConfigs.base()
                    .withFinalFileBytes(4096L)         // small enough that every range rolls
                    .withMergeBudgetBytes(Long.MAX_VALUE)
                    .withMergeParallelism(ranges);
            Path output = Files.createDirectories(root.resolve("out"));
            Path staging = Files.createDirectories(output.resolve("_staging"));
            List<Path> segs = stage(staging, s.segments());
            SortTransform transform = new SortTransform(sortRun(config, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, new SortedParquetWriterFactory(config, SortMode.OBJECTS),
                    SortRun.PROCESS_SOFT_FD_LIMIT));
            SortTransformResult result =
                    transform.transform(segs, output, staging, PublishListener.NO_OP,
                            units -> { }, FinalPassListener.NO_OP);

            assertThat(result.finalFiles()).as("ranges rolled into more parts than ranges")
                    .hasSizeGreaterThan(ranges);

            List<Integer> indices = new ArrayList<>();
            List<Integer> finalAt = new ArrayList<>();
            for (int i = 0; i < result.finalFiles().size(); i++) {
                Map<String, String> kv = footerKv(result.finalFiles().get(i));
                indices.add(Integer.parseInt(kv.get(SortedParquetWriter.FILE_INDEX_KEY)));
                if (kv.containsKey(SortedParquetWriter.FILE_FINAL_KEY)) {
                    finalAt.add(i);
                }
            }
            List<Integer> expected = new ArrayList<>();
            for (int i = 1; i <= result.finalFiles().size(); i++) {
                expected.add(i);
            }
            assertThat(indices).as("contiguous global file_index in filename order")
                    .containsExactlyElementsOf(expected);
            assertThat(finalAt).as("exactly one file_final, on the last part")
                    .containsExactly(result.finalFiles().size() - 1);

            // The stamp is a claim ABOUT the rows, so it is only worth anything if the rows are right.
            assertThat(readAll(result.finalFiles())).as("still the exact input, in order")
                    .containsExactlyElementsOf(s.allEntries().stream().sorted(cmp).toList());
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    private static Map<String, String> footerKv(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFooter().getFileMetaData().getKeyValueMetaData();
        }
    }

    /** Unique, densely-ordered keys — many pages per segment and always splittable. */
    private Scenario manyDistinctKeys(int segmentCount, int entryCount) {
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            // Round-robin so every segment spans the WHOLE keyspace: each range must then read
            // from every segment, which is what makes the per-segment page skip meaningful.
            ListEntry e = new ObjectEntry(KeyBytes.ofUtf8(String.format("key-%08d", i)), i % 7, 0L,
                    null, null, null, false, null, null, null, null);
            all.add(e);
            segs.get(i % segmentCount).add(e);
        }
        for (List<ListEntry> seg : segs) {
            seg.sort(cmp);
        }
        return new Scenario(segs, all);
    }

    // ---------------------------------------------------------------------
    // Duplicate-hook equivalence: the page skip hands a range whole straddling
    // pages, so an unscoped hook would let two adjacent ranges report the same
    // out-of-range duplicate pair.
    // ---------------------------------------------------------------------

    @Property(tries = 40)
    void duplicateHookFiresExactlyAsOftenAsTheSerialMerge(
            @ForAll @IntRange(min = 2, max = 5) int segmentCount,
            @ForAll @IntRange(min = 1, max = 200) int entryCount,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        // SMALL_ALPHABET maximises adjacent-equal pairs, which is what the hook reports.
        Scenario s = build(segmentCount, entryCount, KeyStyle.SMALL_ALPHABET, seed);
        Path root = Files.createTempDirectory("prange-pagerun-hook-");
        try {
            AtomicInteger serialHits = new AtomicInteger();
            AtomicInteger parallelHits = new AtomicInteger();
            run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE,
                    (prev, dup) -> serialHits.incrementAndGet());
            run(s, ranges, root, "parallel", Long.MAX_VALUE, Long.MAX_VALUE,
                    (prev, dup) -> parallelHits.incrementAndGet());
            assertThat(parallelHits.get())
                    .as("parallel duplicate-hook count == serial (no double-count across boundaries)")
                    .isEqualTo(serialHits.get());
            List<ListEntry> sorted = new ArrayList<>(s.allEntries());
            sorted.sort(cmp);
            long expectedPairs = 0;
            for (int i = 1; i < sorted.size(); i++) {
                if (cmp.compare(sorted.get(i - 1), sorted.get(i)) == 0) {
                    expectedPairs++;
                }
            }
            assertThat(serialHits.get())
                    .as("serial hook count equals every adjacent comparator-equal pair")
                    .isEqualTo(expectedPairs);
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    // ---------------------------------------------------------------------
    // Adversarial: the completeness cross-check DEMOTES on this path (only the
    // last range walks a segment to EOF), so pin that it still fires.
    // ---------------------------------------------------------------------

    /**
     * A segment whose trailer under-declares {@code totalEntries} must still fail an R&gt;1 parallel
     * merge — the worst failure this path could have is publishing a segment that silently lost
     * rows.
     *
     * <p><b>What actually catches it, and why that matters.</b> Ranges that abandon their tail never
     * reach {@code checkComplete}, so the obvious worry is that a corrupt trailer slips past. It does
     * not, because the last unbounded range drains every page of every segment to EOF and runs the
     * cross-check. This test pins the END-TO-END guarantee (corrupt trailer ⇒ merge fails), including
     * the post-embedded-sample integrity timing where the failure may arrive after sibling ranges
     * have started writing temporary parts.
     */
    @Example
    void aCorruptTrailerStillFailsTheParallelMerge() throws IOException {
        Path root = Files.createTempDirectory("prange-pagerun-corrupt-");
        try {
            Scenario s = build(4, 200, KeyStyle.DENSE_SEQUENTIAL, 42L);
            Path staging = Files.createDirectories(root.resolve("parallel").resolve("_staging"));
            List<Path> segs = stage(staging, s.segments());
            corruptTotalEntries(segs.get(0));

            SortConfig config = SortConfigs.base().withMergeParallelism(4);
            SortTransform transform = new SortTransform(sortRun(config, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                    SortRun.PROCESS_SOFT_FD_LIMIT));
            assertThatThrownBy(() -> transform.transform(segs, Files.createDirectories(
                    root.resolve("parallel").resolve("data")), staging, PublishListener.NO_OP,
                    units -> { }, FinalPassListener.NO_OP))
                    .isInstanceOf(IOException.class);
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    /** Flip the trailer's {@code totalEntries} down by one — a segment that silently lost a row. */
    private static void corruptTotalEntries(Path segment) throws IOException {
        // Fixed trailer tail: trailerStart u64 + totalRecords u32 + totalEntries u64 + maxRecordLen u32
        // + magic u32 = 28 bytes from EOF; totalEntries starts 16 bytes before the end.
        try (FileChannel ch = FileChannel.open(segment, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long pos = ch.size() - 16;
            ByteBuffer buf = ByteBuffer.allocate(8);
            ch.read(buf, pos);
            buf.flip();
            long declared = buf.getLong();
            ByteBuffer out = ByteBuffer.allocate(8).putLong(declared - 1).flip();
            ch.write(out, pos);
        }
    }

    // =====================================================================
    // Harness
    // =====================================================================

    private record Scenario(List<List<ListEntry>> segments, List<ListEntry> allEntries) {
    }

    private Scenario build(int segmentCount, int entryCount, KeyStyle style, long seed) {
        Random r = new Random(seed);
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            ListEntry e = entry(key(style, r), r);
            all.add(e);
            segs.get(r.nextInt(segmentCount)).add(e);
        }
        for (List<ListEntry> seg : segs) {
            seg.sort(cmp);   // each staging segment is individually sorted
        }
        return new Scenario(segs, all);
    }

    private Scenario wideOwnerScenario(int segmentCount, int rowsPerSegment, int ownerBytes) {
        List<List<ListEntry>> segments = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (int segment = 0; segment < segmentCount; segment++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int row = 0; row < rowsPerSegment; row++) {
                int ordinal = segment * rowsPerSegment + row;
                ListEntry entry = new ObjectEntry(KeyBytes.ofUtf8(String.format("k%08d", ordinal)), ordinal,
                        0L, null, null, null, false, "owner-" + segment,
                        wideOwner(ordinal, ownerBytes), null, null);
                rows.add(entry);
                all.add(entry);
            }
            segments.add(rows);
        }
        return new Scenario(segments, all);
    }

    /**
     * Build and persist the wide-owner fixture in a separate frame, then return only durable inputs
     * and a weak canary. The perf measurement proves that this construction state is reclaimable
     * before it samples merge-process heap.
     *
     * <p>The real writer's compressed {@code dataSize()} controls the 4 MiB roll threshold. Thus
     * the 112,000-row shape is characterization-only evidence that this encoding reaches the 41st
     * writer; {@link #outputPartFdGuardR8BudgetMathSmoke()} is the deterministic writer-41 contract.
     */
    private WideStressStaging stageWideWriterStress(Path staging, int ranges) throws IOException {
        // 112,000 rows × 2 KiB clears 41 × 4 MiB after Parquet compression, while each 1,000-row
        // page-run page remains 2 MiB so the 8 × 8 input-frontier fleet fits in the perf test JVM.
        Scenario scenario = wideOwnerScenario(ranges, 14_000, 2 * 1024);
        WeakReference<Scenario> fixture = new WeakReference<>(scenario);
        ParallelKickoff kickoff = parallelKickoff(stage(staging, scenario.segments()));
        List<byte[]> boundaries = ParallelRangeMerge.boundaries(kickoff.descriptors(),
                kickoff.candidates(), ranges, SortMetrics.NO_OP);
        assertThat(boundaries).hasSize(ranges - 1);
        assertThat(rangeRows(scenario.allEntries(), boundaries))
                .as("wide rows yield enough page-minimum samples to balance every range")
                .containsExactly(14_000, 14_000, 14_000, 14_000,
                        14_000, 14_000, 14_000, 14_000);
        return new WideStressStaging(kickoff, boundaries, fixture);
    }

    private static String wideOwner(int ordinal, int bytes) {
        char[] chars = new char[bytes];
        int state = 0x9e3779b9 ^ ordinal;
        for (int i = 0; i < chars.length; i++) {
            state ^= state << 13;
            state ^= state >>> 17;
            state ^= state << 5;
            chars[i] = (char) ('!' + Math.floorMod(state, 90));
        }
        return new String(chars);
    }

    private static List<Integer> rangeRows(List<ListEntry> entries, List<byte[]> boundaries) {
        List<Integer> rows = new ArrayList<>(Collections.nCopies(boundaries.size() + 1, 0));
        for (ListEntry entry : entries) {
            int range = 0;
            while (range < boundaries.size()
                    && KeyBytes.compareUnsigned(entry.key().rawUnsafe(), boundaries.get(range)) >= 0) {
                range++;
            }
            rows.set(range, rows.get(range) + 1);
        }
        return rows;
    }

    private static void awaitCollected(WeakReference<?> fixture) throws InterruptedException {
        for (int i = 0; i < MAX_GC_ATTEMPTS && fixture.get() != null; i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertThat(fixture.get())
                .as("wide fixture construction state is reclaimable before heap characterization")
                .isNull();
    }

    private static long settleHeapBytes() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            System.gc();
            Thread.sleep(10);
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    /** Samples one simultaneous process-heap total while the range workers execute. */
    private static final class HeapSampler {
        private final AtomicBoolean running;
        private final AtomicLong sampledPeak;
        private final Thread thread;

        private HeapSampler(AtomicBoolean running, AtomicLong sampledPeak, Thread thread) {
            this.running = running;
            this.sampledPeak = sampledPeak;
            this.thread = thread;
        }

        static HeapSampler start(long baseline) {
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong sampledPeak = new AtomicLong(baseline);
            Thread thread = Thread.ofPlatform().name("wp5-process-heap-sampler").daemon().start(() -> {
                while (running.get()) {
                    sampledPeak.accumulateAndGet(
                            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            });
            return new HeapSampler(running, sampledPeak, thread);
        }

        long stop() throws InterruptedException {
            running.set(false);
            thread.join();
            return sampledPeak.get();
        }
    }

    /** Four splittable key ranges, with every key's versions divided across two page-run segments. */
    private Scenario versionClustersAcrossSegments() {
        List<List<ListEntry>> segments = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (char key = 'a'; key <= 'd'; key++) {
            List<ListEntry> even = new ArrayList<>();
            List<ListEntry> odd = new ArrayList<>();
            for (int version = 0; version < 32; version++) {
                ListEntry entry = new ObjectEntry(KeyBytes.ofUtf8(String.valueOf(key)), version, 0L,
                        null, null, String.format("v%04d", version), version == 31,
                        null, null, null, null);
                (version % 2 == 0 ? even : odd).add(entry);
                all.add(entry);
            }
            even.sort(cmp);
            odd.sort(cmp);
            segments.add(even);
            segments.add(odd);
        }
        return new Scenario(segments, all);
    }

    private static void deleteTreeBestEffort(Path root) {
        try {
            Sweeps.deleteTree(root);
        } catch (IOException ignored) {
            // A test failure is more useful than a best-effort temporary-tree cleanup failure.
        }
    }

    private void assertParallelMatchesSerial(Scenario s, int ranges, long finalFileBytes,
                                             long mergeBudgetBytes) throws IOException {
        Path root = Files.createTempDirectory("prange-pagerun-");
        try {
            SortTransformResult serial =
                    run(s, 1, root, "serial", finalFileBytes, Long.MAX_VALUE, DuplicateHook.NO_OP);
            SortTransformResult parallel =
                    run(s, ranges, root, "parallel", finalFileBytes, mergeBudgetBytes, DuplicateHook.NO_OP);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> serialRows = readAll(serial.finalFiles());
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());

            assertThat(serial.totalRows()).as("serial rows out == rows in").isEqualTo(input.size());
            assertThat(parallel.totalRows()).as("parallel rows out == rows in").isEqualTo(input.size());

            assertThat(serialRows).as("serial is the exact input multiset")
                    .containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("parallel is globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("parallel is the exact input multiset (no dropped/duplicated row)")
                    .containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("parallel equals serial position-for-position")
                    .containsExactlyElementsOf(serialRows);

            List<String> names = parallel.finalFiles().stream()
                    .map(p -> p.getFileName().toString()).toList();
            for (int i = 0; i < names.size(); i++) {
                assertThat(names.get(i)).isEqualTo(String.format("part-%05d.parquet", i));
            }
        } finally {
            deleteTreeBestEffort(root);
        }
    }

    private SortTransformResult run(Scenario s, int parallelism, Path root, String name,
                                    long finalFileBytes, long mergeBudgetBytes, DuplicateHook hook)
            throws IOException {
        return run(s, parallelism, root, name, finalFileBytes, mergeBudgetBytes, hook, SortMetrics.NO_OP);
    }

    private SortTransformResult run(Scenario s, int parallelism, Path root, String name,
                                    long finalFileBytes, long mergeBudgetBytes, DuplicateHook hook,
                                    SortMetrics metrics) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segs = stage(staging, s.segments());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(finalFileBytes)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withMergeParallelism(parallelism);
        SortTransform transform = new SortTransform(sortRun(config, hook, metrics,
                SortedFileWriterFactory.DEFAULT, SortRun.PROCESS_SOFT_FD_LIMIT));
        return transform.transform(segs, output, staging, PublishListener.NO_OP,
                units -> { }, FinalPassListener.NO_OP);
    }

    private SortRun sortRun(SortConfig config, DuplicateHook hook, SortMetrics metrics,
                            SortedFileWriterFactory writerFactory, IntSupplier softFdLimitSupplier) {
        return new SortRun(config, cmp, hook, EqualKeyPolicy.ALLOW, metrics, writerFactory,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                softFdLimitSupplier, StaleFinalSweep.OWN_PARTS_ONLY);
    }

    /** Stage each pre-sorted segment as a page-run {@code .pageseg} file — the LIVE listing format. */
    private List<Path> stage(Path dir, List<List<ListEntry>> segs) throws IOException {
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            Path path = dir.resolve("seg-" + i + ".pageseg");
            try (SortedCursor cursor = new InMemoryCursor(segs.get(i), cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
    }

    private byte[] key(KeyStyle style, Random r) {
        return switch (style) {
            case DENSE_SEQUENTIAL -> String.format("k%03d", r.nextInt(64)).getBytes(StandardCharsets.UTF_8);
            case SMALL_ALPHABET -> new byte[]{(byte) ('a' + r.nextInt(5))};
            case CLUSTERED -> String.format("c%d-%03d", r.nextInt(3), r.nextInt(8))
                    .getBytes(StandardCharsets.UTF_8);
            case BINARY_ADVERSARIAL -> BIN_POOL[r.nextInt(BIN_POOL.length)].clone();
        };
    }

    /**
     * A {@link ListEntry} whose payload is a deterministic function of its comparator identity, so any
     * two equal-comparing rows are byte-identical — the regime in which serial and parallel output are
     * required to match position-for-position (see the Parquet sibling's class doc for why
     * distinct-payload ties are excluded).
     */
    private ListEntry entry(byte[] key, Random r) {
        int kind = r.nextInt(5);
        String version = r.nextInt(4) == 0 ? null : "v" + r.nextInt(3);
        int identity = java.util.Arrays.hashCode(key) * 31 + (version == null ? 0 : version.hashCode());
        if (kind <= 2) {
            return new ObjectEntry(KeyBytes.of(key), Math.floorMod(identity, 3), 0L, null, null,
                    version, version != null && identity % 2 == 0, null, null, null, null);
        }
        if (kind == 3) {
            return new DeleteMarkerEntry(KeyBytes.of(key), version, identity % 2 == 0, 0L, null);
        }
        // CommonPrefixEntry: a third row_type at the same key, so range assignment must keep
        // cross-row_type rows together as well as versions.
        return new CommonPrefixEntry(KeyBytes.of(key));
    }

    private List<ListEntry> readAll(List<Path> files) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader reader = new SegmentReader(f)) {
                while (reader.hasNext()) {
                    out.add(reader.next());
                }
            }
        }
        return out;
    }

    private void assertKeyAtomicAndStrictlyDisjoint(List<Path> files) throws IOException {
        byte[] previousMax = null;
        for (Path file : files) {
            List<ListEntry> rows = readAll(List.of(file));
            assertThat(rows).isNotEmpty();
            byte[] min = rows.getFirst().key().rawUnsafe();
            byte[] max = rows.getLast().key().rawUnsafe();
            assertThat(rows).allSatisfy(row ->
                    assertThat(KeyBytes.compareUnsigned(row.key().rawUnsafe(), min)).isZero());
            if (previousMax != null) {
                assertThat(KeyBytes.compareUnsigned(previousMax, min))
                        .as("adjacent rolled page-run files have strict maxKey < minKey")
                        .isNegative();
            }
            previousMax = max;
        }
    }

    /** {@link SortMetrics} that counts engagement reasons; ranges record from several threads. */
    private static final class CountingMetrics implements SortMetrics {
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

        @Override
        public void recordPageAwareOverlapCluster() {
        }

        @Override
        public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        }

        @Override
        public void recordRangeIndexBytes(long bytes) {
        }

        long count(String key) {
            LongAdder a = counts.get(key);
            return a == null ? 0 : a.sum();
        }
    }

    private static long stagedBytes(List<Path> segments) throws IOException {
        long total = 0;
        for (Path segment : segments) {
            total += Files.size(segment);
        }
        return total;
    }

    private static ListAppender<ILoggingEvent> attachTransformLog() {
        Logger logger = (Logger) LoggerFactory.getLogger(SortTransform.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private static void detachTransformLog(ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(SortTransform.class)).detachAppender(appender);
        appender.stop();
    }

    private static void assertNoOwnedDebris(Path staging) throws IOException {
        for (String glob : List.of(StagingNames.RANGE_TMP_GLOB,
                StagingNames.RANGE_LEGACY_CASCADE_PARQUET_GLOB,
                StagingNames.RANGE_CASCADE_PAGE_RUN_GLOB,
                StagingNames.RANGE_PROOF_TMP_GLOB)) {
            try (var files = Files.newDirectoryStream(staging, glob)) {
                assertThat(files.iterator().hasNext()).as("no debris matching %s", glob).isFalse();
            }
        }
    }

    private static void assertNoLiveWorkers(String prefix) {
        assertThat(Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith(prefix)))
                .isEmpty();
    }

    /** Writer double with deterministic failure/blocking modes and explicit close accounting. */
    private static final class TrackingWriterFactory implements SortedFileWriterFactory {
        private final CountDownLatch writerReached;
        private final CountDownLatch releaseSibling;
        private final boolean blockAll;
        private final long bytesPerRow;
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();
        private final AtomicBoolean cooperativelyCancelled = new AtomicBoolean();

        TrackingWriterFactory(CountDownLatch writerReached, CountDownLatch releaseSibling) {
            this.writerReached = writerReached;
            this.releaseSibling = releaseSibling;
            this.blockAll = false;
            this.bytesPerRow = 1;
        }

        TrackingWriterFactory(long bytesPerRow) {
            this.writerReached = null;
            this.releaseSibling = null;
            this.blockAll = false;
            this.bytesPerRow = bytesPerRow;
        }

        TrackingWriterFactory(CountDownLatch writerReached) {
            this.writerReached = writerReached;
            this.releaseSibling = null;
            this.blockAll = true;
            this.bytesPerRow = 1;
        }

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            opened.incrementAndGet();
            openNow.incrementAndGet();
            boolean laterFailingRange = path.getFileName().toString().startsWith("prange-1-");
            return new SortedFileWriter() {
                private long rows;
                private final AtomicBoolean isClosed = new AtomicBoolean();

                @Override
                public void write(ListEntry e) throws IOException {
                    if (blockAll) {
                        writerReached.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException("writer interrupted", interrupted);
                        }
                    } else if (writerReached != null && laterFailingRange) {
                        try {
                            if (!writerReached.await(2, TimeUnit.SECONDS)) {
                                throw new IOException("earlier range never started writing");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException("failing writer interrupted", interrupted);
                        }
                        throw new IOException("injected later range failure");
                    } else if (writerReached != null) {
                        writerReached.countDown();
                        while (!Thread.currentThread().isInterrupted()) {
                            LockSupport.park();
                        }
                        // Return normally with the flag still set. RolledPartWriter's next safe-point
                        // check, not this writer, must perform cooperative cancellation.
                        cooperativelyCancelled.set(true);
                    }
                    rows++;
                }

                @Override
                public long rows() {
                    return rows;
                }

                @Override
                public long dataSize() {
                    return rows * bytesPerRow;
                }

                @Override
                public void setFileIndex(int ignored) {
                }

                @Override
                public void close() throws IOException {
                    if (isClosed.compareAndSet(false, true)) {
                        try {
                            channel.close();
                        } finally {
                            openNow.decrementAndGet();
                            closed.incrementAndGet();
                        }
                    }
                }
            };
        }
    }

    /** Counts the real final writers used by the perf fixture without changing their roll behavior. */
    private static final class CountingWriterFactory implements SortedFileWriterFactory {
        private final SortedFileWriterFactory delegate;
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();

        CountingWriterFactory(SortedFileWriterFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            SortedFileWriter writer = delegate.create(path, fileIndex);
            opened.incrementAndGet();
            openNow.incrementAndGet();
            return new SortedFileWriter() {
                private final AtomicBoolean isClosed = new AtomicBoolean();

                @Override
                public void write(ListEntry entry) throws IOException {
                    writer.write(entry);
                }

                @Override
                public long rows() {
                    return writer.rows();
                }

                @Override
                public long dataSize() {
                    return writer.dataSize();
                }

                @Override
                public void markFinal() {
                    writer.markFinal();
                }

                @Override
                public void setFileIndex(int index) {
                    writer.setFileIndex(index);
                }

                @Override
                public Optional<FinalPartMetadata> finalMetadata() {
                    return writer.finalMetadata();
                }

                @Override
                public void close() throws IOException {
                    if (isClosed.compareAndSet(false, true)) {
                        try {
                            writer.close();
                        } finally {
                            openNow.decrementAndGet();
                            closed.incrementAndGet();
                        }
                    }
                }
            };
        }
    }

    private static final class SelfInterruptingWriterFactory implements SortedFileWriterFactory {
        private final AtomicInteger openNow = new AtomicInteger();

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            openNow.incrementAndGet();
            return new SortedFileWriter() {
                private long rows;
                private boolean closed;

                @Override
                public void write(ListEntry e) {
                    rows++;
                    Thread.currentThread().interrupt();
                }

                @Override
                public long rows() {
                    return rows;
                }

                @Override
                public long dataSize() {
                    return rows;
                }

                @Override
                public void setFileIndex(int ignored) {
                }

                @Override
                public void close() throws IOException {
                    if (!closed) {
                        closed = true;
                        try {
                            channel.close();
                        } finally {
                            openNow.decrementAndGet();
                        }
                    }
                }
            };
        }
    }

}
