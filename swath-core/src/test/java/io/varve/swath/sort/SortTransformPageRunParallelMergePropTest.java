/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * PROP guard for the parallel range merge over <b>page-run</b> staging — the live listing lane's
 * format, and therefore the only one that decides whether {@code swath.sort.merge-parallelism}
 * does anything on a real run.
 *
 * <p>{@link SortTransformParallelMergePropTest} is the sibling guard over columnar Parquet staging.
 * The two paths differ in every mechanism that could lose a row — a page skip
 * ({@link RangeScopedPageFrontier}) instead of a row-group skip, {@link PageAwareMerger}'s
 * decode-free page-whole fast path instead of the entry-typed {@link StreamingMerger}, the
 * {@code [lo, hi)} trim applied ABOVE the merge ({@link RangeFilteredCursor}) instead of around each
 * input, and page-run cascade intermediates instead of Parquet ones — so the Parquet guard says
 * nothing about this one.
 *
 * <p>The core property is the same and is checked against TWO independent oracles: byte-exact
 * equivalence to the serial merge of the same segments, and the exact input multiset (so two
 * symmetric bugs that both drop the same row cannot pass).
 */
class SortTransformPageRunParallelMergePropTest {

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
            deleteRecursively(root);
        }
    }

    /**
     * Drive {@link ParallelRangeMerge} directly, past {@link SortTransform}'s clamp, so the cascade
     * branches are genuinely reached — see the twin in {@code SortTransformParallelMergePropTest}.
     * Cascade intermediates on this path keep the page-run format, which is what makes it worth
     * exercising separately from the columnar one.
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
        SortRun run = new SortRun(config, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT);
        ParallelRangeMerge merge = new ParallelRangeMerge(run, RangeMergeTimer.NO_OP);
        List<byte[]> boundaries = ParallelRangeMerge.boundaries(segs, ranges, SortMetrics.NO_OP);
        if (boundaries == null) {
            return null;
        }
        List<ParallelRangeMerge.RangeResult> results =
                merge.run(segs, staging, boundaries, units -> { });

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

    private record CascadeRun(List<Path> parts, long rows, long cascadedPasses) {
    }

    /**
     * Rolled output: each range also splits into several parts. Stacks intra-range file boundaries on
     * top of the inter-range ones — the whole {@code part-00001..N} sequence must still be the exact
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
            assertThat(parallel.finalFiles())
                    .as("one part per range").hasSize(ranges);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());
            assertThat(parallelRows).as("exact input multiset").containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("position-for-position equal to serial")
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            deleteRecursively(root);
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
        try {
            // Price an open page-run stream exactly as the merge does — the trailer's maxRecordLen —
            // so the budget below is the real bound rather than a guess that could drift with the
            // page format.
            Path probeDir = Files.createDirectories(root.resolve("probe"));
            long perStream = 0;
            for (Path seg : stage(probeDir, s.segments())) {
                perStream = Math.max(perStream, PageRunSegmentReader.readTrailer(seg).maxRecordLen());
            }
            long budget = perStream * segmentCount * allowed;

            CountingMetrics metrics = new CountingMetrics();
            SortTransformResult serial =
                    run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE, DuplicateHook.NO_OP,
                            SortMetrics.NO_OP);
            SortTransformResult clamped =
                    run(s, 8, root, "clamped", Long.MAX_VALUE, budget, DuplicateHook.NO_OP, metrics);

            assertThat(metrics.count("SORT.merge_range_clamped"))
                    .as("the clamp bit, and the run said so").isEqualTo(1);
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
            deleteRecursively(root);
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
            assertThat(metrics.count("SORT.merge_range_unsplittable"))
                    .as("and the decline is visible in the metrics").isEqualTo(1);
            assertThat(fellBack.finalFiles())
                    .as("serial output shape").hasSize(serial.finalFiles().size());
            assertThat(readAll(fellBack.finalFiles()))
                    .containsExactlyElementsOf(readAll(serial.finalFiles()));
        } finally {
            deleteRecursively(root);
        }
    }

    /**
     * The completeness stamp, on the LIVE staging format and with ranges that ROLL. This is the case
     * the stamp exists for and the one the columnar sibling test cannot reach: several ranges each
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
            SortTransform transform = new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, new SortedParquetWriterFactory(config, SortMode.OBJECTS)));
            SortTransformResult result =
                    transform.transform(segs, output, staging, PublishListener.NO_OP);

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
            deleteRecursively(root);
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
            // Non-vacuity, conditioned on the input: equal counts are only meaningful when there was
            // something to count. A tiny generated case can legitimately contain no equal-comparing
            // pair at all, so require a positive baseline exactly when the input has one.
            List<ListEntry> sorted = new ArrayList<>(s.allEntries());
            sorted.sort(cmp);
            long expectedPairs = 0;
            for (int i = 1; i < sorted.size(); i++) {
                if (cmp.compare(sorted.get(i - 1), sorted.get(i)) == 0) {
                    expectedPairs++;
                }
            }
            if (expectedPairs > 0) {
                assertThat(serialHits.get())
                        .as("input holds %d equal-comparing pair(s), so the hook must have fired",
                                expectedPairs)
                        .isPositive();
            }
        } finally {
            deleteRecursively(root);
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
     * not, because {@link ParallelRangeMerge#boundaries} drains every page of every segment to EOF
     * while sampling boundary keys, and that pass runs the cross-check before any range opens. This
     * test therefore pins the END-TO-END guarantee (corrupt trailer ⇒ merge fails) rather than any
     * one mechanism; it would still pass if the sampling pass were replaced, provided whatever
     * replaced it kept the check. Note the consequence for anyone optimising the sampler: making it
     * stop early — for instance by seeking rather than walking — would remove this check, and the
     * guarantee would then rest solely on the last range draining its inputs.
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
            SortTransform transform = new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
            assertThatThrownBy(() -> transform.transform(segs, Files.createDirectories(
                    root.resolve("parallel").resolve("data")), staging, PublishListener.NO_OP))
                    .isInstanceOf(IOException.class);
        } finally {
            deleteRecursively(root);
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
                assertThat(names.get(i)).isEqualTo(String.format("part-%05d.parquet", i + 1));
            }
        } finally {
            deleteRecursively(root);
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
        SortTransform transform = new SortTransform(
                new SortRun(config, cmp, hook, metrics, SortedFileWriterFactory.DEFAULT));
        return transform.transform(segs, output, staging, PublishListener.NO_OP);
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

    /** {@link SortMetrics} that counts engagement reasons; ranges record from several threads. */
    private static final class CountingMetrics implements SortMetrics {
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

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup of a test temp tree
                }
            });
        }
    }
}
