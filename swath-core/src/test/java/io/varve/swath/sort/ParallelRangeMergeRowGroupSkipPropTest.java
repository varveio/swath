/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * ADVERSARIAL, byte-exact PROP guard for the row-group skip in the parallel range-merge
 * path ({@code swath.sort.merge-parallelism > 1}). Row-group skip is the <b>silent-data-loss</b> risk of the
 * feature: if a key-range wrongly prunes a physical Parquet row group that still holds in-range keys,
 * those rows vanish with NO error. This guard is the primary defense.
 *
 * <p><b>What it attacks.</b> Each range reads only the physical row groups overlapping its
 * {@code [lo, hi)} ({@link ParallelRangeMerge#selectRowGroups} — the pure decision function — feeding a
 * block-scoped {@link SegmentReader}), with {@link RangeFilteredStream} trimming straddling boundary
 * groups per row. The properties below are:
 *
 * <ol>
 *   <li><b>Byte-exact equivalence with skip FORCED to engage</b> ({@link
 *       #rowGroupSkipDropsNoRowAndIsByteExactToSerial}): staging segments are written with a tiny
 *       {@code segmentRowGroupBytes} (4096) and ~200-byte keys so every segment has MANY row groups,
 *       then the SAME input is run serial (parallelism=1, NO skip) and parallel (R groups, skip
 *       engaged). The parallel output must equal (a) the exact input MULTISET (the anti-silent-loss
 *       property, checked against the original input as independent ground truth), (b) globally
 *       ascending, and (c) — with globally unique keys — position-for-position identical to serial.
 *       Engagement is asserted via the {@code SORT/merge_range_rowgroup_skipped} counter.</li>
 *   <li><b>Direct attack on {@link ParallelRangeMerge#selectRowGroups}</b> ({@link
 *       #selectRowGroupsNeverExcludesAnOverlappingGroup}): for random spans (with physical-block gaps
 *       simulating omitted empty groups) and random {@code [lo, hi)}, EVERY group whose key interval
 *       overlaps {@code [lo, hi)} must be in the returned block indices — checked against an
 *       independent interval-overlap ground truth, no Parquet I/O.</li>
 *   <li><b>Non-tautology proofs</b> ({@link #theOverlapCheckerCatchesAShrunkSelection},
 *       {@link #aWrongSkipDropsInRangeRowsAtTheParquetLevel}): a deliberately-wrong skip (covered span
 *       shrunk by one) is shown to LOSE rows — proving the guard has teeth.</li>
 * </ol>
 *
 * <p>jqwik has no {@code @TempDir}, so each I/O method creates and recursively deletes its own temp
 * root; a deterministic {@code seed} makes every failing case reproducible.
 */
class ParallelRangeMergeRowGroupSkipPropTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** Tiny row-group size (bytes) for staging segments ⇒ MANY physical row groups per segment. */
    private static final long TINY_ROW_GROUP_BYTES = 4096L;
    /** Per-key padding so ~200-byte keys blow past {@link #TINY_ROW_GROUP_BYTES} within a few rows. */
    private static final int PAD = 200;

    enum KeyStyle {
        /** {@code %08d}: dense, adjacent — sampled boundaries land exactly on real group first-keys. */
        DENSE,
        /** {@code %019d} of a random long: sparse, big gaps between adjacent keys. */
        SPARSE,
        /** {@code c<0..2>-<nnnnnnnn>}: dense runs separated by cluster gaps — boundary-adjacent keys. */
        CLUSTERED,
        /** {@code 0x00}/{@code 0xFF} prefixes + shared prefixes + empty key — raw S3 byte order. */
        ADVERSARIAL_BYTES
    }

    private static final byte[][] BIN_PREFIX = {
            new byte[]{},
            new byte[]{0},
            new byte[]{0, 0},
            new byte[]{0, (byte) 0xFF},
            new byte[]{(byte) 0xFF},
            new byte[]{(byte) 0xFF, 0},
            new byte[]{(byte) 0xFF, (byte) 0xFF},
    };

    // =====================================================================
    // Property 1: byte-exact, skip forced to engage, many randomized inputs.
    // =====================================================================

    @Property(tries = 14)
    void rowGroupSkipDropsNoRowAndIsByteExactToSerial(
            @ForAll @IntRange(min = 2, max = 4) int segmentCount,
            @ForAll @IntRange(min = 120, max = 280) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        Random r = new Random(seed);
        List<byte[]> keys = uniqueKeys(style, entryCount, r);
        Scenario s = scenario(keys, segmentCount, r);
        SkipOutcome o = assertRowGroupSkipByteExact(s, ranges);
        // The moment the keyspace actually split into >1 range over multi-row-group segments, the skip
        // MUST have pruned at least one group somewhere — otherwise it is not being exercised at all.
        if (o.parallelFiles() > 1) {
            assertThat(o.skipEngaged())
                    .as("row-group skip never engaged despite %d parallel ranges (seed=%d, style=%s)",
                            o.parallelFiles(), seed, style)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /** Range count ∈ {2,3,5} exactly, every key style, with sizes that GUARANTEE many row groups + engagement. */
    @Example
    void skipEngagesAndIsByteExactAcrossStylesAndRangeCounts() throws IOException {
        for (KeyStyle style : KeyStyle.values()) {
            Random r = new Random(0xB0DACAFEL + style.ordinal());
            List<byte[]> keys = uniqueKeys(style, 300, r);
            Scenario s = scenario(keys, 3, r);
            for (int ranges : new int[]{2, 3, 5}) {
                SkipOutcome o = assertRowGroupSkipByteExact(s, ranges);
                assertThat(o.skipEngaged())
                        .as("skip must engage for style=%s R=%d", style, ranges)
                        .isGreaterThanOrEqualTo(1);
            }
        }
    }

    /** Degenerate segment shapes mixed together: empty segment + single-row-group segment + many-group. */
    @Example
    void degenerateSegmentShapesLoseNoRow() throws IOException {
        Random r = new Random(0x5E6E17L);
        List<byte[]> many = uniqueKeys(KeyStyle.DENSE, 250, r);
        List<List<ListEntry>> segments = new ArrayList<>();
        segments.add(entries(many));                                   // many row groups
        segments.add(entries(List.of(kb("zzz-000"), kb("zzz-001"), kb("zzz-002"))));  // single row group
        segments.add(new ArrayList<>());                              // empty segment (no row groups)
        List<ListEntry> input = flatten(segments);
        Scenario s = new Scenario(segments, input);
        for (int ranges : new int[]{2, 3, 5}) {
            SkipOutcome o = assertRowGroupSkipByteExact(s, ranges);
            assertThat(o.skipEngaged()).as("multi-group segment ⇒ skip engages, R=%d", ranges)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    // =====================================================================
    // Property 2: direct attack on the pure decision function selectRowGroups.
    // =====================================================================

    @Property(tries = 600)
    void selectRowGroupsNeverExcludesAnOverlappingGroup(
            @ForAll @IntRange(min = 1, max = 10) int groupCount,
            @ForAll long seed) {
        Random r = new Random(seed);
        List<SortedFileIndex.RowGroupSpan> spans = randomSpans(groupCount, r);
        byte[][] bounds = randomBounds(spans, r);
        byte[] lo = bounds[0];
        byte[] hi = bounds[1];

        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, lo, hi);

        // THE anti-silent-loss invariant: every group whose key interval overlaps [lo,hi) is selected.
        assertThat(missingOverlappingBlocks(spans, lo, hi, sel.blockIndices()))
                .as("selectRowGroups pruned an OVERLAPPING row group — SILENT DATA LOSS "
                        + "(seed=%d, lo=%s, hi=%s)", seed, show(lo), show(hi))
                .isEmpty();

        // Read/skip accounting vs the independent interval-overlap count. The selection is
        // CONSERVATIVE: to never drop a key that overflows/straddles a boundary it is
        // predecessor-inclusive at the lo bound, so it may read up to ONE extra group beyond the
        // interval-exact overlap — but NEVER fewer (the anti-loss direction). Read+skipped still
        // partition every group.
        int overlap = overlappingBlocks(spans, lo, hi).size();
        assertThat(sel.groupsRead()).as("groupsRead >= interval overlap (never under-reads — anti-loss)")
                .isGreaterThanOrEqualTo(overlap);
        assertThat(sel.groupsRead()).as("conservative over-read is at most one predecessor group")
                .isLessThanOrEqualTo(overlap + 1);
        assertThat(sel.groupsRead() + sel.groupsSkipped()).as("groupsRead + groupsSkipped == all groups")
                .isEqualTo(groupCount);

        // Selection is a contiguous physical block span (conservative may over-read, never under-read).
        int[] blocks = sel.blockIndices();
        for (int i = 1; i < blocks.length; i++) {
            assertThat(blocks[i]).isEqualTo(blocks[i - 1] + 1);
        }
    }

    // =====================================================================
    // Non-tautology proof (pure): the overlap checker REJECTS a wrong skip.
    // =====================================================================

    @Example
    void theOverlapCheckerCatchesAShrunkSelection() {
        List<SortedFileIndex.RowGroupSpan> spans = List.of(span("a", 0), span("d", 1), span("g", 2));

        // Correct unbounded selection reads every group and passes the anti-loss checker.
        ParallelRangeMerge.RangeSelection real = ParallelRangeMerge.selectRowGroups(spans, null, null);
        assertThat(real.blockIndices()).containsExactly(0, 1, 2);
        assertThat(missingOverlappingBlocks(spans, null, null, real.blockIndices())).isEmpty();

        // Wrong skip A — covered span shrunk by one (drop lastCovered): loses overlapping group 2.
        assertThat(missingOverlappingBlocks(spans, null, null, new int[]{0, 1}))
                .as("checker must flag the dropped overlapping group").containsExactly(2);

        // Wrong skip B — firstCovered made EXCLUSIVE at a lo boundary: loses the group that owns lo.
        // The selection is predecessor-inclusive at lo (conservative), so lo=d reads {0,1,2} — a
        // SUPERSET of the interval-overlapping {1,2}; the anti-loss check is that no overlapping group
        // is missing (it over-reads block 0, which RangeFilteredStream trims per-row).
        ParallelRangeMerge.RangeSelection loSel = ParallelRangeMerge.selectRowGroups(spans, key("d"), null);
        assertThat(loSel.blockIndices()).as("conservative lo=d selection is a superset of the overlap")
                .containsExactly(0, 1, 2);
        assertThat(missingOverlappingBlocks(spans, key("d"), null, loSel.blockIndices()))
                .as("conservative selection never misses an interval-overlapping group").isEmpty();
        assertThat(missingOverlappingBlocks(spans, key("d"), null, new int[]{2}))
                .as("dropping the lo-owning group is silent loss").contains(1);
    }

    // =====================================================================
    // Non-tautology proof (real Parquet I/O): a wrong skip drops in-range rows.
    // =====================================================================

    @Example
    void aWrongSkipDropsInRangeRowsAtTheParquetLevel() throws IOException {
        Path root = Files.createTempDirectory("rgskip-mutant-");
        try {
            List<ListEntry> rows = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                rows.add(objOf(denseKey(i)));
            }
            Path seg = writeMultiGroupSegment(root, "seg.parquet", rows);
            List<SortedFileIndex.RowGroupSpan> spans = SortedFileIndex.rowGroupSpans(seg);
            assertThat(spans.size()).as("tiny row groups ⇒ many physical groups").isGreaterThan(2);

            byte[] lo = denseKey(90);
            byte[] hi = denseKey(210);   // an interior range straddling several groups
            ParallelRangeMerge.RangeSelection real = ParallelRangeMerge.selectRowGroups(spans, lo, hi);
            assertThat(real.blockIndices().length).as("range straddles multiple groups").isGreaterThan(1);

            // Ground truth: whole-file (null blocks) range-filtered — the rows the range MUST emit.
            List<String> whole = readRange(seg, null, lo, hi);
            // Real skip must be byte-identical to reading every group.
            assertThat(readRange(seg, real.blockIndices(), lo, hi))
                    .as("row-group skip is byte-identical to reading every group").isEqualTo(whole);

            // Wrong skip: drop the LAST covered physical block ⇒ silent loss of its in-range rows.
            int[] shrunk = Arrays.copyOf(real.blockIndices(), real.blockIndices().length - 1);
            List<String> shrunkRows = readRange(seg, shrunk, lo, hi);
            assertThat(shrunkRows).as("wrong skip loses in-range rows (silent data loss)").isNotEqualTo(whole);
            assertThat(shrunkRows.size()).isLessThan(whole.size());
            assertThat(whole).containsAll(shrunkRows);   // the loss is pure subtraction, never garbage
        } finally {
            deleteRecursively(root);
        }
    }

    // =====================================================================
    // Property 3 (DUPLICATE-FIRST-KEY silent loss): swath allows MANY rows per key
    // (versions / cross row_types), so one key can OVERFLOW consecutive physical row groups, making
    // two adjacent groups share the SAME decoded firstKey (e.g. spans [a@0, d@1, d@2, g@3]). A
    // firstCovered rule of "largest i with firstKey_i <= lo" SKIPS the earlier equal-first-key blocks
    // when lo lands on that key — silent data loss; do not select rows this way. Property 2's
    // generator could not catch this: its TreeSet forces DISTINCT first keys, and its interval oracle
    // ([k_i,k_{i+1})) is itself WRONG for duplicates (it calls the middle equal-first-key block
    // "empty"). The properties below use a ROW-CONTENT ground truth instead: each physical block
    // carries its ACTUAL key-set, and the correct selection is every block that holds ANY key in
    // [lo,hi). Bounds are drawn from a pool INCLUDING the duplicated keys — the exact hotspot.
    // =====================================================================

    /** A physical row group paired with the ACTUAL distinct keys its rows hold (row-content truth). */
    private record BlockContent(SortedFileIndex.RowGroupSpan span, List<byte[]> keys) {
    }

    @Property(tries = 800)
    void selectRowGroupsNeverDropsABlockHoldingAnInRangeKeyWithDuplicateFirstKeys(
            @ForAll @IntRange(min = 2, max = 7) int distinctValues,
            @ForAll long seed) {
        Random r = new Random(seed);
        List<BlockContent> blocks = dupFirstKeyBlocks(distinctValues, r);
        List<SortedFileIndex.RowGroupSpan> spans = blocks.stream().map(BlockContent::span).toList();
        byte[][] bounds = dupBounds(blocks, r);
        byte[] lo = bounds[0];
        byte[] hi = bounds[1];

        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, lo, hi);

        // ROW-CONTENT anti-loss invariant: every block that actually holds a key in [lo,hi) is read.
        assertThat(missingContentBlocks(blocks, lo, hi, sel.blockIndices()))
                .as("selectRowGroups DROPPED a block holding an in-range key — DUPLICATE-FIRST-KEY "
                        + "silent data loss (seed=%d, lo=%s, hi=%s)", seed, show(lo), show(hi))
                .isEmpty();

        // Still a contiguous physical span (conservative may over-read a straddle group, never under).
        int[] blk = sel.blockIndices();
        for (int i = 1; i < blk.length; i++) {
            assertThat(blk[i]).isEqualTo(blk[i - 1] + 1);
        }
    }

    /**
     * The generator MUST actually exercise the duplicate-first-key class (else the property above is
     * vacuous re: the bug) AND the rejected firstCovered rule MUST drop a content-covered block on
     * some case (else the guard has no teeth). This scans deterministic seeds and asserts both are
     * reachable.
     */
    @Example
    void theDuplicateFirstKeyClassIsReachableAndThePreFixRuleLosesRows() {
        int seedsWithDuplicateFirstKeys = 0;
        int seedsWherePreFixDropsAContentBlock = 0;
        for (long seed = 0; seed < 400; seed++) {
            Random r = new Random(0xD00D_0000L + seed);
            List<BlockContent> blocks = dupFirstKeyBlocks(3 + (int) (seed % 5), r);
            List<SortedFileIndex.RowGroupSpan> spans = blocks.stream().map(BlockContent::span).toList();
            if (hasDuplicateFirstKeys(spans)) {
                seedsWithDuplicateFirstKeys++;
            }
            byte[][] bounds = dupBounds(blocks, r);
            int[] preFix = preFixOldSelection(spans, bounds[0], bounds[1]);
            if (!missingContentBlocks(blocks, bounds[0], bounds[1], preFix).isEmpty()) {
                seedsWherePreFixDropsAContentBlock++;
            }
        }
        assertThat(seedsWithDuplicateFirstKeys)
                .as("generator must produce duplicate first keys (the whole point of this class)")
                .isGreaterThan(0);
        assertThat(seedsWherePreFixDropsAContentBlock)
                .as("the prior firstCovered rule must silently drop a content-covered block "
                        + "somewhere — proving the row-content oracle has teeth the interval oracle lacked")
                .isGreaterThan(0);
    }

    /**
     * Concrete, self-contained RED analog. Spans model key {@code d} overflowing/straddling three
     * consecutive blocks: block0 {@code [a,d]} (fk=a), block1 {@code [d]} (fk=d), block2 {@code [d,g]}
     * (fk=d) — blocks 1 and 2 share firstKey {@code d}. A range {@code lo=d} must cover ALL THREE
     * (every one holds {@code d}). The FIXED product rule does. The rejected firstCovered rule keeps
     * only block2, silently dropping the {@code d} rows in blocks 0 and 1 — and the interval oracle
     * is itself blind to block1, which is exactly why the unique-key guard could never catch this.
     */
    @Example
    void aDuplicateFirstKeyRunIsNeverPrunedAtItsLoBoundary() {
        List<BlockContent> blocks = List.of(
                new BlockContent(span("a", 0), keys("a", "d")),   // straddle: 'a' + first 'd' rows
                new BlockContent(span("d", 1), keys("d")),        // all-'d' overflow group
                new BlockContent(span("d", 2), keys("d", "g")));  // trailing 'd' rows + 'g'
        List<SortedFileIndex.RowGroupSpan> spans = blocks.stream().map(BlockContent::span).toList();
        byte[] lo = key("d");

        // FIXED product rule: covers every block holding 'd'.
        ParallelRangeMerge.RangeSelection sel = ParallelRangeMerge.selectRowGroups(spans, lo, null);
        assertThat(sel.blockIndices()).as("fixed rule reads the whole duplicate-first-key run")
                .containsExactly(0, 1, 2);
        assertThat(missingContentBlocks(blocks, lo, null, sel.blockIndices()))
                .as("fixed rule loses no in-range block").isEmpty();

        // RED teeth: the rejected firstCovered rule ("largest i with firstKey_i <= lo") keeps only
        // block 2 and SILENTLY DROPS blocks 0 and 1 — both of which hold in-range 'd' rows.
        int[] preFix = preFixOldSelection(spans, lo, null);
        assertThat(preFix).as("prior rule starts at the LAST equal-first-key block").containsExactly(2);
        assertThat(missingContentBlocks(blocks, lo, null, preFix))
                .as("prior firstCovered rule silently drops the earlier equal-first-key blocks")
                .containsExactly(0, 1);

        // The interval oracle (Property 2's ground truth) is itself blind to block 1 here: it
        // models block1 as [d,d) = empty, so it could never have flagged this loss. This is why
        // extending the oracle to row-content was necessary.
        assertThat(overlappingBlocks(spans, lo, null))
                .as("interval oracle wrongly treats the middle equal-first-key block as empty")
                .doesNotContain(1);
    }

    // =====================================================================
    // Property 4 (end-to-end): a single key with MANY distinct-version rows overflows
    // >=2 physical row groups; parallel merge WITH skip must lose no row of it across R in {2,3,5}.
    // =====================================================================

    @Example
    void aKeySpanningMultipleRowGroupsLosesNoRowAcrossR() throws IOException {
        Random r = new Random(0x5CA17ED253L);
        int[] hotOrdinals = {40, 90, 150};   // interior of the %08d dense background — boundaries can land here
        int hotVersions = 300;               // ~206-byte distinct versionIds ⇒ each hot key overflows >=2 groups
        // (Parquet only checks row-group size after ~100 buffered rows, so a hot key needs well over
        // that many rows before its bytes actually roll into a second physical group.)

        // Sanity: a hot key on its own genuinely overflows >=2 physical row groups (not vacuous).
        Path probe = Files.createTempDirectory("dup-e2e-probe-");
        try {
            byte[] hot = denseKey(hotOrdinals[0]);
            Path seg = writeMultiGroupSegment(probe, "hot.parquet", hotRows(hot, hotVersions));
            long spansWithHotFirstKey = SortedFileIndex.rowGroupSpans(seg).stream()
                    .filter(s -> KeyBytes.compareUnsigned(s.firstKey(), hot) == 0).count();
            assertThat(spansWithHotFirstKey)
                    .as("a multi-version key must overflow >=2 physical row groups (duplicate first keys)")
                    .isGreaterThanOrEqualTo(2);
        } finally {
            deleteRecursively(probe);
        }

        // Corpus: unique-key background spread across 3 segments + hot keys, each hot key's rows kept
        // together in ONE segment so they stay consecutive and overflow that segment's row groups.
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> input = new ArrayList<>();
        for (byte[] k : uniqueKeys(KeyStyle.DENSE, 180, r)) {
            ListEntry e = objOf(k);
            input.add(e);
            segs.get(r.nextInt(3)).add(e);
        }
        for (int idx = 0; idx < hotOrdinals.length; idx++) {
            byte[] hot = denseKey(hotOrdinals[idx]);
            List<ListEntry> rows = hotRows(hot, hotVersions);
            input.addAll(rows);
            segs.get(idx % 3).addAll(rows);   // whole hot run in one segment
        }
        Scenario s = new Scenario(segs, input);

        for (int ranges : new int[]{2, 3, 5}) {
            SkipOutcome o = assertRowGroupSkipByteExact(s, ranges);
            // Distinct versionIds ⇒ the multi-version key's rows are fully ordered (no equal-comparing
            // ties), so assertRowGroupSkipByteExact's strict position-for-position check to the serial
            // run holds AND the exact-input-multiset check proves no hot-key row was dropped.
            assertThat(o.skipEngaged())
                    .as("row-group skip must engage for the multi-group-key corpus at R=%d", ranges)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    // =====================================================================
    // Harness
    // =====================================================================

    private record Scenario(List<List<ListEntry>> segments, List<ListEntry> input) {
    }

    private record SkipOutcome(long skipEngaged, int parallelFiles) {
    }

    /** Run serial (R=1, no skip) and parallel (R, skip) over the SAME scenario; assert byte-exact. */
    private SkipOutcome assertRowGroupSkipByteExact(Scenario s, int ranges) throws IOException {
        Path root = Files.createTempDirectory("rgskip-prop-");
        try {
            SortTransformResult serial = run(s.segments(), 1, root, "serial", new ThreadSafeMetrics());
            ThreadSafeMetrics pm = new ThreadSafeMetrics();
            SortTransformResult parallel = run(s.segments(), ranges, root, "parallel", pm);

            List<ListEntry> input = s.input();
            List<ListEntry> serialRows = readAll(serial.finalFiles());
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());

            // Row-count conservation, both paths.
            assertThat(serial.totalRows()).as("serial rows out == in").isEqualTo(input.size());
            assertThat(parallel.totalRows()).as("parallel rows out == in").isEqualTo(input.size());

            // Serial is a correct, complete sort of the input (independent ground truth).
            assertThat(serialRows).as("serial ascending").isSortedAccordingTo(cmp);
            assertThat(serialRows).as("serial == exact input multiset")
                    .containsExactlyInAnyOrderElementsOf(input);

            // Parallel (WITH skip) must ALWAYS be a valid, complete global sort of the input.
            assertThat(parallelRows).as("parallel (skip) ascending — no reorder").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("parallel (skip) == exact input multiset — NO ROW LOST/DUPLICATED")
                    .containsExactlyInAnyOrderElementsOf(input);

            // Unique keys ⇒ position-for-position byte-exact to serial (no cross-stream tie ambiguity).
            assertThat(parallelRows).as("parallel (skip) byte-exact to serial position-for-position")
                    .containsExactlyElementsOf(serialRows);

            assertSequentialPartNames(parallel.finalFiles());
            return new SkipOutcome(pm.count("SORT.merge_range_rowgroup_skipped"), parallel.finalFiles().size());
        } finally {
            deleteRecursively(root);
        }
    }

    private SortTransformResult run(List<List<ListEntry>> segments, int parallelism, Path root,
                                    String name, SortMetrics metrics) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segs = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            segs.add(writeMultiGroupSegment(staging, "seg-" + i + ".parquet", segments.get(i)));
        }
        SortTransform transform = new SortTransform(new SortRun(config(parallelism), cmp, DuplicateHook.NO_OP, metrics, SortedFileWriterFactory.DEFAULT));
        return transform.transform(segs, output, staging, PublishListener.NO_OP);
    }

    /** Large fan-in/budget ⇒ no cascade; single final file per range (finalFileBytes = MAX). */
    private SortConfig config(int mergeParallelism) {
        return SortConfigs.base().withMergeParallelism(mergeParallelism);
    }

    /** Write the entries as ONE sorted segment with TINY row groups ⇒ many physical row groups. */
    private Path writeMultiGroupSegment(Path dir, String name, List<ListEntry> entries) throws IOException {
        List<ListEntry> sorted = new ArrayList<>(entries);
        sorted.sort(cmp);
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, TINY_ROW_GROUP_BYTES);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    private List<String> readRange(Path seg, int[] blocks, byte[] lo, byte[] hi) throws IOException {
        List<String> out = new ArrayList<>();
        try (RangeFilteredStream s = new RangeFilteredStream(new SegmentReader(seg, blocks), lo, hi)) {
            while (s.hasNext()) {
                out.add(s.next().key().asString());
            }
        }
        return out;
    }

    private List<ListEntry> readAll(List<Path> files) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader r = new SegmentReader(f)) {
                while (r.hasNext()) {
                    out.add(r.next());
                }
            }
        }
        return out;
    }

    private void assertSequentialPartNames(List<Path> files) {
        List<String> names = files.stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).as("final files in ascending name order").isSorted();
        for (int i = 0; i < names.size(); i++) {
            assertThat(names.get(i)).isEqualTo(String.format("part-%05d.parquet", i));
        }
    }

    // --- scenario / key generation -------------------------------------

    /** Distribute unique keys across {@code segmentCount} segments (each individually sorted). */
    private Scenario scenario(List<byte[]> keys, int segmentCount, Random r) {
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> input = new ArrayList<>();
        for (byte[] k : keys) {
            ListEntry e = objOf(k);
            input.add(e);
            segs.get(r.nextInt(segmentCount)).add(e);
        }
        return new Scenario(segs, input);
    }

    /** {@code count} globally-unique keys of the given shape, each padded to force multi-row-group files. */
    private List<byte[]> uniqueKeys(KeyStyle style, int count, Random r) {
        Set<String> seen = new LinkedHashSet<>();   // dedup by content via a hex key
        List<byte[]> out = new ArrayList<>();
        int guard = 0;
        while (out.size() < count && guard++ < count * 64) {
            byte[] k = padded(candidate(style, r, out.size()));
            String h = hex(k);
            if (seen.add(h)) {
                out.add(k);
            }
        }
        // Deterministic top-up (guarantees exactly `count`, even if the random stream saturated a style).
        int i = 0;
        while (out.size() < count) {
            byte[] k = padded(("topup-" + (i++) + "-").getBytes(StandardCharsets.UTF_8));
            if (seen.add(hex(k))) {
                out.add(k);
            }
        }
        return out;
    }

    private byte[] candidate(KeyStyle style, Random r, int ordinal) {
        return switch (style) {
            case DENSE -> String.format("%08d", ordinal).getBytes(StandardCharsets.UTF_8);
            case SPARSE -> String.format("%019d", Math.abs(r.nextLong())).getBytes(StandardCharsets.UTF_8);
            case CLUSTERED -> String.format("c%d-%08d", r.nextInt(3), ordinal).getBytes(StandardCharsets.UTF_8);
            case ADVERSARIAL_BYTES -> {
                byte[] prefix = BIN_PREFIX[r.nextInt(BIN_PREFIX.length)];
                byte[] body = new byte[4];
                r.nextBytes(body);
                // 4-byte big-endian ordinal keeps every adversarial key globally unique.
                byte[] uniq = new byte[]{
                        (byte) (ordinal >>> 24), (byte) (ordinal >>> 16),
                        (byte) (ordinal >>> 8), (byte) ordinal};
                yield concat(concat(prefix, body), uniq);
            }
        };
    }

    /** Append filler so every key is ~200+ bytes ⇒ a handful of rows per 4 KB row group. */
    private static byte[] padded(byte[] base) {
        byte[] out = Arrays.copyOf(base, base.length + PAD);
        for (int i = base.length; i < out.length; i++) {
            out[i] = 'x';
        }
        return out;
    }

    private List<ListEntry> entries(List<byte[]> keys) {
        List<ListEntry> out = new ArrayList<>();
        for (byte[] k : keys) {
            out.add(objOf(k));
        }
        return out;
    }

    private List<ListEntry> flatten(List<List<ListEntry>> segs) {
        List<ListEntry> out = new ArrayList<>();
        segs.forEach(out::addAll);
        return out;
    }

    private ObjectEntry objOf(byte[] key) {
        return new ObjectEntry(KeyBytes.of(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    private static byte[] kb(String s) {
        return padded(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] denseKey(int i) {
        return padded(String.format("%08d", i).getBytes(StandardCharsets.UTF_8));
    }

    // --- pure selectRowGroups ground truth -----------------------------

    /** n distinct sorted keys, physical block indices monotonic with GAPS (omitted empty groups). */
    private List<SortedFileIndex.RowGroupSpan> randomSpans(int n, Random r) {
        TreeSet<byte[]> keys = new TreeSet<>(KeyBytes::compareUnsigned);
        int guard = 0;
        while (keys.size() < n && guard++ < n * 64) {
            byte[] k = new byte[1 + r.nextInt(3)];   // tiny keyspace ⇒ boundaries often hit a first-key
            r.nextBytes(k);
            keys.add(k);
        }
        List<byte[]> ordered = new ArrayList<>(keys);
        List<SortedFileIndex.RowGroupSpan> spans = new ArrayList<>();
        int block = r.nextInt(2);
        for (byte[] key : ordered) {
            spans.add(new SortedFileIndex.RowGroupSpan(key, block, 1L + r.nextInt(5)));
            block += 1 + r.nextInt(3);   // physical gaps between covered groups (empty blocks in between)
        }
        return spans;
    }

    private byte[][] randomBounds(List<SortedFileIndex.RowGroupSpan> spans, Random r) {
        List<byte[]> pool = new ArrayList<>();
        for (SortedFileIndex.RowGroupSpan s : spans) {
            pool.add(s.firstKey());                         // EXACT boundary — the off-by-one hotspot
            pool.add(concat(s.firstKey(), new byte[]{(byte) r.nextInt(256)}));   // just above a first-key
        }
        for (int i = 0; i < 3; i++) {
            byte[] k = new byte[1 + r.nextInt(3)];
            r.nextBytes(k);
            pool.add(k);
        }
        byte[] a = maybeNull(pool, r);
        byte[] b = maybeNull(pool, r);
        if (a != null && b != null && KeyBytes.compareUnsigned(a, b) > 0) {
            byte[] t = a;
            a = b;
            b = t;
        }
        return new byte[][]{a, b};
    }

    private static byte[] maybeNull(List<byte[]> pool, Random r) {
        return r.nextInt(5) == 0 ? null : pool.get(r.nextInt(pool.size())).clone();
    }

    /**
     * Independent interval-overlap ground truth: sorted group {@code i} owns keys {@code [k_i,
     * k_{i+1})} (last is {@code [k_{n-1}, +inf)}), overlapping {@code [lo, hi)} iff {@code k_i < hi}
     * and {@code upper_i > lo}. Returns the physical block indices of the overlapping groups.
     */
    private static List<Integer> overlappingBlocks(List<SortedFileIndex.RowGroupSpan> spans, byte[] lo, byte[] hi) {
        int n = spans.size();
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            byte[] ki = spans.get(i).firstKey();
            boolean belowHi = hi == null || KeyBytes.compareUnsigned(ki, hi) < 0;
            boolean aboveLo;
            if (lo == null || i == n - 1) {
                aboveLo = true;   // unbounded lo, or last group's upper bound is +inf
            } else {
                aboveLo = KeyBytes.compareUnsigned(spans.get(i + 1).firstKey(), lo) > 0;
            }
            if (belowHi && aboveLo) {
                out.add(spans.get(i).blockIndex());
            }
        }
        return out;
    }

    private static List<Integer> missingOverlappingBlocks(List<SortedFileIndex.RowGroupSpan> spans,
                                                          byte[] lo, byte[] hi, int[] selected) {
        Set<Integer> sel = Arrays.stream(selected).boxed().collect(Collectors.toSet());
        return overlappingBlocks(spans, lo, hi).stream().filter(b -> !sel.contains(b)).toList();
    }

    // --- duplicate-first-key (row-content) ground truth ----------------

    /**
     * Build a sorted physical-block sequence where a key value may OVERFLOW/straddle several
     * consecutive blocks, so adjacent blocks can share the same decoded {@code firstKey}. Each block
     * carries its ACTUAL distinct key-set (row-content truth), unlike Property 2's interval model.
     * Physical block indices advance with GAPS (omitted empty groups), matching {@code rowGroupSpans}.
     */
    private List<BlockContent> dupFirstKeyBlocks(int distinctValues, Random r) {
        TreeSet<byte[]> vs = new TreeSet<>(KeyBytes::compareUnsigned);
        int guard = 0;
        while (vs.size() < distinctValues && guard++ < distinctValues * 64) {
            byte[] k = new byte[1 + r.nextInt(2)];   // tiny keyspace ⇒ boundaries often hit a real key
            r.nextBytes(k);
            vs.add(k);
        }
        List<byte[]> values = new ArrayList<>(vs);
        // Global sorted row sequence: each value repeated 1..5 times so some values overflow a block.
        List<byte[]> rows = new ArrayList<>();
        for (byte[] v : values) {
            int reps = 1 + r.nextInt(5);
            for (int i = 0; i < reps; i++) {
                rows.add(v);
            }
        }
        // Cut the row sequence into contiguous non-empty blocks; a value straddling a cut yields two
        // adjacent blocks with an equal firstKey — the class Property 2 could not generate.
        List<BlockContent> blocks = new ArrayList<>();
        int block = r.nextInt(2);
        int i = 0;
        while (i < rows.size()) {
            int j = i + 1;
            while (j < rows.size() && r.nextInt(100) >= 40) {   // random chance to end the block each row
                j++;
            }
            List<byte[]> content = distinctInOrder(rows.subList(i, j));
            SortedFileIndex.RowGroupSpan span =
                    new SortedFileIndex.RowGroupSpan(rows.get(i).clone(), block, (long) (j - i));
            blocks.add(new BlockContent(span, content));
            block += 1 + r.nextInt(3);   // physical gaps between covered groups (empty blocks in between)
            i = j;
        }
        return blocks;
    }

    /** Bounds drawn from a pool INCLUDING the (possibly duplicated) block keys — the exact hotspot. */
    private byte[][] dupBounds(List<BlockContent> blocks, Random r) {
        List<byte[]> pool = new ArrayList<>();
        for (BlockContent b : blocks) {
            for (byte[] k : b.keys()) {
                pool.add(k.clone());                                             // EXACT key — hotspot
                pool.add(concat(k, new byte[]{(byte) r.nextInt(256)}));          // just above a key
            }
        }
        for (int i = 0; i < 2; i++) {
            byte[] k = new byte[1 + r.nextInt(2)];
            r.nextBytes(k);
            pool.add(k);
        }
        byte[] a = maybeNull(pool, r);
        byte[] b = maybeNull(pool, r);
        if (a != null && b != null && KeyBytes.compareUnsigned(a, b) > 0) {
            byte[] t = a;
            a = b;
            b = t;
        }
        return new byte[][]{a, b};
    }

    /**
     * ROW-CONTENT ground truth: the physical block indices whose actual key-set intersects
     * {@code [lo, hi)} ({@code lo} inclusive, {@code hi} exclusive; {@code null} = unbounded). Unlike
     * {@link #overlappingBlocks} (the interval model {@code [k_i, k_{i+1})}, wrong for duplicate first
     * keys), this asks per real key: is any key of the block {@code >= lo} and {@code < hi}?
     */
    private static List<Integer> contentOverlapBlocks(List<BlockContent> blocks, byte[] lo, byte[] hi) {
        List<Integer> out = new ArrayList<>();
        for (BlockContent b : blocks) {
            boolean any = false;
            for (byte[] k : b.keys()) {
                boolean geLo = lo == null || KeyBytes.compareUnsigned(k, lo) >= 0;
                boolean ltHi = hi == null || KeyBytes.compareUnsigned(k, hi) < 0;
                if (geLo && ltHi) {
                    any = true;
                    break;
                }
            }
            if (any) {
                out.add(b.span().blockIndex());
            }
        }
        return out;
    }

    private static List<Integer> missingContentBlocks(List<BlockContent> blocks, byte[] lo, byte[] hi,
                                                      int[] selected) {
        Set<Integer> sel = Arrays.stream(selected).boxed().collect(Collectors.toSet());
        return contentOverlapBlocks(blocks, lo, hi).stream().filter(b -> !sel.contains(b)).toList();
    }

    /**
     * The rejected (buggy) selection rule, replicated locally so the RED proof is self-contained and
     * PERMANENT (product code stays untouched): {@code firstCovered} = the LARGEST {@code i} with
     * {@code firstKey_i <= lo}. On a duplicate-first-key run this jumps PAST the earlier equal-first-key
     * blocks and silently drops their rows.
     */
    private static int[] preFixOldSelection(List<SortedFileIndex.RowGroupSpan> spans, byte[] lo, byte[] hi) {
        int n = spans.size();
        if (n == 0) {
            return new int[0];
        }
        int firstCovered = 0;
        if (lo != null) {
            for (int i = 0; i < n; i++) {
                if (KeyBytes.compareUnsigned(spans.get(i).firstKey(), lo) <= 0) {
                    firstCovered = i;   // largest i with firstKey_i <= lo — the off-by-one that lost rows
                }
            }
        }
        int lastCovered = n - 1;
        if (hi != null) {
            int t = -1;
            for (int i = 0; i < n; i++) {
                if (KeyBytes.compareUnsigned(spans.get(i).firstKey(), hi) < 0) {
                    t = i;
                } else {
                    break;
                }
            }
            lastCovered = t;
        }
        if (firstCovered > lastCovered) {
            return new int[0];
        }
        int pFirst = spans.get(firstCovered).blockIndex();
        int pLast = spans.get(lastCovered).blockIndex();
        int[] blocks = new int[pLast - pFirst + 1];
        for (int b = pFirst; b <= pLast; b++) {
            blocks[b - pFirst] = b;
        }
        return blocks;
    }

    private static boolean hasDuplicateFirstKeys(List<SortedFileIndex.RowGroupSpan> spans) {
        for (int i = 1; i < spans.size(); i++) {
            if (KeyBytes.compareUnsigned(spans.get(i - 1).firstKey(), spans.get(i).firstKey()) == 0) {
                return true;
            }
        }
        return false;
    }

    private static List<byte[]> distinctInOrder(List<byte[]> slice) {
        List<byte[]> out = new ArrayList<>();
        for (byte[] k : slice) {
            boolean seen = false;
            for (byte[] e : out) {
                if (KeyBytes.compareUnsigned(e, k) == 0) {
                    seen = true;
                    break;
                }
            }
            if (!seen) {
                out.add(k.clone());
            }
        }
        return out;
    }

    private static List<byte[]> keys(String... ss) {
        List<byte[]> out = new ArrayList<>();
        for (String s : ss) {
            out.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return out;
    }

    /** {@code n} rows of ONE key with distinct, bulky (~206-byte) versionIds ⇒ overflows >=2 groups. */
    private List<ListEntry> hotRows(byte[] key, int n) {
        List<ListEntry> out = new ArrayList<>();
        for (int v = 0; v < n; v++) {
            out.add(hotRow(key, v));
        }
        return out;
    }

    private ListEntry hotRow(byte[] key, int v) {
        // Distinct + bulky versionId: defeats dictionary collapse (repeated key column) and crosses the
        // tiny row-group threshold, so this single key's rows span several physical groups. Distinct
        // versions also make the rows fully ordered (no equal-comparing ties) — strict byte-exactness.
        String version = String.format("v%08d-", v) + "x".repeat(200);
        return new ObjectEntry(KeyBytes.of(key), 1L, 0L, null, null, version, false, null, null, null, null);
    }

    // --- small utils ---------------------------------------------------

    private static SortedFileIndex.RowGroupSpan span(String firstKey, int blockIndex) {
        return new SortedFileIndex.RowGroupSpan(firstKey.getBytes(StandardCharsets.UTF_8), blockIndex, 1L);
    }

    private static byte[] key(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }

    private static String show(byte[] b) {
        return b == null ? "null" : hex(b);
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
                    // best-effort temp cleanup
                }
            });
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
