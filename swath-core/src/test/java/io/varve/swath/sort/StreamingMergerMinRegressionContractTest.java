/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression guard for the {@link StreamingMerger} route's bypass of the page-min monotonicity
 * guard.
 *
 * <p><b>The route.</b> {@link KWayMerge} opens the {@link PageAwareMerger} (and hence that reader)
 * <em>only when EVERY segment in the group exposes a page frontier</em>;
 * a group that MIXES a {@code .pageseg} with a columnar Parquet segment (the {@code CaptureSorter}
 * fixture format, and any legacy staging segment) falls back to the entry-typed
 * {@link StreamingMerger}, which opens the page-run input through {@link PageRunSegmentReader}
 * instead. {@code StreamingMerger} is a k-way entry heap: it ASSUMES each input run is sorted, so a
 * min-regressed page-run segment merged alongside a Parquet segment must be caught before it
 * reaches that assumption.
 *
 * <p><b>Where the guard lives.</b> The guard lives in {@link PageRunSegmentIo#nextPage()} — the ONE
 * page-advance primitive both readers use ({@code nextBody()} is private) — not in
 * {@link PageFrontierReader#advance()} alone, because the mixed/StreamingMerger route never goes
 * through {@code PageFrontierReader}. A violation fails the mixed route as
 * {@link SegmentCorruptionException} ({@code error_class=page_run_min_regression}), after bumping
 * {@code SORT.page_run_min_regression}, exactly as the frontier route does.
 *
 * <p>{@link PageRunSegmentReader} is itself a {@link PageAwareMerger} over its own single frontier,
 * so a {@code .pageseg} presents a genuinely sorted run on <i>every</i> route: disjoint pages keep
 * the decode-free whole-page fast path, and pages whose ranges legally overlap (ascending or equal
 * mins — {@link PageRunSegmentWriter#flush} legitimately emits overlapping adjacent pages) take the
 * key-merge fallback instead of a file-order concatenation, which would otherwise silently misorder
 * legally-written, overlapping-page segments.
 *
 * <p><b>What is pinned.</b>
 * <ol>
 *   <li>{@link #mixedPageRunAndParquetGroupRejectsAMinRegressionInsteadOfMisordering} — the violation is
 *       REJECTED on the StreamingMerger route (typed as {@link SegmentCorruptionException}, counter fired,
 *       nothing published).</li>
 *   <li>{@link #withoutTheReadTimeGuardTheSameMixedGroupSilentlyMisordersTheStreamingMerge} — the SAME
 *       bytes, read by an unguarded entry stream and fed to the REAL {@link StreamingMerger} alongside
 *       the REAL Parquet {@link SegmentReader}, merge "successfully" and emit {@code c,d,m,n,a,b} — out
 *       of order, no exception, no counter. The witness that the fixture reaches the defect and the
 *       guard (not luck) is what saves the merge. Mirrors {@code
 *       PageAwareMergerMinRegressionContractTest#withoutTheReadTimeGuardTheSameFixtureSilentlyMisordersTheMerge}
 *       for the entry-typed route.</li>
 *   <li>The {@code legal*} tests — NEGATIVE CONTROLS on the SAME mixed route against over-rejection:
 *       overlapping-but-ascending mins, EQUAL mins, a single-page segment and an EMPTY segment must all
 *       merge cleanly with the counter silent. Only a strict min REGRESSION is illegal.</li>
 * </ol>
 */
class StreamingMergerMinRegressionContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private int seq;

    // ------------------------------------------------- (A) the violation is rejected on the mixed route

    @Test
    void mixedPageRunAndParquetGroupRejectsAMinRegressionInsteadOfMisordering(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path regressed = descendingMinPageRun(dirs.staging);
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("c", "d"));
        List<Path> staging = List.of(regressed, parquet);

        // Route precondition: the Parquet segment has no page frontier, so KWayMerge's
        // allSupportPageFrontier is FALSE and this group takes the StreamingMerger fallback — the one
        // route a guard placed only in PageFrontierReader#advance() would never see.
        assertThat(SortTransform.isPageRunSegment(regressed)).isTrue();
        assertThat(SortTransform.isPageRunSegment(parquet))
                .as("a columnar Parquet segment in the group is what forces the StreamingMerger route")
                .isFalse();

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Throwable thrown = catchThrowable(() -> transform(metrics)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP));

        assertThat(thrown)
                .as("a page-run segment whose page minKeys regress must FAIL the merge as corrupt on the "
                        + "StreamingMerger route too — that merger assumes each input run is sorted, so "
                        + "merging such a segment can only misorder")
                .isNotNull()
                .isInstanceOfAny(IOException.class, UncheckedIOException.class);
        SegmentCorruptionException corruption = corruptionCause(thrown);
        assertThat(corruption.errorClass())
                .as("the failure is CLASSIFIED (this is what reaches summary.json's error_class), not a "
                        + "bare unclassified IOException")
                .isEqualTo("page_run_min_regression");

        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("the violation must be counted BEFORE the throw (the run aborts; the counter is the "
                        + "post-hoc evidence in summary.json's meters[])")
                .isEqualTo(1);
        assertThat(finalFiles(dirs.output))
                .as("nothing is published from a corrupt merge — the abort precedes any rename into data/")
                .isEmpty();
    }

    // ------------------------------------- (B) evidence: without the guard, the fixture truly misorders

    @Test
    void withoutTheReadTimeGuardTheSameMixedGroupSilentlyMisordersTheStreamingMerge(@TempDir Path root)
            throws IOException {
        // The SAME bytes, read by the trusting entry stream (PageRunRawFixtures.trustingEntryStream —
        // a reader with no ordering guard at all), fed to the REAL StreamingMerger alongside the REAL
        // columnar SegmentReader — i.e. precisely the streams KWayMerge#openMerger opens for a mixed
        // group. Trace: heap heads {S->"m", P->"c"}; P wins, emits c then d on the same-reader fast path
        // ("d" <= "m"); P exhausts; S then drains its pages in FILE order — m, n, THEN a, b, because the
        // trusting reader concatenates page [a..b] after page [m..n]. Output is c,d,m,n,a,b: no exception,
        // no counter, just wrong data. That is the silent corruption the guard closes.
        Dirs dirs = dirs(root);
        Path regressed = descendingMinPageRun(dirs.staging);
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("c", "d"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<ListEntry> out;
        try (EntryStream trusting = PageRunRawFixtures.trustingEntryStream(regressed);
             EntryStream columnar = new SegmentReader(parquet)) {
            out = SortTestSupport.drain(new StreamingMerger(List.of(trusting, columnar), cmp,
                    DuplicateHook.NO_OP, n -> { }));
        }

        assertThat(keys(out))
                .as("the prior behavior on the StreamingMerger route, reproduced exactly: the merger trusts "
                        + "the page-run input as a sorted run and drains its regressed page LAST")
                .containsExactly("c", "d", "m", "n", "a", "b");
        assertThat(isGloballySorted(out))
                .as("...and the merged output is NOT globally sorted — a silent, unflagged data corruption")
                .isFalse();
        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("nothing fired: the pre-fix reader had no idea anything was wrong")
                .isZero();
    }

    // ------------------------------- (C) negative controls on the SAME route: no over-rejection

    /**
     * A page-run segment built by the PRODUCTION {@link PageRunSegmentWriter#flush} from two
     * interleaved node runs, so it is beyond dispute a segment swath itself ships: {@code flush}
     * orders pages by first key, giving pages {@code [a..m]} then {@code [c..z]} — mins ASCEND
     * (a &lt; c, so this is not a min regression) but the page RANGES OVERLAP (m &gt;= c). The
     * {@link PageAwareMerger} handles exactly this with its key-merge fallback ({@code
     * SORT.page_overlap_keymerge} — see {@code KWayMergeCascadePageGuardTest}); the entry-typed
     * {@link PageRunSegmentReader} is itself built as one, so on the mixed route the merge still
     * resolves the overlap correctly instead of handing {@link StreamingMerger} the unsorted
     * file-order run {@code a, m, c, z}.
     */
    @Test
    void legalOverlappingButAscendingPageMinsMustStillMergeInOrderOnTheMixedRoute(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path pageRun = flushedPageRun(dirs.staging, "seg-0.pageseg",
                List.of(List.of(obj("a"), obj("m")), List.of(obj("c"), obj("z"))));
        assertAscendingMins(pageRun);
        assertThat(hasOverlappingAdjacentPages(pageRun))
                .as("control precondition: the production writer stores overlapping adjacent page RANGES "
                        + "(mins ascend, so the guard is correctly silent)")
                .isTrue();
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("b", "n"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(metrics)
                .transform(List.of(pageRun, parquet), dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("range OVERLAP is not a min regression — the guard must not reject it (no over-rejection)")
                .isZero();
        assertThat(publishedKeys(result))
                .as("a merge either rejects an input it cannot handle or merges it correctly — it must never "
                        + "publish silently misordered rows (§0 global sort order)")
                .containsExactly("a", "b", "c", "m", "n", "z");
    }

    /**
     * The legality control for the fixture above: the exact same {@code .pageseg} bytes, merged on
     * the all-page-run route ({@link PageAwareMerger}), come out perfectly sorted — proving the
     * segment itself is legal, independent of which merge route reads it.
     */
    @Test
    void theSameOverlappingSegmentMergesCorrectlyOnTheAllPageRunRoute(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path pageRun = flushedPageRun(dirs.staging, "seg-0.pageseg",
                List.of(List.of(obj("a"), obj("m")), List.of(obj("c"), obj("z"))));
        Path partner = flushedPageRun(dirs.staging, "seg-1.pageseg", List.of(List.of(obj("b"), obj("n"))));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(metrics)
                .transform(List.of(pageRun, partner), dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(publishedKeys(result))
                .as("the page-aware route resolves intra-segment page overlap with its key-merge fallback")
                .containsExactly("a", "b", "c", "m", "n", "z");
        assertThat(metrics.count("SORT.page_overlap_keymerge"))
                .as("...and it says so: the overlap guard engaged").isPositive();
        assertThat(metrics.count("SORT.page_run_min_regression")).isZero();
    }

    /**
     * Two node runs that both start on key {@code m} (distinct versions): {@code flush} keeps them
     * adjacent with EQUAL mins — non-decreasing, hence legal and not a min regression — but the
     * pages' ranges overlap ({@code [m..n]}, {@code [m..z]}). The entry-typed reader resolves the
     * overlap with its key-merge fallback instead of a file-order concatenation, which would
     * otherwise yield the unsorted run {@code m, n, m, z}.
     */
    @Test
    void legalEqualPageMinsMustStillMergeInOrderOnTheMixedRoute(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path pageRun = flushedPageRun(dirs.staging, "seg-0.pageseg",
                List.of(List.of(obj("m", "x"), obj("n", "x")), List.of(obj("m", "y"), obj("z", "y"))));
        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(pageRun);
        assertThat(mins).hasSize(2);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("control precondition: the production writer stores EQUAL adjacent page mins").isZero();
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("a"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(metrics)
                .transform(List.of(pageRun, parquet), dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("equal mins are non-decreasing — legal, must not be rejected (no over-rejection)")
                .isZero();
        assertThat(publishedKeys(result))
                .as("both \"m\" rows survive (§0.5: the sort never dedupes) AND the published order is global")
                .containsExactly("a", "m", "m", "n", "z");
    }

    @Test
    void legalSinglePagePageRunMergesCleanlyOnTheMixedRoute(@TempDir Path root) throws IOException {
        // A one-page segment has no PREVIOUS page min — the guard must be a pure no-op, not a null-deref
        // or a spurious rejection.
        Dirs dirs = dirs(root);
        Path pageRun = dirs.staging.resolve("seg-0.pageseg");
        PageRunRawFixtures.writeRawPageRun(pageRun, List.of(List.of(obj("b"), obj("y"))), cmp);
        assertThat(PageRunRawFixtures.pageMinKeysInFileOrder(pageRun)).hasSize(1);
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("a", "z"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(metrics)
                .transform(List.of(pageRun, parquet), dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(publishedKeys(result)).containsExactly("a", "b", "y", "z");
        assertThat(metrics.count("SORT.page_run_min_regression")).isZero();
    }

    @Test
    void legalEmptyPageRunMergesCleanlyOnTheMixedRoute(@TempDir Path root) throws IOException {
        // A zero-page segment never advances a page at all: the guard must not fire, and the merge must
        // still publish the other segment's rows.
        Dirs dirs = dirs(root);
        Path pageRun = dirs.staging.resolve("seg-0.pageseg");
        PageRunRawFixtures.writeRawPageRun(pageRun, List.of(), cmp);
        assertThat(PageRunRawFixtures.pageMinKeysInFileOrder(pageRun)).isEmpty();
        Path parquet = parquetSegment(dirs.staging, "seg-1.parquet", objects("a", "b"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = transform(metrics)
                .transform(List.of(pageRun, parquet), dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(publishedKeys(result)).containsExactly("a", "b");
        assertThat(metrics.count("SORT.page_run_min_regression")).isZero();
    }

    // ------------------------------------------------------------------------------------- fixtures

    /**
     * The min-regression fixture on the entry-typed route:
     * <pre>
     * seg-0.pageseg (FILE order): page0 = [m..n], page1 = [a..b]   // mins DESCEND — the violation
     * seg-1.parquet             : [c..d]                           // b &lt; c &lt; d &lt; m
     * </pre>
     * Unbuildable through {@link PageRunSegmentWriter#flush} (it sorts pages by first key), hence the
     * hand-framed {@link PageRunRawFixtures} bytes: physically byte-perfect (magic, per-record CRC32C,
     * complete trailer), illegal only in page ORDER.
     */
    private Path descendingMinPageRun(Path stagingDir) throws IOException {
        Path path = stagingDir.resolve("seg-0.pageseg");
        PageRunRawFixtures.writeRawPageRun(path,
                List.of(List.of(obj("m"), obj("n")), List.of(obj("a"), obj("b"))), cmp);
        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(path);
        assertThat(mins).hasSize(2);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("fixture precondition: the segment must physically store page mins that REGRESS "
                        + "(\"m\" then \"a\")")
                .isNegative();
        return path;
    }

    /**
     * A page-run segment produced by the REAL {@link PageRunSegmentWriter#flush} (each inner list is one
     * node run ⇒ one page, ordered by first key) — not a hand-framed fixture. Used by the legality controls
     * so their inputs are provably segments swath itself ships.
     */
    private Path flushedPageRun(Path dir, String name, List<List<ListEntry>> nodeRuns) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfig.fromSystemProperties(), cmp);
        long node = 0;
        for (List<ListEntry> run : nodeRuns) {
            buffer.admit(node++, run);
        }
        Path path = dir.resolve(name);
        new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    /** A COLUMNAR Parquet staging segment (the CaptureSorter fixture format) — its presence in the
     *  group is what makes {@code allSupportPageFrontier} false and selects the {@link StreamingMerger}. */
    private Path parquetSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    // -------------------------------------------------------------------------------------- helpers

    private SortTransform transform(SortMetrics metrics) {
        return new SortTransform(new SortRun(SortConfig.fromSystemProperties(), cmp, DuplicateHook.NO_OP, metrics, SortedFileWriterFactory.DEFAULT));
    }

    /** The first {@link SegmentCorruptionException} in {@code t}'s cause chain (the shape ListRunner's
     *  {@code segmentErrorClass} walk depends on) — fails the test if the failure was unclassified. */
    private static SegmentCorruptionException corruptionCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SegmentCorruptionException sce) {
                return sce;
            }
        }
        return fail("expected a SegmentCorruptionException in the cause chain, got: " + t);
    }

    private void assertAscendingMins(Path pageRun) throws IOException {
        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(pageRun);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("control precondition: the mins must ASCEND (only the page ranges overlap)")
                .isPositive();
    }

    /** True iff the physical segment holds two adjacent pages whose RANGES overlap (legal; the page-aware
     *  merger's key-merge fallback exists precisely for it). */
    private static boolean hasOverlappingAdjacentPages(Path file) throws IOException {
        try (PageFrontierReader reader = new PageFrontierReader(file)) {
            byte[] prevMax = null;
            while (reader.hasPage()) {
                if (prevMax != null && Arrays.compareUnsigned(reader.minKey(), prevMax) <= 0) {
                    return true;
                }
                prevMax = reader.maxKey().clone();
                reader.advance();
            }
        }
        return false;
    }

    private List<String> publishedKeys(SortTransformResult result) throws IOException {
        List<String> out = new ArrayList<>();
        for (Path f : result.finalFiles()) {
            try (SegmentReader r = new SegmentReader(f)) {
                while (r.hasNext()) {
                    out.add(r.next().key().asString());
                }
            }
        }
        return out;
    }

    private static List<Path> finalFiles(Path outputDir) throws IOException {
        Path dataDir = outputDir;
        if (!Files.isDirectory(dataDir)) {
            return List.of();
        }
        try (var s = Files.newDirectoryStream(dataDir, "part-*.parquet")) {
            List<Path> out = new ArrayList<>();
            s.forEach(out::add);
            return out;
        }
    }

    private static List<String> keys(List<ListEntry> entries) {
        List<String> out = new ArrayList<>(entries.size());
        for (ListEntry e : entries) {
            out.add(e.key().asString());
        }
        return out;
    }

    private boolean isGloballySorted(List<ListEntry> out) {
        for (int i = 1; i < out.size(); i++) {
            if (cmp.compare(out.get(i - 1), out.get(i)) > 0) {
                return false;
            }
        }
        return true;
    }

    private List<ListEntry> objects(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(obj(k));
        }
        return out;
    }

    private ObjectEntry obj(String key) {
        return obj(key, "v" + String.format("%08d", seq));
    }

    private ObjectEntry obj(String key, String version) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, version,
                false, null, null, null, null);
    }

    private record Dirs(Path output, Path staging) {
    }

    private static Dirs dirs(Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("out/_staging"));
        return new Dirs(output, staging);
    }
}
