/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial guard for the <b>intra-segment page-min regression</b>.
 *
 * <p><b>The contract.</b> {@link PageAwareMerger}'s whole-page fast path decides "this page is globally
 * next" from the frontier alone: {@code frontier.peek().minKey() > pageMax}. That is a sound global test
 * ONLY because a segment's frontier {@code minKey} is a lower bound on everything the segment has LEFT —
 * which holds only if page {@code minKey}s never regress within a segment. {@link
 * PageRunSegmentWriter#flush} establishes that ascent (it sorts pages by first key); nothing used to
 * verify it on READ. A segment whose pages regress therefore did not fail — it produced <b>globally
 * misordered output, silently</b>. This guard makes the read side VERIFY the precondition
 * ({@link PageFrontierReader#advance()} fails the segment as corrupt after bumping {@code
 * SORT.page_run_min_regression}).
 *
 * <p><b>Why the fixture is the deliverable.</b> No {@code flush()}-staged fixture can express the
 * violation — {@code flush()} sorts the pages, so the suite was structurally blind here. These tests
 * hand-frame the {@code .pageseg} bytes ({@link PageRunRawFixtures#writeRawPageRun}), producing a segment
 * that is byte-perfect physically (magic, per-record CRC32C, complete trailer) and illegal only in page
 * order.
 *
 * <p><b>What is pinned, in three directions.</b>
 * <ol>
 *   <li>{@link #descendingPageMinsInOneSegmentAreRejectedNotSilentlyMisordered} — the violation is
 *       REJECTED (IO-failure family + the engagement counter), never merged.</li>
 *   <li>{@link #withoutTheReadTimeGuardTheSameFixtureSilentlyMisordersTheMerge} — the same fixture, fed to
 *       the same real merger through an unguarded (trusting) frontier, reproduces the exact silent
 *       misorder {@code c,d,a,b,m,n}. This is what proves the fixture reaches the defect, and that the
 *       guard — not luck — is what saves the merge.</li>
 *   <li>{@link #overlappingPagesWithAscendingMinsAreRejected} verifies the stronger disjointness
 *       contract; {@link #equalBoundaryIsAcceptedForVersions} is its mode-aware negative control.</li>
 * </ol>
 */
class PageAwareMergerMinRegressionContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private int seq;

    // ------------------------------------------------------------------ (1) the violation is rejected

    @Test
    void descendingPageMinsInOneSegmentAreRejectedNotSilentlyMisordered(@TempDir Path dir) throws IOException {
        List<Path> files = descendingMinFixture(dir);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> drainThroughRealReaders(files, metrics))
                .as("a segment whose page minKeys regress must FAIL as corrupt — the merger's frontier is "
                        + "not a valid lower bound on such a segment, so merging it can only misorder")
                .isInstanceOfAny(IOException.class, UncheckedIOException.class);

        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("the violation must be counted (post-hoc: a run that aborted this way is visible in "
                        + "summary.json), not just thrown")
                .isGreaterThan(0);
    }

    // -------------------------------------------- (2) RED evidence: the fixture really reaches the bug

    @Test
    void withoutTheReadTimeGuardTheSameFixtureSilentlyMisordersTheMerge(@TempDir Path dir) throws IOException {
        // The SAME bytes, read by the unguarded TrustingPageFrontier test double instead of the guarded
        // PageFrontierReader, fed to the REAL PageAwareMerger. Trace: frontier {S->"m", T->"c"}; plan() pops T, pageMax="d", T
        // exhausts; the frontier now holds only S at min "m"; "m" > "d" ⇒ whole ⇒ c,d stream out. THEN S's
        // pageX is popped and its own next page (min "a") forces the key-merge ⇒ a,b,m,n. Output is
        // c,d,a,b,m,n — no exception, no alarm counter, just wrong data. This is the defect the read-time
        // guard closes, and it is why the guard belongs on the READ side: the merger cannot detect it by itself.
        List<Path> files = descendingMinFixture(dir);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        List<PageFrontierStream> trusting = new ArrayList<>();
        for (Path f : files) {
            trusting.add(new PageRunRawFixtures.TrustingPageFrontier(f));
        }
        List<ListEntry> out = drain(trusting, metrics);

        assertThat(keys(out))
                .as("the prior behavior, reproduced exactly: the whole-page fast path emits T's page ahead "
                        + "of S's [a..b] page because S's frontier still advertises min \"m\"")
                .containsExactly("c", "d", "a", "b", "m", "n");
        assertThat(isGloballySorted(out))
                .as("...and the output is NOT globally sorted — a silent, unflagged data corruption")
                .isFalse();
        assertThat(metrics.count("SORT.page_whole_emitted"))
                .as("the misorder comes from the whole-page fast path trusting the frontier")
                .isGreaterThan(0);
    }

    // ------------------------------------------------------- (3) negative controls: no over-rejection

    @Test
    void overlappingPagesWithAscendingMinsAreRejected(@TempDir Path dir) throws IOException {
        // Pages [a..m] then [c..z]: mins ascend, but the raw ranges overlap.
        List<ListEntry> pageLow = List.of(obj("a"), obj("m"));
        List<ListEntry> pageHigh = List.of(obj("c"), obj("z"));
        Path file = dir.resolve("overlap-ascending.pageseg");
        PageRunRawFixtures.writeRawPageRun(file, List.of(pageLow, pageHigh), cmp);

        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(file);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("control precondition: the mins must ASCEND (only the ranges overlap)").isPositive();

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        assertThatThrownBy(() -> drainThroughRealReaders(List.of(file), metrics))
                .isInstanceOf(UncheckedIOException.class)
                .hasRootCauseInstanceOf(SegmentCorruptionException.class)
                .hasStackTraceContaining("error_class=page_run_page_overlap");
        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("ascending mins are not a min regression").isZero();
        assertThat(metrics.count("SORT.page_run_page_overlap")).isEqualTo(1);
    }

    @Test
    void equalBoundaryIsAcceptedForVersions(@TempDir Path dir) throws IOException {
        // VERSIONS may split one equal-key cluster across a page boundary: prev.max == next.min.
        List<ListEntry> page0 = List.of(obj("m", "x"));
        List<ListEntry> page1 = List.of(obj("m", "y"), obj("z", "y"));
        Path file = dir.resolve("equal-mins.pageseg");
        PageRunRawFixtures.writeRawPageRun(
                file, List.of(page0, page1), cmp, SortMode.VERSIONS);

        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(file);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("control precondition: the two page mins must be EQUAL").isZero();

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        List<ListEntry> out = drainThroughRealReaders(List.of(file), metrics);

        assertThat(out).isEqualTo(sortedOracle(page0, page1));
        assertThat(isGloballySorted(out)).isTrue();
        assertThat(metrics.count("SORT.page_run_min_regression"))
                .as("equal mins are non-decreasing — legal, must not be rejected").isZero();
        assertThat(metrics.count("SORT.page_run_page_overlap")).isZero();
    }

    // ------------------------------------------------------------------------------------- fixtures

    /**
     * The min-regression fixture, exactly as specified:
     * <pre>
     * segment S (FILE order): pageX = [m..n], pageY = [a..b]   // mins DESCEND — the violation
     * segment T (FILE order): pageT = [c..d]                   // b &lt; c &lt; d &lt; m
     * </pre>
     * Unbuildable through {@link PageRunSegmentWriter#flush} (it would sort S's pages to [a..b],[m..n]).
     */
    private List<Path> descendingMinFixture(Path dir) throws IOException {
        Path segS = dir.resolve("S-descending-mins.pageseg");
        PageRunRawFixtures.writeRawPageRun(segS,
                List.of(List.of(obj("m"), obj("n")), List.of(obj("a"), obj("b"))), cmp);

        Path segT = dir.resolve("T.pageseg");
        PageRunRawFixtures.writeRawPageRun(segT, List.of(List.of(obj("c"), obj("d"))), cmp);

        List<byte[]> mins = PageRunRawFixtures.pageMinKeysInFileOrder(segS);
        assertThat(mins).hasSize(2);
        assertThat(Arrays.compareUnsigned(mins.get(1), mins.get(0)))
                .as("fixture precondition: S must physically store page mins that REGRESS (\"m\" then \"a\")")
                .isNegative();

        return List.of(segS, segT);
    }

    // -------------------------------------------------------------------------------------- helpers

    /** Drive the merger through the PRODUCTION frontier reader, wired to {@code metrics} (the min-regression counter). */
    private List<ListEntry> drainThroughRealReaders(List<Path> files, SortMetrics metrics) throws IOException {
        List<PageFrontierStream> frontiers = new ArrayList<>();
        for (Path f : files) {
            frontiers.add(new PageFrontierReader(f, metrics));
        }
        return drain(frontiers, metrics);
    }

    private List<ListEntry> drain(List<PageFrontierStream> frontiers, SortMetrics metrics) {
        List<ListEntry> out = new ArrayList<>();
        try (PageAwareMerger merger = new PageAwareMerger(
                frontiers, cmp, MergeScope.CROSS_SEGMENT, metrics)) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    /** Oracle = the GENERATED entries, sorted — never a re-read of the staged bytes. */
    @SafeVarargs
    private List<ListEntry> sortedOracle(List<ListEntry>... pages) {
        List<ListEntry> all = new ArrayList<>();
        for (List<ListEntry> page : pages) {
            all.addAll(page);
        }
        all.sort(cmp);
        return all;
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

    private ObjectEntry obj(String key) {
        return obj(key, "v" + String.format("%08d", seq));
    }

    private ObjectEntry obj(String key, String version) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), seq++, 0L, null, null, version,
                false, null, null, null, null);
    }
}
