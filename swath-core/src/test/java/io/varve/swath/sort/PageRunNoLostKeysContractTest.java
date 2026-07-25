/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial guard: <b>"no lost keys" on the live page-run path, against a NON-CIRCULAR oracle</b>.
 *
 * <p><b>The gap this closes.</b> The existing page-run property oracles ({@code
 * PageAwareMergerContractTest#propertyRandomShapesAlwaysEqualTheSortedOracle}, {@code
 * VersionsKeyMergeContractTest}) compare the merger's output against {@code sortedOracle(files)} — which
 * <em>re-reads the staged {@code .pageseg} files</em>. Both sides of that comparison are derived from the
 * same staged bytes, so a row the STAGING WRITER ({@link PageRunSegmentWriter#flush}: page re-pack, page
 * sort, framing, trailer counts) dropped or duplicated is invisible to both: the oracle loses it too and
 * the assertion stays green.
 *
 * <p><b>The oracle here is the GENERATED input</b> — the {@code List<ListEntry>} handed to the buffer,
 * retained in memory and never re-read from disk. Asserting {@code containsExactlyInAnyOrderElementsOf}
 * against it pins, end-to-end across admit → seal → page re-pack → page sort → frame → trailer → frontier
 * read → page-aware merge: <b>no drops, no invented rows, exact multiplicity</b> (§0.5: swath {@code
 * --sort} is a sorter, never a deduper) — and asserting sortedness under the full §0.3 comparator pins the
 * order on top of the multiset.
 *
 * <p>The generator is deliberately dup-heavy and page-boundary-heavy: a tiny keyspace, EXACT duplicates
 * (equal under the comparator AND by value, so a deduper would silently swallow them), repeated
 * {@code (key, version)} pairs across segments, mixed row types, and small random page sizes so keys land
 * adjacent to page edges.
 */
class PageRunNoLostKeysContractTest {

    private final ListEntryComparator cmp = new ListEntryComparator();
    private final SortConfig config = SortConfig.fromSystemProperties();

    @Test
    void pageRunMergeIsTheInputMultiset(@TempDir Path dir) throws IOException {
        for (long seed = 0; seed < 60; seed++) {
            Path caseDir = Files.createDirectories(dir.resolve("seed-" + seed));
            Random rnd = new Random(seed);

            int keyspace = 4 + rnd.nextInt(10);       // tiny alphabet ⇒ heavy equal-key clustering
            int nEntries = 20 + rnd.nextInt(140);
            int nSegs = 1 + rnd.nextInt(4);

            List<ListEntry> generated = new ArrayList<>();
            List<List<ListEntry>> bySeg = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                bySeg.add(new ArrayList<>());
            }
            ListEntry previous = null;
            for (int i = 0; i < nEntries; i++) {
                // 1-in-4 rows are an EXACT repeat of the previous row (same key, version, row type, payload)
                // ⇒ a true multiset duplicate that a deduper would eat; the rest draw from a tiny (key,
                // version) pool so duplicates also arise across segments and across page boundaries.
                ListEntry e = (previous != null && rnd.nextInt(4) == 0)
                        ? previous
                        : entry(rnd, keyspace);
                previous = e;
                generated.add(e);
                bySeg.get(rnd.nextInt(nSegs)).add(e);
            }

            List<Path> files = new ArrayList<>();
            for (int s = 0; s < nSegs; s++) {
                List<ListEntry> entries = new ArrayList<>(bySeg.get(s));
                Collections.shuffle(entries, rnd);   // arrival order ⇒ per-page re-pack + overlapping pages
                files.add(stage(caseDir, "seg-" + s + ".pageseg", entries, rnd));
            }

            SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
            List<ListEntry> out = drainPageAware(files, metrics);

            assertThat(out)
                    .as("seed %d: the merged output is EXACTLY the generated input multiset — no drop, no "
                            + "invented row, exact multiplicity (oracle = the generated entries, NOT a "
                            + "re-read of the staged files)", seed)
                    .containsExactlyInAnyOrderElementsOf(generated);
            assertThat(out)
                    .as("seed %d: and it is globally sorted under the §0.3 comparator", seed)
                    .isSortedAccordingTo(cmp);
            assertThat(metrics.count("SORT.page_run_min_regression"))
                    .as("seed %d: the shipped writer must never emit a page-min regression", seed)
                    .isZero();
        }
    }

    @Test
    void everyEntryIdenticalUnderTheComparatorStillSurvivesEveryStage(@TempDir Path dir) throws IOException {
        // The degenerate extreme of §0.5: 25 rows that are ALL exactly equal. Every adjacent pair fires the
        // DuplicateHook; a sorter must still emit 25 rows. (Pages here are adjacent-tie pages, so
        // PageBlock#orderedUnderFullComparator is false and flush's re-pack path runs on every one.)
        List<ListEntry> generated = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            generated.add(object("dup", "v1"));
        }
        Path file = stage(dir, "all-equal.pageseg", generated, new Random(7));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        AtomicInteger hookFirings = new AtomicInteger();
        List<ListEntry> out = drainPageAware(List.of(file), metrics,
                (previous, current) -> hookFirings.incrementAndGet());

        assertThat(out).as("a sorter is not a deduper — all 25 identical rows survive")
                .containsExactlyInAnyOrderElementsOf(generated);
        assertThat(hookFirings.get())
                .as("the DuplicateHook reports every adjacent equal pair (report, never drop)")
                .isEqualTo(24);
    }

    // -------------------------------------------------------------------------------------- helpers

    /** Stage {@code entries} through the REAL production staging writer, chunked into random pages. */
    private Path stage(Path dir, String name, List<ListEntry> entries, Random rnd) throws IOException {
        SortBuffer buffer = new SortBuffer(config, cmp);
        long node = 0;
        int i = 0;
        while (i < entries.size()) {
            int len = 1 + rnd.nextInt(8);
            buffer.admit(node++, new ArrayList<>(entries.subList(i, Math.min(entries.size(), i + len))));
            i += len;
        }
        Path path = dir.resolve(name);
        new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    private List<ListEntry> drainPageAware(List<Path> files, SortMetrics metrics) throws IOException {
        return drainPageAware(files, metrics, DuplicateHook.NO_OP);
    }

    private List<ListEntry> drainPageAware(List<Path> files, SortMetrics metrics, DuplicateHook hook)
            throws IOException {
        List<PageFrontierStream> frontiers = new ArrayList<>();
        for (Path f : files) {
            frontiers.add(new PageFrontierReader(f, metrics));
        }
        List<ListEntry> out = new ArrayList<>();
        try (PageAwareMerger merger = new PageAwareMerger(frontiers, cmp, hook, metrics)) {
            while (merger.hasNext()) {
                out.add(merger.next());
            }
        }
        return out;
    }

    /** A row from a tiny (key, version, row-type) pool — value-equal rows recur by construction. */
    private ListEntry entry(Random rnd, int keyspace) {
        String key = String.format("k%03d", rnd.nextInt(keyspace));
        String version = rnd.nextInt(4) == 0 ? null : "v" + rnd.nextInt(3);
        return switch (rnd.nextInt(6)) {
            case 0 -> new DeleteMarkerEntry(KeyBytes.ofUtf8(key), version, false, 0L, null);
            case 1 -> new CommonPrefixEntry(KeyBytes.ofUtf8(key));
            default -> object(key, version);
        };
    }

    /** Fixed payload ⇒ two rows with the same (key, version) are EQUAL by value, i.e. a real duplicate. */
    private static ObjectEntry object(String key, String version) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, version,
                false, null, null, null, null);
    }
}
