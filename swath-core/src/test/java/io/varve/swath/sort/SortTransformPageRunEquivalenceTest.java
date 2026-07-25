/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The merge runs on <b>page-run</b> staging segments ({@link PageRunSegmentWriter} /
 * {@link PageRunSegmentReader}) and produces the SAME final sorted Parquet output the
 * columnar-Parquet staging path produces — byte-for-byte at the entry level, over adversarial input
 * (many interleaved segments, equal keys across segments, a pinned tiny fan-in that forces a
 * cascade). {@link SortTransform#segmentIo}'s reader dispatches on the staging extension, so both
 * formats route through the IDENTICAL {@link KWayMerge} algorithm and final {@link
 * SortedParquetWriter} — this test proves the swap is output-preserving.
 */
class SortTransformPageRunEquivalenceTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** fanIn=2 + mergeBudget=2×rowGroup ⇒ effectiveFanIn=2, so >2 segments cascade (multi-pass). */
    private SortConfig cascadingConfig() {
        return SortConfigs.base().withFanIn(2).withMergeBudgetBytes(2L << 20);
    }

    @Test
    void pageRunStagingCascadeMergeProducesTheExactGlobalSort(@TempDir Path root) throws IOException {
        // Five segments, keys interleaved across the whole keyspace so a real k-way merge (and, with
        // effectiveFanIn=2, a cascade) is exercised; distinct keys ⇒ the expected output is the full
        // sorted set.
        List<List<ListEntry>> segs = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (int s = 0; s < 5; s++) {
            List<ListEntry> seg = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                ListEntry e = object(String.format("k%05d", i * 5 + s));   // stride-5 interleave
                seg.add(e);
                all.add(e);
            }
            segs.add(seg);   // already ascending within the segment
        }
        List<String> expected = all.stream().map(e -> e.key().asString()).sorted().toList();

        Path staging = Files.createDirectories(root.resolve("prun/_staging"));
        Path output = Files.createDirectories(root.resolve("prun"));
        List<Path> stagingSegments = stagePageRun(staging, segs);

        SortTransformResult result = new SortTransform(new SortRun(cascadingConfig(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT))
                .transform(stagingSegments, output, staging, PublishListener.NO_OP);

        List<String> keys = finalKeys(result.finalFiles());
        assertThat(result.totalRows()).isEqualTo(200);
        assertThat(keys).containsExactlyElementsOf(expected);
        assertThat(keys).isSorted();
        assertThat(result.cascadedPasses()).isGreaterThan(0);   // the cascade actually fired
        assertThat(Files.exists(staging)).isFalse();            // staging reclaimed on publish (§6)
    }

    @Test
    void pageRunAndParquetStagingYieldByteIdenticalFinalOutput(@TempDir Path root) throws IOException {
        // Adversarial: overlapping ranges with EQUAL keys shared across segments (page-boundary dup
        // path) plus interleaving. Both staging formats feed the identical merge, so their final
        // outputs must be entry-for-entry identical regardless of any dedup semantics.
        List<List<ListEntry>> segs = List.of(
                sorted("a", "a", "c", "e", "g", "m"),
                sorted("a", "b", "d", "f", "h", "m"),
                sorted("b", "c", "i", "j", "k", "m"));

        Path prunStaging = Files.createDirectories(root.resolve("pr/_staging"));
        Path prunOut = Files.createDirectories(root.resolve("pr"));
        SortTransformResult pageRun = new SortTransform(new SortRun(cascadingConfig(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT))
                .transform(stagePageRun(prunStaging, segs), prunOut, prunStaging, PublishListener.NO_OP);

        Path pqStaging = Files.createDirectories(root.resolve("pq/_staging"));
        Path pqOut = Files.createDirectories(root.resolve("pq"));
        SortTransformResult parquet = new SortTransform(new SortRun(cascadingConfig(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT))
                .transform(stageParquet(pqStaging, segs), pqOut, pqStaging, PublishListener.NO_OP);

        assertThat(pageRun.totalRows()).isEqualTo(parquet.totalRows());
        // Entry-for-entry identity of the final decoded rows — the byte-identical-output claim.
        assertThat(finalEntries(pageRun.finalFiles())).isEqualTo(finalEntries(parquet.finalFiles()));
        assertThat(finalKeys(pageRun.finalFiles())).isSorted();

        // ABSOLUTE, not relative. The equivalence assertions above compare the two mergers against
        // EACH OTHER: if BOTH dropped one of the three "m" rows they would still agree, and this test would
        // stay green while the DuplicateHook contract (§0.5: swath --sort never drops user entries — it is a
        // sorter, not a deduper) was being violated. Pin the exact staged multiset instead.
        List<String> expected = segs.stream()
                .flatMap(List::stream)
                .map(e -> e.key().asString())
                .sorted()
                .toList();
        assertThat(expected).hasSize(18);
        assertThat(Collections.frequency(expected, "a")).isEqualTo(3);
        assertThat(Collections.frequency(expected, "m")).isEqualTo(3);

        for (SortTransformResult result : List.of(pageRun, parquet)) {
            List<String> keys = finalKeys(result.finalFiles());
            assertThat(result.totalRows())
                    .as("every staged row reaches the final output — absolute row count").isEqualTo(18);
            assertThat(keys)
                    .as("the final keys ARE the staged multiset, in global order (no drop, no dedup)")
                    .containsExactlyElementsOf(expected);
            assertThat(Collections.frequency(keys, "a"))
                    .as("all three duplicate \"a\" rows survive").isEqualTo(3);
            assertThat(Collections.frequency(keys, "m"))
                    .as("all three duplicate \"m\" rows (one per segment) survive").isEqualTo(3);
        }
    }

    // --- helpers ---

    private List<ListEntry> sorted(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(object(k));
        }
        out.sort(cmp);
        return out;
    }

    /** Stage each pre-sorted segment as a page-run {@code .pageseg} file (the live format). */
    private List<Path> stagePageRun(Path dir, List<List<ListEntry>> segs) throws IOException {
        PageRunSegmentWriter writer = new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
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

    /** Stage each pre-sorted segment as a columnar {@code .parquet} file (the equivalence baseline). */
    private List<Path> stageParquet(Path dir, List<List<ListEntry>> segs) throws IOException {
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            Path path = dir.resolve("seg-" + i + ".parquet");
            try (SortedCursor cursor = new InMemoryCursor(segs.get(i), cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
    }

    private List<String> finalKeys(List<Path> files) throws IOException {
        List<String> out = new ArrayList<>();
        for (ListEntry e : finalEntries(files)) {
            out.add(e.key().asString());
        }
        return out;
    }

    private List<ListEntry> finalEntries(List<Path> files) throws IOException {
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
}
