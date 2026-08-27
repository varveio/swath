/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The runtime merge-entry fan-in clamp in {@link SortTransform}. The fd limit is injected (never
 * the real process {@code ulimit}) so the clamp — and the {@code SORT.merge_fanin_*} counters —
 * fire deterministically. Also covers the default-off multi-part rolling at the transform level
 * (small {@code final-file-bytes} ⇒ several range-disjoint {@code part-NNNNN.parquet} whose
 * concatenation is the global sort).
 */
class SortTransformFanInClampTest {

    private static final int HEALTHY_FD = 10_000;
    private final ListEntryComparator cmp = new ListEntryComparator();

    /** A generous static fan-in (512) with an unbounded budget ⇒ static effectiveFanIn == 512. */
    private SortConfig ampleFanIn() {
        return SortConfigs.base();
    }

    @Test
    void lowFdLimitClampsFanInFiresMergeFaninClampedAndStaysCorrect(@TempDir Path root) throws IOException {
        // static fan-in 512, injected soft fd limit 200 (fd bound 72) ⇒ the fd clamp trims fan-in to 72;
        // 5 segments < 72 so it is still single-pass, but the clamp event is recorded.
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Result r = runPageRun(root, ampleFanIn(), metrics, () -> 200);

        assertThat(metrics.count("SORT.merge_fanin_clamped")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_fanin_fd_clamped")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_fanin_mem_clamped")).isZero();   // budget unbounded ⇒ memory never binds
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isZero();       // 5 segs < clamped 72 ⇒ single pass
        assertThat(r.keys).isSorted();
        assertThat(r.keys).containsExactlyElementsOf(r.expected);
    }

    @Test
    void healthyFdAndAmpleFanInStaysSinglePassWithZeroCascadeAndZeroClamp(@TempDir Path root) throws IOException {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Result r = runPageRun(root, ampleFanIn(), metrics, () -> HEALTHY_FD);

        // The single-pass design point: fan-in >= segment count AND a healthy fd limit ⇒ no cascade.
        assertThat(r.result.cascadedPasses()).isZero();
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isZero();
        assertThat(metrics.count("SORT.merge_fanin_clamped")).isZero();
        assertThat(r.keys).isSorted();
        assertThat(r.keys).containsExactlyElementsOf(r.expected);
    }

    @Test
    void pinnedSmallFanInStillEngagesTheCascadeBackstop(@TempDir Path root) throws IOException {
        // fan-in pinned to 2 ⇒ 5 segments cascade multi-pass even under a healthy fd limit (the backstop).
        SortConfig pinned = SortConfigs.base().withFanIn(2);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Result r = runPageRun(root, pinned, metrics, () -> HEALTHY_FD);

        assertThat(r.result.cascadedPasses()).isGreaterThan(0);
        assertThat(metrics.count("SORT.merge_pass_cascaded")).isGreaterThan(0);
        assertThat(r.keys).isSorted();
        assertThat(r.keys).containsExactlyElementsOf(r.expected);
    }

    @Test
    void pageTrailerRecordSizeRefinementClampsFanInAndFiresMemClamped(@TempDir Path root) throws IOException {
        // A tiny per-stream estimate (8 bytes) makes the STATIC budget bound huge ⇒ static effectiveFanIn
        // == raw fan-in 512; but the EXACT bound from the page trailers (mergeBudget / real packed-page
        // size) is far smaller, so the merge-entry memory clamp — not the fd clamp — reduces the fan-in.
        long budget = 100_000L;
        SortConfig cfg = SortConfigs.base().withMergeBudgetBytes(budget).withMergePerStreamBytes(8L);
        assertThat(cfg.effectiveFanIn()).isEqualTo(512);   // static: fan-in binds, memory estimate does not

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        Result r = runPageRun(root, cfg, metrics, () -> HEALTHY_FD);

        assertThat(metrics.count("SORT.merge_fanin_clamped")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_fanin_mem_clamped")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_fanin_fd_clamped")).isZero();   // fd healthy ⇒ fd never binds
        assertThat(r.keys).isSorted();
        assertThat(r.keys).containsExactlyElementsOf(r.expected);
    }

    @Test
    void unreadableTrailerFailsRecordSizePlanningInsteadOfFallingBack(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("broken/_staging"));
        Path segment = stagePageRun(staging, List.of(List.of(object("a")))).get(0);
        byte[] bytes = Files.readAllBytes(segment);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(segment, bytes);

        assertThatThrownBy(() -> PageRunSegmentDescriptor.readAll(List.of(segment)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bad or missing page-run trailer");
    }

    @Test
    void smallFinalFileBytesRollsIntoRangeDisjointPartsInGlobalOrder(@TempDir Path root) throws IOException {
        // A small final-file-bytes rolls the sorted output into multiple part-NNNNN.parquet files;
        // part boundaries are monotonic (no key in part N+1 < any key in part N) and reading all
        // parts in order reconstructs the full global sort.
        List<List<ListEntry>> segs = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (int s = 0; s < 4; s++) {
            List<ListEntry> seg = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                ListEntry e = object(String.format("k%05d", i * 4 + s));
                seg.add(e);
                all.add(e);
            }
            segs.add(seg);
        }
        List<String> expected = all.stream().map(e -> e.key().asString()).sorted().toList();

        Path staging = Files.createDirectories(root.resolve("r/_staging"));
        Path output = Files.createDirectories(root.resolve("r"));
        // final-file-bytes small ⇒ several multi-row parts (not one-per-row); healthy fd so nothing else
        // interferes with the roll behaviour under test.
        SortConfig rolling = SortConfigs.base().withFinalFileBytes(2048L);
        SortTransformResult result = newTransform(rolling, SortMetrics.NO_OP, () -> HEALTHY_FD)
                .transform(stagePageRun(staging, segs), output, staging, PublishListener.NO_OP);

        List<Path> parts = result.finalFiles();
        assertThat(parts.size()).as("small final-file-bytes must roll into multiple parts").isGreaterThan(1);
        for (int i = 0; i < parts.size(); i++) {
            assertThat(parts.get(i).getFileName().toString())
                    .isEqualTo(String.format("part-%05d.parquet", i));
        }
        // Monotonic part boundaries: the last key of part i is strictly before the first key of part i+1.
        String prevMax = null;
        for (Path part : parts) {
            List<String> partKeys = keys(List.of(part));
            assertThat(partKeys).isNotEmpty();
            assertThat(partKeys).isSorted();
            if (prevMax != null) {
                assertThat(prevMax.compareTo(partKeys.get(0))).isLessThan(0);
            }
            prevMax = partKeys.get(partKeys.size() - 1);
        }
        // Concatenating the parts in name order reconstructs the full global sort.
        assertThat(keys(parts)).containsExactlyElementsOf(expected);
    }

    // --- harness ---

    private record Result(SortTransformResult result, List<String> keys, List<String> expected) {
    }

    /** Stage 5 interleaved page-run segments, run the merge with the injected fd limit, read the output. */
    private Result runPageRun(Path root, SortConfig config, SortMetrics metrics, IntSupplier softFdLimit)
            throws IOException {
        List<List<ListEntry>> segs = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (int s = 0; s < 5; s++) {
            List<ListEntry> seg = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                ListEntry e = object(String.format("k%05d", i * 5 + s));
                seg.add(e);
                all.add(e);
            }
            segs.add(seg);
        }
        List<String> expected = all.stream().map(e -> e.key().asString()).sorted().toList();

        Path staging = Files.createDirectories(root.resolve("prun/_staging"));
        Path output = Files.createDirectories(root.resolve("prun"));
        SortTransformResult result = newTransform(config, metrics, softFdLimit)
                .transform(stagePageRun(staging, segs), output, staging, PublishListener.NO_OP);
        return new Result(result, keys(result.finalFiles()), expected);
    }

    private SortTransform newTransform(SortConfig config, SortMetrics metrics, IntSupplier softFdLimit) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, metrics, SortedFileWriterFactory.DEFAULT), false,
                RangeMergeTimer.NO_OP, softFdLimit);
    }

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

    private List<String> keys(List<Path> files) throws IOException {
        List<String> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader r = new SegmentReader(f)) {
                while (r.hasNext()) {
                    out.add(r.next().key().asString());
                }
            }
        }
        return out;
    }
}
