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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link CaptureSorter} — the sort-fixture engine: legacy/unsorted capture parts in, one stamped
 * globally-sorted Parquet file out, via the same staging-segment/{@link KWayMerge}/
 * {@link SortTransform} pipeline {@code --sort} uses. Covers §0.5 (adjacent-equal-key fail-fast,
 * including across a chunk boundary), §0.6 (versioned fail-fast), the atomic tmp-then-rename
 * publish, and the fixed staging dir getting wiped on the next call after a simulated crash.
 */
class CaptureSorterTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    private static SortConfig config(Map<String, String> overrides) {
        return SortConfig.fromProperties(key -> overrides.get(key.substring("swath.sort.".length())));
    }

    @Test
    void sortsUnsortedCaptureIntoOneStampedAscendingFile(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-w0-00000.parquet", objects("e", "b", "f"));
        writePart(captureDir, "part-w1-00000.parquet", objects("a", "d", "c"));

        CaptureSorter sorter = new CaptureSorter(config(Map.of()));
        SortTransformResult result = sorter.sort(captureDir, outputDir);

        assertThat(result.finalFiles()).hasSize(1);
        assertThat(result.totalRows()).isEqualTo(6);
        Path output = result.finalFiles().get(0);
        assertThat(keysOf(output)).containsExactly("a", "b", "c", "d", "e", "f");

        Optional<SortStamp> stamp = SortStamp.read(output);
        assertThat(stamp).isPresent();
        assertThat(stamp.get().mode()).isEqualTo(SortMode.OBJECTS);

        // Staging is transient working state — never left behind after a successful publish.
        assertThat(Files.exists(outputDir.resolve(CaptureSorter.STAGING_DIR_NAME))).isFalse();
    }

    @Test
    void singleInputFileIsAcceptedDirectly(@TempDir Path root) throws IOException {
        Path capture = root.resolve("legacy.parquet");
        writePart(root, "legacy.parquet", objects("c", "a", "b"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        SortTransformResult result = new CaptureSorter(config(Map.of())).sort(capture, outputDir);

        assertThat(keysOf(result.finalFiles().get(0))).containsExactly("a", "b", "c");
    }

    @Test
    void duplicateAdjacentKeyFailsFastNamingTheKey(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "b", "c"));

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class)
                .hasMessageContaining("'b'");
    }

    @Test
    void duplicateKeySplitAcrossTwoStagingChunksIsStillCaught(@TempDir Path root) throws IOException {
        // A tiny segment-entries gate forces "b" to land in one chunk and its duplicate in the next,
        // so only the FINAL cross-segment merge (not the per-chunk local sort) can see them adjacent.
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b"));
        writePart(captureDir, "part-1.parquet", objects("b", "c"));

        SortConfig tinyChunks = SortConfigs.base().withSegmentEntries(2).withMergeBudgetBytes(64L << 20);
        assertThatThrownBy(() -> new CaptureSorter(tinyChunks).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class)
                .hasMessageContaining("'b'");
    }

    /**
     * {@link ListEntryComparator} equality folds in {@code row_type} rank, so the SAME raw key
     * emitted once as {@code OBJECT} and once as {@code COMMON_PREFIX} sorts
     * adjacent in the final output but never compares EQUAL under that comparator — the merge's own
     * comparator-driven {@link DuplicateHook} (exercised by the two tests above) structurally cannot
     * catch it. {@code sort-fixture}'s independent, key-bytes-only read-back check must.
     */
    @Test
    void sameKeyAsObjectAndCommonPrefixFailsFastAcrossRowTypes(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        rows.add(new ObjectEntry(KeyBytes.ofUtf8("b"), 1L, 0L, null, null, null, false, null, null, null, null));
        writePart(captureDir, "part-0.parquet", rows);

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class)
                .hasMessageContaining("'a'");
    }

    /**
     * The cross-row-type check runs inside the {@link CaptureSorter}-only {@code SortedFileWriter}
     * decorator that wraps the final writer (see {@code CaptureSorter.DuplicateKeyCheckingWriterFactory})
     * — its {@code previousKey} state must survive a FINAL-FILE ROLL, not just a single writer's
     * lifetime. Force a roll after every row ({@code finalFileBytes = 1}) so the OBJECT "a" is
     * written to {@code part-00001} and its COMMON_PREFIX duplicate lands in {@code part-00002}
     * — only cross-file state can catch
     * this. Also proves the duplicate-detected path still leaves ONLY {@code .tmp} files behind (no
     * renamed/published final), same as the merge-phase {@link DuplicateHook} discipline.
     */
    @Test
    void duplicateKeyAcrossRowTypesStraddlingAFinalFileRollIsStillCaught(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        writePart(captureDir, "part-0.parquet", rows);

        SortConfig rollEveryRow = SortConfigs.rolledPerEntry();
        assertThatThrownBy(() -> new CaptureSorter(rollEveryRow).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class)
                .hasMessageContaining("'a'");

        // No final (non-tmp) file was ever renamed into place — detected before the rename loop runs.
        try (var stream = Files.newDirectoryStream(outputDir, "part-*.parquet")) {
            assertThat(stream.iterator().hasNext())
                    .as("a duplicate detected mid-write must leave only .tmp files, no published final")
                    .isFalse();
        }
        // Exactly two .tmp files: this makes the ROLL itself a hard precondition of the test, not just
        // an assumption — if writer.dataSize() ever reported 0 after one row (so shouldRoll() never
        // fired), this would silently degrade into same-writer detection and the assertion above would
        // still pass without ever exercising the cross-file previousKey state. The .tmp
        // files live in the SIBLING staging dir (never in outputDir alongside the finals), so a
        // crash never strands a .tmp among the published output.
        Path stagingDir = outputDir.resolve(CaptureSorter.STAGING_DIR_NAME);
        try (var stream = Files.newDirectoryStream(stagingDir, "part-*.parquet.tmp")) {
            List<Path> tmpFiles = new ArrayList<>();
            stream.forEach(tmpFiles::add);
            assertThat(tmpFiles).as("the straddle requires the roll to have actually opened a second file")
                    .hasSize(2);
        }
    }

    @Test
    void duplicateStateIsRangeScopedThroughTheFullParallelCapturePipeline(@TempDir Path root)
            throws Exception {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> duplicateRange = new ArrayList<>(objects("a"));
        duplicateRange.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        writePart(captureDir, "part-0.parquet", duplicateRange);
        writePart(captureDir, "part-1.parquet", objects("z"));

        InterleavingWriterFactory delegate = new InterleavingWriterFactory();
        SortConfig parallel = SortConfigs.base()
                .withSegmentEntries(2)
                .withMergeParallelism(2);
        assertThatThrownBy(() -> new CaptureSorter(parallel, SortMetrics.NO_OP, delegate)
                .sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.DuplicateKeyException.class)
                .hasMessageContaining("'a'");
        assertThat(delegate.rangeZeroWrote.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(delegate.rangeOneWrote.await(1, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * The inline check fires through the same {@link SortMetrics} hook every other sort-fixture
     * algorithm path uses — once per final file created, so post-analysis can see
     * whether a run engaged it.
     */
    @Test
    void inlineDuplicateCheckEmitsAnEngagementCounterPerFinalFile(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "c"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        new CaptureSorter(config(Map.of()), metrics).sort(captureDir, outputDir);

        assertThat(metrics.count("SORT.fixture_dup_check_inline")).isEqualTo(1);
    }

    /**
     * Sibling to the test above, proving the "per final file" part of the semantics (not just "fires
     * at all"): forcing a roll after every row ({@code finalFileBytes = 1}) over N distinct keys opens
     * N final files via the factory, so the engagement counter must fire exactly N times — one call to
     * {@code create()} per file, matching {@code SORT.segment_flushed}'s own per-file granularity.
     */
    @Test
    void inlineDuplicateCheckEngagementCounterFiresOncePerFileAcrossAMultiFileRoll(@TempDir Path root)
            throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "c", "d"));

        SortConfig rollEveryRow = SortConfigs.rolledPerEntry();
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = new CaptureSorter(rollEveryRow, metrics).sort(captureDir, outputDir);

        assertThat(result.finalFiles()).hasSize(4);
        assertThat(metrics.count("SORT.fixture_dup_check_inline")).isEqualTo(4);
    }

    /**
     * Companion to the above: DISTINCT keys with mixed row types are a perfectly legal (if unusual)
     * sort-fixture input — {@code sort-fixture} only enforces key uniqueness and non-versioned, never
     * row-type purity (that is a SERVING-side eligibility concern — a mixed-row-type file
     * this produces is expected to be rejected later by replay's {@code mixed_row_types} check, not
     * rejected here).
     */
    @Test
    void distinctKeysWithMixedRowTypesSortSuccessfully(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a1", "a3"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a2/")));
        writePart(captureDir, "part-0.parquet", rows);

        SortTransformResult result = new CaptureSorter(config(Map.of())).sort(captureDir, outputDir);

        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(keysOf(result.finalFiles().get(0))).containsExactly("a1", "a2/", "a3");
    }

    @Test
    void versionedRowFailsFastNamingTheKey(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a", "b"));
        rows.add(new ObjectEntry(KeyBytes.ofUtf8("m"), 1L, 0L, null, null, "v1", true, null, null, null, null));
        writePart(captureDir, "part-0.parquet", rows);

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.VersionedCaptureException.class)
                .hasMessageContaining("'m'");
    }

    @Test
    void versionedDeleteMarkerAlsoFailsFast(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new DeleteMarkerEntry(KeyBytes.ofUtf8("z"), "v2", true, 0L, null));
        writePart(captureDir, "part-0.parquet", rows);

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(CaptureSorter.VersionedCaptureException.class)
                .hasMessageContaining("'z'");
    }

    @Test
    void staleTmpAndStaleStagingFromACrashAreCleanedOnTheNextRun(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b"));

        // Simulate a crash: a stale FINAL .tmp in outputDir, and stale partial segments left in this
        // engine's own fixed staging dir from an interrupted prior attempt.
        Path staleTmp = Files.createFile(outputDir.resolve("part-00001.parquet.tmp"));
        Path stagingDir = Files.createDirectories(outputDir.resolve(CaptureSorter.STAGING_DIR_NAME));
        Path staleSegment = Files.createFile(stagingDir.resolve("fixture-0.parquet"));

        SortTransformResult result = new CaptureSorter(config(Map.of())).sort(captureDir, outputDir);

        assertThat(Files.exists(staleTmp)).isFalse();
        assertThat(Files.exists(staleSegment)).isFalse();
        assertThat(keysOf(result.finalFiles().get(0))).containsExactly("a", "b");
        assertThat(Files.exists(stagingDir)).isFalse();   // removed again after the clean re-run publishes
    }

    @Test
    void emptyCapturePartsStillPublishOneValidEmptySortedFile(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", List.of());

        SortTransformResult result = new CaptureSorter(config(Map.of())).sort(captureDir, outputDir);

        assertThat(result.totalRows()).isEqualTo(0);
        assertThat(result.finalFiles()).hasSize(1);
        assertThat(SortStamp.read(result.finalFiles().get(0))).isPresent();
    }

    @Test
    void resolvePartsRejectsADirectoryWithNoParquetFiles(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * {@link CaptureSorter}'s {@code outputDir} has
     * NO identity-verified ownership guard (unlike {@code --sort}'s own listing/merge-reentry path,
     * which is gated by {@code ListCommand#isPublishedByThisRun}) — so {@link SortTransform}'s
     * stale-finals sweep must stay scoped to THIS engine's own {@code part-*.parquet} naming, never a
     * blanket {@code *.parquet} glob. A user-supplied {@code --output} dir may already hold unrelated
     * content (here, {@code foo.parquet}) that this engine never created and never read into staging
     * — that content must SURVIVE the sort, not be unrecoverably deleted as a "stale final".
     */
    @Test
    void unrelatedParquetAlreadyInOutputDirSurvivesTheSort(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "c"));

        // Foreign content this engine never created and never read into staging — must never be
        // treated as "this transform's own abandoned prior final" and swept.
        writePart(outputDir, "foo.parquet", objects("x", "y"));
        Path foreign = outputDir.resolve("foo.parquet");
        byte[] foreignBytesBefore = Files.readAllBytes(foreign);

        SortTransformResult result = new CaptureSorter(config(Map.of())).sort(captureDir, outputDir);

        assertThat(Files.exists(foreign))
                .as("unrelated foo.parquet already in outputDir must survive the sort").isTrue();
        assertThat(Files.readAllBytes(foreign))
                .as("foo.parquet's bytes must be untouched").isEqualTo(foreignBytesBefore);
        assertThat(keysOf(result.finalFiles().get(0))).containsExactly("a", "b", "c");
    }

    // --- helpers ---

    private void writePart(Path dir, String name, List<ListEntry> entries) throws IOException {
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        try (SortedCursor cursor = new InMemoryCursor(entries, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
    }

    private List<ListEntry> objects(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(new ObjectEntry(KeyBytes.ofUtf8(k), 1L, 0L, null, null, null, false, null, null, null, null));
        }
        return out;
    }

    private static List<String> keysOf(Path file) throws IOException {
        List<String> out = new ArrayList<>();
        try (SegmentReader r = new SegmentReader(file)) {
            while (r.hasNext()) {
                out.add(r.next().key().asString());
            }
        }
        return out;
    }

    private static ListEntry obj(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false,
                null, null, null, null);
    }

    private static class NoOpWriter implements SortedFileWriter {
        private long rows;

        @Override
        public void write(ListEntry e) {
            rows++;
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
        public void setFileIndex(int fileIndex) {
        }

        @Override
        public void close() {
        }
    }

    private static final class InterleavingWriterFactory implements SortedFileWriterFactory {
        private final CountDownLatch rangeZeroWrote = new CountDownLatch(1);
        private final CountDownLatch rangeOneWrote = new CountDownLatch(1);

        @Override
        public SortedFileWriter create(Path path, int fileIndex) {
            boolean rangeZero = path.getFileName().toString().startsWith("prange-0-");
            return new NoOpWriter() {
                @Override
                public void write(ListEntry e) {
                    try {
                        if (rangeZero && rows() == 0) {
                            rangeZeroWrote.countDown();
                            if (!rangeOneWrote.await(2, TimeUnit.SECONDS)) {
                                throw new AssertionError("range 1 did not interleave");
                            }
                        } else if (!rangeZero) {
                            if (!rangeZeroWrote.await(2, TimeUnit.SECONDS)) {
                                throw new AssertionError("range 0 did not interleave");
                            }
                            rangeOneWrote.countDown();
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("forced interleaving interrupted", interrupted);
                    }
                    super.write(e);
                }
            };
        }
    }
}
