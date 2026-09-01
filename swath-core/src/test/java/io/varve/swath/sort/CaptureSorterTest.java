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
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link CaptureSorter} — the sort-fixture engine: legacy/unsorted capture parts in, one stamped
 * globally-sorted Parquet file out, via the same staging-segment/{@link KWayMerge}/
 * {@link SortTransform} pipeline {@code --sort} uses. Covers §0.5 (raw-key fail-fast in the final
 * drain, including across a chunk boundary), §0.6 (versioned fail-fast), the atomic tmp-then-rename
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

        Optional<SortedParquetStamp> stamp = SortedParquetStamp.read(output);
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
    void arbitraryOverlappingChunksUseTheBoundedPagePipeline(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> evensDescending = new ArrayList<>();
        List<ListEntry> oddsDescending = new ArrayList<>();
        for (int i = 5_999; i >= 0; i--) {
            (i % 2 == 0 ? evensDescending : oddsDescending)
                    .add(obj(String.format("k%05d", i)));
        }
        writePart(captureDir, "part-0.parquet", evensDescending);
        writePart(captureDir, "part-1.parquet", oddsDescending);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortConfig config = SortConfigs.base()
                .withSegmentEntries(1_500)
                .withMergeParallelism(4);
        SortTransformResult result = new CaptureSorter(config, metrics).sort(captureDir, outputDir);

        assertThat(result.totalRows()).isEqualTo(6_000);
        assertThat(keysOf(result.finalFiles().getFirst())).containsExactly(
                java.util.stream.IntStream.range(0, 6_000)
                        .mapToObj(i -> String.format("k%05d", i))
                        .toArray(String[]::new));
        assertThat(metrics.count("SORT.finalization_pipeline")).isEqualTo(1);
    }

    @Test
    void arbitraryFixtureChunksUseTheFixedPageRunTrailer(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>();
        for (int i = 2_499; i >= 0; i--) {
            rows.add(obj(String.format("k%05d", i)));
        }
        writePart(captureDir, "part-0.parquet", rows);

        SortedFileWriterFactory failAfterStaging = (path, fileIndex) -> {
            throw new IOException("stop after staging");
        };
        CaptureSorter sorter = new CaptureSorter(
                SortConfigs.base().withSegmentEntries(3_000), SortMetrics.NO_OP, failAfterStaging);
        assertThatThrownBy(() -> sorter.sort(captureDir, outputDir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stop after staging");

        Path staging = outputDir.resolve(CaptureSorter.STAGING_DIR_NAME);
        Path segment = staging.resolve(StagingNames.fixtureSegment(0));
        assertThat(Files.exists(segment)).isTrue();
        try (PageRunSegmentIo io = PageRunSegmentIo.open(segment, SortMetrics.NO_OP)) {
            PageRunTrailer.Trailer trailer = PageRunTrailer.read(io);
            assertThat(trailer.totalRecords()).isEqualTo(3);
            assertThat(trailer.totalEntries()).isEqualTo(2_500);
        }
    }

    @Test
    void fixtureChunkEncoderEmitsExactlyOneSegmentCounterAtEachInclusiveGate(@TempDir Path root)
            throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("e", "d", "c", "b", "a"));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortTransformResult result = new CaptureSorter(
                SortConfigs.base().withSegmentEntries(2), metrics)
                .sort(captureDir, outputDir);

        assertThat(result.totalRows()).isEqualTo(5);
        assertThat(metrics.count("SORT.segment_flushed"))
                .as("two exact entry-cap chunks plus one drained tail, with no call-site double count")
                .isEqualTo(3);
    }

    @Test
    void duplicateAdjacentKeyFailsFastNamingTheKey(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "b", "c"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        assertThatThrownBy(() -> new CaptureSorter(config(Map.of()), metrics)
                .sort(captureDir, outputDir))
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessage("sort-fixture found a duplicate key "
                        + "(adjacent-equal under the sort order): 'b'");
        assertThat(metrics.count("SORT.equal_key_rejected"))
                .as("one rejection signal for the failing final drain")
                .isEqualTo(1);
    }

    @Test
    void duplicateKeySplitAcrossTwoStagingChunksIsStillCaught(@TempDir Path root) throws IOException {
        // A tiny segment-entries gate forces "b" to land in one chunk and its duplicate in the next,
        // so the shared FINAL drain must carry the comparison across the cross-segment merge.
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b"));
        writePart(captureDir, "part-1.parquet", objects("b", "c"));

        SortConfig tinyChunks = SortConfigs.base().withSegmentEntries(2).withMergeBudgetBytes(64L << 20);
        assertThatThrownBy(() -> new CaptureSorter(tinyChunks).sort(captureDir, outputDir))
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessageContaining("'b'");
    }

    /**
     * {@link ListEntryComparator} equality folds in {@code row_type} rank, so the SAME raw key
     * emitted once as {@code OBJECT} and once as {@code COMMON_PREFIX} sorts
     * adjacent in the final output but never compares EQUAL under that comparator — the merge's own
     * comparator-driven {@link DuplicateHook} structurally cannot catch it. The final drain's
     * raw-key policy must.
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
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessage("sort-fixture found a duplicate key across row types "
                        + "(adjacent-equal key bytes regardless of row_type): 'a'");
    }

    /**
     * The cross-row-type check runs in the shared final drain, whose {@code previousKey} state must
     * see adjacent cross-type rows before publication. Force the
     * roll threshold after every row ({@code finalFileBytes = 1}); key-atomic rolling deliberately
     * defers the roll across the OBJECT/COMMON_PREFIX key group, and the fixture policy rejects the
     * second row before a new part opens. This also proves the duplicate-detected path leaves only a
     * {@code .tmp} file behind (no
     * renamed/published final).
     */
    @Test
    void duplicateKeyAcrossRowTypesIsCaughtBeforeAKeyAtomicFinalRoll(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        writePart(captureDir, "part-0.parquet", rows);

        SortConfig rollEveryRow = SortConfigs.rolledPerEntry().withSegmentEntries(1);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        assertThatThrownBy(() -> new CaptureSorter(rollEveryRow, metrics).sort(captureDir, outputDir))
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessageContaining("'a'");
        assertThat(metrics.count("SORT.equal_key_rejected")).isEqualTo(1);

        // No final (non-tmp) file was ever renamed into place — detected before the rename loop runs.
        try (var stream = Files.newDirectoryStream(outputDir, "part-*.parquet")) {
            assertThat(stream.iterator().hasNext())
                    .as("a duplicate detected mid-write must leave only .tmp files, no published final")
                    .isFalse();
        }
        // The failed pipeline discards its temporary before returning.
        Path stagingDir = outputDir.resolve(CaptureSorter.STAGING_DIR_NAME);
        try (var stream = Files.newDirectoryStream(stagingDir, "part-*.parquet.tmp")) {
            List<Path> tmpFiles = new ArrayList<>();
            stream.forEach(tmpFiles::add);
            assertThat(tmpFiles).isEmpty();
        }

        // A clean re-run owns and removes the failed attempt's tmp/staging state before publishing.
        Path cleanCapture = Files.createDirectories(root.resolve("clean-capture"));
        writePart(cleanCapture, "part-0.parquet", objects("a", "b"));
        SortTransformResult clean = new CaptureSorter(rollEveryRow).sort(cleanCapture, outputDir);
        assertThat(clean.finalFiles()).hasSize(2);
        assertThat(Files.exists(stagingDir)).isFalse();
    }

    /**
     * Successful key-unique fixtures never emit the rejection-only counter.
     */
    @Test
    void distinctKeysDoNotEmitEqualKeyRejected(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "c"));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        new CaptureSorter(config(Map.of()), metrics).sort(captureDir, outputDir);

        assertThat(metrics.count("SORT.equal_key_rejected")).isZero();
    }

    /**
     * A multi-file roll also stays silent: the counter describes an actual rejection, not policy
     * arming or file creation.
     */
    @Test
    void distinctKeysAcrossAMultiFileRollDoNotEmitEqualKeyRejected(@TempDir Path root)
            throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b", "c", "d"));

        SortConfig rollEveryRow = SortConfigs.rolledPerEntry().withSegmentEntries(1);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransformResult result = new CaptureSorter(rollEveryRow, metrics).sort(captureDir, outputDir);

        assertThat(result.finalFiles()).hasSize(4);
        assertThat(metrics.count("SORT.equal_key_rejected")).isZero();
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
                .isInstanceOf(VersionedCaptureException.class)
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
                .isInstanceOf(VersionedCaptureException.class)
                .hasMessageContaining("'z'");
    }

    @Test
    void staleTmpAndStaleStagingFromACrashAreCleanedOnTheNextRun(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));
        writePart(captureDir, "part-0.parquet", objects("a", "b"));

        // Simulate a crash: a stale FINAL .tmp in outputDir, and stale partial segments left in this
        // engine's own fixed staging dir from an interrupted prior attempt.
        Path staleTmp = Files.createFile(outputDir.resolve("part-00000.parquet.tmp"));
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
        assertThat(SortedParquetStamp.read(result.finalFiles().get(0))).isPresent();
    }

    @Test
    void resolvePartsRejectsADirectoryWithNoParquetFiles(@TempDir Path root) throws IOException {
        Path captureDir = Files.createDirectories(root.resolve("capture"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        assertThatThrownBy(() -> new CaptureSorter(config(Map.of())).sort(captureDir, outputDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no *.parquet files found in " + captureDir);
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
        SortTestSupport.writeCanonicalParquet(dir.resolve(name), entries);
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
        try (ParquetEntryReader r = new ParquetEntryReader(file)) {
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
        public void close() {
        }
    }

}
