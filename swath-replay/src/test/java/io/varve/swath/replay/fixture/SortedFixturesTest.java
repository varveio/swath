/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortTransformResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedFixtures} — detection, file resolution, and multi-file-aware index load with its
 * global strictly-ascending sanity check. Builds its stamped fixtures through
 * {@link CaptureSorter} (the same production path {@code sort-fixture} uses) and
 * its unstamped/legacy fixtures through the plain {@code PartWriter} the replay module's other
 * tests already use for parquet fixtures (e.g. {@code SwathRoundTripIT}) — no parquet type needs
 * importing beyond what those existing tests already establish.
 */
class SortedFixturesTest {

    @Test
    void detectsAStampedSortedFixtureButNotAPlainUnstampedPart(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "b", "a", "c");
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path stamped = sortToOneFile(capture, outputDir, config());

        assertThat(SortedFixtures.detect(stamped)).isPresent();

        Path plain = dir.resolve("plain.parquet");
        writeUnsortedPart(plain, "a", "b", "c");
        assertThat(SortedFixtures.detect(plain)).isEmpty();
    }

    @Test
    void resolveFilesAcceptsASingleFileOrGlobsADirectoryInLexicalOrder(@TempDir Path dir) throws IOException {
        Path single = dir.resolve("one.parquet");
        writeUnsortedPart(single, "a");
        assertThat(SortedFixtures.resolveFiles(single)).containsExactly(single);

        Path multi = Files.createDirectories(dir.resolve("multi"));
        Path second = multi.resolve("part-00002.parquet");
        Path first = multi.resolve("part-00001.parquet");
        writeUnsortedPart(second, "z");
        writeUnsortedPart(first, "a");
        assertThat(SortedFixtures.resolveFiles(multi)).containsExactly(first, second);
    }

    @Test
    void resolveFilesPreservesAnEmptyDirectoryAsAnEmptyResult(@TempDir Path dir) throws IOException {
        assertThat(SortedFixtures.resolveFiles(dir)).isEmpty();
    }

    @Test
    void sortMetricsHookRecordsReplayProgress() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FixtureMetrics metrics = new FixtureMetrics(registry);

        metrics.markProgress();

        assertThat(registry.find("swath.replay.sort.progress").counter().count()).isEqualTo(1.0);
    }

    @Test
    void loadIndexAcrossASingleFileIsAscendingAndRecordsMetrics(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "e", "b", "d", "a", "c");
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path stamped = sortToOneFile(capture, outputDir, config());

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FixtureMetrics metrics = new FixtureMetrics(registry);
        IndexLoadResult result = SortedFixtures.loadIndex(List.of(stamped), metrics);

        assertThat(result).isInstanceOf(IndexLoadResult.Loaded.class);
        List<IndexEntry> entries = ((IndexLoadResult.Loaded) result).entries();
        assertThat(entries).isNotEmpty();
        assertThat(entries).allMatch(e -> e.file().equals(stamped));
        assertThat(registry.find("swath.replay.index.load.latency").tag("source", "derived").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("swath.replay.index.entries").summary().totalAmount())
                .isEqualTo(entries.size());
        assertThat(registry.find("swath.replay.serving.fallback").counter()).isNull();
    }

    @Test
    void loadIndexAcrossMultipleRolledFilesIsGloballyAscending(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"),
                "f", "b", "e", "a", "d", "c");
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        // Tiny final-file-bytes forces a roll: one entry per file (multi-file, range-disjoint).
        SortConfig rolling = SortConfigs.rolledPerEntry();
        SortTransformResult result = new CaptureSorter(rolling).sort(capture, outputDir);
        assertThat(result.finalFiles()).hasSizeGreaterThan(1);

        List<Path> files = SortedFixtures.resolveFiles(outputDir);
        assertThat(files).hasSize(result.finalFiles().size());

        FixtureMetrics metrics = new FixtureMetrics(new SimpleMeterRegistry());
        IndexLoadResult loaded = SortedFixtures.loadIndex(files, metrics);

        assertThat(loaded).isInstanceOf(IndexLoadResult.Loaded.class);
        List<IndexEntry> entries = ((IndexLoadResult.Loaded) loaded).entries();
        assertThat(entries).hasSize(files.size());   // one row group (one entry) per rolled file
        for (int i = 1; i < entries.size(); i++) {
            assertThat(entries.get(i - 1).firstKey().compareTo(entries.get(i).firstKey())).isLessThan(0);
        }
    }

    @Test
    void loadIndexFlagsAMisorderedFilePairAsATypedSanityFailureWithFallbackMetric(@TempDir Path dir)
            throws IOException {
        // Two legitimately-sorted single-row-group files, deliberately handed to loadIndex in the
        // WRONG order — the global ascending check must catch the violation at the file boundary.
        Path captureA = Files.createDirectories(dir.resolve("captureA"));
        writeUnsortedPart(captureA.resolve("part-0.parquet"), "d", "e", "f");
        Path outA = Files.createDirectories(dir.resolve("outA"));
        Path fileA = sortToOneFile(captureA, outA, config());   // keys d,e,f

        Path captureB = Files.createDirectories(dir.resolve("captureB"));
        writeUnsortedPart(captureB.resolve("part-0.parquet"), "a", "b", "c");
        Path outB = Files.createDirectories(dir.resolve("outB"));
        Path fileB = sortToOneFile(captureB, outB, config());   // keys a,b,c

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FixtureMetrics metrics = new FixtureMetrics(registry);
        IndexLoadResult result = SortedFixtures.loadIndex(List.of(fileA, fileB), metrics);   // wrong order

        assertThat(result).isInstanceOf(IndexLoadResult.SanityFailed.class);
        assertThat(((IndexLoadResult.SanityFailed) result).reason()).isEqualTo("sanity_failed");
        assertThat(registry.find("swath.replay.serving.fallback").tag("reason", "sanity_failed").counter().count())
                .isEqualTo(1.0);
    }

    /**
     * A legacy delimiter'd capture's {@code COMMON_PREFIX} rows survive
     * {@code sort-fixture} (only {@code version_id} and adjacent-equal-key are checked, never
     * {@code row_type}) into a file stamped {@code mode=objects} unconditionally — so
     * {@link SortedFixtures#loadIndex} must refuse it (rather than build an index whose {@code
     * rowCount} silently counts non-{@code OBJECT} rows) with the typed {@link
     * SortedFixtures#MIXED_ROW_TYPES} reason, and must only bump the {@code serving.fallback} counter
     * when asked to: {@code --serving-mode sorted} passes {@code recordFallbackOnFailure=
     * false} because a hard fail is not a "fallback".
     */
    @Test
    void loadIndexFlagsMixedRowTypesAsATypedFailureAndOnlyRecordsTheFallbackMetricWhenAsked(@TempDir Path dir)
            throws IOException {
        Path mixed = mixedRowTypeSortedFixture(dir);

        SimpleMeterRegistry recordingRegistry = new SimpleMeterRegistry();
        FixtureMetrics recordingMetrics = new FixtureMetrics(recordingRegistry);
        IndexLoadResult recorded = SortedFixtures.loadIndex(List.of(mixed), recordingMetrics);   // 2-arg: records
        assertThat(recorded).isInstanceOf(IndexLoadResult.SanityFailed.class);
        assertThat(((IndexLoadResult.SanityFailed) recorded).reason()).isEqualTo(SortedFixtures.MIXED_ROW_TYPES);
        assertThat(recordingRegistry.find("swath.replay.serving.fallback")
                .tag("reason", "mixed_row_types").counter().count()).isEqualTo(1.0);

        SimpleMeterRegistry suppressedRegistry = new SimpleMeterRegistry();
        FixtureMetrics suppressedMetrics = new FixtureMetrics(suppressedRegistry);
        IndexLoadResult suppressed = SortedFixtures.loadIndex(List.of(mixed), suppressedMetrics, false);
        assertThat(suppressed).isInstanceOf(IndexLoadResult.SanityFailed.class);
        assertThat(((IndexLoadResult.SanityFailed) suppressed).reason()).isEqualTo(SortedFixtures.MIXED_ROW_TYPES);
        assertThat(suppressedRegistry.find("swath.replay.serving.fallback").counter()).isNull();
    }

    @Test
    void loadIndexStillLoadsAPureObjectFixtureAfterTheMixedRowTypeCheck(@TempDir Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        writeUnsortedPart(capture.resolve("part-0.parquet"), "a1", "a3", "a5");
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path stamped = sortToOneFile(capture, outputDir, config());

        IndexLoadResult result = SortedFixtures.loadIndex(List.of(stamped), new FixtureMetrics());
        assertThat(result).isInstanceOf(IndexLoadResult.Loaded.class);
    }

    // --- helpers ---

    private static SortConfig config() {
        return SortConfig.fromSystemProperties();
    }

    private static Path sortToOneFile(Path capture, Path outputDir, SortConfig config) throws IOException {
        SortTransformResult result = new CaptureSorter(config).sort(capture, outputDir);
        assertThat(result.finalFiles()).hasSize(1);
        assertThat(SortedParquetStamp.read(result.finalFiles().get(0)).map(SortedParquetStamp::mode)).contains(SortMode.OBJECTS);
        return result.finalFiles().get(0);
    }

    private static void writeUnsortedPart(Path path, String... keys) throws IOException {
        try (var writer = ParquetFixtures.open(path)) {
            for (String k : keys) {
                writer.write(object(k));
            }
        }
    }

    /**
     * Builds a stamped {@code mode=objects} sorted file whose rows are actually a mix of
     * {@code OBJECT} and {@code COMMON_PREFIX} (the exact real-world shape: a legacy delimiter'd
     * capture, re-sorted by {@code sort-fixture}, which never inspects {@code row_type}).
     */
    static Path mixedRowTypeSortedFixture(Path dir) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("mixed-capture-" + System.nanoTime()));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            writer.write(object("a1"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a2/")));
            writer.write(object("a3"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a4/")));
            writer.write(object("a5"));
            writer.write(new CommonPrefixEntry(KeyBytes.ofUtf8("a6/")));
            writer.write(object("a7"));
        }
        Path outputDir = Files.createDirectories(dir.resolve("mixed-out-" + System.nanoTime()));
        return sortToOneFile(capture, outputDir, config());
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.bare(key);
    }
}
