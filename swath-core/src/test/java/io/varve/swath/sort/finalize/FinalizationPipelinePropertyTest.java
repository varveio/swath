/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.PageCompression;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/** Property guard for pipeline finalization over randomized page-run overlap shapes. */
class FinalizationPipelinePropertyTest {

    private static final byte[][] BINARY_KEYS = {
            new byte[]{},
            new byte[]{0},
            new byte[]{0, 0},
            new byte[]{0, (byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF},
            new byte[]{(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF},
            new byte[]{(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF, 0},
            new byte[]{(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF,
                    (byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF},
    };

    private final ListEntryComparator comparator = new ListEntryComparator();

    enum KeyStyle {
        DENSE_SEQUENTIAL,
        SMALL_ALPHABET,
        CLUSTERED,
        BINARY_ADVERSARIAL
    }

    enum OverlapShape {
        DISJOINT_BANDS,
        SHARED_BANDS,
        SPARSE_OVERLAP
    }

    @Property(tries = 60)
    void pipelineFinalizationIsByteExactAcrossEncoderCounts(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 2, max = 7) int bandCount,
            @ForAll @IntRange(min = 4, max = 12) int rowsPerPage,
            @ForAll KeyStyle style,
            @ForAll OverlapShape overlapShape,
            @ForAll @IntRange(min = 1, max = 6) int encoders,
            @ForAll @IntRange(min = 1, max = 32) int finalFileBytes,
            @ForAll long seed) throws IOException {
        Scenario scenario = build(
                segmentCount, bandCount, rowsPerPage, style, overlapShape, seed);
        Path root = Files.createTempDirectory("pipeline-pagerun-");
        try {
            SortedDatasetResult oneEncoder = run(
                    scenario, 1, root, "one", finalFileBytes);
            SortedDatasetResult parallel = run(
                    scenario, encoders, root, "parallel", finalFileBytes);

            List<ListEntry> expected = scenario.allEntries();
            List<ListEntry> actual = readAll(parallel.finalFiles());
            BenchmarkRowOracle.InputOracle oracle = BenchmarkRowOracle.inputForTesting(expected);
            String expectedFingerprint = BenchmarkRowOracle.validateEntriesForTesting(
                    expected, oracle, comparator).orderedFingerprint();
            String oneEncoderFingerprint = BenchmarkRowOracle.validateEntriesForTesting(
                    readAll(oneEncoder.finalFiles()), oracle, comparator).orderedFingerprint();
            String parallelFingerprint = BenchmarkRowOracle.validateEntriesForTesting(
                    actual, oracle, comparator).orderedFingerprint();
            assertThat(parallel.totalRows()).isEqualTo(expected.size());
            assertThat(parallel.finalFiles())
                    .as("every generated multi-page scenario must exercise a real roll")
                    .hasSizeGreaterThan(1);
            assertThat(actual).isSortedAccordingTo(comparator)
                    .containsExactlyInAnyOrderElementsOf(expected);
            assertThat(oneEncoderFingerprint).isEqualTo(expectedFingerprint);
            assertThat(parallelFingerprint).isEqualTo(expectedFingerprint);
            assertStrictlyDisjointPartBoundaries(parallel.finalFiles());
            for (int i = 0; i < parallel.finalFiles().size(); i++) {
                assertThat(parallel.finalFiles().get(i).getFileName().toString())
                        .isEqualTo(String.format("part-%05d.parquet", i));
                int ordinal = i;
                assertThat(SortedParquetStamp.read(parallel.finalFiles().get(i)))
                        .hasValueSatisfying(stamp -> {
                            assertThat(stamp.fileIndex()).isEqualTo(ordinal + 1);
                            assertThat(stamp.fileFinal())
                                    .isEqualTo(ordinal == parallel.finalFiles().size() - 1);
                        });
            }
        } finally {
            deleteTree(root);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private SortedDatasetResult run(Scenario scenario, int encoders, Path root,
            String name, long finalFileBytes) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segments = stage(staging, scenario.segments());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(finalFileBytes)
                .withMergeBudgetBytes(Long.MAX_VALUE)
                .withMergeParallelism(encoders);
        SortedFileWriterFactory writerFactory =
                new SortedParquetWriterFactory(config, SortMode.VERSIONS);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, writerFactory,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortedDatasetCoordinator(run).transform(segments, output, staging,
                SortedDatasetCommitter.NO_OP, ignored -> { }, FinalPassListener.NO_OP);
    }

    private Scenario build(int segmentCount, int bandCount, int rowsPerPage,
            KeyStyle style, OverlapShape overlapShape, long seed) {
        Random random = new Random(seed);
        List<List<List<ListEntry>>> segments = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segments.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int band = 0; band < bandCount; band++) {
            for (int segment = 0; segment < segmentCount; segment++) {
                if (!participates(
                        overlapShape, band, bandCount, segment, segmentCount, random)) {
                    continue;
                }
                int pageRows = Math.max(1, rowsPerPage + random.nextInt(5) - 2);
                List<ListEntry> page = new ArrayList<>(pageRows);
                for (int row = 0; row < pageRows; row++) {
                    ListEntry entry = entry(key(style, band, row, random), random);
                    page.add(entry);
                    all.add(entry);
                }
                page.sort(comparator);
                segments.get(segment).add(page);
            }
        }
        all.sort(comparator);
        return new Scenario(segments, all);
    }

    private static boolean participates(
            OverlapShape shape, int band, int bandCount,
            int segment, int segmentCount, Random random) {
        return switch (shape) {
            case DISJOINT_BANDS -> segment == Math.min(
                    segmentCount - 1, (int) ((long) band * segmentCount / bandCount));
            case SHARED_BANDS -> true;
            case SPARSE_OVERLAP -> segment == 0 || random.nextBoolean();
        };
    }

    private List<Path> stage(Path directory, List<List<List<ListEntry>>> segments)
            throws IOException {
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            Path path = directory.resolve("seg-" + i + StagingNames.PAGE_RUN_SUFFIX);
            SortTestSupport.writePages(
                    path, segments.get(i), SortMode.VERSIONS, PageCompression.LZ4);
            paths.add(path);
        }
        return paths;
    }

    private byte[] key(KeyStyle style, int band, int row, Random random) {
        return switch (style) {
            case DENSE_SEQUENTIAL ->
                    String.format("b%03d/k%03d", band, row).getBytes(StandardCharsets.UTF_8);
            case SMALL_ALPHABET -> String.format("b%03d/%c", band,
                    (char) ('a' + random.nextInt(5))).getBytes(StandardCharsets.UTF_8);
            case CLUSTERED -> String.format("b%03d/c%d-%03d", band,
                    random.nextInt(3), random.nextInt(8))
                    .getBytes(StandardCharsets.UTF_8);
            case BINARY_ADVERSARIAL -> {
                byte[] suffix = BINARY_KEYS[random.nextInt(BINARY_KEYS.length)];
                byte[] key = Arrays.copyOf(suffix, suffix.length + 2);
                System.arraycopy(key, 0, key, 2, suffix.length);
                key[0] = (byte) band;
                key[1] = 0;
                yield key;
            }
        };
    }

    private ListEntry entry(byte[] key, Random random) {
        int kind = random.nextInt(5);
        String version = random.nextInt(4) == 0 ? null : "v" + random.nextInt(3);
        int identity = java.util.Arrays.hashCode(key) * 31
                + (version == null ? 0 : version.hashCode());
        if (kind <= 2) {
            return new ObjectEntry(KeyBytes.of(key), Math.floorMod(identity, 3), 0L, null, null,
                    version, version != null && identity % 2 == 0, null, null, null, null);
        }
        if (kind == 3) {
            return new DeleteMarkerEntry(
                    KeyBytes.of(key), version, identity % 2 == 0, 0L, null);
        }
        return new CommonPrefixEntry(KeyBytes.of(key));
    }

    private List<ListEntry> readAll(List<Path> files) throws IOException {
        List<ListEntry> rows = new ArrayList<>();
        for (Path file : files) {
            try (ParquetEntryReader reader = new ParquetEntryReader(file)) {
                while (reader.hasNext()) {
                    rows.add(reader.next());
                }
            }
        }
        return rows;
    }

    private void assertStrictlyDisjointPartBoundaries(List<Path> files) throws IOException {
        byte[] previousMax = null;
        for (Path file : files) {
            List<ListEntry> rows = readAll(List.of(file));
            assertThat(rows).isNotEmpty().isSortedAccordingTo(comparator);
            byte[] minimum = rows.getFirst().key().rawUnsafe();
            if (previousMax != null) {
                assertThat(KeyBytes.compareUnsigned(previousMax, minimum)).isNegative();
            }
            previousMax = rows.getLast().key().rawUnsafe();
        }
    }

    private record Scenario(
            List<List<List<ListEntry>>> segments, List<ListEntry> allEntries) {
    }
}
