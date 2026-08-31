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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
            new byte[]{0, (byte) 0xFF},
            new byte[]{(byte) 0xFF},
            new byte[]{(byte) 0xFF, 0},
            new byte[]{(byte) 0xFF, (byte) 0xFF},
    };

    private final ListEntryComparator comparator = new ListEntryComparator();

    enum KeyStyle {
        DENSE_SEQUENTIAL,
        SMALL_ALPHABET,
        CLUSTERED,
        BINARY_ADVERSARIAL
    }

    @Property(tries = 60)
    void pipelineFinalizationIsByteExactAcrossEncoderCounts(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 1, max = 6) int encoders,
            @ForAll @IntRange(min = 1, max = 16_384) int finalFileBytes,
            @ForAll long seed) throws IOException {
        Scenario scenario = build(segmentCount, entryCount, style, seed);
        Path root = Files.createTempDirectory("pipeline-pagerun-");
        try {
            SortTransformResult oneEncoder = run(
                    scenario, 1, root, "one", finalFileBytes);
            SortTransformResult parallel = run(
                    scenario, encoders, root, "parallel", finalFileBytes);

            List<ListEntry> expected = scenario.allEntries();
            List<ListEntry> actual = readAll(parallel.finalFiles());
            assertThat(parallel.totalRows()).isEqualTo(expected.size());
            assertThat(actual).isSortedAccordingTo(comparator)
                    .containsExactlyInAnyOrderElementsOf(expected)
                    .containsExactlyElementsOf(readAll(oneEncoder.finalFiles()));
            assertStrictlyDisjointPartBoundaries(parallel.finalFiles());
            for (int i = 0; i < parallel.finalFiles().size(); i++) {
                assertThat(parallel.finalFiles().get(i).getFileName().toString())
                        .isEqualTo(String.format("part-%05d.parquet", i));
            }
        } finally {
            Sweeps.deleteTree(root);
        }
    }

    private SortTransformResult run(Scenario scenario, int encoders, Path root,
            String name, long finalFileBytes) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segments = stage(staging, scenario.segments());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(finalFileBytes)
                .withMergeBudgetBytes(Long.MAX_VALUE)
                .withMergeParallelism(encoders);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortTransform(run).transform(segments, output, staging,
                PublishListener.NO_OP, ignored -> { }, FinalPassListener.NO_OP);
    }

    private Scenario build(int segmentCount, int entryCount, KeyStyle style, long seed) {
        Random random = new Random(seed);
        List<List<ListEntry>> segments = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segments.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            ListEntry entry = entry(key(style, random), random);
            all.add(entry);
            segments.get(random.nextInt(segmentCount)).add(entry);
        }
        for (List<ListEntry> segment : segments) {
            segment.sort(comparator);
        }
        return new Scenario(segments, all);
    }

    private List<Path> stage(Path directory, List<List<ListEntry>> segments) throws IOException {
        PageRunSegmentWriter writer = new PageRunSegmentWriter(
                comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            Path path = directory.resolve("seg-" + i + StagingNames.PAGE_RUN_SUFFIX);
            try (SortedCursor cursor = new InMemoryCursor(
                    segments.get(i), comparator, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            paths.add(path);
        }
        return paths;
    }

    private byte[] key(KeyStyle style, Random random) {
        return switch (style) {
            case DENSE_SEQUENTIAL ->
                    String.format("k%03d", random.nextInt(64)).getBytes(StandardCharsets.UTF_8);
            case SMALL_ALPHABET -> new byte[]{(byte) ('a' + random.nextInt(5))};
            case CLUSTERED -> String.format("c%d-%03d", random.nextInt(3), random.nextInt(8))
                    .getBytes(StandardCharsets.UTF_8);
            case BINARY_ADVERSARIAL ->
                    BINARY_KEYS[random.nextInt(BINARY_KEYS.length)].clone();
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
            try (SegmentReader reader = new SegmentReader(file)) {
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

    private record Scenario(List<List<ListEntry>> segments, List<ListEntry> allEntries) {
    }
}
