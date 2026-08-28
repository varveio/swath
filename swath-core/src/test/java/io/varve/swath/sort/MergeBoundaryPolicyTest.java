/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MergeBoundaryPolicyTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Property(tries = 60)
    void distinctAndRowsBothPreserveTotalOrderAndMultiplicity(@ForAll long seed)
            throws IOException {
        Scenario scenario = scenario(seed);
        Path root = Files.createTempDirectory("row-boundary-property-");
        try {
            SortTestSupport.CountingMetrics distinctMetrics = new SortTestSupport.CountingMetrics();
            List<ListEntry> distinct = run(root, "distinct", scenario,
                    MergeBoundaryPolicy.DISTINCT, distinctMetrics);
            SortTestSupport.CountingMetrics rowsMetrics = new SortTestSupport.CountingMetrics();
            RunResult rows = runWithFiles(root, "rows", scenario,
                    MergeBoundaryPolicy.ROWS, rowsMetrics);
            List<ListEntry> expected = new ArrayList<>(scenario.allRows());
            expected.sort(CMP);

            assertThat(distinct).containsExactlyElementsOf(expected);
            assertThat(rows.rows()).containsExactlyElementsOf(expected);
            assertThat(rows.rows()).containsExactlyInAnyOrderElementsOf(scenario.allRows());
            assertThat(rowsMetrics.count("SORT.merge_boundary_rows_on")).isEqualTo(1);
            assertThat(distinctMetrics.count("SORT.merge_boundary_rows_on")).isZero();
            assertEqualKeysStayInOneFile(rows.files());
        } finally {
            Sweeps.deleteTree(root);
        }
    }

    @Test
    void hotPrefixMassQuantilesMateriallyLowerMaxMedianRowSkew(@TempDir Path root)
            throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("skew.pageseg"), skewedPages());
        Prepared distinctPrepared = prepared(List.of(segment), 16_384);
        SortTestSupport.CountingMetrics distinctMetrics = new SortTestSupport.CountingMetrics();
        List<byte[]> distinct = ParallelRangeMerge.boundaries(
                distinctPrepared.descriptors(), distinctPrepared.candidates(), 4,
                MergeBoundaryPolicy.DISTINCT, distinctMetrics);
        Prepared rowsPrepared = prepared(List.of(segment), 16_384);
        SortTestSupport.CountingMetrics rowsMetrics = new SortTestSupport.CountingMetrics();
        List<byte[]> rows = ParallelRangeMerge.boundaries(
                rowsPrepared.descriptors(), rowsPrepared.candidates(), 4,
                MergeBoundaryPolicy.ROWS, rowsMetrics);
        List<ListEntry> all = skewedPages().stream().flatMap(List::stream).toList();

        List<Integer> distinctCounts = rangeCounts(all, distinct);
        List<Integer> rowCounts = rangeCounts(all, rows);
        assertThat(distinctCounts).containsExactly(3, 3, 3, 300);
        assertThat(rowCounts).containsExactly(9, 100, 100, 100);
        assertThat(skew(rowCounts)).isLessThan(skew(distinctCounts) / 4.0);
        assertThat(rowsMetrics.count("SORT.merge_boundary_rows_on")).isEqualTo(1);
        assertThat(rowsMetrics.rangeIndexBytes.sum()).isPositive();
        assertStrictlyIncreasing(rows);
    }

    @Test
    void defaultAndExplicitDistinctProduceByteIdenticalParallelOutput(@TempDir Path root)
            throws IOException {
        Scenario scenario = scenario(42L);
        SortTestSupport.CountingMetrics defaultMetrics = new SortTestSupport.CountingMetrics();
        RunResult defaultRun = runWithConfig(root, "default", scenario,
                SortConfigs.base().withMergeParallelism(4).withMergeBudgetBytes(64L << 20),
                defaultMetrics);
        SortTestSupport.CountingMetrics explicitMetrics = new SortTestSupport.CountingMetrics();
        RunResult explicit = runWithConfig(root, "explicit", scenario,
                SortConfigs.base().withMergeParallelism(4).withMergeBudgetBytes(64L << 20)
                        .withMergeBoundaryPolicy(MergeBoundaryPolicy.DISTINCT), explicitMetrics);

        assertThat(defaultRun.files()).hasSameSizeAs(explicit.files());
        for (int i = 0; i < defaultRun.files().size(); i++) {
            assertThat(Files.readAllBytes(defaultRun.files().get(i)))
                    .containsExactly(Files.readAllBytes(explicit.files().get(i)));
        }
        assertThat(defaultMetrics.count("SORT.merge_boundary_rows_on")).isZero();
        assertThat(explicitMetrics.count("SORT.merge_boundary_rows_on")).isZero();
    }

    @Test
    void rowsPolicyUsesOnlyTheBoundedDistinctCandidateSetAndPollsCancellation(
            @TempDir Path root) throws IOException {
        Path segment = SortTestSupport.writeIndexedPages(
                root.resolve("many.pageseg"), manyPages(128));
        Prepared cancelled = prepared(List.of(segment), 8);
        assertThat(cancelled.candidates().size()).isEqualTo(8);
        assertThat(cancelled.candidates().capped()).isTrue();
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> ParallelRangeMerge.boundaries(
                    cancelled.descriptors(), cancelled.candidates(), 4,
                    MergeBoundaryPolicy.ROWS, SortMetrics.NO_OP))
                    .isInstanceOf(MergeCancellation.Cancelled.class);
        } finally {
            Thread.interrupted();
        }

        Prepared prepared = prepared(List.of(segment), 8);
        List<byte[]> retained = prepared.candidates().sortedKeys();
        List<byte[]> boundaries = ParallelRangeMerge.boundaries(
                prepared.descriptors(), prepared.candidates(), 4,
                MergeBoundaryPolicy.ROWS, SortMetrics.NO_OP);
        assertThat(boundaries).hasSize(3).allSatisfy(boundary ->
                assertThat(retained).anySatisfy(candidate ->
                        assertThat(candidate).containsExactly(boundary)));
        assertStrictlyIncreasing(boundaries);
        assertThat(Arrays.stream(RowWeightedBoundaries.class.getDeclaredFields())
                .map(java.lang.reflect.Field::getType))
                .noneMatch(Class::isArray);
    }

    @Test
    void rowHistogramBecomesPrefixMassInPlaceWithoutASecondPolicyArray() {
        long[] histogram = {3, 7, 11, 13};

        long[] prefix = RowWeightedBoundaries.prefixBeforeInPlace(histogram);

        assertThat(prefix).isSameAs(histogram).containsExactly(0, 3, 10, 21);
    }

    private static Scenario scenario(long seed) {
        Random random = new Random(seed);
        List<List<ListEntry>> segments = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        List<ListEntry> all = new ArrayList<>();
        for (int row = 0; row < 150; row++) {
            int key = row < 90 ? row : random.nextInt(90);
            ListEntry entry = SortTestSupport.object(String.format("k%05d", key));
            segments.get(row < 3 ? row : random.nextInt(segments.size())).add(entry);
            all.add(entry);
        }
        segments.forEach(segment -> segment.sort(CMP));
        List<List<List<ListEntry>>> pages = new ArrayList<>();
        for (List<ListEntry> segment : segments) {
            List<List<ListEntry>> segmentPages = new ArrayList<>();
            for (int offset = 0; offset < segment.size();) {
                int length = Math.min(segment.size() - offset, 1 + random.nextInt(7));
                segmentPages.add(List.copyOf(segment.subList(offset, offset + length)));
                offset += length;
            }
            pages.add(segmentPages);
        }
        return new Scenario(pages, List.copyOf(all));
    }

    private static List<List<ListEntry>> skewedPages() {
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int page = 0; page < 9; page++) {
            pages.add(List.of(SortTestSupport.object(String.format("cold/%02d", page))));
        }
        for (int page = 0; page < 3; page++) {
            List<ListEntry> hot = new ArrayList<>();
            for (int row = 0; row < 100; row++) {
                hot.add(SortTestSupport.object(String.format("hot/%d/%03d", page, row)));
            }
            pages.add(List.copyOf(hot));
        }
        return List.copyOf(pages);
    }

    private static List<List<ListEntry>> manyPages(int count) {
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int page = 0; page < count; page++) {
            pages.add(List.of(SortTestSupport.object(String.format("p%05d", page))));
        }
        return pages;
    }

    private static List<ListEntry> run(Path root, String name, Scenario scenario,
                                       MergeBoundaryPolicy policy,
                                       SortTestSupport.CountingMetrics metrics) throws IOException {
        return runWithFiles(root, name, scenario, policy, metrics).rows();
    }

    private static RunResult runWithFiles(Path root, String name, Scenario scenario,
                                          MergeBoundaryPolicy policy,
                                          SortTestSupport.CountingMetrics metrics) throws IOException {
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(4)
                .withMergeBudgetBytes(64L << 20)
                .withMergeBoundaryPolicy(policy);
        return runWithConfig(root, name, scenario, config, metrics);
    }

    private static RunResult runWithConfig(Path root, String name, Scenario scenario,
                                           SortConfig config,
                                           SortTestSupport.CountingMetrics metrics) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segments = new ArrayList<>();
        for (int i = 0; i < scenario.pages().size(); i++) {
            segments.add(SortTestSupport.writeIndexedPages(
                    staging.resolve("segment-" + i + ".pageseg"), scenario.pages().get(i)));
        }
        SortTransform transform = new SortTransform(new SortRun(config, CMP, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, metrics, SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        SortTransformResult result = transform.transform(segments, output, staging,
                PublishListener.NO_OP, units -> { }, FinalPassListener.NO_OP);
        return new RunResult(readAll(result.finalFiles()), result.finalFiles());
    }

    private static Prepared prepared(List<Path> paths, int candidateCap) throws IOException {
        ParallelRangeMerge.BoundaryCandidates candidates =
                new ParallelRangeMerge.BoundaryCandidates(candidateCap);
        List<PageRunSegmentDescriptor> descriptors = PageRunSegmentDescriptor.readAll(
                paths, path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP),
                Optional.of(candidates::add));
        return new Prepared(descriptors, candidates);
    }

    private static List<ListEntry> readAll(List<Path> files) throws IOException {
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

    private static void assertEqualKeysStayInOneFile(List<Path> files) throws IOException {
        Map<String, Integer> owners = new HashMap<>();
        for (int file = 0; file < files.size(); file++) {
            for (ListEntry row : readAll(List.of(files.get(file)))) {
                String key = new String(row.key().rawUnsafe(), StandardCharsets.UTF_8);
                assertThat(owners.putIfAbsent(key, file)).isIn(null, file);
            }
        }
    }

    private static List<Integer> rangeCounts(List<ListEntry> rows, List<byte[]> boundaries) {
        List<Integer> counts = new ArrayList<>();
        for (int range = 0; range <= boundaries.size(); range++) {
            counts.add(0);
        }
        for (ListEntry row : rows) {
            int range = 0;
            while (range < boundaries.size()
                    && Arrays.compareUnsigned(row.key().rawUnsafe(), boundaries.get(range)) >= 0) {
                range++;
            }
            counts.set(range, counts.get(range) + 1);
        }
        return counts;
    }

    private static double skew(List<Integer> counts) {
        List<Integer> sorted = counts.stream().sorted().toList();
        double median = (sorted.get(1) + sorted.get(2)) / 2.0;
        return sorted.getLast() / median;
    }

    private static void assertStrictlyIncreasing(List<byte[]> boundaries) {
        for (int i = 1; i < boundaries.size(); i++) {
            assertThat(Arrays.compareUnsigned(boundaries.get(i - 1), boundaries.get(i))).isNegative();
        }
    }

    private record Scenario(List<List<List<ListEntry>>> pages, List<ListEntry> allRows) {
    }

    private record Prepared(List<PageRunSegmentDescriptor> descriptors,
                            ParallelRangeMerge.BoundaryCandidates candidates) {
    }

    private record RunResult(List<ListEntry> rows, List<Path> files) {
    }
}
