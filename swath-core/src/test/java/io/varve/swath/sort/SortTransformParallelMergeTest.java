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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortTransform}'s off-by-default concurrent range-merge path
 * ({@code swath.sort.merge-parallelism > 1}, {@link ParallelRangeMerge}). Proves the mechanism is
 * CORRECT — the concatenation of the range parts, in filename (== key) order, is a total, gap-free,
 * duplicate-free global sort — for the ordinary case, degenerate keyspaces (fewer distinct keys than
 * R, unsplittable ⇒ serial fallback), the engagement counter, and fail-the-whole-merge on a range
 * error. Byte-exact property coverage exercises this range decomposition under adversarial inputs.
 */
class SortTransformParallelMergeTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void parallelMergeProducesGloballySortedGapFreeOutputMatchingSerial(@TempDir Path root) throws IOException {
        // Three segments whose keys interleave across the whole keyspace, so at least one range must
        // k-way merge sub-streams of ALL three (the cross-segment-in-range case).
        List<List<ListEntry>> segs = List.of(
                objects("a", "d", "g", "j"),
                objects("b", "e", "h", "k"),
                objects("c", "f", "i", "l"));
        List<String> expected = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l");

        // Serial (default parallelism) baseline.
        Dirs serialDirs = dirs(root, "serial");
        List<Path> serialStaging = stage(serialDirs.staging, segs);
        SortTransformResult serial = transform(config(1))
                .transform(serialStaging, serialDirs.output, serialDirs.staging, PublishListener.NO_OP);
        assertThat(keys(serial.finalFiles())).containsExactlyElementsOf(expected);

        // Parallel path with R=3.
        Dirs parDirs = dirs(root, "parallel");
        List<Path> parStaging = stage(parDirs.staging, segs);
        SortTransformResult parallel = transform(config(3))
                .transform(parStaging, parDirs.output, parDirs.staging, PublishListener.NO_OP);

        assertThat(parallel.totalRows()).isEqualTo(12);
        // Filename order == key order == the global sort, no gap, no duplicate.
        assertThat(keys(parallel.finalFiles())).containsExactlyElementsOf(expected);
        List<String> names = parallel.finalFiles().stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).containsExactly(
                "part-00000.parquet", "part-00001.parquet", "part-00002.parquet");
        // Staging fully reclaimed and the dir removed (same publish contract as serial).
        assertThat(Files.exists(parDirs.staging)).isFalse();
        assertThat(listTmp(parDirs.output)).isEmpty();
    }

    @Test
    void multiRowGroupSegmentsAreRowGroupSkippedYetProduceOutputEqualToSerial(@TempDir Path root)
            throws IOException {
        // Row-group skip: three segments each split into MANY row groups (tiny row-group bytes +
        // ~200 B padded keys), interleaved across the whole keyspace. Each R=3 range therefore prunes
        // the non-overlapping row groups of every segment — and the pruned output must still equal both
        // the no-skip serial baseline and the exact expected global sort (skip drops no row).
        List<ListEntry> seg0 = new ArrayList<>();
        List<ListEntry> seg1 = new ArrayList<>();
        List<ListEntry> seg2 = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            String k = String.format("%05d", i) + "x".repeat(200);
            expected.add(k);
            (i % 3 == 0 ? seg0 : i % 3 == 1 ? seg1 : seg2).add(obj(k));
        }

        // Serial baseline (parallelism=1, untouched path — no row-group skip).
        Dirs serialDirs = dirs(root, "mrg-serial");
        List<Path> serialStaging = List.of(
                writeSegment(serialDirs.staging, "seg-0.parquet", seg0, 4096L),
                writeSegment(serialDirs.staging, "seg-1.parquet", seg1, 4096L),
                writeSegment(serialDirs.staging, "seg-2.parquet", seg2, 4096L));
        SortTransformResult serial = transform(config(1))
                .transform(serialStaging, serialDirs.output, serialDirs.staging, PublishListener.NO_OP);
        assertThat(keys(serial.finalFiles())).containsExactlyElementsOf(expected);

        // Parallel path with R=3 + a metrics recorder to prove row-group skip actually ENGAGED.
        Dirs parDirs = dirs(root, "mrg-parallel");
        List<Path> parStaging = List.of(
                writeSegment(parDirs.staging, "seg-0.parquet", seg0, 4096L),
                writeSegment(parDirs.staging, "seg-1.parquet", seg1, 4096L),
                writeSegment(parDirs.staging, "seg-2.parquet", seg2, 4096L));
        ThreadSafeMetrics metrics = new ThreadSafeMetrics();
        SortTransformResult parallel =
                new SortTransform(new SortRun(config(3), cmp, DuplicateHook.NO_OP, metrics, SortedFileWriterFactory.DEFAULT))
                        .transform(parStaging, parDirs.output, parDirs.staging, PublishListener.NO_OP);

        assertThat(parallel.totalRows()).isEqualTo(300);
        assertThat(keys(parallel.finalFiles())).containsExactlyElementsOf(expected);
        // The skip engaged on at least one range (>=1 group pruned) — otherwise this test isn't
        // exercising the mechanism at all.
        assertThat(metrics.count("SORT.merge_range_rowgroup_skipped")).isGreaterThanOrEqualTo(1);
    }

    @Test
    void parallelMergeWithFewerDistinctSampleKeysThanRangesStillTotalAndSorted(@TempDir Path root)
            throws IOException {
        // Only two distinct segment first-keys ("a","b") but R=3 requested ⇒ 2 ranges, still a total,
        // gap-free partition.
        Dirs d = dirs(root, "few");
        List<Path> staging = stage(d.staging, List.of(
                objects("a", "c", "e"),
                objects("b", "d", "f")));
        SortTransformResult r = transform(config(3))
                .transform(staging, d.output, d.staging, PublishListener.NO_OP);

        assertThat(r.totalRows()).isEqualTo(6);
        assertThat(keys(r.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
    }

    @Test
    void anUnsplittableKeyspaceFallsBackToTheSerialPathAndIsCorrect(@TempDir Path root) throws IOException {
        // A single segment ⇒ one distinct sample first-key ⇒ boundaries() returns null ⇒ the untouched
        // serial path runs (one file, byte-identical to today).
        Dirs d = dirs(root, "degenerate");
        List<Path> staging = stage(d.staging, List.of(objects("a", "b", "c", "d")));
        SortTransformResult r = transform(config(4))
                .transform(staging, d.output, d.staging, PublishListener.NO_OP);

        assertThat(r.finalFiles()).hasSize(1);
        assertThat(keys(r.finalFiles())).containsExactly("a", "b", "c", "d");
    }

    @Test
    void engagementCounterFiresOncePerRangeGivingTheRangeCount(@TempDir Path root) throws IOException {
        Dirs d = dirs(root, "counter");
        List<Path> staging = stage(d.staging, List.of(
                objects("a", "d", "g"),
                objects("b", "e", "h"),
                objects("c", "f", "i")));
        ThreadSafeMetrics metrics = new ThreadSafeMetrics();
        new SortTransform(new SortRun(config(3), cmp, DuplicateHook.NO_OP, metrics, SortedFileWriterFactory.DEFAULT))
                .transform(staging, d.output, d.staging, PublishListener.NO_OP);

        // 3 distinct first-keys ⇒ 3 ranges ⇒ the counter total is the range count.
        assertThat(metrics.count("SORT.merge_range_parallel")).isEqualTo(3);
    }

    @Test
    void aFailingRangeFailsTheWholeMerge(@TempDir Path root) throws IOException {
        Dirs d = dirs(root, "fail");
        List<Path> staging = stage(d.staging, List.of(
                objects("a", "d", "g"),
                objects("b", "e", "h"),
                objects("c", "f", "i")));
        // A writer factory that throws for one range's part ⇒ that range's task fails ⇒ the whole
        // merge must fail (never silently publish a partial/gapped output).
        SortedFileWriterFactory boom = (path, fileIndex) -> {
            if (path.getFileName().toString().startsWith("prange-2-")) {
                throw new IOException("injected range failure");
            }
            return SortedFileWriterFactory.DEFAULT.create(path, fileIndex);
        };
        SortTransform transform = new SortTransform(new SortRun(config(3), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, boom));

        assertThatThrownBy(() -> transform.transform(staging, d.output, d.staging, PublishListener.NO_OP))
                .isInstanceOf(IOException.class);
        // Nothing was published into the output data dir.
        try (var s = Files.newDirectoryStream(d.output, "part-*.parquet")) {
            assertThat(s.iterator().hasNext()).isFalse();
        }
    }

    @Test
    void staleParallelRangeTmpFromACrashedPriorAttemptIsSweptOnASuccessfulRerun(@TempDir Path root)
            throws IOException {
        // A crash after ParallelRangeMerge produced range tmp parts but before publish leaves
        // prange-<r>-<local>.parquet.tmp behind. Seed one such leftover, then run a SUCCESSFUL
        // parallel transform() and assert staging is fully reclaimed — an unswept leftover blocks
        // tryDeleteEmptyStagingDir and leaves _staging behind after publish.
        Dirs d = dirs(root, "stale-prange");
        List<Path> staging = stage(d.staging, List.of(
                objects("a", "d", "g"),
                objects("b", "e", "h"),
                objects("c", "f", "i")));
        // Local index 99 so this run's own (single-part-per-range) output never legitimately reuses
        // this exact path — a true orphan, not a file the live run overwrites in place.
        Path staleTmp = Files.createFile(d.staging.resolve("prange-0-99.parquet.tmp"));
        assertThat(Files.exists(staleTmp)).isTrue();

        SortTransformResult r = transform(config(3))
                .transform(staging, d.output, d.staging, PublishListener.NO_OP);

        assertThat(r.totalRows()).isEqualTo(9);
        assertThat(keys(r.finalFiles()))
                .containsExactly("a", "b", "c", "d", "e", "f", "g", "h", "i");
        // The stale leftover must not survive into (or block cleanup of) a successful publish.
        assertThat(Files.exists(d.staging)).isFalse();
    }

    // --- helpers ---

    private SortConfig config(int mergeParallelism) {
        return SortConfigs.base().withMergeBudgetBytes(64L << 20).withMergeParallelism(mergeParallelism);
    }

    private SortTransform transform(SortConfig config) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
    }

    private record Dirs(Path output, Path staging) {
    }

    private static Dirs dirs(Path root, String name) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(root.resolve(name + "/_staging"));
        return new Dirs(output, staging);
    }

    private List<Path> stage(Path stagingDir, List<List<ListEntry>> segments) throws IOException {
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            out.add(writeSegment(stagingDir, "seg-" + i + ".parquet", segments.get(i)));
        }
        return out;
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        return writeSegment(dir, name, sorted, 1L << 20);
    }

    /** Force multiple row groups per segment by shrinking {@code segmentRowGroupBytes} (engages the row-group skip). */
    private Path writeSegment(Path dir, String name, List<ListEntry> sorted, long segmentRowGroupBytes)
            throws IOException {
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, segmentRowGroupBytes);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    private ListEntry obj(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    private List<ListEntry> objects(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(new ObjectEntry(KeyBytes.ofUtf8(k), 1L, 0L, null, null, null, false, null, null, null, null));
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

    private static List<Path> listTmp(Path dir) throws IOException {
        try (var s = Files.newDirectoryStream(dir, "*.tmp")) {
            List<Path> out = new ArrayList<>();
            s.forEach(out::add);
            return out;
        }
    }

    /** Thread-safe {@link SortMetrics} — the parallel path records from several range threads at once. */
    private static final class ThreadSafeMetrics implements SortMetrics {
        private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.computeIfAbsent(outcome + "." + reason, k -> new LongAdder()).increment();
        }

        long count(String key) {
            LongAdder a = counts.get(key);
            return a == null ? 0 : a.sum();
        }
    }
}
