/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.fixture.SegmentReader;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization pins for one- and multi-encoder pipeline publication, captured at the
 * {@link SortTransform} boundary so a change to finalization cannot silently alter observable
 * output.
 *
 * <p><b>The pinned invariant: both encoder shapes stamp identically.</b> Whichever shape produced the output,
 * the published set carries a GLOBAL {@code file_index} = {@code 1..N} in filename order with exactly
 * one {@code file_final}, on {@code N} — the self-describing completeness proof (contracts.md §5,
 * "served-file footer stamp"). That symmetry is what lets {@code merge-parallelism} be a performance
 * knob rather than a change to what the output means.
 *
 * <p>Both loops also share one progress-feed cadence — batched at {@link KWayMerge#PROGRESS_BATCH_ROWS},
 * a full batch each time the counter reaches it and a remainder flush at the end — pinned here for the
 * one-encoder path as an exact ordered sequence; the multi-encoder path drains concurrently, so
 * only its total (every row accounted for) is a deterministic observable (see that test).
 */
class SortRollPublishStampCharacterizationTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /**
     * One-encoder rolled publish: with a 1-byte roll gate every row lands in its own file, so the four
     * rows produce {@code part-00000..part-00003}. The stamp's {@code file_index} remains the GLOBAL
     * 1..N position in filename order and {@code file_final} is present on the LAST file only.
     */
    @Test
    void oneEncoderStampsGlobalFileIndexOneToNAndFinalOnLastOnly(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                SortTestSupport.writePages(dirs.staging.resolve("seg-0.pageseg"),
                        List.of(objects("a"), objects("c"))),
                SortTestSupport.writePages(dirs.staging.resolve("seg-1.pageseg"),
                        List.of(objects("b"), objects("d"))));

        SortConfig config = SortConfigs.base().withFinalFileBytes(1L);
        List<Long> progress = new ArrayList<>();
        List<FinalPart> published = new ArrayList<>();
        SortTransformResult result = stampedTransform(config)
                .transform(staging, dirs.output, dirs.staging,
                        (parts, ignoredRows) -> published.addAll(parts), progress::add,
                        FinalPassListener.NO_OP);

        // Roll cadence: one row per file, four files, named in key order.
        assertThat(result.finalFiles()).hasSize(4);
        assertThat(names(result.finalFiles())).containsExactly(
                "part-00000.parquet", "part-00001.parquet", "part-00002.parquet", "part-00003.parquet");
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        for (Path f : result.finalFiles()) {
            assertThat(keys(List.of(f))).hasSize(1);
        }
        assertThat(published.stream().map(FinalPart::path).toList())
                .as("closed rolled writers retain metadata through the publish handoff")
                .containsExactlyElementsOf(result.finalFiles());
        for (int i = 0; i < published.size(); i++) {
            String expectedKey = String.valueOf((char) ('a' + i));
            FinalPartMetadata metadata = published.get(i).metadata().orElseThrow();
            assertThat(metadata.rows()).isEqualTo(1);
            assertThat(metadata.minKey()).isEqualTo(expectedKey);
            assertThat(metadata.maxKey()).isEqualTo(expectedKey);
        }

        // Footer stamp: file_index is the global 1..N in filename order; file_final on the last file only.
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < result.finalFiles().size(); i++) {
            Path f = result.finalFiles().get(i);
            indices.add(fileIndex(f));
            boolean isLast = i == result.finalFiles().size() - 1;
            assertThat(hasFinalKey(f))
                    .as("file_final present only on the last serial file (%s)", names(List.of(f)).get(0))
                    .isEqualTo(isLast);
        }
        assertThat(indices).containsExactly(1, 2, 3, 4);

        // Every merged row is fed to the progress sink (sub-batch remainder flush here — four rows).
        assertThat(progress.stream().mapToLong(Long::longValue).sum()).isEqualTo(4);
    }

    /**
     * Multi-encoder publish: three disjoint inputs, each row in its own part, so the nine rows produce
     * {@code part-00000..part-00008} — a correct global sort by filename order.
     * The filename ordinal is zero-based while the footer's compatibility-preserving {@code
     * file_index} is the global 1..N position.
     */
    @Test
    void multiEncoderStampsTheSameGlobalFileIndexAndFinalAsOneEncoder(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        // Each segment holds a contiguous third of the keyspace. A 1-byte final-file gate rolls every
        // row into its own part.
        List<Path> staging = List.of(
                SortTestSupport.writePages(dirs.staging.resolve("seg-0.pageseg"),
                        List.of(objects("a"), objects("b"), objects("c"))),
                SortTestSupport.writePages(dirs.staging.resolve("seg-1.pageseg"),
                        List.of(objects("d"), objects("e"), objects("f"))),
                SortTestSupport.writePages(dirs.staging.resolve("seg-2.pageseg"),
                        List.of(objects("g"), objects("h"), objects("i"))));

        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(1L).withMergeParallelism(3).withMergeBudgetBytes(64L << 20);
        List<Long> progress = new ArrayList<>();
        SortTransformResult result = stampedTransform(config)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP, progress::add,
                        FinalPassListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(9);
        assertThat(names(result.finalFiles())).containsExactly(
                "part-00000.parquet", "part-00001.parquet", "part-00002.parquet",
                "part-00003.parquet", "part-00004.parquet", "part-00005.parquet",
                "part-00006.parquet", "part-00007.parquet", "part-00008.parquet");
        assertThat(keys(result.finalFiles()))
                .containsExactly("a", "b", "c", "d", "e", "f", "g", "h", "i");

        // GLOBAL file_index 1..9 in filename order. This is the assertion that distinguishes a stamped
        // multi-encoder publish from an encoder-local stamp.
        List<Integer> indices = new ArrayList<>();
        for (Path f : result.finalFiles()) {
            indices.add(fileIndex(f));
        }
        assertThat(indices).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Exactly one file_final, on the highest index — the other half of the completeness proof. A
        // reader accepts the set iff the indices are contiguous from 1 and exactly one file claims to
        // be last, so a stamp on the wrong file is as broken as no stamp at all.
        for (Path f : result.finalFiles()) {
            boolean last = f.equals(result.finalFiles().get(result.finalFiles().size() - 1));
            assertThat(hasFinalKey(f))
                    .as("file_final present iff last part (%s)", names(List.of(f)).get(0))
                    .isEqualTo(last);
        }

        // Every merged row still reaches the progress sink across encoder threads.
        assertThat(progress.stream().mapToLong(Long::longValue).sum()).isEqualTo(9);
    }

    /**
     * The serial progress feed is batched at {@link KWayMerge#PROGRESS_BATCH_ROWS} (1000): a full batch
     * each time the counter reaches it, then a remainder flush. A single-pass merge (fan-in ≫ segment
     * count, no cascade) of 2500 rows into one file therefore emits exactly {@code 1000, 1000, 500}. The
     * multi-encoder loop shares this cadence, but concurrent emissions interleave nondeterministically,
     * so only its total is a stable observable (pinned above).
     */
    @Test
    void serialProgressFeedIsFullBatchesThenRemainder(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<ListEntry> even = new ArrayList<>();
        List<ListEntry> odd = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            (i % 2 == 0 ? even : odd).add(object(String.format("%06d", i)));
        }
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", even, 1L << 20),
                writeSegment(dirs.staging, "seg-1.parquet", odd, 1L << 20));

        // base(): fan-in 512 ≫ 2 segments and an unbounded budget ⇒ a single merge pass (no cascade),
        // and a single output file ⇒ the whole 2500-row stream drains through one encoder.
        List<Long> batches = new ArrayList<>();
        SortTransformResult result = transform(SortConfigs.base())
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP, batches::add,
                        FinalPassListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(1);
        assertThat(result.totalRows()).isEqualTo(2500);
        assertThat(result.cascadedPasses()).isZero();   // single pass: progress comes only from the roll loop
        assertThat(batches).containsExactly(1000L, 1000L, 500L);
    }

    // --- helpers ---

    private SortTransform transform(SortConfig config) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private SortTransform stampedTransform(SortConfig config) {
        SortedFileWriterFactory stamped = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, stamped,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private record Dirs(Path output, Path staging) {
    }

    private static Dirs dirs(Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("out/_staging"));
        return new Dirs(output, staging);
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted, long ignoredPageRunBytes)
            throws IOException {
        return SortTestSupport.writePageRun(
                dir.resolve(name.replace(".parquet", StagingNames.PAGE_RUN_SUFFIX)), sorted, cmp);
    }

    private static ListEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    private List<ListEntry> objects(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(object(k));
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

    private static List<String> names(List<Path> files) {
        return files.stream().map(p -> p.getFileName().toString()).toList();
    }

    private static int fileIndex(Path file) throws IOException {
        return Integer.parseInt(footerKv(file).get(SortedParquetWriter.FILE_INDEX_KEY));
    }

    private static boolean hasFinalKey(Path file) throws IOException {
        return footerKv(file).containsKey(SortedParquetWriter.FILE_FINAL_KEY);
    }

    private static Map<String, String> footerKv(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFooter().getFileMetaData().getKeyValueMetaData();
        }
    }
}
