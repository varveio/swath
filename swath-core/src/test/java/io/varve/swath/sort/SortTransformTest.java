/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortTransform} end-to-end over real Parquet staging segments: single-file publish, rolled
 * multi-file publish (range-disjoint, named in key order), cascade through the merge, the publish
 * callback ordering (renames done, staging still present), idempotent re-run, and stale-{@code .tmp}
 * cleanup.
 */
class SortTransformTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void singleFilePublishMergesToOneSortedFileAndDeletesStaging(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c", "e")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d", "f")));

        SortTransform transform = transform(SortConfig.fromSystemProperties());
        SortTransformResult result = transform.transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(1);
        assertThat(result.totalRows()).isEqualTo(6);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
        assertThat(staging).allMatch(p -> !Files.exists(p));           // staging deleted after publish
        // Staging dir cleaned on successful publish: the DIRECTORY itself is removed, not just its
        // contents — the sorter owns it exclusively.
        assertThat(Files.exists(dirs.staging)).isFalse();
        assertThat(listTmp(dirs.output)).isEmpty();                    // no stale .tmp
        assertThat(result.finalFiles().get(0).getFileName().toString()).isEqualTo("part-00001.parquet");
    }

    @Test
    void transformWithProgressCallbackReportsEveryMergedRow(@TempDir Path root) throws IOException {
        // §3.2: the progress-callback overload is the merge-phase feed for swath.progress.units
        // (RunMetrics.recordProgress, wired by ListRunner). Batched at PROGRESS_BATCH_ROWS (1000);
        // this run is far below that, so the only invocation is the final-remainder flush in
        // the roll loop's `finally` (RolledPartWriter#drain) — but every merged row must still be accounted for.
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c", "e")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d", "f")));

        List<Long> batches = new ArrayList<>();
        SortTransformResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP, batches::add);

        assertThat(result.totalRows()).isEqualTo(6);
        assertThat(batches).isNotEmpty();
        assertThat(batches.stream().mapToLong(Long::longValue).sum()).isEqualTo(6);
    }

    @Test
    void onFinalPassStartingFiresExactlyOnceAfterAnyCascadePassesComplete(@TempDir Path root)
            throws IOException {
        // Phase.WRITING becomes reachable via this hook (ListRunner wires it to
        // RunMetrics.setPhase(Phase.WRITING)). Prove it fires exactly once, and only once any
        // cascade passes are already done — fanIn=2 with 5 segments forces a cascade first.
        Dirs dirs = dirs(root);
        List<Path> staging = new ArrayList<>();
        String[] keys = {"e", "b", "d", "a", "c"};
        for (int i = 0; i < keys.length; i++) {
            staging.add(writeSegment(dirs.staging, "seg-" + i + ".parquet", objects(keys[i])));
        }
        SortConfig smallFanIn = SortConfigs.base().withFanIn(2);

        int[] fired = {0};
        SortTransformResult result = transform(smallFanIn)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP, units -> { },
                        () -> fired[0]++);

        assertThat(fired[0]).isEqualTo(1);
        assertThat(result.cascadedPasses()).isGreaterThan(0);   // sanity: a cascade really ran first
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void rolledOutputProducesRangeDisjointFilesNamedInKeyOrder(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c", "e")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d", "f")));

        // final-file-bytes = 1 ⇒ roll after every row: one entry per file.
        SortConfig rolling = SortConfigs.base().withFinalFileBytes(1L);
        SortTransformResult result = transform(rolling)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(6);
        List<String> names = result.finalFiles().stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).containsExactly(
                "part-00001.parquet", "part-00002.parquet", "part-00003.parquet",
                "part-00004.parquet", "part-00005.parquet", "part-00006.parquet");
        // Lexical file order == key order, and files are range-disjoint (one key each, ascending).
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
        for (Path f : result.finalFiles()) {
            assertThat(keys(List.of(f))).hasSize(1);
        }
    }

    @Test
    void cascadesThroughTheMergeWhenSegmentsExceedFanIn(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = new ArrayList<>();
        String[] keys = {"e", "b", "d", "a", "c"};
        for (int i = 0; i < keys.length; i++) {
            staging.add(writeSegment(dirs.staging, "seg-" + i + ".parquet", objects(keys[i])));
        }
        SortConfig smallFanIn = SortConfigs.base().withFanIn(2);
        SortTransformResult result = transform(smallFanIn)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
        // Staging dir fully reclaimed (originals + cascade intermediates) AND the dir itself removed:
        // a directory-stream listing would now throw NoSuchFileException, so assert non-existence
        // directly.
        assertThat(Files.exists(dirs.staging)).isFalse();
    }

    @Test
    void publishCallbackFiresAfterRenamesButBeforeStagingDelete(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b")));

        boolean[] observed = {false};
        PublishListener listener = (finalFiles, rows) -> {
            observed[0] = true;
            assertThat(finalFiles).allMatch(Files::exists);            // renamed into place already
            assertThat(rows).isEqualTo(2);
            assertThat(staging).allMatch(Files::exists);               // staging NOT yet deleted
            assertThat(Files.exists(dirs.staging)).as("staging dir not yet removed").isTrue();
        };
        transform(SortConfig.fromSystemProperties()).transform(staging, dirs.output, dirs.staging, listener);

        assertThat(observed[0]).isTrue();
        assertThat(staging).allMatch(p -> !Files.exists(p));           // deleted after the callback
        assertThat(Files.exists(dirs.staging)).as("staging dir removed after the callback").isFalse();
    }

    @Test
    void reRunIsIdempotent(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        SortTransform transform = transform(SortConfig.fromSystemProperties());

        List<Path> staging1 = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));
        SortTransformResult first = transform.transform(staging1, dirs.output, dirs.staging, PublishListener.NO_OP);
        assertThat(Files.exists(dirs.staging)).as("first publish removed the empty staging dir").isFalse();

        // A crash-then-retry re-stages the same segments and re-runs the merge into the same output.
        // The caller (ListCommand.runSortedParquet) recreates the staging dir before writing new
        // segments on a real resume; mirror that here since the first publish removed it.
        Files.createDirectories(dirs.staging);
        List<Path> staging2 = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));
        SortTransformResult second = transform.transform(staging2, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(keys(second.finalFiles())).containsExactly("a", "b", "c", "d");
        assertThat(second.totalRows()).isEqualTo(first.totalRows());
        assertThat(second.finalFiles().stream().map(p -> p.getFileName().toString()).toList())
                .isEqualTo(first.finalFiles().stream().map(p -> p.getFileName().toString()).toList());
    }

    @Test
    void staleTmpFromACrashedPublishIsCleanedBeforeReRun(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        // A previous run crashed mid-publish, leaving a partial .tmp behind.
        Path stale = Files.createFile(dirs.output.resolve("part-00001.parquet.tmp"));
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b")));

        SortTransformResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(Files.exists(stale)).isFalse();                    // stale tmp removed
        assertThat(keys(result.finalFiles())).containsExactly("a", "b");
        assertThat(listTmp(dirs.output)).isEmpty();
        assertThat(Files.exists(dirs.staging)).as("staging dir removed after publish").isFalse();
    }

    @Test
    void tinyMergeBudgetForcesACascadeEvenUnderAGenerousFanInAndStillProducesCorrectOutput(
            @TempDir Path root) throws IOException {
        // SortTransform must bound the merge pass width by SortConfig#effectiveFanIn() (I11), not
        // the raw fan-in knob — otherwise a generous raw fan-in (512 default) lets a merge hold many
        // segment-row-group-preloading readers open at once, making merge-phase memory a function of
        // segment count rather than the budget knob. Prove the WIRING is live (not just the formula)
        // by giving a tiny merge-budget alongside a generous raw fan-in: passing the raw fanIn
        // straight through would let 10 segments fit a single pass (no cascade); effectiveFanIn()=3
        // forces one.
        Dirs dirs = dirs(root);
        List<Path> staging = new ArrayList<>();
        String[] keys = {"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"};
        for (int i = 0; i < keys.length; i++) {
            staging.add(writeSegment(dirs.staging, "seg-" + i + ".parquet", objects(keys[i])));
        }
        SortConfig tinyBudget = SortConfigs.base().withMergeBudgetBytes(3L * (64L << 10));   // 192 KiB budget / 64 KiB per stream = 3
        assertThat(tinyBudget.effectiveFanIn()).isEqualTo(3);

        SortTransformResult result = transform(tinyBudget)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(keys(result.finalFiles()))
                .containsExactly("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        assertThat(result.totalRows()).isEqualTo(10);
        // The budget-derived bound (3), not the raw fan-in (512), is what forced this cascade.
        assertThat(result.cascadedPasses()).isGreaterThan(0);
    }

    @Test
    void staleFinalFilesFromAnAbandonedPriorAttemptAreSweptBeforeRepublish(@TempDir Path root) throws IOException {
        // A retry that produces FEWER final files than an abandoned prior attempt (here: a stale
        // extra final left behind, as if a previous run's roll knob or segment mix produced more
        // files) must not leave the extra stale final lying outside the new manifest.
        Dirs dirs = dirs(root);
        Path staleFinal = Files.createFile(dirs.output.resolve("part-99999.parquet"));
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b")));

        List<Path> published = new ArrayList<>();
        PublishListener listener = (finalFiles, rows) -> published.addAll(finalFiles);
        SortTransformResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, listener);

        assertThat(Files.exists(staleFinal)).as("stale final swept before republish").isFalse();
        assertThat(keys(result.finalFiles())).containsExactly("a", "b");
        assertThat(result.finalFiles()).containsExactly(dirs.output.resolve("part-00001.parquet"));
        // The manifest callback saw exactly the files this run produced — no stale survivor mixed in.
        assertThat(published).containsExactlyElementsOf(result.finalFiles());
        // Only the produced final remains on disk under the output dir root.
        try (var s = Files.newDirectoryStream(dirs.output, "part-*.parquet")) {
            List<Path> onDisk = new ArrayList<>();
            s.forEach(onDisk::add);
            assertThat(onDisk).containsExactly(dirs.output.resolve("part-00001.parquet"));
        }
    }

    @Test
    void staleMergeIntermediatesFromACrashedCascadeAreSweptBeforeReMerge(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        // A previous merge attempt crashed mid-cascade, leaving an orphaned merge-*.parquet
        // intermediate behind (owned content this transform re-derives fresh every time).
        Path staleIntermediate = writeSegment(dirs.staging, "merge-0.parquet", objects("z"));
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));

        SortTransformResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(Files.exists(staleIntermediate))
                .as("m2: stale cascade intermediate swept before the re-merge").isFalse();
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        // With the stale leftover gone, staging is genuinely empty and removed — without this
        // sweep, the foreign-looking leftover would block the empty-staging-dir cleanup.
        assertThat(Files.exists(dirs.staging)).isFalse();
    }

    @Test
    void liveSortNeverAppliesTheSortFixtureCrossRowTypeDuplicateCheck(@TempDir Path root) throws IOException {
        // sort-fixture's key-unique-across-row-types policy (CaptureSorter.DuplicateKeyCheckingWriterFactory)
        // wraps SortedFileWriterFactory OUTSIDE this class; swath --sort (ListRunner) drives this class
        // with an unwrapped SortedFileWriterFactory (SortedParquetWriterFactory/DEFAULT directly), so a
        // same-key OBJECT/COMMON_PREFIX pair — adjacent under ListEntryComparator but never
        // comparator-EQUAL — must publish successfully here, never throwing
        // CaptureSorter.DuplicateKeyException (§0.5: --sort never drops or rejects user entries).
        Dirs dirs = dirs(root);
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", rows));

        SortTransformResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(result.totalRows()).isEqualTo(2);
        assertThat(keys(result.finalFiles())).containsExactly("a", "a");
    }

    @Test
    void cascadePredictedCounterFiresWhenSegmentsExceedEffectiveFanIn(@TempDir Path root) throws IOException {
        // Sort observability polish: the merge-kickoff advisory (SORT.merge_cascade_predicted) must
        // fire exactly when segments > effectiveFanIn is already knowable up front — mirroring the
        // tiny-merge-budget setup above (effectiveFanIn()=3, 10 segments) that forces a real cascade.
        Dirs dirs = dirs(root);
        List<Path> staging = new ArrayList<>();
        String[] keys = {"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"};
        for (int i = 0; i < keys.length; i++) {
            staging.add(writeSegment(dirs.staging, "seg-" + i + ".parquet", objects(keys[i])));
        }
        SortConfig tinyBudget = SortConfigs.base().withMergeBudgetBytes(3L * (64L << 10));   // 192 KiB budget / 64 KiB per stream = 3
        assertThat(tinyBudget.effectiveFanIn()).isEqualTo(3);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        transformWithMetrics(tinyBudget, metrics)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(metrics.count("SORT.merge_cascade_predicted")).isEqualTo(1);
    }

    @Test
    void cascadePredictedCounterNeverFiresWhenSegmentsFitWithinEffectiveFanIn(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        transformWithMetrics(SortConfig.fromSystemProperties(), metrics)
                .transform(staging, dirs.output, dirs.staging, PublishListener.NO_OP);

        assertThat(metrics.count("SORT.merge_cascade_predicted")).isZero();
    }

    @Test
    void tmpFilesAreWrittenUnderStagingNotOutput_soDataDirIsPureParquet(@TempDir Path root) throws IOException {
        // The *.tmp of each final must live in the SIBLING staging dir, never inside the
        // pure-parquet output (data/) dir — a crash then never strands a *.tmp in data/. Mirror the
        // real ListRunner layout: output = <root>/data, staging = <root>/_staging (siblings).
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        // final-file-bytes tiny ⇒ roll into several files, so several tmp paths are exercised.
        SortConfig rolling = SortConfigs.base().withFinalFileBytes(1L);
        List<Path> staged = List.of(
                writeSegment(staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(staging, "seg-1.parquet", objects("b", "d")));

        List<Path> tmpPathsSeen = new ArrayList<>();
        SortedFileWriterFactory spy = (path, fileIndex) -> {
            tmpPathsSeen.add(path);
            return SortedFileWriterFactory.DEFAULT.create(path, fileIndex);
        };
        SortTransform transform = new SortTransform(new SortRun(rolling, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, spy));
        SortTransformResult result = transform.transform(staged, output, staging, PublishListener.NO_OP);

        // Every tmp the writer was ever handed lived under staging, never under the data/ output dir.
        assertThat(tmpPathsSeen).isNotEmpty();
        for (Path tmp : tmpPathsSeen) {
            assertThat(tmp.getFileName().toString()).endsWith(".tmp");
            assertThat(tmp.getParent()).isEqualTo(staging);
        }
        // The finals landed under data/, and data/ holds ONLY *.parquet (no *.tmp) at publish.
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        assertThat(listTmp(output)).isEmpty();
        try (var s = Files.list(output)) {
            assertThat(s.map(p -> p.getFileName().toString())).allMatch(n -> n.endsWith(".parquet"));
        }
    }

    // --- helpers ---

    private SortTransform transform(SortConfig config) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
    }

    private SortTransform transformWithMetrics(SortConfig config, SortMetrics metrics) {
        return new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP, metrics, SortedFileWriterFactory.DEFAULT));
    }

    private record Dirs(Path output, Path staging) {
    }

    private static Dirs dirs(Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("out/_staging"));
        return new Dirs(output, staging);
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
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
}
