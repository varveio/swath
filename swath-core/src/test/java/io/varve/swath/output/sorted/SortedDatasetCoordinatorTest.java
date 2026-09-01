/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPart;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.finalize.SortTestSupport;
import io.varve.swath.sort.spill.PageRunCatalog;
import io.varve.swath.sort.spill.PageRunCorruptionException;
import io.varve.swath.sort.spill.PageRunDescriptor;
import io.varve.swath.sort.spill.PageRunRawFixtures;
import io.varve.swath.sort.spill.PageRunReader;
import io.varve.swath.sort.spill.SpillTestFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedDatasetCoordinator} end-to-end over real page-run staging segments: single-file publish, rolled
 * multi-file publish (strictly key-disjoint, named in key order), cascade through the merge, the publish
 * callback ordering (renames done, staging still present), idempotent re-run, and stale-{@code .tmp}
 * cleanup.
 */
class SortedDatasetCoordinatorTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void singleFilePublishMergesToOneSortedFileAndDeletesStaging(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c", "e")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d", "f")));

        SortedDatasetCoordinator transform = transform(SortConfig.fromSystemProperties());
        SortedDatasetResult result = transform.transform(staging, dirs.output, dirs.staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(1);
        assertThat(result.totalRows()).isEqualTo(6);
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f");
        assertThat(staging).allMatch(p -> !Files.exists(p));           // staging deleted after publish
        // Staging dir cleaned on successful publish: the DIRECTORY itself is removed, not just its
        // contents — the sorter owns it exclusively.
        assertThat(Files.exists(dirs.staging)).isFalse();
        assertThat(listTmp(dirs.output)).isEmpty();                    // no stale .tmp
        assertThat(result.finalFiles().get(0).getFileName().toString()).isEqualTo("part-00000.parquet");
    }

    @Test
    void transformWithProgressCallbackReportsEveryMergedRow(@TempDir Path root) throws IOException {
        // §3.2: the progress-callback overload is the merge-phase feed for swath.progress.units
        // (RunMetrics.recordProgress, wired by ListRunner). Batched at PROGRESS_BATCH_ROWS (1000);
        // this run is far below that, so the only invocation is the final-remainder flush in
        // the finalization loop's `finally` — but every merged row must still be accounted for.
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c", "e")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d", "f")));

        List<Long> batches = new ArrayList<>();
        SortedDatasetResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP, batches::add,
                        FinalPassListener.NO_OP);

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
        SortedDatasetResult result = transform(smallFanIn)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP, units -> { },
                        measured -> fired[0]++);

        assertThat(fired[0]).isEqualTo(1);
        assertThat(result.cascadedPasses()).isGreaterThan(0);   // sanity: a cascade really ran first
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void rolledOutputProducesRangeDisjointFilesNamedInKeyOrder(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(
                SortTestSupport.writePages(dirs.staging.resolve("seg-0.pageseg"),
                        List.of(objects("a"), objects("c"), objects("e"))),
                SortTestSupport.writePages(dirs.staging.resolve("seg-1.pageseg"),
                        List.of(objects("b"), objects("d"), objects("f"))));

        // final-file-bytes = 1 ⇒ roll after every row: one entry per file.
        SortConfig rolling = SortConfigs.base().withFinalFileBytes(1L);
        SortedDatasetResult result = transform(rolling)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(6);
        List<String> names = result.finalFiles().stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).containsExactly(
                "part-00000.parquet", "part-00001.parquet", "part-00002.parquet",
                "part-00003.parquet", "part-00004.parquet", "part-00005.parquet");
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
        SortedDatasetResult result = transform(smallFanIn)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d", "e");
        // Staging dir fully reclaimed (originals + cascade intermediates) AND the dir itself removed:
        // a directory-stream listing would now throw NoSuchFileException, so assert non-existence
        // directly.
        assertThat(Files.exists(dirs.staging)).isFalse();
    }

    @Test
    void retainedStagingKeepsOnlyOriginalSegmentsAndSignalsItsEngagement(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        List<Path> originals = new ArrayList<>();
        for (String key : List.of("e", "b", "d", "a", "c")) {
            originals.add(writeSegment(dirs.staging, "seg-" + originals.size() + ".parquet", objects(key)));
        }
        // These model the leftovers that can coexist with retained originals after a prior crash.
        Path staleCascade = Files.createFile(dirs.staging.resolve("merge-99.pageseg"));
        Path staleRangeTmp = Files.createFile(dirs.staging.resolve("prange-0-99.parquet.tmp"));
        Path staleFinalTmp = Files.createFile(dirs.staging.resolve("part-99.parquet.tmp"));
        Path arbitraryOrphan = Files.createFile(dirs.staging.resolve("orphan.pageseg"));
        Path orphanTree = Files.createDirectories(dirs.staging.resolve("orphan-tree"));
        Files.createFile(orphanTree.resolve("nested.tmp"));
        Path external = Files.writeString(root.resolve("external.txt"), "keep");
        Path orphanLink = Files.createSymbolicLink(
                dirs.staging.resolve("orphan-link"), external);
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        SortConfig retained = SortConfigs.base().withFanIn(2)
                .withStagingRetention(StagingRetention.RETAIN_ORIGINALS);
        SortedDatasetResult result = transformWithMetrics(retained, metrics)
                .transform(originals, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(result.cascadedPasses()).isPositive();
        assertThat(originals).allMatch(Files::exists);
        assertThat(staleCascade).doesNotExist();
        assertThat(staleRangeTmp).doesNotExist();
        assertThat(staleFinalTmp).doesNotExist();
        assertThat(arbitraryOrphan).doesNotExist();
        assertThat(orphanTree).doesNotExist();
        assertThat(orphanLink).doesNotExist();
        assertThat(external).hasContent("keep");
        try (var entries = Files.list(dirs.staging)) {
            assertThat(entries.toList()).containsExactlyInAnyOrderElementsOf(originals);
        }
        assertThat(metrics.count("SORT.staging_retained")).isEqualTo(1);
    }

    @Test
    void retainedStagingRejectsAnOriginalOutsideTheOwnedDirectoryBeforePublishing(
            @TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path outside = writeSegment(root, "outside.parquet", objects("a"));
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");

        assertThatThrownBy(() -> transform(SortConfigs.base()
                .withStagingRetention(StagingRetention.RETAIN_ORIGINALS))
                .transform(List.of(outside), dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not an immediate child");

        assertThat(priorFinal).hasContent("prior");
        assertThat(outside).exists();
    }

    @Test
    void defaultStagingPolicyRejectsAnOriginalOutsideTheOwnedDirectoryBeforePublishing(
            @TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path outside = writeSegment(root, "outside.parquet", objects("a"));
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedDatasetCoordinator transform = transform(SortConfigs.base());

        assertThatThrownBy(() -> transform.transform(List.of(outside), dirs.output, dirs.staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not an immediate child");

        assertThat(priorFinal).hasContent("prior");
        assertThat(outside).exists();
    }

    @Test
    void symlinkedInputInsideStagingIsRejectedAndItsOutsideTargetSurvives(
            @TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path outside = writeSegment(root, "outside.parquet", objects("a"));
        Path link = Files.createSymbolicLink(
                dirs.staging.resolve("linked" + StagingNames.PAGE_RUN_SUFFIX), outside);
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedDatasetCoordinator transform = transform(SortConfigs.base());

        assertThatThrownBy(() -> transform.transform(List.of(link), dirs.output, dirs.staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing or not an ordinary file");

        assertThat(Files.isSymbolicLink(link)).isTrue();
        assertThat(outside).exists();
        assertThat(priorFinal).hasContent("prior");
    }

    @Test
    void exactDuplicateInputsAreRejectedBeforePublishing(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path segment = writeSegment(dirs.staging, "seg-0.parquet", objects("a"));
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedDatasetCoordinator transform = transform(SortConfigs.base());

        assertThatThrownBy(() -> transform.transform(List.of(segment, segment), dirs.output,
                dirs.staging, SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("duplicate sort staging segment");

        assertThat(segment).exists();
        assertThat(priorFinal).hasContent("prior");
    }

    @Test
    void hardLinkedInputAliasesAreRejectedBeforePublishing(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path segment = writeSegment(dirs.staging, "seg-0.parquet", objects("a"));
        Path hardLink = Files.createLink(dirs.staging.resolve("seg-1.pageseg"), segment);
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedDatasetCoordinator transform = transform(SortConfigs.base());

        assertThatThrownBy(() -> transform.transform(List.of(segment, hardLink), dirs.output,
                dirs.staging, SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("duplicate physical sort staging segment");

        assertThat(segment).exists();
        assertThat(hardLink).exists();
        assertThat(priorFinal).hasContent("prior");
    }

    @Test
    void normalizedAliasInputsAreRejectedBeforePublishing(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path segment = writeSegment(dirs.staging, "seg-0.parquet", objects("a"));
        Path alias = dirs.staging.resolve(".").resolve(segment.getFileName());
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedDatasetCoordinator transform = transform(SortConfigs.base());

        assertThatThrownBy(() -> transform.transform(List.of(segment, alias), dirs.output,
                dirs.staging, SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("duplicate sort staging segment");

        assertThat(priorFinal).hasContent("prior");
        assertThat(segment).exists();
    }

    @Test
    void catalogDescriptorAssemblyRejectsNormalizedAliasesInsteadOfKeepingTheFirst(
            @TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = writeSegment(staging, "seg-0.parquet", objects("a"));
        PageRunDescriptor descriptor = PageRunCatalog.preflight(List.of(segment),
                path -> PageRunReader.open(path, SortMetrics.NO_OP))
                .descriptors().getFirst();
        Path alias = staging.resolve(".").resolve(segment.getFileName());
        PageRunDescriptor duplicate = new PageRunDescriptor(alias,
                descriptor.fileSize(), descriptor.trailerStart(), descriptor.trailer(),
                descriptor.maxRawPayloadLength(), descriptor.maxKeyLength(),
                descriptor.physicalFormat(), descriptor.headerBytes(), descriptor.orderingMode());

        assertThatThrownBy(() -> SpillTestFixtures.catalog(List.of(descriptor, duplicate)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate page-run catalog path");
    }

    @Test
    void sourceDrainAndClosedFinalRowsMustAgreeBeforePublication(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path segment = writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b"));
        Path priorFinal = Files.writeString(dirs.output.resolve("part-00000.parquet"), "prior");
        SortedFileWriterFactory dropsSecondRow = (path, fileIndex) -> {
            SortedFileWriter delegate = SortedFileWriterFactory.DEFAULT.create(path, fileIndex);
            return new SortTestSupport.DelegatingSortedFileWriter(delegate) {
                @Override
                public void write(ListEntry entry) throws IOException {
                    if (!entry.key().asString().equals("b")) {
                        delegate().write(entry);
                    }
                }
            };
        };
        SortRun run = new SortRun(SortConfigs.base(), cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, dropsSecondRow,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);

        assertThatThrownBy(() -> new SortedDatasetCoordinator(run).transform(List.of(segment), dirs.output,
                dirs.staging, SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort output cardinality mismatch before publication")
                .hasMessageContaining("source_rows=2")
                .hasMessageContaining("drained_rows=2")
                .hasMessageContaining("final_part_rows=1");

        assertThat(priorFinal).hasContent("prior");
        assertThat(segment).exists();
    }

    @Test
    void checkpointRetentionNamesRejectTraversal(@TempDir Path root)
            throws IOException {
        assertThatThrownBy(() -> StagingReconciliation.fromNames(List.of("../escape.pageseg")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsafe retained sort staging segment name");
    }

    @Test
    void publishCallbackFiresAfterRenamesButBeforeStagingDelete(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b")));

        boolean[] observed = {false};
        SortedDatasetCommitter listener = (finalFiles, rows) -> {
            observed[0] = true;
            assertThat(finalFiles).allMatch(p -> Files.exists(p.path())); // renamed into place already
            assertThat(rows).isEqualTo(2);
            assertThat(staging).allMatch(Files::exists);               // staging NOT yet deleted
            assertThat(Files.exists(dirs.staging)).as("staging dir not yet removed").isTrue();
        };
        transform(SortConfig.fromSystemProperties()).transform(staging, dirs.output, dirs.staging,
                listener, units -> { }, FinalPassListener.NO_OP);

        assertThat(observed[0]).isTrue();
        assertThat(staging).allMatch(p -> !Files.exists(p));           // deleted after the callback
        assertThat(Files.exists(dirs.staging)).as("staging dir removed after the callback").isFalse();
    }

    @Test
    void reRunIsIdempotent(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        SortedDatasetCoordinator transform = transform(SortConfig.fromSystemProperties());

        List<Path> staging1 = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));
        SortedDatasetResult first = transform.transform(staging1, dirs.output, dirs.staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP);
        assertThat(Files.exists(dirs.staging)).as("first publish removed the empty staging dir").isFalse();

        // A crash-then-retry re-stages the same segments and re-runs the merge into the same output.
        // The caller (ListCommand.runSortedParquet) recreates the staging dir before writing new
        // segments on a real resume; mirror that here since the first publish removed it.
        Files.createDirectories(dirs.staging);
        List<Path> staging2 = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));
        SortedDatasetResult second = transform.transform(staging2, dirs.output, dirs.staging,
                SortedDatasetCommitter.NO_OP, units -> { }, FinalPassListener.NO_OP);

        assertThat(keys(second.finalFiles())).containsExactly("a", "b", "c", "d");
        assertThat(second.totalRows()).isEqualTo(first.totalRows());
        assertThat(second.finalFiles().stream().map(p -> p.getFileName().toString()).toList())
                .isEqualTo(first.finalFiles().stream().map(p -> p.getFileName().toString()).toList());
    }

    @Test
    void staleTmpFromACrashedPublishIsCleanedBeforeReRun(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        // A previous run crashed mid-publish, leaving a partial .tmp behind.
        Path stale = Files.createFile(dirs.output.resolve("part-00000.parquet.tmp"));
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b")));

        SortedDatasetResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(Files.exists(stale)).isFalse();                    // stale tmp removed
        assertThat(keys(result.finalFiles())).containsExactly("a", "b");
        assertThat(listTmp(dirs.output)).isEmpty();
        assertThat(Files.exists(dirs.staging)).as("staging dir removed after publish").isFalse();
    }

    @Test
    void unsupportedStagingIsRejectedBeforeAnyDestructiveSweep(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path unsupported = Files.createFile(dirs.staging.resolve("fixture-0.parquet"));
        Path staleTmp = Files.createFile(dirs.staging.resolve("part-00000.parquet.tmp"));
        Path staleFinal = Files.createFile(dirs.output.resolve("part-00000.parquet"));

        assertThatThrownBy(() -> transform(SortConfigs.base())
                .transform(List.of(unsupported), dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected .pageseg");

        assertThat(unsupported).exists();
        assertThat(staleTmp).exists();
        assertThat(staleFinal).exists();
    }

    @Test
    void corruptPageRunIsRejectedBeforeAnyDestructiveSweep(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path corrupt = Files.writeString(dirs.staging.resolve("fixture-0.pageseg"), "not a page run");
        Path staleTmp = Files.createFile(dirs.staging.resolve("part-00000.parquet.tmp"));
        Path staleFinal = Files.createFile(dirs.output.resolve("part-00000.parquet"));

        assertThatThrownBy(() -> transform(SortConfigs.base())
                .transform(List.of(corrupt), dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("file too small to be a page-run segment");

        assertThat(corrupt).exists();
        assertThat(staleTmp).exists();
        assertThat(staleFinal).exists();
    }

    @Test
    void bodyCorruptionLeavesPriorPublishedFinalIntact(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path corrupt = writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b"));
        byte[] segmentBytes = Files.readAllBytes(corrupt);
        segmentBytes[SpillTestFixtures.pageRunHeaderBytes() + 8] ^= 0x01;
        Files.write(corrupt, segmentBytes);
        Path priorFinal = dirs.output.resolve("part-00000.parquet");
        byte[] priorContents = "prior published output".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(priorFinal, priorContents);

        assertThatThrownBy(() -> transform(SortConfigs.base())
                .transform(List.of(corrupt), dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(PageRunCorruptionException.class)
                .hasMessageContaining("error_class=page_run_body_corruption");

        assertThat(Files.readAllBytes(priorFinal))
                .as("replacement is generated completely before prior finals are swept")
                .containsExactly(priorContents);
    }

    @Test
    void crcValidMalformedBodyPublishesNoReplacementAndLeavesPriorFinalIntact(
            @TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        Path corrupt = writeSegment(dirs.staging, "seg-0.parquet", objects("a", "b"));
        byte[] segmentBytes = Files.readAllBytes(corrupt);
        int frameOffset = SpillTestFixtures.pageRunHeaderBytes();
        int bodyLength = ByteBuffer.wrap(segmentBytes, frameOffset, 4).getInt();
        int bodyOffset = frameOffset + 8;
        ByteBuffer body = ByteBuffer.wrap(segmentBytes, bodyOffset, bodyLength).slice();
        int minLength = body.getShort() & 0xFFFF;
        body.position(body.position() + minLength);
        int maxLength = body.getShort() & 0xFFFF;
        body.position(body.position() + maxLength);
        body.putInt(0);   // framed pages must contain at least one row
        CRC32C crc = new CRC32C();
        crc.update(segmentBytes, bodyOffset, bodyLength);
        ByteBuffer.wrap(segmentBytes, frameOffset + 4, 4).putInt((int) crc.getValue());
        Files.write(corrupt, segmentBytes);

        Path priorFinal = dirs.output.resolve("part-00000.parquet");
        byte[] priorContents = "prior published output".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(priorFinal, priorContents);
        List<List<FinalPart>> published = new ArrayList<>();

        assertThatThrownBy(() -> transform(SortConfigs.base())
                .transform(List.of(corrupt), dirs.output, dirs.staging,
                        (parts, rows) -> published.add(parts), units -> { },
                        FinalPassListener.NO_OP))
                .isInstanceOf(PageRunCorruptionException.class)
                .hasMessageContaining("error_class=page_run_body_corruption");

        assertThat(published).isEmpty();
        assertThat(Files.readAllBytes(priorFinal)).containsExactly(priorContents);
    }

    @Test
    void serialInteriorRowRegressionFailsBeforePublication(@TempDir Path root)
            throws IOException {
        Dirs dirs = dirs(root);
        Path corrupt = PageRunRawFixtures.writeInteriorRowRegression(
                dirs.staging.resolve("seg-0.pageseg"));
        Path priorFinal = dirs.output.resolve("part-00000.parquet");
        byte[] priorContents = "prior published output".getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        Files.write(priorFinal, priorContents);
        List<List<FinalPart>> published = new ArrayList<>();

        assertThatThrownBy(() -> transform(SortConfigs.base().withMergeParallelism(1))
                .transform(List.of(corrupt), dirs.output, dirs.staging,
                        (parts, rows) -> published.add(parts), units -> { },
                        FinalPassListener.NO_OP))
                .isInstanceOf(PageRunCorruptionException.class)
                .hasMessageContaining("error_class=page_run_body_corruption")
                .hasStackTraceContaining("decoded row order regressed inside persisted page");

        assertThat(published).isEmpty();
        assertThat(priorFinal).hasBinaryContent(priorContents);
        assertThat(corrupt).exists();
    }

    @Test
    void tinyMergeBudgetForcesACascadeEvenUnderAGenerousFanInAndStillProducesCorrectOutput(
            @TempDir Path root) throws IOException {
        // SortedDatasetCoordinator must bound the merge pass width by SortConfig#effectiveFanIn() (I11), not
        // the raw fan-in knob — otherwise a generous raw fan-in (512 default) lets a merge hold many
        // page-run readers open at once, making merge-phase memory a function of
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
        // The same budget also prices final encoder admission (one writer, sized from
        // final-row-group-bytes), so it must clear that floor too; keep the 3:1 ratio of
        // merge-budget-bytes to merge-per-stream-bytes so effectiveFanIn() still lands on 3.
        SortConfig tinyBudget = SortConfigs.base()
                .withMergePerStreamBytes(8L << 20)
                .withMergeBudgetBytes(24L << 20);
        assertThat(tinyBudget.effectiveFanIn()).isEqualTo(3);

        SortedDatasetResult result = transform(tinyBudget)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

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
        SortedDatasetCommitter listener = (finalFiles, rows) ->
                published.addAll(finalFiles.stream().map(FinalPart::path).toList());
        SortedDatasetResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, listener,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(Files.exists(staleFinal)).as("stale final swept before republish").isFalse();
        assertThat(keys(result.finalFiles())).containsExactly("a", "b");
        assertThat(result.finalFiles()).containsExactly(dirs.output.resolve("part-00000.parquet"));
        // The manifest callback saw exactly the files this run produced — no stale survivor mixed in.
        assertThat(published).containsExactlyElementsOf(result.finalFiles());
        // Only the produced final remains on disk under the output dir root.
        try (var s = Files.newDirectoryStream(dirs.output, "part-*.parquet")) {
            List<Path> onDisk = new ArrayList<>();
            s.forEach(onDisk::add);
            assertThat(onDisk).containsExactly(dirs.output.resolve("part-00000.parquet"));
        }
    }

    @Test
    void staleMergeIntermediatesFromACrashedCascadeAreSweptBeforeReMerge(@TempDir Path root) throws IOException {
        Dirs dirs = dirs(root);
        // A previous merge attempt crashed mid-cascade, leaving an orphaned merge-*.parquet
        // intermediate behind (owned content this transform re-derives fresh every time).
        Path staleIntermediate = Files.writeString(dirs.staging.resolve("merge-0.parquet"),
                "legacy cascade debris");
        List<Path> staging = List.of(
                writeSegment(dirs.staging, "seg-0.parquet", objects("a", "c")),
                writeSegment(dirs.staging, "seg-1.parquet", objects("b", "d")));

        SortedDatasetResult result = transform(SortConfig.fromSystemProperties())
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(Files.exists(staleIntermediate))
                .as("m2: stale cascade intermediate swept before the re-merge").isFalse();
        assertThat(keys(result.finalFiles())).containsExactly("a", "b", "c", "d");
        // With the stale leftover gone, staging is genuinely empty and removed — without this
        // sweep, the foreign-looking leftover would block the empty-staging-dir cleanup.
        assertThat(Files.exists(dirs.staging)).isFalse();
    }

    @Test
    void liveAllowPolicyPreservesEqualRawKeysAcrossARollThreshold(@TempDir Path root)
            throws IOException {
        // Live --sort selects ALLOW, so a same-key OBJECT/COMMON_PREFIX pair must publish without
        // rejection. A one-byte target also proves the shared raw-key comparison still defers the
        // roll and keeps the group in one file.
        Dirs dirs = dirs(root);
        List<ListEntry> rows = new ArrayList<>(objects("a"));
        rows.add(new CommonPrefixEntry(KeyBytes.ofUtf8("a")));
        List<Path> staging = List.of(writeSegment(dirs.staging, "seg-0.parquet", rows));

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortedDatasetResult result = transformWithMetrics(SortConfigs.rolledPerEntry(), metrics)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(result.totalRows()).isEqualTo(2);
        assertThat(result.finalFiles()).hasSize(1);
        assertThat(keys(result.finalFiles())).containsExactly("a", "a");
        assertThat(metrics.count("SORT.equal_key_rejected")).isZero();
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
        // The same budget also prices final encoder admission (one writer, sized from
        // final-row-group-bytes), so it must clear that floor too; keep the 3:1 ratio of
        // merge-budget-bytes to merge-per-stream-bytes so effectiveFanIn() still lands on 3.
        SortConfig tinyBudget = SortConfigs.base()
                .withMergePerStreamBytes(8L << 20)
                .withMergeBudgetBytes(24L << 20);
        assertThat(tinyBudget.effectiveFanIn()).isEqualTo(3);

        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        transformWithMetrics(tinyBudget, metrics)
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

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
                .transform(staging, dirs.output, dirs.staging, SortedDatasetCommitter.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

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
        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(new SortRun(rolling, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, spy,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        SortedDatasetResult result = transform.transform(staged, output, staging, SortedDatasetCommitter.NO_OP,
                units -> { }, FinalPassListener.NO_OP);

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

    private SortedDatasetCoordinator transform(SortConfig config) {
        return new SortedDatasetCoordinator(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private SortedDatasetCoordinator transformWithMetrics(SortConfig config, SortMetrics metrics) {
        return new SortedDatasetCoordinator(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, metrics, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
    }

    private record Dirs(Path output, Path staging) {
    }

    private static Dirs dirs(Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("out/_staging"));
        return new Dirs(output, staging);
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        return SortTestSupport.writePageRun(
                dir.resolve(name.replace(".parquet", StagingNames.PAGE_RUN_SUFFIX)), sorted, cmp);
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
            try (ParquetEntryReader r = new ParquetEntryReader(f)) {
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
