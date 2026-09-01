/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPartMetadata;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.SpillTestFixtures;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SortFinalizerPreparationTest {

    @Test
    void admissionRejectsNormalizedAliasInputsBeforeOpening(@TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        ListEntryComparator comparator = new ListEntryComparator();
        Path segment = SortTestSupport.writePageRun(
                staging.resolve("seg-0.pageseg"), List.of(SortTestSupport.object("a")), comparator);
        Path alias = staging.resolve(".").resolve(segment.getFileName());
        AtomicInteger opens = new AtomicInteger();
        SortRun run = new SortRun(SortConfigs.base(), comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        SortFinalizer finalizer = new SortFinalizer(run, path -> {
            opens.incrementAndGet();
            return io.varve.swath.sort.spill.PageRunReader.open(path, SortMetrics.NO_OP);
        });

        assertThatThrownBy(() -> finalizer.admit(List.of(segment, alias), Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("duplicate page-run catalog path");

        assertThat(opens).hasValue(0);
        assertThat(segment).exists();
    }

    @Test
    void prepareReturnsACompleteUnpublishedSetWithoutTouchingSourcesOrPriorGeneration(
            @TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        Path prior = Files.writeString(output.resolve("part-00000.parquet"), "prior-good");
        ListEntryComparator comparator = new ListEntryComparator();
        List<Path> sources = List.of(
                SortTestSupport.writePageRun(staging.resolve("seg-0.pageseg"),
                        List.of(SortTestSupport.object("a")), comparator),
                SortTestSupport.writePageRun(staging.resolve("seg-1.pageseg"),
                        List.of(SortTestSupport.object("b")), comparator));
        SortRun run = new SortRun(SortConfigs.rolledPerEntry(), comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        SortFinalizer finalizer = new SortFinalizer(run);
        StagingReconciliation owned = StagingReconciliation.fromPaths(staging, sources);

        PreparedSortedParts prepared = finalizer.prepare(new SortFinalizer.Request(
                finalizer.admit(sources, Map.of()), staging, ignored -> { },
                FinalPassListener.NO_OP, owned));

        assertThat(prepared.sourceRows()).isEqualTo(2);
        assertThat(prepared.outputRows()).isEqualTo(2);
        assertThat(prepared.parts()).hasSize(2);
        assertThat(prepared.parts()).allSatisfy(part -> {
            assertThat(part.temporaryPath()).isRegularFile();
            assertThat(part.temporaryPath().getParent()).isEqualTo(staging);
            assertThat(part.temporaryPath().getFileName().toString())
                    .startsWith("pipeline-").endsWith(".parquet.tmp");
            assertThat(part.rows()).isEqualTo(1);
            assertThat(part.bytes()).isEqualTo(Files.size(part.temporaryPath()));
        });
        for (int i = 1; i < prepared.parts().size(); i++) {
            assertThat(Arrays.compareUnsigned(
                    prepared.parts().get(i - 1).rawMaxKey(),
                    prepared.parts().get(i).rawMinKey())).isNegative();
        }
        assertThat(sources).allSatisfy(source -> assertThat(source).isRegularFile());
        assertThat(prior).hasContent("prior-good");
        try (var visible = Files.list(output)) {
            assertThat(visible.toList()).containsExactly(prior);
        }
    }

    @Test
    void directCallerOwnsDisposableCleanupAfterPrepareFailure(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        ListEntryComparator comparator = new ListEntryComparator();
        List<Path> sources = List.of(SortTestSupport.writePageRun(
                staging.resolve("seg-0.pageseg"),
                List.of(SortTestSupport.object("a")), comparator));
        SortedFileWriterFactory wrongDurableBytes = (path, index) ->
                new SortTestSupport.DelegatingSortedFileWriter(
                        SortedFileWriterFactory.DEFAULT.create(path, index)) {
                    private long durableBytes;

                    @Override
                    public void close() throws IOException {
                        super.close();
                        durableBytes = Files.size(path);
                    }

                    @Override
                    public Optional<FinalPartMetadata> finalMetadata() {
                        return Optional.of(new FinalPartMetadata(
                                rows(), durableBytes + 1, "not-used", "a", "a", 0, 0, 1));
                    }
                };
        SortRun run = new SortRun(SortConfigs.base(), comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, wrongDurableBytes,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        SortFinalizer finalizer = new SortFinalizer(run);
        StagingReconciliation owned = StagingReconciliation.fromPaths(staging, sources);

        assertThatThrownBy(() -> finalizer.prepare(new SortFinalizer.Request(
                finalizer.admit(sources, Map.of()), staging, ignored -> { },
                FinalPassListener.NO_OP, owned)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("metadata disagrees with durable file");

        try (var entries = Files.newDirectoryStream(staging, StagingNames.PIPELINE_TMP_GLOB)) {
            assertThat(entries).as("the direct caller, not SortFinalizer, owns failure cleanup")
                    .hasSize(1);
        }
        owned.sweepDisposables(StagingNames.PIPELINE_TMP_GLOB);
        try (var entries = Files.newDirectoryStream(staging, StagingNames.PIPELINE_TMP_GLOB)) {
            assertThat(entries).isEmpty();
        }
        assertThat(sources).allSatisfy(source -> assertThat(source).isRegularFile());
    }

    @Test
    void aCascadeGroupFailingMidWriteNeverLeavesTheDurableIntermediateName(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        ListEntryComparator comparator = new ListEntryComparator();
        List<Path> sources = SortTestSupport.writeCascadeSources(staging);
        SortTestSupport.corruptLateCascadeSourcePage(sources);
        SortRun run = new SortRun(SortConfigs.base().withFanIn(2), comparator, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        SortFinalizer finalizer = new SortFinalizer(run);
        StagingReconciliation owned = StagingReconciliation.fromPaths(staging, sources);

        assertThatThrownBy(() -> finalizer.prepare(new SortFinalizer.Request(
                finalizer.admit(sources, Map.of()), staging, ignored -> { },
                FinalPassListener.NO_OP, owned)))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("cascade page merge read failed")
                .rootCause().hasMessageContaining("record CRC32C mismatch");

        assertThat(staging.resolve(StagingNames.cascadeIntermediate(0)))
                .as("the group that finished durably keeps the intermediate name")
                .isRegularFile();
        assertThat(staging.resolve(StagingNames.cascadeIntermediate(1)))
                .as("the group that failed mid-write never claims one")
                .doesNotExist();
        Path unfinished = staging.resolve(StagingNames.cascadeIntermediateTmp(1));
        assertThat(unfinished).isRegularFile();
        assertThat(Files.size(unfinished))
                .as("the failure really landed mid-write, after page bytes were appended")
                .isGreaterThan(SpillTestFixtures.pageRunHeaderBytes());
        assertThat(sources).allSatisfy(source -> assertThat(source).isRegularFile());

        owned.sweepDisposableWorkingFiles();
        try (var remaining = Files.list(staging)) {
            assertThat(remaining)
                    .as("the caller's sweep reclaims both cascade names and leaves the sources")
                    .containsExactlyInAnyOrderElementsOf(sources);
        }
    }
}
