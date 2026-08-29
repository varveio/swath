/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MergeDiskAdmissionTest {

    private static final long GIB = 1L << 30;
    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void productionProbeRecognizesSiblingDirectoriesOnTheSameStore(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));

        MergeDiskPolicy.Snapshot snapshot = MergeDiskPolicy.enforced().snapshot(staging, output);

        assertThat(snapshot.sharedStore()).isTrue();
        assertThat(snapshot.stagingUsableBytes()).isPositive();
        assertThat(snapshot.outputUsableBytes()).isPositive();
    }

    @Test
    void secondSampleRefusesBeforeProofPathIsCreatedOrZeroFilled(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = List.of(
                SortTestSupport.writePageRun(staging.resolve("seg-0.pageseg"),
                        List.of(object("a"), object("d")), comparator),
                SortTestSupport.writePageRun(staging.resolve("seg-1.pageseg"),
                        List.of(object("b"), object("e")), comparator),
                SortTestSupport.writePageRun(staging.resolve("seg-2.pageseg"),
                        List.of(object("c"), object("f")), comparator));
        AtomicInteger probes = new AtomicInteger();
        Object store = new Object();
        MergeDiskPolicy policy = MergeDiskPolicy.enforced(path ->
                new MergeDiskPolicy.Space(store, probes.getAndIncrement() < 2 ? 2L * GIB : 0L));
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(3)
                .withMergeBudgetBytes(64L << 20)
                .withMinParallelStagedBytes(0L);
        SortTransform transform = new SortTransform(new SortRun(
                config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY, policy));

        assertThatThrownBy(() -> transform.transform(segments, output, staging,
                PublishListener.NO_OP, ignored -> { }, FinalPassListener.NO_OP))
                .isInstanceOf(MergeDiskExhaustedException.class)
                .hasMessageContaining("merge needs");

        assertThat(probes).hasValue(4);
        assertThat(staging.resolve(StagingNames.rangeProofTmp())).doesNotExist();
        assertThat(segments).allMatch(Files::exists);
        try (var files = Files.list(output)) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void explicitBypassSkipsBothSamples(@TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = List.of(SortTestSupport.writePageRun(
                staging.resolve("seg-0.pageseg"), List.of(object("a")), comparator));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransform transform = new SortTransform(new SortRun(
                SortConfigs.base(), comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, metrics,
                SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY,
                MergeDiskPolicy.bypassed()));

        SortTransformResult result = transform.transform(segments, output, staging,
                PublishListener.NO_OP, ignored -> { }, FinalPassListener.NO_OP);

        assertThat(result.totalRows()).isOne();
        assertThat(metrics.count("SORT.merge_disk_policy_bypassed")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_disk_policy_enforced")).isZero();
    }

    @Test
    void staleDisposableProofIsSweptBeforeAdmissionSamplesFreeSpace(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = List.of(SortTestSupport.writePageRun(
                staging.resolve("seg-0.pageseg"), List.of(object("a")), comparator));
        Path staleProof = Files.writeString(staging.resolve(StagingNames.rangeProofTmp()), "stale");
        Object store = new Object();
        MergeDiskPolicy policy = MergeDiskPolicy.enforced(path -> {
            assertThat(staleProof).doesNotExist();
            return new MergeDiskPolicy.Space(store, 2L * GIB);
        });
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortTransform transform = new SortTransform(new SortRun(
                SortConfigs.base(), comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, metrics,
                SortedFileWriterFactory.DEFAULT,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY, policy));

        SortTransformResult result = transform.transform(segments, output, staging,
                PublishListener.NO_OP, ignored -> { }, FinalPassListener.NO_OP);

        assertThat(result.totalRows()).isOne();
        assertThat(metrics.count("SORT.merge_disk_policy_enforced")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_disk_policy_bypassed")).isZero();
    }

    @Test
    void resourceAndDiskClampsBothRemainObservable(@TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = List.of(
                SortTestSupport.writePageRun(staging.resolve("seg-0.pageseg"),
                        List.of(object("a"), object("d")), comparator),
                SortTestSupport.writePageRun(staging.resolve("seg-1.pageseg"),
                        List.of(object("b"), object("e")), comparator),
                SortTestSupport.writePageRun(staging.resolve("seg-2.pageseg"),
                        List.of(object("c"), object("f")), comparator));
        long usable = GIB + PageRunProofSpool.logicalBytes(2, segments.size());
        Object store = new Object();
        MergeDiskPolicy policy = MergeDiskPolicy.enforced(
                ignored -> new MergeDiskPolicy.Space(store, usable));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        SortConfig config = SortConfigs.base().withMergeParallelism(4)
                .withMergeBudgetBytes(64L << 20).withMinParallelStagedBytes(0L);
        int fdLimitForThreeRanges = MergeFdBudget.FD_HEADROOM + 13;
        SortTransform transform = new SortTransform(new SortRun(
                config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, metrics,
                SortedFileWriterFactory.DEFAULT, MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES,
                RangeMergeTimer.NO_OP, () -> fdLimitForThreeRanges,
                StaleFinalSweep.OWN_PARTS_ONLY, policy));

        SortTransformResult result = transform.transform(segments, output, staging,
                PublishListener.NO_OP, ignored -> { }, FinalPassListener.NO_OP);

        assertThat(result.finalizationParallelism()).isEqualTo(2);
        assertThat(metrics.count("SORT.merge_range_fd_limited")).isEqualTo(1);
        assertThat(metrics.count("SORT.merge_range_disk_limited")).isEqualTo(1);
    }
}
