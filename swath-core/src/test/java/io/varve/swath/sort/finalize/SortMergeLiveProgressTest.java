/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static io.varve.swath.sort.finalize.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedCursor;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.PageCodec;
import io.varve.swath.sort.spill.PageRunSegmentWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What live progress reports during a REAL merge — driven through {@link SortedDatasetCoordinator} exactly as
 * {@code ListRunner} wires it (staged rows recorded up front, the progress callback feeding {@code
 * swath.progress.units}, the final-pass hook advancing the phase), because the failure this pins is
 * invisible to a synthetic single-pass value: a cascading merge rewrites every staged row once per
 * pass, so cumulative merge work legitimately runs past the staged total.
 */
class SortMergeLiveProgressTest {

    private static final int SEGMENTS = 5;
    private static final int ROWS_PER_SEGMENT = 30;
    private static final long STAGED_ROWS = (long) SEGMENTS * ROWS_PER_SEGMENT;

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void aCascadingMergeNeverReportsMoreDoneThanTheRowsItWasHanded(@TempDir Path root) throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<ProgressEvent> samples = new ArrayList<>();

        SortedDatasetResult result = mergeWithProgress(root, metrics, samples);

        assertThat(result.cascadedPasses()).as("the case under test is a genuine multi-pass merge")
                .isGreaterThan(0);
        assertThat(samples).isNotEmpty();
        assertThat(samples).allSatisfy(event -> {
            if (event.completion() != null) {
                assertThat(event.completion().done()).isLessThanOrEqualTo(event.completion().total());
            }
        });
        // The proof that this is not vacuous: mid-cascade the run really had moved more rows than it
        // was handed, which is exactly what a units-since-merge-start numerator would have reported.
        assertThat(samples).anySatisfy(event -> {
            assertThat(event.merging().sessionRowsMerged()).isGreaterThan(STAGED_ROWS);
            assertThat(event.phase()).isEqualTo(Phase.MERGING);
            assertThat(event.completion()).isNull();
        });
    }

    @Test
    void theFinalPassCountsItsOwnRowsAndEndsAtExactlyOneHundredPercent(@TempDir Path root) throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<ProgressEvent> samples = new ArrayList<>();

        mergeWithProgress(root, metrics, samples);

        ProgressEvent last = samples.get(samples.size() - 1);
        assertThat(last.phase()).isEqualTo(Phase.WRITING);
        assertThat(last.completion())
                .isEqualTo(new ProgressEvent.Completion(STAGED_ROWS, STAGED_ROWS, ProgressEvent.Unit.ROWS));
        assertThat(last.completion().fraction()).isEqualTo(1.0);
        assertThat(last.merging().sessionRowsMerged())
                .as("total merge work spans every pass, so it exceeds the staged rows")
                .isGreaterThan(STAGED_ROWS);
    }

    /** A cascading pipeline reports work without inventing a final-pass percentage. */
    @Test
    void aParallelPipelineRequestThatCascadesStillReportsHonestly(@TempDir Path root)
            throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<ProgressEvent> samples = Collections.synchronizedList(new ArrayList<>());

        // Fan-in 2 against five staged segments forces the pipeline to cascade before encoding.
        SortedDatasetResult result = mergePipelineWithProgress(root, metrics, samples,
                parallelConfig().withFanIn(2));

        assertThat(result.cascadedPasses()).as("the case under test is a genuine multi-pass merge")
                .isGreaterThan(0);
        assertThat(result.finalFiles()).hasSize(1);
        assertThat(samples).isNotEmpty();
        assertThat(samples)
                .filteredOn(event -> event.phase() == Phase.MERGING)
                .as("a cascading merge has no honest denominator, so it reports no percentage")
                .isNotEmpty()
                .allSatisfy(event -> assertThat(event.completion()).isNull());
        // Not vacuous: the merge really did move more rows than it was handed, which is exactly what
        // a staged-rows percentage would have reported as a finished, then over-finished, merge.
        assertThat(samples.get(samples.size() - 1).merging().sessionRowsMerged())
                .isGreaterThan(STAGED_ROWS);
    }

    /** A single-pass pipeline keeps its percentage because the staged-row denominator is exact. */
    @Test
    void aSinglePassParallelPipelineStillEndsAtExactlyOneHundredPercent(@TempDir Path root)
            throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<ProgressEvent> samples = Collections.synchronizedList(new ArrayList<>());

        // Fan-in wider than the staged segment count: the pipeline does not cascade.
        SortedDatasetResult result = mergePipelineWithProgress(root, metrics, samples,
                parallelConfig().withFanIn(SEGMENTS + 1));

        assertThat(result.cascadedPasses()).isZero();
        assertThat(samples).allSatisfy(event ->
                assertThat(event.completion().done()).isLessThanOrEqualTo(event.completion().total()));
        assertThat(samples.get(samples.size() - 1).completion())
                .isEqualTo(new ProgressEvent.Completion(STAGED_ROWS, STAGED_ROWS, ProgressEvent.Unit.ROWS));
    }

    /**
     * Three encoders over a merge budget wide enough that only {@code fanIn} determines whether the
     * pipeline cascades.
     */
    private SortConfig parallelConfig() {
        return SortConfigs.base().withMergeParallelism(3).withMergeBudgetBytes(1L << 30);
    }

    /** The production wiring again, over page-run staging. */
    private SortedDatasetResult mergePipelineWithProgress(Path root, RunMetrics metrics,
                                                          List<ProgressEvent> samples, SortConfig config)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("run/_staging"));
        Path output = Files.createDirectories(root.resolve("run"));
        List<Path> segments = stage(staging);

        metrics.recordSortStaged(segments.size(), STAGED_ROWS);
        metrics.setPhase(Phase.MERGING);
        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(
                new SortRun(config, cmp, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                        SortMetrics.NO_OP,
                        SortedFileWriterFactory.DEFAULT,
                        SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        return transform.transform(segments, output, staging, SortedDatasetCommitter.NO_OP,
                units -> {
                    metrics.recordProgress(units);
                    samples.add(metrics.progressEvent(Duration.ofSeconds(1)));
                },
                metrics::startFinalMergePass);
    }

    /** The production wiring: fan-in pinned to 2 so five staged segments genuinely cascade. */
    private SortedDatasetResult mergeWithProgress(Path root, RunMetrics metrics, List<ProgressEvent> samples)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("run/_staging"));
        Path output = Files.createDirectories(root.resolve("run"));
        List<Path> segments = stage(staging);

        metrics.recordSortStaged(segments.size(), STAGED_ROWS);
        metrics.setPhase(Phase.MERGING);
        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(
                new SortRun(SortConfigs.base().withFanIn(2), cmp, DuplicateHook.NO_OP,
                        EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                        SortedFileWriterFactory.DEFAULT,
                        SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));
        return transform.transform(segments, output, staging, SortedDatasetCommitter.NO_OP,
                units -> {
                    metrics.recordProgress(units);
                    samples.add(metrics.progressEvent(Duration.ofSeconds(1)));
                },
                metrics::startFinalMergePass);
    }

    private List<Path> stage(Path dir) throws IOException {
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        List<Path> out = new ArrayList<>();
        for (int s = 0; s < SEGMENTS; s++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int i = 0; i < ROWS_PER_SEGMENT; i++) {
                rows.add(object(String.format("k%05d", i * SEGMENTS + s)));
            }
            Path path = dir.resolve("seg-" + s + ".pageseg");
            try (SortedCursor cursor = SortTestSupport.cursor(rows, cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
    }
}
