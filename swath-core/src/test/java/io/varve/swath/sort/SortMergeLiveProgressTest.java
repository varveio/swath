/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.RunMetrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What live progress reports during a REAL merge — driven through {@link SortTransform} exactly as
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

        SortTransformResult result = mergeWithProgress(root, metrics, samples);

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

    /** The production wiring: fan-in pinned to 2 so five staged segments genuinely cascade. */
    private SortTransformResult mergeWithProgress(Path root, RunMetrics metrics, List<ProgressEvent> samples)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("run/_staging"));
        Path output = Files.createDirectories(root.resolve("run"));
        List<Path> segments = stage(staging);

        metrics.recordSortStaged(segments.size(), STAGED_ROWS);
        metrics.setPhase(Phase.MERGING);
        SortTransform transform = new SortTransform(
                new SortRun(SortConfigs.base().withFanIn(2), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                        SortedFileWriterFactory.DEFAULT),
                false, RangeMergeTimer.NO_OP);
        return transform.transform(segments, output, staging, PublishListener.NO_OP,
                units -> {
                    metrics.recordProgress(units);
                    samples.add(metrics.progressEvent(Duration.ofSeconds(1)));
                },
                () -> metrics.setPhase(Phase.WRITING));
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
            try (SortedCursor cursor = new InMemoryCursor(rows, cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
    }
}
