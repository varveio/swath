/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPartMetadata;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortCardinalityException;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortOrderException;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortTestSupport;
import io.varve.swath.sort.SortTransform;
import io.varve.swath.sort.SortedFileWriterFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetPublisherInvariantTest {

    @Test
    void cardinalityMismatchIsTypedAndInstrumentedOnce() {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> DatasetPublisher.requireExactCardinality(3, 2, 2, metrics))
                .isInstanceOfSatisfying(SortCardinalityException.class, failure ->
                        assertThat(failure.errorClass())
                                .isEqualTo(SortCardinalityException.ERROR_CLASS))
                .hasMessageContaining("source_rows=3")
                .hasMessageContaining("drained_rows=2")
                .hasMessageContaining("final_part_rows=2");
        assertThat(metrics.count("SORT.sort_output_cardinality_mismatch")).isEqualTo(1);
    }

    @Test
    void exactCardinalityEmitsNoFailureSignal() throws Exception {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        DatasetPublisher.requireExactCardinality(3, 3, 3, metrics);

        assertThat(metrics.count("SORT.sort_output_cardinality_mismatch")).isZero();
    }

    @Test
    void nonUtf8RawBoundsRejectCrossPartOverlap() {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        FinalPartMetadata high = metadata(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x80});
        FinalPartMetadata low = metadata(new byte[] {0x7f}, new byte[] {0x7f});

        assertThatThrownBy(() -> DatasetPublisher.requireDisjointParts(
                List.of(high, low), metrics))
                .isInstanceOfSatisfying(SortOrderException.class, failure ->
                        assertThat(failure.errorClass()).isEqualTo(SortOrderException.ERROR_CLASS))
                .hasMessageContaining("raw unsigned key order");
        assertThat(metrics.count("SORT.cross_part_overlap_rejected")).isEqualTo(1);
    }

    @Test
    void nonUtf8RawBoundsPermitStrictUnsignedAdjacency() {
        DatasetPublisher.requireDisjointParts(List.of(
                metadata(new byte[] {0x7f}, new byte[] {0x7f}),
                metadata(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x80})),
                SortMetrics.NO_OP);
    }

    private static FinalPartMetadata metadata(byte[] min, byte[] max) {
        // Both invalid-UTF-8 bounds would become the same replacement-character String. The publisher
        // must compare these raw bytes instead of the lossy display fields.
        return new FinalPartMetadata(1, 1, "md5", "�", "�", 0, 0, 0, min, max);
    }

    @Test
    void stagingReplacementBeforePublishCannotDeletePriorFinal(@TempDir Path root)
            throws IOException {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = SortTestSupport.writePageRun(
                staging.resolve("seg-0.pageseg"), List.of(SortTestSupport.object("a")),
                new ListEntryComparator());
        Path prior = Files.writeString(
                output.resolve(StagingNames.finalPart(0)), "prior-good");
        Path originalStaging = root.resolve("original-staging");
        AtomicInteger publications = new AtomicInteger();
        PublicationStepHook replaceAfterClose = (step, ignored) -> {
            if (step == PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE) {
                Files.move(staging, originalStaging);
                Files.createSymbolicLink(staging, originalStaging);
            }
        };
        SortRun run = new SortRun(
                SortConfigs.base().withMergeParallelism(1), new ListEntryComparator(),
                DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW, SortMetrics.NO_OP,
                SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);

        assertThatThrownBy(() -> new SortTransform(run, replaceAfterClose).transform(
                List.of(segment), output, staging,
                (parts, rows) -> publications.incrementAndGet(), ignored -> { },
                FinalPassListener.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort staging directory identity changed");

        assertThat(publications).hasValue(0);
        assertThat(prior).hasContent("prior-good");
        assertThat(originalStaging.resolve(segment.getFileName())).exists();
        try (var finals = Files.newDirectoryStream(output, StagingNames.OWN_FINAL_GLOB)) {
            assertThat(finals).containsExactly(prior);
        }
    }
}
