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
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortTestSupport;
import io.varve.swath.sort.SortedFileWriterFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SortedDatasetPublisherInvariantTest {

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

        assertThatThrownBy(() -> new SortedDatasetCoordinator(run, replaceAfterClose).transform(
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
