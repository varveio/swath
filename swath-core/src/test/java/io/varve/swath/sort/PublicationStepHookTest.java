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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PublicationStepHookTest {

    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void serialTailReportsEachBoundaryInMutationOrder(@TempDir Path root) throws Exception {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = stage(staging, List.of(objects("a", "c"), objects("b", "d")));
        List<Hit> hits = new ArrayList<>();
        List<String> listenerState = new ArrayList<>();

        transform(SortConfigs.base(), (step, ordinal) -> hits.add(new Hit(step, ordinal)))
                .transform(segments, output, staging, (parts, rows) -> {
                    listenerState.add("listener");
                    assertThat(hits.getLast().step()).isEqualTo(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC);
                    assertThat(segments).allMatch(Files::exists);
                }, units -> { }, FinalPassListener.NO_OP);

        assertThat(listenerState).containsExactly("listener");
        assertThat(hits).containsExactly(
                hit(PublicationStep.AFTER_WORKING_SWEEP),
                hit(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE),
                hit(PublicationStep.AFTER_STALE_FINAL_SWEEP),
                new Hit(PublicationStep.AFTER_PART_RENAME, 0),
                hit(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC),
                hit(PublicationStep.AFTER_PUBLISH_LISTENER),
                hit(PublicationStep.AFTER_STAGING_COMPLETION));
        assertThat(Files.exists(staging)).isFalse();
    }

    @Test
    void pipelineTailUsesGlobalPartOrdinalsAndTheSamePublicationOrder(@TempDir Path root)
            throws Exception {
        Path output = Files.createDirectories(root.resolve("data"));
        Path staging = Files.createDirectories(root.resolve("_staging"));
        List<Path> segments = stage(staging, List.of(
                objects("a", "b", "c"), objects("d", "e", "f"), objects("g", "h", "i")));
        List<Hit> hits = new ArrayList<>();

        transform(SortConfigs.base().withMergeParallelism(3)
                        .withMergeBudgetBytes(64L << 20).withFinalFileBytes(1),
                (step, ordinal) -> hits.add(new Hit(step, ordinal)))
                .transform(segments, output, staging, PublishListener.NO_OP,
                        units -> { }, FinalPassListener.NO_OP);

        assertThat(hits).containsExactly(
                hit(PublicationStep.AFTER_WORKING_SWEEP),
                hit(PublicationStep.AFTER_ALL_TMP_PARTS_DURABLE),
                hit(PublicationStep.AFTER_STALE_FINAL_SWEEP),
                new Hit(PublicationStep.AFTER_PART_RENAME, 0),
                new Hit(PublicationStep.AFTER_PART_RENAME, 1),
                new Hit(PublicationStep.AFTER_PART_RENAME, 2),
                hit(PublicationStep.AFTER_OUTPUT_DIRECTORY_SYNC),
                hit(PublicationStep.AFTER_PUBLISH_LISTENER),
                hit(PublicationStep.AFTER_STAGING_COMPLETION));
    }

    private SortTransform transform(SortConfig config, PublicationStepHook hook) {
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new SortTransform(run, hook);
    }

    private List<Path> stage(Path staging, List<List<ListEntry>> segmentRows) throws IOException {
        List<Path> segments = new ArrayList<>();
        for (int i = 0; i < segmentRows.size(); i++) {
            segments.add(SortTestSupport.writePageRun(
                    staging.resolve("seg-" + i + StagingNames.PAGE_RUN_SUFFIX),
                    segmentRows.get(i), comparator));
        }
        return List.copyOf(segments);
    }

    private static List<ListEntry> objects(String... keys) {
        List<ListEntry> rows = new ArrayList<>();
        for (String key : keys) {
            rows.add(new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null,
                    false, null, null, null, null));
        }
        return rows;
    }

    private static Hit hit(PublicationStep step) {
        return new Hit(step, -1);
    }

    private record Hit(PublicationStep step, int ordinal) {
    }
}
