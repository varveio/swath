/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.PublicationPendingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.sorted.CommittedPublicationCleanupException;
import io.varve.swath.output.sorted.PublicationStep;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.ForwardingCheckpointStore;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exact observability contract for a committed publication whose PUBLISHED latch also fails. */
final class ListRunnerPostPublishPhaseLatchTest {

    private static final String HASH = "post-publish-phase-latch-hash";

    @Test
    void phaseLatchFailureAddsItsStableReasonToCleanupPending(@TempDir Path root)
            throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("a".getBytes(StandardCharsets.UTF_8),
                        "b".getBytes(StandardCharsets.UTF_8)))
                .build();

        try (SqliteCheckpointStore real = SqliteCheckpointStore.open(
                root.resolve("checkpoint.sqlite"), ctx.metrics())) {
            RunMeta run = real.openRun(sortKey(outputDir), false, false);
            real.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = real.loadResumable(run.id(), true);
            CheckpointStore failingLatch = new ForwardingCheckpointStore(real) {
                @Override
                public void setSortPhase(long runId, SortPhase phase) throws CheckpointException {
                    if (phase == SortPhase.PUBLISHED) {
                        throw new CheckpointException("injected PUBLISHED latch failure");
                    }
                    super.setSortPhase(runId, phase);
                }
            };
            ListRunner runner = new ListRunner((step, ordinal) -> {
                if (step == PublicationStep.AFTER_PUBLISH_LISTENER) {
                    throw new IOException("injected committed cleanup failure");
                }
            });

            Throwable failure = catchThrowable(() -> runner.runToSortedParquetWorkStealing(
                    ctx, fetcher, outputDir, stagingDir, parquetSpec(), failingLatch, run.id(),
                    2, seeds, SortConfigs.base(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false));

            assertThat(failure).isInstanceOf(PublicationPendingException.class)
                    .hasCauseInstanceOf(CommittedPublicationCleanupException.class);
            assertThat(failure.getCause().getSuppressed()).singleElement()
                    .satisfies(suppressed -> assertThat(suppressed)
                            .isInstanceOf(CheckpointException.class)
                            .hasMessageContaining("injected PUBLISHED latch failure"));
            assertThat(DatasetLayout.of(outputDir).success()).exists();
            assertThat(real.sortPhase(run.id())).isEqualTo(SortPhase.MERGING);
            assertThat(reasonCount(ctx, "post_publish_cleanup_pending")).isEqualTo(1.0);
            assertThat(reasonCount(ctx, "post_publish_phase_latch_failed")).isEqualTo(1.0);
        }
    }

    private static RunKey sortKey(Path outputDir) {
        return new RunKey("s3", null, "bucket", new byte[0], HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false,
                        outputDir.toString(), false, null, null), true);
    }

    private static ListRunner.ParquetSpec parquetSpec() {
        return new ListRunner.ParquetSpec(new byte[0], 64, 16, FilterChain.EMPTY,
                1, 256L * 1024, 4, HASH, null, null, 0L, 0L, "bucket");
    }

    private static double reasonCount(RunContext ctx, String reason) {
        return ctx.meterRegistry().get("swath.steal_reason")
                .tags("outcome", "SORT", "reason", reason).counter().count();
    }
}
