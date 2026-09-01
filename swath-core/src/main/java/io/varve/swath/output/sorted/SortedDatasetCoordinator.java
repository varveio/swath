/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.finalize.PreparedSortedParts;
import io.varve.swath.sort.finalize.SortFinalizer;
import io.varve.swath.sort.spill.PageRunFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The batch merge/publish step of {@code --sort}. Sealed page-run segments are cascaded as needed,
 * routed by frame-header references, encoded into globally ordered final parts, and published only
 * after every part is durable and verified. The publication and resume contracts are owned by
 * {@code docs/internals/contracts.md} §6.
 */
public final class SortedDatasetCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SortedDatasetCoordinator.class);

    private final SortedDatasetPublisher datasetPublisher;
    private final SortFinalizer sortFinalizer;

    /** Build one transform from the complete immutable run policy. */
    public SortedDatasetCoordinator(SortRun run) {
        this(run, PublicationStepHook.NO_OP);
    }

    /** Build a transform with the internal deterministic publication crash-test seam. */
    public SortedDatasetCoordinator(SortRun run, PublicationStepHook publicationStepHook) {
        this(run, publicationStepHook, new SortFinalizer(run));
    }

    /** Build a transform around an injected finalizer for deterministic boundary tests. */
    SortedDatasetCoordinator(
            SortRun run, PublicationStepHook publicationStepHook, SortFinalizer sortFinalizer) {
        this.datasetPublisher = new SortedDatasetPublisher(run, publicationStepHook, log);
        this.sortFinalizer = sortFinalizer;
    }

    /** Merge and publish checkpoint-owned page-run segments. */
    public SortedDatasetResult transform(List<Path> stagingSegments, Path outputDir, Path stagingDir,
            SortedDatasetCommitter publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        return transform(stagingSegments, Map.of(), outputDir, stagingDir, publishListener,
                progressCallback, onFinalPassStarting);
    }

    /** Merge with checkpoint-declared PageRun formats keyed by staging path. */
    public SortedDatasetResult transform(List<Path> stagingSegments,
            Map<Path, PageRunFormat> expectedFormats, Path outputDir, Path stagingDir,
            SortedDatasetCommitter publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        try {
            return transformInterruptibly(stagingSegments, expectedFormats, outputDir, stagingDir,
                    publishListener, progressCallback, onFinalPassStarting);
        } catch (UncheckedIOException unchecked) {
            throw unchecked.getCause();
        }
    }

    private SortedDatasetResult transformInterruptibly(List<Path> stagingSegments,
            Map<Path, PageRunFormat> expectedFormats, Path outputDir, Path stagingDir,
            SortedDatasetCommitter publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        sortFinalizer.requireSourceNames(stagingSegments);
        SortedPublicationContext publicationContext = datasetPublisher.publicationContext(
                stagingSegments, outputDir, stagingDir, publishListener);
        SortFinalizer.Admission admission = sortFinalizer.admit(
                publicationContext.ownedInputs().ownedPaths(), expectedFormats);
        datasetPublisher.sweepWorking(publicationContext);
        try {
            PreparedSortedParts prepared = sortFinalizer.prepare(new SortFinalizer.Request(
                    admission, stagingDir, progressCallback, onFinalPassStarting,
                    publicationContext.ownedInputs()));
            return result(datasetPublisher.publish(prepared, publicationContext));
        } catch (CommittedPublicationCleanupException committedCleanup) {
            SortedDatasetCommit committed = committedCleanup.publishedCommitOrNull();
            if (committed == null) {
                throw committedCleanup;
            }
            throw committedCleanup.withPublishedResult(result(committed));
        } catch (IOException | RuntimeException | Error failure) {
            try {
                publicationContext.ownedInputs().sweepDisposables(
                        StagingNames.PIPELINE_TMP_GLOB);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static SortedDatasetResult result(SortedDatasetCommit commit) {
        PreparedSortedParts.MergeStatistics statistics = commit.mergeStatistics();
        return new SortedDatasetResult(commit.finalFiles(), commit.outputBytes(), commit.totalRows(),
                statistics.mergePasses(), statistics.cascadedPasses(), statistics.pagesForwarded(),
                statistics.finalizationParallelism());
    }
}
