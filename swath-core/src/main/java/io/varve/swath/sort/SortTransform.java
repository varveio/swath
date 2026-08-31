/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.sorted.CommittedPublicationCleanupException;
import io.varve.swath.output.sorted.DatasetPublisher;
import io.varve.swath.output.sorted.PublicationStepHook;
import io.varve.swath.output.sorted.PublishListener;
import io.varve.swath.output.sorted.SortedDatasetCommit;
import io.varve.swath.output.sorted.SortedPublicationContext;
import io.varve.swath.output.sorted.StagingNames;
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
public final class SortTransform {

    private static final Logger log = LoggerFactory.getLogger(SortTransform.class);

    private final DatasetPublisher datasetPublisher;
    private final SortFinalizer sortFinalizer;

    /** Build one transform from the complete immutable run policy. */
    public SortTransform(SortRun run) {
        this(run, PublicationStepHook.NO_OP);
    }

    /** Build a transform with the internal deterministic publication crash-test seam. */
    public SortTransform(SortRun run, PublicationStepHook publicationStepHook) {
        this(run, publicationStepHook, path -> PageRunSegmentIo.open(path, run.metrics()));
    }

    SortTransform(SortRun run, PublicationStepHook publicationStepHook,
            PageRunCatalog.Opener catalogOpener) {
        this.datasetPublisher = new DatasetPublisher(run, publicationStepHook, log);
        this.sortFinalizer = new SortFinalizer(run, catalogOpener);
    }

    /** Merge and publish checkpoint-owned page-run segments. */
    public SortTransformResult transform(List<Path> stagingSegments, Path outputDir, Path stagingDir,
            PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        return transform(stagingSegments, Map.of(), outputDir, stagingDir, publishListener,
                progressCallback, onFinalPassStarting);
    }

    /** Merge with checkpoint-declared PageRun formats keyed by staging path. */
    public SortTransformResult transform(List<Path> stagingSegments,
            Map<Path, PageRunFormat> expectedFormats, Path outputDir, Path stagingDir,
            PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        try {
            return transformInterruptibly(stagingSegments, expectedFormats, outputDir, stagingDir,
                    publishListener, progressCallback, onFinalPassStarting);
        } catch (MergeCancellation.Cancelled cancelled) {
            Thread.currentThread().interrupt();
            throw new IOException("sort merge interrupted", cancelled);
        } catch (UncheckedIOException unchecked) {
            throw unchecked.getCause();
        }
    }

    private SortTransformResult transformInterruptibly(List<Path> stagingSegments,
            Map<Path, PageRunFormat> expectedFormats, Path outputDir, Path stagingDir,
            PublishListener publishListener, LongConsumer progressCallback,
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
            throw committedCleanup.withPublishedResult(result(committedCleanup.publishedCommit()));
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

    private static SortTransformResult result(SortedDatasetCommit commit) {
        PreparedSortedParts.MergeStatistics statistics = commit.mergeStatistics();
        return new SortTransformResult(commit.finalFiles(), commit.outputBytes(), commit.totalRows(),
                statistics.mergePasses(), statistics.cascadedPasses(), statistics.pagesForwarded(),
                statistics.finalizationParallelism());
    }
}
