/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

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

    private final SortRun run;
    private final DatasetPublisher datasetPublisher;
    private final MergePlanner mergePlanner;
    private final PageRunCatalog.Opener catalogOpener;

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
        this.run = run;
        this.catalogOpener = catalogOpener;
        this.datasetPublisher = new DatasetPublisher(run, publicationStepHook, log);
        this.mergePlanner = new MergePlanner(run);
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
        PageRunCatalog.requirePageRunNames(stagingSegments);
        StagingReconciliation ownedInputs =
                StagingReconciliation.fromPaths(stagingDir, stagingSegments);
        StagingReconciliation.DirectoryAuthority outputAuthority =
                StagingReconciliation.DirectoryAuthority.capture(
                        outputDir, "sort output directory");
        stagingSegments = ownedInputs.ownedPaths();
        StagingReconciliation retainedOriginals =
                datasetPublisher.retainedOriginals(ownedInputs);

        PageRunCatalog catalog = PageRunCatalog.preflight(
                stagingSegments, catalogOpener, expectedFormats, run.metrics());
        datasetPublisher.sweepWorking(outputDir, stagingDir, ownedInputs, outputAuthority);

        Finalization.Request request = new Finalization.Request(
                outputDir, stagingDir, publishListener, progressCallback, onFinalPassStarting,
                ownedInputs, retainedOriginals, outputAuthority);
        return new Finalization(run, mergePlanner, datasetPublisher)
                .run(catalog, request, run.config().mergeParallelism());
    }
}
