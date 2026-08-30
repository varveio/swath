/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

/** Coordinates cascade reduction, bounded pipeline execution, ordered assembly, and publication. */
final class PipelineFinalization {
    private final SortRun run;
    private final MergePlanner planner;
    private final DatasetPublisher publisher;
    private final PipelinePartSizer.Target partTarget;

    PipelineFinalization(SortRun run, MergePlanner planner, DatasetPublisher publisher,
            PipelinePartSizer.Target partTarget) {
        this.run = run;
        this.planner = planner;
        this.publisher = publisher;
        this.partTarget = partTarget;
    }

    SortTransformResult run(PageRunCatalog sourceCatalog, Request request, int encoderCount)
            throws IOException {
        SortConfig config = run.config();
        SortMetrics metrics = run.metrics();
        metrics.recordStealReason("SORT", "finalization_pipeline");

        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(
                run.comparator(), run.hook(), metrics, config.segmentCodec(), run.orderingMode());
        PageRunMergeIo io = new PageRunMergeIo(run, segmentWriter, request.stagingDir(),
                request.ownedInputs(),
                "merge-", null, sourceCatalog.byPath(), frontier -> { }, -1, null, null);
        KWayMerge<Path> cascade = new KWayMerge<>(run.comparator(),
                planner.pipelineFanIn(sourceCatalog, encoderCount),
                io, run.hook(), metrics);
        List<Path> survivors = cascade.reduceToFanIn(
                sourceCatalog.paths(), request.progressCallback());
        cascade.recordFinalPass();
        PageRunCatalog pipelineCatalog = survivors.equals(sourceCatalog.paths())
                ? sourceCatalog
                : PageRunCatalog.preflight(survivors,
                        path -> PageRunSegmentIo.open(path, metrics), Optional.empty(),
                        Map.of(), metrics);

        PipelineFailure failure = new PipelineFailure();
        PipelinePartSizer sizer = new PipelinePartSizer(partTarget, config.finalFileBytes());
        SegmentReaderSlots readers = null;
        PartEncoders encoders = null;
        try {
            readers = new SegmentReaderSlots(
                    pipelineCatalog, config.mergeBudgetBytes(), metrics, failure);
            encoders = new PartEncoders(encoderCount, request.stagingDir(), run.finalWriterFactory(),
                    run.comparator(), run.hook(), run.equalKeyPolicy(), metrics, failure, sizer,
                    request.progressCallback());
            request.onFinalPassStarting().onFinalPassStarting(true);
            MergeRouter.Result routed = new MergeRouter(
                    readers, encoders, sizer, run.comparator(), config.mergeBudgetBytes(), metrics,
                    failure)
                    .route(pipelineCatalog.descriptors().size());
            readers.close();
            readers = null;
            List<PartEncoders.CompletedPart> completed = encoders.finish(routed.parts());
            encoders = null;
            List<Path> paths = completed.stream().map(PartEncoders.CompletedPart::path).toList();
            List<SortedFileWriter> writers = completed.stream()
                    .map(PartEncoders.CompletedPart::writer).toList();
            DatasetPublisher.PendingParts pending = publisher.preclosedParts(
                    request.outputDir(), request.stagingDir(), paths, writers,
                    request.ownedInputs(), request.outputAuthority());
            publisher.verifyCardinality(pending, sourceCatalog.totalEntries(), routed.rows());
            SortTransformResult result = new SortTransformResult(
                    pending.finalFiles(), pending.outputBytes(), routed.rows(),
                    cascade.mergePasses(), cascade.cascadedPasses(), routed.pagesForwarded(),
                    encoderCount);
            try {
                publisher.publish(pending, routed.rows(), request.publishListener(),
                        request.ownedInputs(), request.retainedOriginals(), io.intermediates());
            } catch (CommittedPublicationCleanupException e) {
                throw e.withPublishedResult(result);
            }
            return result;
        } catch (Throwable thrown) {
            failure.record(thrown);
            if (readers != null) {
                readers.close();
            }
            if (encoders != null) {
                encoders.close();
            }
            try {
                request.ownedInputs().sweepDisposables(StagingNames.PIPELINE_TMP_GLOB);
            } catch (IOException cleanupFailure) {
                thrown.addSuppressed(cleanupFailure);
            }
            throw asRuntimeFailure(thrown);
        }
    }

    private static RuntimeException asRuntimeFailure(Throwable failure) throws IOException {
        Throwable cause = failure instanceof PipelineFailure.Failed && failure.getCause() != null
                ? failure.getCause() : failure;
        if (cause instanceof MergeCancellation.Cancelled cancelled) {
            return cancelled;
        }
        if (cause instanceof IOException io) {
            throw io;
        }
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new IOException("sort finalization pipeline failed", cause);
    }

    /** Immutable invocation state keeps the lifecycle entry point independent of argument order. */
    record Request(Path outputDir, Path stagingDir, PublishListener publishListener,
                   LongConsumer progressCallback, FinalPassListener onFinalPassStarting,
                   StagingReconciliation ownedInputs, StagingReconciliation retainedOriginals,
                   StagingReconciliation.DirectoryAuthority outputAuthority) {
    }
}
