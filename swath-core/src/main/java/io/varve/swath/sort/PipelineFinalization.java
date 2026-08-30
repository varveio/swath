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

    PipelineFinalization(SortRun run, MergePlanner planner, DatasetPublisher publisher) {
        this.run = run;
        this.planner = planner;
        this.publisher = publisher;
    }

    SortTransformResult run(PageRunCatalog sourceCatalog, Path outputDir, Path stagingDir,
            PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting, StagingReconciliation ownedInputs,
            StagingReconciliation retainedOriginals,
            StagingReconciliation.DirectoryAuthority outputAuthority) throws IOException {
        SortConfig config = run.config();
        SortMetrics metrics = run.metrics();
        metrics.recordStealReason("SORT", "finalization_pipeline");

        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(
                run.comparator(), run.hook(), metrics, config.segmentCodec(), run.orderingMode());
        PageRunMergeIo io = new PageRunMergeIo(run, segmentWriter, stagingDir, ownedInputs,
                "merge-", null, sourceCatalog.byPath(), frontier -> { }, -1, null, null);
        KWayMerge<Path> cascade = new KWayMerge<>(run.comparator(), planner.serialFanIn(sourceCatalog),
                io, run.hook(), metrics);
        List<Path> survivors = cascade.reduceToFanIn(sourceCatalog.paths(), progressCallback);
        cascade.recordFinalPass();
        PageRunCatalog pipelineCatalog = PageRunCatalog.preflight(survivors,
                path -> PageRunSegmentIo.open(path, metrics), Optional.empty(),
                Map.of(), metrics);

        int encoderCount = config.mergeParallelism();
        PipelineFailure failure = new PipelineFailure();
        PipelinePartSizer sizer = new PipelinePartSizer(config.finalFileBytes());
        SegmentReaderSlots readers = null;
        PartEncoders encoders = null;
        try {
            readers = new SegmentReaderSlots(
                    pipelineCatalog, config.mergeBudgetBytes(), metrics, failure);
            encoders = new PartEncoders(encoderCount, stagingDir, run.finalWriterFactory(),
                    run.comparator(), run.hook(), run.equalKeyPolicy(), metrics, failure, sizer,
                    progressCallback);
            onFinalPassStarting.onFinalPassStarting(true);
            MergeRouter.Result routed = new MergeRouter(
                    readers, encoders, sizer, run.comparator(), metrics, failure)
                    .route(pipelineCatalog.descriptors().size());
            readers.close();
            readers = null;
            List<PartEncoders.CompletedPart> completed = encoders.finish(routed.parts());
            encoders = null;
            List<Path> paths = completed.stream().map(PartEncoders.CompletedPart::path).toList();
            List<SortedFileWriter> writers = completed.stream()
                    .map(PartEncoders.CompletedPart::writer).toList();
            DatasetPublisher.PendingParts pending = publisher.parallelParts(
                    outputDir, stagingDir, paths, writers, ownedInputs, outputAuthority);
            publisher.verifyCardinality(pending, sourceCatalog.totalEntries(), routed.rows());
            SortTransformResult result = new SortTransformResult(
                    pending.finalFiles(), pending.outputBytes(), routed.rows(),
                    cascade.mergePasses(), cascade.cascadedPasses(), routed.pagesForwarded(),
                    encoderCount);
            try {
                publisher.publish(pending, routed.rows(), publishListener, ownedInputs,
                        retainedOriginals, io.intermediates());
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
                ownedInputs.sweepDisposables(StagingNames.PIPELINE_TMP_GLOB);
            } catch (IOException cleanupFailure) {
                thrown.addSuppressed(cleanupFailure);
            }
            throw rethrow(thrown);
        }
    }

    private static IOException rethrow(Throwable failure) throws IOException {
        Throwable cause = failure instanceof PipelineFailure.Failed && failure.getCause() != null
                ? failure.getCause() : failure;
        if (cause instanceof MergeCancellation.Cancelled cancelled) {
            throw cancelled;
        }
        if (cause instanceof IOException io) {
            return io;
        }
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IOException("sort finalization pipeline failed", cause);
    }
}
