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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates cascade reduction, bounded pipeline execution, ordered assembly, and publication. */
final class PipelineFinalization {
    private static final Logger log = LoggerFactory.getLogger(PipelineFinalization.class);

    private final SortRun run;
    private final MergePlanner planner;
    private final DatasetPublisher publisher;

    PipelineFinalization(SortRun run, MergePlanner planner, DatasetPublisher publisher) {
        this.run = run;
        this.planner = planner;
        this.publisher = publisher;
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
        MergePlanner.PipelinePlan plan = planner.pipelineParallelism(encoderCount, pipelineCatalog);
        recordEncoderClamp(encoderCount, plan, pipelineCatalog);
        encoderCount = plan.encoders();
        PipelinePartSizer sizer = new PipelinePartSizer(
                run.pipelinePartTarget(), config.finalFileBytes());
        SegmentReaderSlots readers = null;
        PartEncoders encoders = null;
        try {
            readers = new SegmentReaderSlots(
                    pipelineCatalog, SegmentReaderSlots.planned(
                            plan, pipelineCatalog.descriptors().size()), metrics, failure);
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
            throw checkedFailure(thrown);
        }
    }

    private static IOException checkedFailure(Throwable failure) {
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

    private void recordEncoderClamp(int requested, MergePlanner.PipelinePlan plan,
            PageRunCatalog catalog) {
        if (plan.reason() == MergePlanner.PipelineClampReason.NONE) {
            return;
        }
        log.warn("sort_pipeline_encoders_clamped requested={} effective={} segments={} reason={} "
                        + "merge_budget_bytes={} slot_depth={} page_bytes={}",
                requested, plan.encoders(), catalog.descriptors().size(), plan.reason().logValue(),
                run.config().mergeBudgetBytes(), plan.slotDepth(), plan.pageBytes());
        switch (plan.reason()) {
            case FD_CLAMPED ->
                    run.metrics().recordStealReason("SORT", "pipeline_encoders_fd_clamped");
            case HEAP_CLAMPED ->
                    run.metrics().recordStealReason("SORT", "pipeline_encoders_heap_clamped");
            case NONE -> throw new AssertionError("unreachable pipeline clamp");
        }
    }

    /** Immutable invocation state keeps the lifecycle entry point independent of argument order. */
    record Request(Path outputDir, Path stagingDir, PublishListener publishListener,
                   LongConsumer progressCallback, FinalPassListener onFinalPassStarting,
                   StagingReconciliation ownedInputs, StagingReconciliation retainedOriginals,
                   StagingReconciliation.DirectoryAuthority outputAuthority) {
    }
}
