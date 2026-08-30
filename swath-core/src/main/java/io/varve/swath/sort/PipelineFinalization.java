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

/**
 * Finalization lifecycle owner for the pipeline arm. Cascade width conservatively reserves the
 * requested output descriptors, but encoder admission occurs only after cascade because survivor
 * count and page maxima are the resources the final pass actually owns. The final stages share one
 * failure relay and hand only durable, globally ordered parts to the publisher. Any failure
 * quiesces every stage before owned pipeline temporaries are swept; source segments and prior
 * published parts remain recoverable.
 */
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

    /** Execute cascade, routing, encoding, verification, and publication as one failure domain. */
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
                        path -> PageRunSegmentIo.open(path, metrics),
                        Optional.of(ignored -> { }),
                        Map.of(), metrics);
        MergePlanner.PipelinePlan plan = planner.pipelineParallelism(encoderCount, pipelineCatalog);
        recordEncoderClamp(encoderCount, plan, pipelineCatalog);
        int effectiveEncoders = plan.encoders();
        PipelineFailure failure = new PipelineFailure();
        PipelinePartSizer sizer = new PipelinePartSizer(
                run.pipelinePartTarget(), config.finalFileBytes());
        List<PageRunSegmentIo> channels = List.of();
        SegmentHeaderCursors cursors = null;
        PartEncoders encoders = null;
        try {
            channels = openChannels(pipelineCatalog, plan, metrics);
            cursors = new SegmentHeaderCursors(channels,
                    SegmentHeaderCursors.planned(channels.size()), metrics, failure);
            encoders = new PartEncoders(effectiveEncoders, channels, plan.clusterBudgetBytes(),
                    request.stagingDir(),
                    run.finalWriterFactory(),
                    run.comparator(), run.hook(), run.equalKeyPolicy(), metrics, failure, sizer,
                    request.progressCallback());
            request.onFinalPassStarting().onFinalPassStarting(true);
            MergeRouter.Result routed = new MergeRouter(
                    cursors, encoders::submit, sizer, metrics, failure,
                    encoders::awaitFirstCompletion)
                    .route(pipelineCatalog.descriptors().size());
            if (routed.refs() != pipelineCatalog.totalRecords()) {
                throw new IllegalStateException("pipeline reference count mismatch: planned="
                        + routed.refs() + " source=" + pipelineCatalog.totalRecords());
            }
            cursors.close();
            cursors = null;
            List<PartEncoders.CompletedPart> completed = encoders.finish(routed.parts());
            encoders = null;
            closeChannels(channels);
            channels = List.of();
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
                    effectiveEncoders);
            try {
                publisher.publish(pending, routed.rows(), request.publishListener(),
                        request.ownedInputs(), request.retainedOriginals(), io.intermediates());
            } catch (CommittedPublicationCleanupException e) {
                throw e.withPublishedResult(result);
            }
            return result;
        } catch (Throwable thrown) {
            failure.record(thrown);
            if (cursors != null) {
                cursors.close();
            }
            if (encoders != null) {
                encoders.close();
            }
            try {
                closeChannels(channels);
            } catch (IOException closeFailure) {
                thrown.addSuppressed(closeFailure);
            }
            try {
                request.ownedInputs().sweepDisposables(StagingNames.PIPELINE_TMP_GLOB);
            } catch (IOException cleanupFailure) {
                thrown.addSuppressed(cleanupFailure);
            }
            Throwable cause = failureCause(thrown);
            if (cause instanceof MergeCancellation.Cancelled cancelled) {
                throw cancelled;
            }
            if (cause instanceof IOException ioFailure) {
                throw ioFailure;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("sort finalization pipeline failed", cause);
        }
    }

    /** Open exactly one shared positional-read channel per surviving segment. */
    private static List<PageRunSegmentIo> openChannels(PageRunCatalog catalog,
            MergePlanner.PipelinePlan plan, SortMetrics metrics) throws IOException {
        List<PageRunSegmentIo> channels = new java.util.ArrayList<>(catalog.descriptors().size());
        try {
            for (PageRunSegmentDescriptor descriptor : catalog.descriptors()) {
                int decodedLimit = descriptor.hasDecodedPageMaximum()
                        ? descriptor.maxRawPayloadLength() : plan.legacyDecodedLimit();
                channels.add(PageRunSegmentIo.open(descriptor.path(), metrics, decodedLimit));
            }
            return List.copyOf(channels);
        } catch (Throwable failure) {
            try {
                closeChannels(channels);
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof IOException ioFailure) {
                throw ioFailure;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IOException("failed to open pipeline segment channels", failure);
        }
    }

    private static void closeChannels(List<PageRunSegmentIo> channels) throws IOException {
        IOException failure = null;
        for (PageRunSegmentIo channel : channels) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Unwrap the relay exception without changing the original checked/unchecked failure type. */
    private static Throwable failureCause(Throwable failure) {
        return failure instanceof PipelineFailure.Failed && failure.getCause() != null
                ? failure.getCause() : failure;
    }

    /** Record the single binding encoder resource without duplicating the no-clamp guard. */
    private void recordEncoderClamp(int requested, MergePlanner.PipelinePlan plan,
            PageRunCatalog catalog) {
        switch (plan.reason()) {
            case NONE -> {
                return;
            }
            case FD_CLAMPED ->
                    run.metrics().recordStealReason("SORT", "pipeline_encoders_fd_clamped");
            case HEAP_CLAMPED ->
                    run.metrics().recordStealReason("SORT", "pipeline_encoders_heap_clamped");
        }
        log.warn("sort_pipeline_encoders_clamped requested={} effective={} segments={} reason={} "
                        + "merge_budget_bytes={} cursor_depth={} ref_bytes={} page_bytes={} "
                        + "cluster_budget_bytes={}",
                requested, plan.encoders(), catalog.descriptors().size(), plan.reason().logValue(),
                run.config().mergeBudgetBytes(), plan.cursorDepth(), plan.refBytes(), plan.pageBytes(),
                plan.clusterBudgetBytes());
    }

    /** Immutable invocation state keeps the lifecycle entry point independent of argument order. */
    record Request(Path outputDir, Path stagingDir, PublishListener publishListener,
                   LongConsumer progressCallback, FinalPassListener onFinalPassStarting,
                   StagingReconciliation ownedInputs, StagingReconciliation retainedOriginals,
                   StagingReconciliation.DirectoryAuthority outputAuthority) {
    }
}
