/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.sorted.CommittedPublicationCleanupException;
import io.varve.swath.output.sorted.DatasetPublisher;
import io.varve.swath.output.sorted.PublishListener;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StagingReconciliation;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finalization lifecycle owner. Cascade width conservatively reserves the
 * requested output descriptors, but encoder admission occurs only after cascade because survivor
 * count and page maxima are the resources the final pass actually owns. The final stages share one
 * failure relay and hand only durable, globally ordered parts to the publisher. Any failure
 * quiesces every stage before owned pipeline temporaries are swept; source segments and prior
 * published parts remain recoverable.
 *
 * <p>The calling thread is the assembler. It never publishes a part as an encoder completes:
 * completion order is nondeterministic, and exposing that order would break strict cross-part raw
 * bounds. Instead it waits for dense ordinals, closes the shared channels, then delegates adjacency,
 * cardinality, renaming, manifest, and success-marker ownership to {@link DatasetPublisher}. This
 * also keeps the publication commit point single-sourced.
 *
 * <p>Cascade intermediates belong to the ordinary merge reconciliation. Pipeline temporaries use a
 * separate owned glob and remain disposable until the whole ordered set passes verification. Thus a
 * failed encoder can never make a footer-closed but unverified file resumable or consumer-visible.
 */
final class Finalization {
    private static final Logger log = LoggerFactory.getLogger(Finalization.class);

    private final SortRun run;
    private final MergePlanner planner;
    private final DatasetPublisher publisher;

    /**
     * Bind the run's policy, resource planner, and sole publication owner. Keeping publication here
     * prevents encoder threads from racing manifest order or cleanup authority.
     */
    Finalization(SortRun run, MergePlanner planner, DatasetPublisher publisher) {
        this.run = run;
        this.planner = planner;
        this.publisher = publisher;
    }

    /**
     * Execute cascade, routing, encoding, verification, and publication as one failure domain.
     * Encoder admission follows cascade because survivor count and maxima can differ from the source
     * catalog. Every asynchronous stage is quiesced before channel close and temporary-file sweep.
     */
    SortTransformResult run(PageRunCatalog sourceCatalog, Request request, int encoderCount)
            throws IOException {
        SortConfig config = run.config();
        SortMetrics metrics = run.metrics();
        metrics.recordStealReason("SORT", "finalization_pipeline");

        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(
                run.comparator(), run.hook(), metrics, config.segmentCodec(), run.orderingMode());
        PageRunMergeIo io = new PageRunMergeIo(run, segmentWriter, request.stagingDir(),
                request.ownedInputs(), "merge-");
        KWayMerge<Path> cascade = new KWayMerge<>(run.comparator(),
                planner.pipelineFanIn(sourceCatalog, encoderCount),
                io, run.hook(), metrics);
        List<Path> survivors = cascade.reduceToFanIn(
                sourceCatalog.paths(), request.progressCallback());
        cascade.recordFinalPass();
        PageRunCatalog pipelineCatalog = survivors.equals(sourceCatalog.paths())
                ? sourceCatalog
                : PageRunCatalog.preflight(survivors,
                        path -> PageRunSegmentIo.open(path, metrics));
        MergePlanner.PipelinePlan plan = planner.pipelineParallelism(encoderCount, pipelineCatalog);
        recordEncoderClamp(encoderCount, plan, pipelineCatalog);
        int effectiveEncoders = plan.encoders();
        FinalizationFailure failure = new FinalizationFailure();
        PartSizer sizer = new PartSizer(
                run.partTarget(), config.finalFileBytes());
        List<PageRunSegmentIo> channels = List.of();
        SegmentHeaderCursors cursors = null;
        PartEncoders encoders = null;
        try {
            channels = openChannels(pipelineCatalog, metrics);
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
                    encoders::awaitFirstCompletion, plan.planRefLimit())
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
            if (encoders != null) {
                encoders.close();
            }
            if (cursors != null) {
                cursors.close();
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

    /**
     * Open exactly one shared positional-read channel per surviving segment. Current segments use
     * its preflight-observed decoded-page maximum so admission and retained-byte pricing use the
     * same exact per-descriptor value.
     */
    private static List<PageRunSegmentIo> openChannels(PageRunCatalog catalog,
            SortMetrics metrics) throws IOException {
        List<PageRunSegmentIo> channels = new java.util.ArrayList<>(catalog.descriptors().size());
        try {
            for (PageRunSegmentDescriptor descriptor : catalog.descriptors()) {
                channels.add(PageRunSegmentIo.open(
                        descriptor.path(), metrics, descriptor.maxRawPayloadLength()));
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

    /**
     * Attempt every close and preserve all close failures. Stopping at the first failure would leak
     * later descriptors and could prevent a resumable retry from replacing intermediates on Windows.
     */
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

    /**
     * Unwrap the relay exception without changing the original checked/unchecked failure type. The
     * typed merge-memory and cancellation dispositions are decided above this class, so wrapping
     * everything as a generic I/O failure would change operator-visible retry semantics.
     */
    private static Throwable failureCause(Throwable failure) {
        return failure instanceof FinalizationFailure.Failed && failure.getCause() != null
                ? failure.getCause() : failure;
    }

    /**
     * Record the single binding encoder resource without duplicating the no-clamp guard. FD and heap
     * reasons stay distinct because their remediation differs.
     */
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
                        + "merge_budget_bytes={} cursor_depth={} ref_bytes={} read_page_bytes={} "
                        + "retained_page_bytes={} "
                        + "cluster_budget_bytes={}",
                requested, plan.encoders(), catalog.descriptors().size(), plan.reason().logValue(),
                run.config().mergeBudgetBytes(), plan.cursorDepth(), plan.refBytes(),
                plan.readPageBytes(), plan.retainedPageBytes(), plan.clusterBudgetBytes());
    }

    /**
     * Immutable invocation state keeps lifecycle authority explicit: output publication, staging
     * ownership, retained originals, progress, and the final-pass latch travel as one value rather
     * than as interchangeable path/callback arguments.
     */
    record Request(Path outputDir, Path stagingDir, PublishListener publishListener,
                   LongConsumer progressCallback, FinalPassListener onFinalPassStarting,
                   StagingReconciliation ownedInputs, StagingReconciliation retainedOriginals,
                   StagingReconciliation.DirectoryAuthority outputAuthority) {
    }
}
