/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.output.parquet.sorted.SortedParquetIndex;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.sort.FinalPartMetadata;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.SortCardinalityException;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortOrderException;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.spill.PageRunCatalog;
import io.varve.swath.sort.spill.PageRunFormat;
import io.varve.swath.sort.spill.PageRunSegmentDescriptor;
import io.varve.swath.sort.spill.PageRunSegmentIo;
import io.varve.swath.sort.spill.PageRunSegmentWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Algorithmic owner for admitting staged runs and preparing a complete unpublished sorted part set.
 * Cascade width conservatively reserves requested output descriptors, but encoder admission occurs
 * only after cascade because survivor count and page maxima are the resources the final pass
 * actually owns.
 *
 * <p>The calling thread is the assembler. It waits for dense ordinals, joins every encoder, closes
 * all shared channels, and proves strict raw-byte adjacency plus exact cardinality before returning.
 * It never assigns consumer-visible names, invokes a dataset commit hook, or destructively cleans
 * staging. A direct caller of {@link #prepare} owns disposable cleanup after failure; the production
 * caller is the sorted-dataset coordinator.
 */
public final class SortFinalizer {
    private static final Logger log = LoggerFactory.getLogger(SortFinalizer.class);

    private final SortRun run;
    private final MergePlanner planner;
    private final PageRunCatalog.Opener catalogOpener;

    /** Build one finalizer from the complete immutable run policy. */
    public SortFinalizer(SortRun run) {
        this(run, path -> PageRunSegmentIo.open(path, run.metrics()));
    }

    /** Reject unsupported source naming before publication authority interprets the paths. */
    public void requireSourceNames(List<Path> stagingSegments) {
        PageRunCatalog.requirePageRunNames(stagingSegments);
    }

    public SortFinalizer(SortRun run, PageRunCatalog.Opener catalogOpener) {
        this.run = run;
        this.planner = new MergePlanner(run);
        this.catalogOpener = catalogOpener;
    }

    /**
     * Admit and preflight every source run before publication-side working-file cleanup can mutate
     * the staging directory.
     */
    public Admission admit(List<Path> stagingSegments, Map<Path, PageRunFormat> expectedFormats)
            throws IOException {
        return new Admission(PageRunCatalog.preflight(
                stagingSegments, catalogOpener, expectedFormats, run.metrics()));
    }

    /**
     * Execute cascade, routing, encoding, and verification. Every asynchronous stage is quiesced
     * before channel close. On failure this method performs no destructive staging mutation; its
     * caller owns cleanup of disposable files after the exception escapes.
     */
    public PreparedSortedParts prepare(Request request) throws IOException {
        try {
            return prepareInterruptibly(request);
        } catch (MergeCancellation.Cancelled cancelled) {
            Thread.currentThread().interrupt();
            throw new IOException("sort merge interrupted", cancelled);
        }
    }

    private PreparedSortedParts prepareInterruptibly(Request request) throws IOException {
        PageRunCatalog sourceCatalog = request.admission().catalog;
        int encoderCount = run.config().mergeParallelism();
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
        PartSizer sizer = new PartSizer(run.partTarget(), config.finalFileBytes());
        List<PageRunSegmentIo> channels = List.of();
        SegmentHeaderCursors cursors = null;
        PartEncoders encoders = null;
        try {
            channels = openChannels(pipelineCatalog, metrics);
            cursors = new SegmentHeaderCursors(channels,
                    SegmentHeaderCursors.planned(channels.size()), metrics, failure);
            encoders = new PartEncoders(effectiveEncoders, channels, plan.clusterBudgetBytes(),
                    request.stagingDir(), run.finalWriterFactory(), run.comparator(), run.hook(),
                    run.equalKeyPolicy(), metrics, failure, sizer, request.progressCallback());
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
            List<PreparedSortedParts.Part> parts = prepareParts(completed, metrics);
            long writtenRows = 0;
            for (PreparedSortedParts.Part part : parts) {
                writtenRows = Math.addExact(writtenRows, part.rows());
            }
            requirePreparedSet(parts, sourceCatalog.totalEntries(), routed.rows(), writtenRows,
                    metrics);
            return new PreparedSortedParts(parts, sourceCatalog.totalEntries(), routed.rows(),
                    new PreparedSortedParts.MergeStatistics(
                            cascade.mergePasses(), cascade.cascadedPasses(),
                            routed.pagesForwarded(), effectiveEncoders),
                    new PreparedSortedParts.CleanupToken(io.intermediates()));
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

    private static List<PreparedSortedParts.Part> prepareParts(
            List<PartEncoders.CompletedPart> completed, SortMetrics metrics) throws IOException {
        List<PreparedSortedParts.Part> parts = new ArrayList<>(completed.size());
        for (PartEncoders.CompletedPart completedPart : completed) {
            Path path = completedPart.path();
            SortedFileWriter writer = completedPart.writer();
            long rows = writer.rows();
            long bytes = Files.size(path);
            Optional<FinalPartMetadata> metadata = writer.finalMetadata();
            byte[] min;
            byte[] max;
            if (metadata.isPresent()) {
                FinalPartMetadata captured = metadata.orElseThrow();
                if (captured.rows() != rows || captured.bytes() != bytes) {
                    throw new IOException("closed sorted part metadata disagrees with durable file: "
                            + path);
                }
                min = captured.rawMinKey();
                max = captured.rawMaxKey();
            } else {
                metrics.recordStealReason("SORT", "cross_part_bounds_fallback_scan");
                SortedParquetIndex.Bounds bounds = SortedParquetIndex.bounds(
                        path, MergeCancellation::check);
                if (bounds.rowCount() != rows) {
                    throw new IOException("closed sorted part row count disagrees with durable file: "
                            + path);
                }
                min = bounds.firstKey();
                max = bounds.lastKey();
            }
            parts.add(new PreparedSortedParts.Part(path, rows, bytes, min, max, metadata));
        }
        return List.copyOf(parts);
    }

    /** Enforce strict raw-byte adjacency after durable close and before publication can begin. */
    static void requireDisjointParts(List<PreparedSortedParts.Part> parts) {
        requireDisjointParts(parts, SortMetrics.NO_OP);
    }

    static void requireDisjointParts(List<PreparedSortedParts.Part> parts, SortMetrics metrics) {
        byte[] previousMax = null;
        int previousPart = -1;
        for (int i = 0; i < parts.size(); i++) {
            PreparedSortedParts.Part part = parts.get(i);
            byte[] currentMin = part.rawMinKey();
            if (currentMin == null) {
                continue;
            }
            if (previousMax != null && Arrays.compareUnsigned(previousMax, currentMin) >= 0) {
                metrics.recordStealReason("SORT", "cross_part_overlap_rejected");
                throw new SortOrderException("sorted output parts overlap at adjacency "
                        + previousPart + " -> " + i + " under raw unsigned key order");
            }
            previousMax = part.rawMaxKey();
            previousPart = i;
        }
    }

    /** Preserve the established validation precedence: order failures outrank count failures. */
    static void requirePreparedSet(List<PreparedSortedParts.Part> parts, long sourceRows,
            long routedRows, long finalPartRows, SortMetrics metrics) throws IOException {
        requireDisjointParts(parts, metrics);
        requireExactCardinality(sourceRows, routedRows, finalPartRows, metrics);
    }

    /** Refuse a prepared value unless source, router, and durable part counts agree exactly. */
    public static void requireExactCardinality(
            long sourceRows, long routedRows, long finalPartRows) throws IOException {
        requireExactCardinality(sourceRows, routedRows, finalPartRows, SortMetrics.NO_OP);
    }

    static void requireExactCardinality(long sourceRows, long routedRows, long finalPartRows,
            SortMetrics metrics) throws IOException {
        if (sourceRows != routedRows || sourceRows != finalPartRows) {
            metrics.recordStealReason("SORT", "sort_output_cardinality_mismatch");
            throw new SortCardinalityException(
                    "sort output cardinality mismatch before publication: source_rows="
                    + sourceRows + " drained_rows=" + routedRows
                    + " final_part_rows=" + finalPartRows);
        }
    }

    private static List<PageRunSegmentIo> openChannels(PageRunCatalog catalog,
            SortMetrics metrics) throws IOException {
        List<PageRunSegmentIo> channels = new ArrayList<>(catalog.descriptors().size());
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

    private static Throwable failureCause(Throwable failure) {
        return failure instanceof FinalizationFailure.Failed && failure.getCause() != null
                ? failure.getCause() : failure;
    }

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
                        + "retained_page_bytes={} cluster_budget_bytes={}",
                requested, plan.encoders(), catalog.descriptors().size(), plan.reason().logValue(),
                run.config().mergeBudgetBytes(), plan.cursorDepth(), plan.refBytes(),
                plan.readPageBytes(), plan.retainedPageBytes(), plan.clusterBudgetBytes());
    }

    /** Opaque admitted source catalog; only this finalizer interprets its planning state. */
    public static final class Admission {
        private final PageRunCatalog catalog;

        private Admission(PageRunCatalog catalog) {
            this.catalog = catalog;
        }
    }

    /** Invocation state for preparation only; it carries no publication destination or committer. */
    public record Request(
            Admission admission,
            Path stagingDir,
            LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting,
            StagingReconciliation ownedInputs) {
    }
}
