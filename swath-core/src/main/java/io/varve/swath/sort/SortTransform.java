/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The batch merge/publish step of {@code --sort}: cascaded {@link KWayMerge} over staged {@code
 * .pageseg} page-run segments into final sorted Parquet, written to {@code *.tmp} and renamed in key order, with
 * staging deleted or exactly reconciled only after the {@link PublishListener} fires. The full publish/resume state
 * machine (manifest-last commit, idempotent re-entry, stale-tmp/stale-final cleanup) is
 * {@code docs/internals/contracts.md} §6; this class's own re-entry sweep-scope safety proof lives at
 * {@link DatasetPublisher#cleanStaleFinals}, not here.
 *
 * <p>Multi-file output rolls the sorted stream into range-disjoint files named {@code
 * part-00000.parquet}, {@code part-00001.parquet}, … (lexical order == key order), starting a
 * fresh file each time the current one reaches {@code finalFileBytes} (default ~1&nbsp;GiB;
 * {@code docs/internals/contracts.md} §7).
 *
 * <p>The {@link KWayMerge} pass width is a runtime-clamped fan-in
 * ({@link MergePlanner#serialFanIn}), never the raw {@code fan-in} knob: the MIN of the
 * static budget ({@link SortConfig#effectiveFanIn()}), the process fd budget
 * ({@link MergeFdBudget}), and, for page-run input, a record-size refinement read from each
 * segment's trailer. The merge's active structures remain functions of configured segment/fan-in
 * knobs rather than total object count (I11); {@code merge-budget-bytes} is a planning budget, not
 * a byte-exact JVM heap meter. A clamp that forces the fan-in below the segment count
 * degrades to the {@link KWayMerge} cascade backstop instead of crashing ({@code
 * docs/internals/contracts.md} §7, {@code fan-in}/{@code merge-budget-bytes}).
 *
 * <p>Library-only: this step manages files inside the caller-supplied output and staging
 * directories only (it never destructively cleans a path it does not own).
 */
public final class SortTransform {

    private static final Logger log = LoggerFactory.getLogger(SortTransform.class);

    // The complete run policy threads whole through to ParallelRangeMerge; the individual fields
    // below are its hot-path aliases.
    private final SortRun run;
    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final EqualKeyPolicy equalKeyPolicy;
    private final SortMetrics metrics;
    private final MergeInputProfile inputProfile;
    // Per-range merge-latency seam for the parallel path (NO_OP off that path).
    private final RangeMergeTimer rangeTimer;
    // Package-private post-worker proof failure seam for the parallel coordinator.
    private final PageRunZoneVerifier.ProofReaderFactory proofReaderFactory;
    // Owns the package-private crash hook; production installs NO_OP, so each boundary costs one
    // non-allocating no-op call and carries no global state.
    private final DatasetPublisher datasetPublisher;
    // The "how wide can this merge pass be" cluster (static budget estimate + fd/record-size
    // runtime clamps + their observability) lives in MergePlanner, not here.
    private final MergePlanner mergePlanner;

    /** Build one transform from the complete immutable run policy. */
    public SortTransform(SortRun run) {
        this(run, PublicationStepHook.NO_OP, PageRunProofSpool.Reader::new);
    }

    SortTransform(SortRun run, PublicationStepHook publicationStepHook) {
        this(run, publicationStepHook, PageRunProofSpool.Reader::new);
    }

    SortTransform(SortRun run, PublicationStepHook publicationStepHook,
            PageRunZoneVerifier.ProofReaderFactory proofReaderFactory) {
        this.run = run;
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.equalKeyPolicy = run.equalKeyPolicy();
        this.metrics = run.metrics();
        this.inputProfile = run.inputProfile();
        this.rangeTimer = run.rangeMergeTimer();
        PublicationStepHook checkedPublicationHook =
                Objects.requireNonNull(publicationStepHook, "publicationStepHook");
        this.proofReaderFactory = Objects.requireNonNull(proofReaderFactory, "proofReaderFactory");
        this.datasetPublisher = new DatasetPublisher(run, checkedPublicationHook, log);
        this.mergePlanner = new MergePlanner(run);
    }

    /**
     * Merge {@code stagingSegments} into the final sorted output under {@code outputDir}, using
     * {@code stagingDir} for cascade intermediates. {@code publishListener} fires after the renames
     * and before staging deletion/reconciliation (the manifest-last commit point). {@code progressCallback} is
     * invoked with the row count of each completed batch — {@code swath.progress.units}'
     * merge-phase feed, wired by {@code ListRunner} to {@code RunMetrics.recordProgress}. Batched
     * ({@link KWayMerge#PROGRESS_BATCH_ROWS}), not per-row, and threaded through <em>every</em> merge
     * pass, not just the final one: {@link KWayMerge#merge(List, LongConsumer)} advances it as each
     * intermediate cascade pass drains rows, and
     * {@link RolledPartWriter#drain} advances it again as the final streaming pass writes rolled output. A
     * cascade's progress total is therefore rows-per-pass, not a single row-count total — intended:
     * progress is a monotonic units-of-work counter, not required to equal total rows for a
     * multi-pass merge. {@code onFinalPassStarting} runs once, right before the merge starts
     * writing the output it will publish. On the serial path every cascade pass is complete by then,
     * so the remaining work is one pass over the staged rows; on the parallel range-merge path it is
     * only when no range has to cascade, which is what the listener's flag carries.
     */
    public SortTransformResult transform(List<Path> stagingSegments, Path outputDir, Path stagingDir,
                                         PublishListener publishListener, LongConsumer progressCallback,
                                         FinalPassListener onFinalPassStarting)
            throws IOException {
        try {
            return transformInterruptibly(stagingSegments, outputDir, stagingDir, publishListener,
                    progressCallback, onFinalPassStarting);
        } catch (MergeCancellation.Cancelled cancelled) {
            // Internal cancellation must not leak through this public IOException API. All merge
            // resources unwind before this boundary; retain the caller's ordinary interrupt state.
            Thread.currentThread().interrupt();
            throw new IOException("sort merge interrupted", cancelled);
        }
    }

    private SortTransformResult transformInterruptibly(List<Path> stagingSegments, Path outputDir,
            Path stagingDir, PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        PageRunCatalog.requirePageRunNames(stagingSegments);
        StagingReconciliation retainedOriginals =
                datasetPublisher.retainedOriginals(stagingSegments, stagingDir);
        boolean parallelKickoff = config.mergeParallelism() > 1
                && inputProfile.parallelRangesAllowed();
        MergePlanner.BoundaryCandidates boundaryCandidates =
                new MergePlanner.BoundaryCandidates();
        Optional<Consumer<byte[]>> boundaryKeySink = parallelKickoff
                ? Optional.of(boundaryCandidates::add)
                : Optional.empty();
        // Validate and retain every trailer before deleting any disposable working file. Fan-in and
        // range planning consume these descriptors; parallel candidates also stream their embedded
        // sample keys into one globally bounded set during this same open. An unreadable segment is
        // not an optional memory refinement and must fail at kickoff while the prior output and
        // working evidence are intact. Explicit serial and arbitrary-run merges skip extensions.
        PageRunCatalog catalog =
                PageRunCatalog.preflight(stagingSegments,
                        path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), boundaryKeySink);
        // Disposable working files are cleared before work; prior finals remain until their complete
        // replacements are durable. DatasetPublisher owns this and every later physical mutation.
        datasetPublisher.sweepWorking(outputDir, stagingDir);

        // When the configured/default swath.sort.merge-parallelism survives the staged-size,
        // memory, and fd gates, split the keyspace into
        // contiguous ordered ranges and merge them concurrently (ParallelRangeMerge). A degenerate
        // keyspace (fewer than two distinct sample keys) returns null and falls through to the serial
        // path below — so both merge-parallelism=1 and any unsplittable keyspace take the
        // EXACT untouched serial code, byte-for-byte identical.
        // Parallel ranges consume the same page-run frontier as the serial path. The default is
        // core-derived and capped; effectiveRanges() may still route an ordinary run to the serial
        // path, and the completeness stamp makes multi-file parallel output self-describing.
        if (parallelKickoff) {
            SortTransformResult parallel = tryTransformParallel(catalog, outputDir, stagingDir,
                    publishListener, progressCallback, onFinalPassStarting, boundaryCandidates,
                    retainedOriginals);
            if (parallel != null) {
                return parallel;
            }
        } else if (config.mergeParallelism() > 1) {
            metrics.recordStealReason("SORT", "merge_range_frontier_disabled");
        }

        DatasetPublisher.PendingParts pending = datasetPublisher.serialParts(outputDir, stagingDir);
        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(comparator, hook, metrics, config.segmentCodec());
        PageRunMergeIo io = new PageRunMergeIo(run, segmentWriter, stagingDir,
                "merge-", null, Map.of(), frontier -> { }, -1, null, null);
        // Fan-in: see the class javadoc for the runtime-clamp policy. serialFanIn() computes it and,
        // as a side effect, fires the cascade-predicted warning + clamp metrics once at kickoff.
        int runtimeFanIn = mergePlanner.serialFanIn(catalog);
        KWayMerge<Path> merge = new KWayMerge<>(comparator, runtimeFanIn, io, hook, metrics);

        long totalRows;
        try (SortedCursor merged = merge.merge(stagingSegments, progressCallback)) {
            // merge() above already ran every cascade pass to completion before returning this
            // cursor (see KWayMerge#merge) — so by this point only the final streaming pass remains,
            // and it drains exactly the staged rows once: an honest completion denominator.
            onFinalPassStarting.onFinalPassStarting(true);
            totalRows = RolledPartWriter.drain(merged, config.finalFileBytes(),
                    pending::openNext,
                    true, progressCallback, metrics, equalKeyPolicy, comparator);
        }
        datasetPublisher.allTmpPartsDurable();
        // Merge engagement counts (read after the cursor is fully drained + closed above, so the
        // final streaming pass's fast-path total has accumulated) — surfaced for the run's meters/summary.
        long mergePasses = merge.mergePasses();
        long cascadedPasses = merge.cascadedPasses();
        long fastPathEmissions = merge.fastPathEmissions();

        datasetPublisher.publish(pending, totalRows, publishListener, stagingSegments,
                retainedOriginals, io.intermediates());
        return new SortTransformResult(pending.finalFiles(), totalRows,
                mergePasses, cascadedPasses, fastPathEmissions, 1);
    }

    /**
     * The concurrent range-merge path. Returns {@code null} for a
     * degenerate keyspace that cannot be split into more than one range, so the caller falls back to
     * the untouched serial merge. Otherwise it merges the ranges concurrently
     * ({@link ParallelRangeMerge}), then does the SAME serial publish as the serial path — rename the
     * ordered range parts into one ascending {@code part-00000.parquet}… sequence (filename order ==
     * key order == global sort), fire the publish listener, complete the configured staging policy.
     * Immediately before that
     * rename it writes the multi-file completeness stamp ({@code file_index} 1..N, one
     * {@code file_final} on N), which is the point at which the global part order is first known.
     */
    private SortTransformResult tryTransformParallel(PageRunCatalog catalog,
            Path outputDir,
            Path stagingDir, PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting,
            MergePlanner.BoundaryCandidates boundaryCandidates,
            StagingReconciliation retainedOriginals) throws IOException {
        List<PageRunSegmentDescriptor> segmentDescriptors = catalog.descriptors();
        List<Path> stagingSegments = catalog.paths();
        ParallelRangeMerge rangeMerge =
                new ParallelRangeMerge(run, mergePlanner, proofReaderFactory);
        // Clamp R to what the merge budget and the descriptor budget can actually carry over THIS many
        // staged segments, BEFORE sampling boundaries for a range count we would not honour. Past that
        // bound every range cascades and the parallel merge is slower than the serial one it replaced
        // -- silently, since the engagement counter still fires once per range. See
        // MergePlanner#effectiveRanges.
        int requestedRanges = config.mergeParallelism();
        MergePlanner.EffectiveRanges rangePlan =
                mergePlanner.effectiveRanges(requestedRanges, catalog);
        int desiredRanges = rangePlan.ranges();
        if (rangePlan.reason() != MergePlanner.ClampReason.NONE) {
            // WARN, not debug: the operator asked for something the run could not give them. Keep
            // the typed reason in both the log and metrics so a size-floor decline is never reported
            // as an unsplittable keyspace (and an fd failure is not mistaken for a memory cascade).
            log.warn("sort_merge_range_clamped requested={} effective={} segments={} "
                            + "reason={} merge_budget_bytes={}",
                    requestedRanges, desiredRanges, stagingSegments.size(),
                    rangePlan.reason().logValue(), config.mergeBudgetBytes());
            switch (rangePlan.reason()) {
                case BELOW_STAGED_FLOOR ->
                        metrics.recordStealReason("SORT", "merge_range_below_staged_floor");
                case FD_EXHAUSTED ->
                        metrics.recordStealReason("SORT", "merge_range_fd_exhausted");
                case FD_LIMITED ->
                        metrics.recordStealReason("SORT", "merge_range_fd_limited");
                case WOULD_CASCADE ->
                        metrics.recordStealReason("SORT", "merge_range_would_cascade");
                case NONE -> throw new AssertionError("unreachable unclamped range plan");
            }
        }
        if (desiredRanges <= 1) {
            // A size/budget/fd policy decline is not an unsplittable keyspace. That signal belongs
            // exclusively to the boundary sampler below.
            return null;
        }
        // Boundary sampling is this path's SERIAL fraction: new page-run staging reads its bounded
        // trailer samples; legacy or invalid extensions scan only the affected segments. Recorded to its own timer as well as
        // logged, because the run report is what an A/B actually reads -- folded into merge_ms this
        // term is invisible, and it is the one that does NOT shrink as R rises.
        long boundariesStartNanos = System.nanoTime();
        List<byte[]> boundaries = mergePlanner.boundaries(catalog, boundaryCandidates, desiredRanges);
        long boundariesNanos = System.nanoTime() - boundariesStartNanos;
        rangeTimer.recordBoundarySampling(boundariesNanos);
        log.info("sort_merge_boundaries segments={} ranges={} boundary_policy_requested={} duration_ms={}",
                stagingSegments.size(), boundaries == null ? 1 : boundaries.size() + 1,
                config.mergeBoundaryPolicy().configValue(), boundariesNanos / 1_000_000L);
        if (boundaries == null) {
            // Instrumentation (AGENTS.md "instrument every new algo path"): without this, a run that
            // ASKED for a parallel merge and silently got the serial one is indistinguishable in the
            // metrics from a run that never asked. merge_range_parallel firing means engaged;
            // this firing means requested-but-unsplittable; neither firing means never requested.
            metrics.recordStealReason("SORT", "merge_range_unsplittable");
            return null;   // keyspace unsplittable — use the serial path
        }
        mergePlanner.warnIfCascadePredicted(stagingSegments.size(), config.effectiveFanIn());
        // The whole parallel phase is merge-and-write; mark Phase.WRITING reachable once up front.
        // Unlike the serial path, the cascade passes are NOT behind us here: every range k-way-merges
        // all the staged segments and cascades whenever they outnumber its own fan-in, rewriting its
        // rows once per pass. So the staged rows are this phase's denominator only when no range can
        // cascade; otherwise the parallel merge reports work and no percentage, exactly as the serial
        // cascade does.
        onFinalPassStarting.onFinalPassStarting(
                segmentDescriptors.size()
                        <= mergePlanner.perRangeFanIn(boundaries.size() + 1, catalog));
        List<ParallelRangeWorker.Result> results =
                rangeMerge.run(catalog, stagingDir, boundaries, progressCallback);

        List<Path> tmpsInOrder = new ArrayList<>();
        List<SortedFileWriter> partsInOrder = new ArrayList<>();
        long totalRows = 0;
        long mergePasses = 0;
        long cascadedPasses = 0;
        long fastPathEmissions = 0;
        // results are in RANGE order, and ranges are contiguous and ascending, so concatenating each
        // range's parts in its own write order gives the output's global key order — which is exactly
        // the roll sequence the completeness stamp describes.
        for (ParallelRangeWorker.Result rr : results) {
            tmpsInOrder.addAll(rr.tmpParts());
            partsInOrder.addAll(rr.writers());
            totalRows += rr.rows();
            mergePasses += rr.mergePasses();
            cascadedPasses += rr.cascadedPasses();
            fastPathEmissions += rr.fastPathEmissions();
        }

        DatasetPublisher.PendingParts pending = datasetPublisher.parallelParts(
                outputDir, stagingDir, tmpsInOrder, partsInOrder);
        datasetPublisher.publish(pending, totalRows, publishListener, stagingSegments,
                retainedOriginals, List.of());
        return new SortTransformResult(pending.finalFiles(), totalRows,
                mergePasses, cascadedPasses, fastPathEmissions, results.size());
    }

}
