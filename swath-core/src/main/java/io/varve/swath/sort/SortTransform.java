/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The batch merge/publish step of {@code --sort}: cascaded {@link KWayMerge} over staged {@code
 * .pageseg} page-run segments into final sorted Parquet, written to {@code *.tmp} and renamed in key order, with
 * staging deleted only after the {@link PublishListener} fires. The full publish/resume state
 * machine (manifest-last commit, idempotent re-entry, stale-tmp/stale-final cleanup) is
 * {@code docs/internals/contracts.md} §6; this class's own re-entry sweep-scope safety proof lives at
 * {@link #cleanStaleFinals}, not here.
 *
 * <p>Multi-file output rolls the sorted stream into range-disjoint files named {@code
 * part-00000.parquet}, {@code part-00001.parquet}, … (lexical order == key order), starting a
 * fresh file each time the current one reaches {@code finalFileBytes} (default ~1&nbsp;GiB;
 * {@code docs/internals/contracts.md} §7).
 *
 * <p>The {@link KWayMerge} pass width is a runtime-clamped fan-in
 * ({@link MergeFanInPlanner#clampedMergeFanIn}), never the raw {@code fan-in} knob: the MIN of the
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

    private static final String FINAL_PREFIX = "part-";
    private static final String FINAL_SUFFIX = ".parquet";
    private static final String TMP_SUFFIX = ".tmp";
    /**
     * The page-run staging/intermediate file extension — distinct from the FINAL output's
     * {@code .parquet}. Every producer and cascade intermediate stamps this suffix; transform input
     * is preflighted before any stale-output sweep so an unsupported staging format cannot trigger
     * destructive re-entry cleanup.
     */
    static final String SEGMENT_SUFFIX = ".pageseg";

    // The sort-run inputs, held whole so the quintet threads straight through to ParallelRangeMerge
    // without re-listing loose positional params; the individual fields below are its hot-path aliases.
    private final SortRun run;
    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final boolean identityVerifiedWideSweep;
    private final boolean pageFrontierEnabled;
    // Per-range merge-latency seam for the parallel path (NO_OP off that path).
    private final RangeMergeTimer rangeTimer;
    private final IntSupplier softFdLimitSupplier;
    // The "how wide can this merge pass be" cluster (static budget estimate + fd/record-size
    // runtime clamps + their observability) lives in MergeFanInPlanner, not here.
    private final MergeFanInPlanner fanInPlanner;

    /**
     * The safe default for every caller without an identity-verified ownership guard over {@code
     * outputDir} (e.g. {@link CaptureSorter}'s sort-fixture path, and every direct test caller):
     * {@code identityVerifiedWideSweep=false} — see {@link #cleanStaleFinals} for the sweep-scope
     * safety proof — and no range-merge timer ({@link RangeMergeTimer#NO_OP}).
     */
    public SortTransform(SortRun run) {
        this(run, false, RangeMergeTimer.NO_OP);
    }

    /**
     * The full public constructor.
     *
     * <p>{@code identityVerifiedWideSweep}: pass {@code true} only when the caller has already
     * identity-verified {@code outputDir} as belonging to THIS run before ever reaching this
     * transform — see {@link #cleanStaleFinals} for the sweep-scope safety proof and which caller
     * qualifies.
     *
     * <p>{@code rangeTimer}: the per-range merge-latency seam ({@code
     * swath.sort.merge.range.latency}) for the parallel range-merge path. {@code
     * ListRunner} wires the live {@code RunMetrics}; every other caller (and the serial path) leaves
     * it {@link RangeMergeTimer#NO_OP}.
     */
    public SortTransform(SortRun run, boolean identityVerifiedWideSweep, RangeMergeTimer rangeTimer) {
        this(run, identityVerifiedWideSweep, rangeTimer, MergeFdBudget::softOpenFileLimit, true);
    }

    /**
     * As {@link #SortTransform(SortRun, boolean, RangeMergeTimer)}, plus {@code softFdLimitSupplier}:
     * the process SOFT open-file limit source for the runtime merge-entry fd clamp. Package-private
     * test seam — production always uses {@link MergeFdBudget#softOpenFileLimit()} (the three-arg
     * constructor); tests inject a fixed value so the fd clamp engages (or is proven not to)
     * deterministically, independent of the real ulimit.
     */
    SortTransform(SortRun run, boolean identityVerifiedWideSweep, RangeMergeTimer rangeTimer,
                  IntSupplier softFdLimitSupplier) {
        this(run, identityVerifiedWideSweep, rangeTimer, softFdLimitSupplier, true);
    }

    private SortTransform(SortRun run, boolean identityVerifiedWideSweep, RangeMergeTimer rangeTimer,
                          IntSupplier softFdLimitSupplier, boolean pageFrontierEnabled) {
        this.run = run;
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.identityVerifiedWideSweep = identityVerifiedWideSweep;
        this.pageFrontierEnabled = pageFrontierEnabled;
        this.rangeTimer = rangeTimer;
        this.softFdLimitSupplier = softFdLimitSupplier;
        this.fanInPlanner = new MergeFanInPlanner(config, metrics, softFdLimitSupplier);
    }

    /**
     * Build the bounded entry-stream variant for arbitrary pre-existing captures. Their independently
     * sorted chunks can overlap across many consecutive pages, unlike the mostly range-disjoint live
     * listing runs the page-frontier fast path exploits. The storage format and cascade framework stay
     * identical; only frontier/range routing is disabled.
     */
    static SortTransform forArbitraryRuns(SortRun run) {
        return new SortTransform(run, false, RangeMergeTimer.NO_OP,
                MergeFdBudget::softOpenFileLimit, false);
    }

    /**
     * Merge {@code stagingSegments} into the final sorted output under {@code outputDir}, using
     * {@code stagingDir} for cascade intermediates. {@code publishListener} fires after the renames
     * and before staging deletion (the manifest-last commit point). No merge-progress callback —
     * see the overload below for the production (§3.2) path.
     */
    public SortTransformResult transform(List<Path> stagingSegments, Path outputDir, Path stagingDir,
                                         PublishListener publishListener) throws IOException {
        return transform(stagingSegments, outputDir, stagingDir, publishListener, units -> { },
                FinalPassListener.NO_OP);
    }

    /**
     * Same as {@link #transform(List, Path, Path, PublishListener)}, plus {@code progressCallback}
     * (§3.2): invoked with the row count of each completed batch — {@code swath.progress.units}'
     * merge-phase feed, wired by {@code ListRunner} to {@code RunMetrics.recordProgress}. Batched
     * ({@link KWayMerge#PROGRESS_BATCH_ROWS}), not per-row, and threaded through <em>every</em> merge
     * pass, not just the final one: {@link KWayMerge#merge(List, LongConsumer)} advances it as each
     * intermediate cascade pass drains rows, and
     * {@link RolledPartWriter#drain} advances it again as the final streaming pass writes rolled output. A
     * cascade's progress total is therefore rows-per-pass, not a single row-count total — intended:
     * progress is a monotonic units-of-work counter, not required to equal total rows for a
     * multi-pass merge. No final-pass-starting hook — see the overload below.
     */
    public SortTransformResult transform(List<Path> stagingSegments, Path outputDir, Path stagingDir,
                                         PublishListener publishListener, LongConsumer progressCallback)
            throws IOException {
        return transform(stagingSegments, outputDir, stagingDir, publishListener, progressCallback,
                FinalPassListener.NO_OP);
    }

    /**
     * Same as {@link #transform(List, Path, Path, PublishListener, LongConsumer)}, plus {@code
     * onFinalPassStarting} (see {@link FinalPassListener}): run once, right before the merge starts
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
        requirePageRunSegments(stagingSegments);
        // Validate and retain every trailer before deleting any disposable working file. Fan-in and
        // range planning consume these descriptors; an unreadable segment is not an optional memory
        // refinement and must fail at kickoff while the prior output and working evidence are intact.
        List<PageRunSegmentDescriptor> segmentDescriptors =
                PageRunSegmentDescriptor.readAll(stagingSegments);
        // See cleanStaleTmp/cleanStaleFinals/cleanStaleMergeIntermediates/cleanStalePrangeTmp
        // below for what each sweep removes and why. Disposable working files are cleared before
        // work; prior published finals remain until their complete replacements are ready.
        cleanStaleTmp(stagingDir);
        cleanStaleTmp(outputDir);
        cleanStaleMergeIntermediates(stagingDir);
        cleanStalePrangeTmp(stagingDir);

        // When the configured/default swath.sort.merge-parallelism survives the staged-size,
        // memory, and fd gates, split the keyspace into
        // contiguous ordered ranges and merge them concurrently (ParallelRangeMerge). A degenerate
        // keyspace (fewer than two distinct sample keys) returns null and falls through to the serial
        // path below — so both merge-parallelism=1 and any unsplittable keyspace take the
        // EXACT untouched serial code, byte-for-byte identical.
        // Parallel ranges consume the same page-run frontier as the serial path. The default is
        // core-derived and capped; effectiveRanges() may still route an ordinary run to the serial
        // path, and the completeness stamp makes multi-file parallel output self-describing.
        if (config.mergeParallelism() > 1 && pageFrontierEnabled) {
            SortTransformResult parallel = tryTransformParallel(segmentDescriptors, outputDir, stagingDir,
                    publishListener, progressCallback, onFinalPassStarting);
            if (parallel != null) {
                return parallel;
            }
        } else if (config.mergeParallelism() > 1) {
            metrics.recordStealReason("SORT", "merge_range_frontier_disabled");
        }

        List<Path> intermediates = new ArrayList<>();
        SortedFileWriterFactory outputSequence = finalWriterFactory.forOutputSequence();
        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(comparator, hook, metrics, config.segmentCodec());
        KWayMerge.SegmentIo<Path> io = segmentIo(segmentWriter, stagingDir, intermediates);
        // Fan-in: see the class javadoc for the runtime-clamp policy. plan() computes it and,
        // as a side effect, fires the cascade-predicted warning + clamp metrics once at kickoff.
        int runtimeFanIn = fanInPlanner.plan(segmentDescriptors);
        KWayMerge<Path> merge = new KWayMerge<>(comparator, runtimeFanIn, io, hook, metrics);

        List<Path> finalFiles = new ArrayList<>();
        List<Path> tmpFiles = new ArrayList<>();
        // Every writer ever opened, including rolled writers already closed by drain's private
        // bounded open-writer list. Retain these objects because close() publishes immutable
        // FinalPartMetadata that finalParts() hands to the manifest listener after all renames.
        List<SortedFileWriter> finalWriters = new ArrayList<>();
        long totalRows;
        try (SortedCursor merged = merge.merge(stagingSegments, progressCallback)) {
            // merge() above already ran every cascade pass to completion before returning this
            // cursor (see KWayMerge#merge) — so by this point only the final streaming pass remains,
            // and it drains exactly the staged rows once: an honest completion denominator.
            onFinalPassStarting.onFinalPassStarting(true);
            totalRows = RolledPartWriter.drain(merged, config.finalFileBytes(),
                    () -> openNextFile(outputDir, stagingDir, finalFiles, tmpFiles, finalWriters,
                            outputSequence),
                    true, progressCallback, metrics);
        }
        // Merge engagement counts (read after the cursor is fully drained + closed above, so the
        // final streaming pass's fast-path total has accumulated) — surfaced for the run's meters/summary.
        long mergePasses = merge.mergePasses();
        long cascadedPasses = merge.cascadedPasses();
        long fastPathEmissions = merge.fastPathEmissions();

        // The complete replacement is now durable under tmp names. Keep prior finals intact through
        // every staging read and output close; only now remove stale/excess parts and rename the new
        // set into place. This avoids destroying a recoverable published output when a segment has
        // magic-preserving body corruption that the O(1) trailer preflight cannot prove absent.
        cleanStaleFinals(outputDir);
        for (int i = 0; i < finalFiles.size(); i++) {
            atomicRename(tmpFiles.get(i), finalFiles.get(i));
        }
        Durability.directory(outputDir);

        // Publish commit point (manifest.json is written here) — AFTER renames, BEFORE staging delete.
        publishListener.onPublished(finalParts(finalFiles, finalWriters), totalRows);

        // Staging is internal working state — delete what we own (originals + any intermediates).
        for (Path p : stagingSegments) {
            Files.deleteIfExists(p);
        }
        for (Path p : intermediates) {
            Files.deleteIfExists(p);
        }
        // "Staging dir cleaned on successful publish": remove the now-empty staging dir
        // itself, not just its contents — but only if nothing unexpected is left in it (never a
        // recursive wipe of foreign content the sorter doesn't own).
        tryDeleteEmptyStagingDir(stagingDir);
        return new SortTransformResult(List.copyOf(finalFiles), totalRows,
                mergePasses, cascadedPasses, fastPathEmissions, 1);
    }

    /**
     * The concurrent range-merge path. Returns {@code null} for a
     * degenerate keyspace that cannot be split into more than one range, so the caller falls back to
     * the untouched serial merge. Otherwise it merges the ranges concurrently
     * ({@link ParallelRangeMerge}), then does the SAME serial publish as the serial path — rename the
     * ordered range parts into one ascending {@code part-00000.parquet}… sequence (filename order ==
     * key order == global sort), fire the publish listener, delete staging. Immediately before that
     * rename it writes the multi-file completeness stamp ({@code file_index} 1..N, one
     * {@code file_final} on N), which is the point at which the global part order is first known.
     */
    private SortTransformResult tryTransformParallel(List<PageRunSegmentDescriptor> segmentDescriptors,
            Path outputDir,
            Path stagingDir, PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        List<Path> stagingSegments = PageRunSegmentDescriptor.paths(segmentDescriptors);
        ParallelRangeMerge rangeMerge =
                new ParallelRangeMerge(run, rangeTimer, softFdLimitSupplier, segmentDescriptors);
        // Clamp R to what the merge budget and the descriptor budget can actually carry over THIS many
        // staged segments, BEFORE sampling boundaries for a range count we would not honour. Past that
        // bound every range cascades and the parallel merge is slower than the serial one it replaced
        // -- silently, since the engagement counter still fires once per range. See
        // ParallelRangeMerge#effectiveRanges.
        int requestedRanges = config.mergeParallelism();
        ParallelRangeMerge.EffectiveRanges rangePlan =
                rangeMerge.effectiveRangesForDescriptors(requestedRanges, segmentDescriptors);
        int desiredRanges = rangePlan.ranges();
        if (rangePlan.reason() != ParallelRangeMerge.ClampReason.NONE) {
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
        List<byte[]> boundaries = ParallelRangeMerge.boundariesForDescriptors(
                segmentDescriptors, desiredRanges, metrics);
        long boundariesNanos = System.nanoTime() - boundariesStartNanos;
        rangeTimer.recordBoundarySampling(boundariesNanos);
        log.info("sort_merge_boundaries segments={} ranges={} duration_ms={}",
                stagingSegments.size(), boundaries == null ? 1 : boundaries.size() + 1,
                boundariesNanos / 1_000_000L);
        if (boundaries == null) {
            // Instrumentation (AGENTS.md "instrument every new algo path"): without this, a run that
            // ASKED for a parallel merge and silently got the serial one is indistinguishable in the
            // metrics from a run that never asked. merge_range_parallel firing means engaged;
            // this firing means requested-but-unsplittable; neither firing means never requested.
            metrics.recordStealReason("SORT", "merge_range_unsplittable");
            return null;   // keyspace unsplittable — use the serial path
        }
        fanInPlanner.warnIfCascadePredicted(stagingSegments.size(), config.effectiveFanIn());
        // The whole parallel phase is merge-and-write; mark Phase.WRITING reachable once up front.
        // Unlike the serial path, the cascade passes are NOT behind us here: every range k-way-merges
        // all the staged segments and cascades whenever they outnumber its own fan-in, rewriting its
        // rows once per pass. So the staged rows are this phase's denominator only when no range can
        // cascade; otherwise the parallel merge reports work and no percentage, exactly as the serial
        // cascade does.
        onFinalPassStarting.onFinalPassStarting(
                stagingSegments.size() <= rangeMerge.perRangeFanIn(boundaries.size() + 1, stagingSegments));
        List<ParallelRangeMerge.RangeResult> results =
                rangeMerge.run(stagingSegments, stagingDir, boundaries, progressCallback);

        List<Path> tmpsInOrder = new ArrayList<>();
        List<SortedFileWriter> partsInOrder = new ArrayList<>();
        long totalRows = 0;
        long mergePasses = 0;
        long cascadedPasses = 0;
        long fastPathEmissions = 0;
        // results are in RANGE order, and ranges are contiguous and ascending, so concatenating each
        // range's parts in its own write order gives the output's global key order — which is exactly
        // the roll sequence the completeness stamp describes.
        for (ParallelRangeMerge.RangeResult rr : results) {
            tmpsInOrder.addAll(rr.tmpParts());
            partsInOrder.addAll(rr.writers());
            totalRows += rr.rows();
            mergePasses += rr.mergePasses();
            cascadedPasses += rr.cascadedPasses();
            fastPathEmissions += rr.fastPathEmissions();
        }

        // THE COMPLETENESS STAMP. Every part is still open precisely so this can happen: only here is
        // the full ordered part list known, so only here can a part be told its 1-based position in the
        // global sequence and the last one be marked final. Closing is what writes the footer, so the
        // assignment must precede it -- and it must precede the rename too, since a renamed file is
        // published.
        //
        // What protects a FAILED publish is not the stamp but the rename: these are `.tmp` files in
        // stagingDir, and nothing here is visible to a reader until the rename loop below moves it to
        // outputDir. So a part abandoned mid-close may well carry a full stamp, including file_final --
        // it is simply never published, and the next run's cleanStalePrangeTmp sweeps it. The
        // guarantee is "never renamed => never seen", not "never stamped".
        try {
            for (int i = 0; i < partsInOrder.size(); i++) {
                partsInOrder.get(i).setFileIndex(i + 1);
            }
            if (!partsInOrder.isEmpty()) {
                partsInOrder.get(partsInOrder.size() - 1).markFinal();
            }
            RolledPartWriter.closeInOrder(partsInOrder);
            partsInOrder.clear();
        } catch (IOException | RuntimeException e) {
            // Release the rest without letting a second failure replace the first: the original close
            // error is what says why the publish failed.
            try {
                RolledPartWriter.closeQuietly(partsInOrder);
            } catch (IOException | RuntimeException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }

        List<Path> finalFiles = new ArrayList<>();
        List<SortedFileWriter> finalWriters = new ArrayList<>();
        if (tmpsInOrder.isEmpty()) {
            // Empty listing: publish one valid, self-describing empty sorted file (matches the serial
            // path). This single file legitimately carries the completeness stamp.
            List<Path> tf = new ArrayList<>();
            SortedFileWriter writer = openNextFile(outputDir, stagingDir, finalFiles, tf, finalWriters,
                    finalWriterFactory.forOutputSequence());
            writer.markFinal();
            writer.close();
            cleanStaleFinals(outputDir);
            atomicRename(tf.get(0), finalFiles.get(0));
        } else {
            cleanStaleFinals(outputDir);
            int filenameIndex = 0;
            for (Path tmp : tmpsInOrder) {
                String name = finalPartName(filenameIndex++);
                Path finalPath = outputDir.resolve(name);
                atomicRename(tmp, finalPath);
                finalFiles.add(finalPath);
            }
            for (ParallelRangeMerge.RangeResult rr : results) {
                finalWriters.addAll(rr.writers());
            }
        }
        Durability.directory(outputDir);
        publishListener.onPublished(finalParts(finalFiles, finalWriters), totalRows);
        for (Path p : stagingSegments) {
            Files.deleteIfExists(p);
        }
        tryDeleteEmptyStagingDir(stagingDir);
        return new SortTransformResult(List.copyOf(finalFiles), totalRows,
                mergePasses, cascadedPasses, fastPathEmissions, results.size());
    }

    /** Delete {@code stagingDir} iff it is now empty; foreign content is logged and left in place. */
    private static void tryDeleteEmptyStagingDir(Path stagingDir) {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try (DirectoryStream<Path> remaining = Files.newDirectoryStream(stagingDir)) {
            if (remaining.iterator().hasNext()) {
                log.info("sort staging dir left in place: unexpected content remains in {}", stagingDir);
                return;
            }
        } catch (IOException e) {
            log.debug("failed to list sort staging dir {} before removal; leaving it in place", stagingDir, e);
            return;
        }
        try {
            Files.delete(stagingDir);
        } catch (IOException e) {
            log.debug("failed to remove empty sort staging dir {}", stagingDir, e);
        }
    }

    private SortedFileWriter openNextFile(Path outputDir, Path stagingDir, List<Path> finalFiles,
                                          List<Path> tmpFiles, List<SortedFileWriter> finalWriters,
                                          SortedFileWriterFactory outputSequence) throws IOException {
        int filenameIndex = finalFiles.size();
        String name = finalPartName(filenameIndex);
        Path finalPath = outputDir.resolve(name);
        // Write the tmp OUTSIDE data/ — into the sibling staging dir on the same
        // filesystem — and atomically rename it INTO data/ (see transform()). data/ (outputDir) thus
        // only ever holds finalized *.parquet; a crash never strands a *.tmp inside the pure-parquet dir.
        Path tmpPath = stagingDir.resolve(name + TMP_SUFFIX);
        // The public filename follows Spark/Hadoop's zero-based part ordinal. The footer's
        // file_index remains a 1-based position so existing sorted fixtures retain their stamp
        // semantics and replay's completeness check remains backward-compatible.
        SortedFileWriter writer = outputSequence.create(tmpPath, filenameIndex + 1);
        finalFiles.add(finalPath);
        tmpFiles.add(tmpPath);
        finalWriters.add(writer);
        return writer;
    }

    /** Spark/Hadoop-style public part name with a dense, zero-based ordinal. */
    private static String finalPartName(int filenameIndex) {
        return String.format("%s%05d%s", FINAL_PREFIX, filenameIndex, FINAL_SUFFIX);
    }

    private static List<FinalPart> finalParts(List<Path> paths, List<SortedFileWriter> writers) {
        if (paths.size() != writers.size()) {
            throw new IllegalStateException("final part path/writer count mismatch: paths="
                    + paths.size() + " writers=" + writers.size());
        }
        List<FinalPart> parts = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            // This handoff runs only after all closes and renames succeeded. A partial close never
            // reaches the publish listener and therefore cannot make its metadata trustworthy.
            parts.add(new FinalPart(paths.get(i), writers.get(i).finalMetadata()));
        }
        return List.copyOf(parts);
    }

    private KWayMerge.SegmentIo<Path> segmentIo(PageRunSegmentWriter segmentWriter, Path stagingDir,
                                                List<Path> intermediates) {
        int[] seq = {0};
        return new KWayMerge.SegmentIo<>() {
            @Override
            public EntryStream open(Path segment) throws IOException {
                // Generic entry-stream fallback retained for KWayMerge's storage seam. Production
                // page-run groups take the decode-free frontier below.
                return new PageRunSegmentReader(segment, comparator, metrics);
            }

            @Override
            public Path writeIntermediate(SortedCursor sorted) throws IOException {
                Path dest = stagingDir.resolve("merge-" + (seq[0]++) + SEGMENT_SUFFIX);
                segmentWriter.writeIntermediate(sorted, dest);
                intermediates.add(dest);
                return dest;
            }

            @Override
            public void delete(Path segment) throws IOException {
                Files.deleteIfExists(segment);
            }

            @Override
            public boolean supportsPageFrontier(Path segment) {
                return pageFrontierEnabled;
            }

            @Override
            public PageFrontierStream openFrontier(Path segment) throws IOException {
                // The reader carries the live metrics so an intra-segment minKey REGRESSION
                // (the merger's one unverified precondition) is counted — SORT.page_run_min_regression —
                // before it fails the run as segment corruption, instead of misordering silently.
                return new PageFrontierReader(segment, metrics);
            }
        };
    }

    /** Whether {@code segment} has the required page-run staging/intermediate suffix.
     *  Package-private: {@link MergeFanInPlanner} shares this instead of duplicating the
     *  suffix check. */
    static boolean isPageRunSegment(Path segment) {
        return segment.getFileName().toString().endsWith(SEGMENT_SUFFIX);
    }

    /** Reject unsupported staging before cleanup can remove any prior working files. */
    private static void requirePageRunSegments(List<Path> segments) throws IOException {
        for (Path segment : segments) {
            if (!isPageRunSegment(segment)) {
                throw new IllegalArgumentException(
                        "unsupported sort staging segment (expected " + SEGMENT_SUFFIX + "): " + segment);
            }
        }
    }

    private static void atomicRename(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Remove any {@code part-*.parquet.tmp} left by a crash mid-publish, so a re-run is clean.
     * Called for BOTH the staging dir (the tmp's home) and {@code data/} (in case an existing dataset
     * written under an older layout stranded a tmp there), so a leftover {@code *.tmp} never survives
     * into published {@code data/}.
     */
    private static void cleanStaleTmp(Path dir) throws IOException {
        sweep(dir, FINAL_PREFIX + "*" + FINAL_SUFFIX + TMP_SUFFIX);
    }

    /**
     * Remove any {@code prange-*.parquet.tmp} left by a crashed prior {@link ParallelRangeMerge}
     * attempt — those range-local tmp parts live only in {@code
     * stagingDir} (never {@code outputDir}; see {@link ParallelRangeMerge#openRangePart}), so unlike
     * {@link #cleanStaleTmp} this only needs to sweep the one directory. Without this, a crash after
     * the parallel path produced tmp range parts but before publish would strand them forever — {@link
     * #cleanStaleTmp} does not match the {@code prange-} naming, and a subsequent SUCCESSFUL run
     * would then find {@code stagingDir} non-empty and skip {@link #tryDeleteEmptyStagingDir},
     * violating {@code docs/internals/contracts.md} §6 ("staging dir cleaned on successful publish").
     */
    private static void cleanStalePrangeTmp(Path stagingDir) throws IOException {
        sweep(stagingDir, "prange-*" + FINAL_SUFFIX + TMP_SUFFIX);
    }

    /**
     * Remove stale FINAL files already in {@code outputDir}, after the complete replacement has been
     * generated and closed under tmp names. These are an abandoned prior merge attempt's finals,
     * orphaned outside any manifest. Without this, a retry that produces FEWER final files
     * than the abandoned attempt (a changed roll knob, or a different segment mix after a partial
     * resume) would leave the extras lying around outside the new manifest.
     *
     * <p><b>Sweep scope</b>, set by {@link #identityVerifiedWideSweep} per caller:
     * <ul>
     *   <li>{@code true} — sweeps ALL {@code data/*.parquet}. Safe only because the caller
     *       ({@code ListCommand#isPublishedByThisRun}, via {@link Manifest#readIdentity}) has
     *       already refused to treat a foreign dataset's manifest as this run's own before reaching
     *       the merge-pending branch that calls {@link #transform} — so by the time this sweep
     *       runs, {@code outputDir} can only hold this run's own abandoned prior attempt or
     *       nothing.</li>
     *   <li>{@code false} (default) — sweeps ONLY this transform's own {@code part-*.parquet}
     *       naming. {@link CaptureSorter}'s sort-fixture path has no such ownership guard (a
     *       user-supplied {@code --output} dir may hold unrelated {@code *.parquet} this engine
     *       never created), so a wide sweep there would unrecoverably delete foreign content;
     *       narrowing to the engine's own naming still removes every abandoned final, which is
     *       always {@code part-*}.</li>
     * </ul>
     */
    private void cleanStaleFinals(Path outputDir) throws IOException {
        String glob = identityVerifiedWideSweep ? "*" + FINAL_SUFFIX : FINAL_PREFIX + "*" + FINAL_SUFFIX;
        sweep(outputDir, stale -> log.info("sweeping stale sorted output before replacement publish: {}", stale), glob);
    }

    /**
     * Remove any {@code merge-*} cascade intermediates left by a crashed prior merge. Sweeps BOTH
     * the page-run {@code merge-*.pageseg} this transform writes and legacy/planted
     * {@code merge-*.parquet} debris (older datasets and the SORT-RESUME tests plant the latter).
     */
    private static void cleanStaleMergeIntermediates(Path stagingDir) throws IOException {
        // "merge-*" covers the serial path's own intermediates AND the parallel path's per-range
        // "merge-r<range>-<n>" ones, in both staging formats.
        sweep(stagingDir, "merge-*" + SEGMENT_SUFFIX, "merge-*" + FINAL_SUFFIX);
    }

    /** Delete every file matching any of {@code globs} in {@code dir}; a no-op if {@code dir} doesn't
     *  exist. Shared shape behind every {@code cleanStale*} sweep above. */
    private static void sweep(Path dir, String... globs) throws IOException {
        sweep(dir, stale -> { }, globs);
    }

    /** As {@link #sweep(Path, String...)}, plus a hook run just before each file is deleted (e.g.
     *  {@link #cleanStaleFinals}'s per-file log line). */
    private static void sweep(Path dir, Consumer<Path> beforeDelete, String... globs) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        for (String glob : globs) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
                for (Path stale : stream) {
                    beforeDelete.accept(stale);
                    Files.deleteIfExists(stale);
                }
            }
        }
    }
}
