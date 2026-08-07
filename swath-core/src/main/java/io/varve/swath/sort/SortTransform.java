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
 * .pageseg} page-run segments (or, for {@link CaptureSorter}'s fixture path, staged columnar
 * Parquet) into final sorted Parquet, written to {@code *.tmp} and renamed in key order, with
 * staging deleted only after the {@link PublishListener} fires. The full publish/resume state
 * machine (manifest-last commit, idempotent re-entry, stale-tmp/stale-final cleanup) is
 * {@code docs/internals/contracts.md} §6; this class's own re-entry sweep-scope safety proof lives at
 * {@link #cleanStaleFinals}, not here.
 *
 * <p>Multi-file output rolls the sorted stream into range-disjoint files named {@code
 * part-00001.parquet}, {@code part-00002.parquet}, … (lexical order == key order), starting a
 * fresh file each time the current one reaches {@code finalFileBytes} (default ~1&nbsp;GiB;
 * {@code docs/internals/contracts.md} §7).
 *
 * <p>The {@link KWayMerge} pass width is a runtime-clamped fan-in
 * ({@link MergeFanInPlanner#clampedMergeFanIn}), never the raw {@code fan-in} knob: the MIN of the
 * static budget ({@link SortConfig#effectiveFanIn()}), the process fd budget
 * ({@link MergeFdBudget}), and, for page-run input, the exact per-segment memory bound read from
 * each segment's trailer — so a single-pass open never exceeds the fd limit or the merge-memory
 * budget regardless of segment count (I11). A clamp that forces the fan-in below the segment count
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
     * {@code .parquet}. The listing lane ({@link SortLane}) and this transform's cascade
     * {@code writeIntermediate} both stamp it, and {@code segmentIo}'s {@code open} dispatches on it:
     * a {@code .pageseg} input is read by {@link PageRunSegmentReader}, anything else (a legacy
     * {@code .parquet} staging segment from {@link CaptureSorter}'s fixture path, or the off-by-default
     * parallel path's own {@code merge-r*.parquet} intermediates) by the columnar {@link SegmentReader}.
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
    // Row-group skip: per-range merge-latency seam for the parallel path (NO_OP off that path).
    private final RangeMergeTimer rangeTimer;
    // The "how wide can this merge pass be" cluster (static budget bound + the fd/exact-
    // memory runtime clamps + their observability) lives in MergeFanInPlanner, not here.
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
     * <p>{@code rangeTimer} (row-group skip): the per-range merge-latency seam ({@code
     * swath.sort.merge.range.latency}) for the off-by-default parallel range-merge path. {@code
     * ListRunner} wires the live {@code RunMetrics}; every other caller (and the serial path) leaves
     * it {@link RangeMergeTimer#NO_OP}.
     */
    public SortTransform(SortRun run, boolean identityVerifiedWideSweep, RangeMergeTimer rangeTimer) {
        this(run, identityVerifiedWideSweep, rangeTimer, MergeFdBudget::softOpenFileLimit);
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
        this.run = run;
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.identityVerifiedWideSweep = identityVerifiedWideSweep;
        this.rangeTimer = rangeTimer;
        this.fanInPlanner = new MergeFanInPlanner(config, metrics, softFdLimitSupplier);
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
        // See cleanStaleTmp/cleanStaleFinals/cleanStaleMergeIntermediates/cleanStalePrangeTmp
        // below for what each sweep removes and why; all four run before any new work below so a
        // crashed prior attempt never blocks or contaminates this one.
        cleanStaleTmp(stagingDir);
        cleanStaleTmp(outputDir);
        cleanStaleFinals(outputDir);
        cleanStaleMergeIntermediates(stagingDir);
        cleanStalePrangeTmp(stagingDir);

        // Off by default: when swath.sort.merge-parallelism > 1, split the keyspace into
        // contiguous ordered ranges and merge them concurrently (ParallelRangeMerge). A degenerate
        // keyspace (fewer than two distinct sample keys) returns null and falls through to the serial
        // path below — so both merge-parallelism=1 (the default) and any unsplittable keyspace take the
        // EXACT untouched serial code, byte-for-byte identical.
        // The parallel range-merge path now reads BOTH staging formats: columnar Parquet (via
        // SortedFileIndex + a row-group-scoped SegmentReader) and page-run (via a page-scoped
        // RangeScopedPageFrontier feeding the ordinary PageRunSegmentReader). Page-run is the live
        // listing lane's format, so before this the knob could not engage on a real listing run at
        // all -- it silently fell back to the serial merge, which made every A/B of the knob on a
        // live bucket a no-op. Still off by default: mergeParallelism() > 1 is opt-in, and the
        // decision to SHIP multi-file sorted output produced by a concurrent merge stays reserved
        // (SortConfig#DEFAULT_MERGE_PARALLELISM), not least because of the completeness-stamp gap.
        if (config.mergeParallelism() > 1) {
            SortTransformResult parallel = tryTransformParallel(stagingSegments, outputDir, stagingDir,
                    publishListener, progressCallback, onFinalPassStarting);
            if (parallel != null) {
                return parallel;
            }
        }

        List<Path> intermediates = new ArrayList<>();
        PageRunSegmentWriter segmentWriter = new PageRunSegmentWriter(comparator, hook, metrics, config.segmentCodec());
        KWayMerge.SegmentIo<Path> io = segmentIo(segmentWriter, stagingDir, intermediates);
        // Fan-in: see the class javadoc for the runtime-clamp proof (I11). plan() computes it and,
        // as a side effect, fires the cascade-predicted warning + clamp metrics once at kickoff.
        int runtimeFanIn = fanInPlanner.plan(stagingSegments);
        KWayMerge<Path> merge = new KWayMerge<>(comparator, runtimeFanIn, io, hook, metrics);

        List<Path> finalFiles = new ArrayList<>();
        List<Path> tmpFiles = new ArrayList<>();
        long totalRows;
        try (SortedCursor merged = merge.merge(stagingSegments, progressCallback)) {
            // merge() above already ran every cascade pass to completion before returning this
            // cursor (see KWayMerge#merge) — so by this point only the final streaming pass remains,
            // and it drains exactly the staged rows once: an honest completion denominator.
            onFinalPassStarting.onFinalPassStarting(true);
            totalRows = RolledPartWriter.drain(merged, config.finalFileBytes(),
                    () -> openNextFile(outputDir, stagingDir, finalFiles, tmpFiles), true, progressCallback);
        }
        // Merge engagement counts (read after the cursor is fully drained + closed above, so the
        // final streaming pass's fast-path total has accumulated) — surfaced for the run's meters/summary.
        long mergePasses = merge.mergePasses();
        long cascadedPasses = merge.cascadedPasses();
        long fastPathEmissions = merge.fastPathEmissions();

        // Rename each final file into place in name (== key) order, then a directory barrier.
        for (int i = 0; i < finalFiles.size(); i++) {
            atomicRename(tmpFiles.get(i), finalFiles.get(i));
        }
        Durability.directory(outputDir);

        // Publish commit point (manifest.json is written here) — AFTER renames, BEFORE staging delete.
        publishListener.onPublished(List.copyOf(finalFiles), totalRows);

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
                mergePasses, cascadedPasses, fastPathEmissions);
    }

    /**
     * Off by default: the concurrent range-merge path. Returns {@code null} for a
     * degenerate keyspace that cannot be split into more than one range, so the caller falls back to
     * the untouched serial merge. Otherwise it merges the ranges concurrently
     * ({@link ParallelRangeMerge}), then does the SAME serial publish as the serial path — rename the
     * ordered range parts into one ascending {@code part-00001.parquet}… sequence (filename order ==
     * key order == global sort), fire the publish listener, delete staging. The multi-file completeness
     * stamp ({@code file_index}/{@code file_final}) is not implemented on this path.
     */
    private SortTransformResult tryTransformParallel(List<Path> stagingSegments, Path outputDir,
            Path stagingDir, PublishListener publishListener, LongConsumer progressCallback,
            FinalPassListener onFinalPassStarting) throws IOException {
        // Boundary sampling is this path's new SERIAL fraction: on page-run staging it walks every
        // page's frontier across every segment before any range starts. Timed on its own so an A/B can
        // see it separately from the merge it precedes (it is the first thing to optimise -- by
        // stride-sampling, or by reusing the listing phase's own keyspace partition -- if it is large).
        long boundariesStartNanos = System.nanoTime();
        List<byte[]> boundaries =
                ParallelRangeMerge.boundaries(stagingSegments, config.mergeParallelism(), metrics);
        log.info("sort_merge_boundaries segments={} ranges={} duration_ms={}",
                stagingSegments.size(), boundaries == null ? 1 : boundaries.size() + 1,
                (System.nanoTime() - boundariesStartNanos) / 1_000_000L);
        if (boundaries == null) {
            return null;   // keyspace unsplittable — use the serial path
        }
        fanInPlanner.warnIfCascadePredicted(stagingSegments.size(), config.effectiveFanIn());
        ParallelRangeMerge rangeMerge =
                new ParallelRangeMerge(run, rangeTimer);
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
        long totalRows = 0;
        long mergePasses = 0;
        long cascadedPasses = 0;
        long fastPathEmissions = 0;
        for (ParallelRangeMerge.RangeResult rr : results) {
            tmpsInOrder.addAll(rr.tmpParts());
            totalRows += rr.rows();
            mergePasses += rr.mergePasses();
            cascadedPasses += rr.cascadedPasses();
            fastPathEmissions += rr.fastPathEmissions();
        }

        List<Path> finalFiles = new ArrayList<>();
        if (tmpsInOrder.isEmpty()) {
            // Empty listing: publish one valid, self-describing empty sorted file (matches the serial
            // path). This single file legitimately carries the completeness stamp.
            List<Path> tf = new ArrayList<>();
            SortedFileWriter writer = openNextFile(outputDir, stagingDir, finalFiles, tf);
            writer.markFinal();
            writer.close();
            atomicRename(tf.get(0), finalFiles.get(0));
        } else {
            int index = 1;
            for (Path tmp : tmpsInOrder) {
                String name = String.format("%s%05d%s", FINAL_PREFIX, index++, FINAL_SUFFIX);
                Path finalPath = outputDir.resolve(name);
                atomicRename(tmp, finalPath);
                finalFiles.add(finalPath);
            }
        }
        Durability.directory(outputDir);
        publishListener.onPublished(List.copyOf(finalFiles), totalRows);
        for (Path p : stagingSegments) {
            Files.deleteIfExists(p);
        }
        tryDeleteEmptyStagingDir(stagingDir);
        return new SortTransformResult(List.copyOf(finalFiles), totalRows,
                mergePasses, cascadedPasses, fastPathEmissions);
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
                                          List<Path> tmpFiles) throws IOException {
        int index = finalFiles.size() + 1;
        String name = String.format("%s%05d%s", FINAL_PREFIX, index, FINAL_SUFFIX);
        Path finalPath = outputDir.resolve(name);
        // Write the tmp OUTSIDE data/ — into the sibling staging dir on the same
        // filesystem — and atomically rename it INTO data/ (see transform()). data/ (outputDir) thus
        // only ever holds finalized *.parquet; a crash never strands a *.tmp inside the pure-parquet dir.
        Path tmpPath = stagingDir.resolve(name + TMP_SUFFIX);
        SortedFileWriter writer = finalWriterFactory.create(tmpPath, index);
        finalFiles.add(finalPath);
        tmpFiles.add(tmpPath);
        return writer;
    }

    private KWayMerge.SegmentIo<Path> segmentIo(PageRunSegmentWriter segmentWriter, Path stagingDir,
                                                List<Path> intermediates) {
        int[] seq = {0};
        return new KWayMerge.SegmentIo<>() {
            @Override
            public EntryStream open(Path segment) throws IOException {
                // Page-run staging + cascade intermediates stream via PageRunSegmentReader; a
                // legacy columnar Parquet staging segment (CaptureSorter's fixture path) still reads
                // via SegmentReader. The format is keyed off the file extension the producer stamped.
                // This is the StreamingMerger's page-run read path — the one
                // taken whenever a merge group MIXES page-run and Parquet segments — so it carries the
                // live metrics too; the intra-segment minKey-regression guard is the same one the
                // frontier path runs (both go through PageRunSegmentIo#nextPage), and the counter it
                // bumps (SORT.page_run_min_regression) must reach the run summary from either route.
                // The reader is handed the SAME comparator this merge runs under — it needs it
                // to key-merge a segment's overlapping pages into the sorted run StreamingMerger assumes.
                return isPageRunSegment(segment)
                        ? new PageRunSegmentReader(segment, comparator, metrics)
                        : new SegmentReader(segment);
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
                // A page-run staging/intermediate segment can be read as a decode-free page
                // frontier; a legacy columnar Parquet staging segment cannot.
                return isPageRunSegment(segment);
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

    /** Whether {@code segment} is a page-run staging/intermediate file (vs. a columnar Parquet one).
     *  Package-private: {@link MergeFanInPlanner#exactMemoryFanIn} shares this instead of duplicating the
     *  {@code .pageseg} suffix check. */
    static boolean isPageRunSegment(Path segment) {
        return segment.getFileName().toString().endsWith(SEGMENT_SUFFIX);
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
     * Remove stale FINAL files already in {@code outputDir} — an abandoned prior merge attempt's
     * finals, orphaned outside any manifest. Without this, a retry that produces FEWER final files
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
        sweep(outputDir, stale -> log.info("sweeping stale sorted output before re-merge: {}", stale), glob);
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
