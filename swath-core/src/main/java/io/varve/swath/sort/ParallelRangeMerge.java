/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heap-, staged-size-, and fd-gated (configured by {@code swath.sort.merge-parallelism}): partition the keyspace into
 * {@code R} contiguous ordered ranges and merge each range independently on its own thread, each
 * producing its own ordered part file(s). The concatenation of the ranges' outputs, in range order,
 * is the global sort with no duplicate and no gap — given the input segments already had none — so
 * {@link SortTransform} renames them into a single ascending {@code part-00001.parquet}… sequence.
 *
 * <p><b>Why this is correct.</b> Ranges split on the <b>key bytes</b> (the primary component of
 * {@link ListEntryComparator}); each row is assigned to exactly one range by an exact per-row key
 * compare ({@link RangeFilteredStream}), so a key's versions/cross-type rows stay grouped in one
 * range and no row is dropped or duplicated regardless of how the boundary keys are chosen. Boundary
 * choice therefore only affects <em>balance</em>, never correctness — a badly chosen boundary just
 * yields uneven (or empty) ranges, still a total, gap-free partition. Boundaries are sampled from the
 * observed key distribution, evenly spaced through the sorted distinct sample (see {@link
 * #sampleKeys}: page min-keys for page-run staging, row-group first-keys for columnar Parquet).
 *
 * <p><b>Both staging formats.</b> This path runs over the live listing lane's page-run
 * ({@code .pageseg}) segments AND over columnar Parquet ({@link CaptureSorter}'s fixture path). Each
 * format brings its own skip — a page skip ({@link RangeScopedPageFrontier}) or a row-group skip
 * ({@link #selectRowGroups}) — and its own per-open-stream memory price, which is what the per-range
 * fan-in divides the budget by ({@link #perRangeFanIn}). Page-run inputs keep {@link PageAwareMerger}'s
 * decode-free page-whole fast path inside each range, so a range runs the same merge algorithm the
 * serial path would; the {@code [lo, hi)} trim therefore sits ABOVE the merge
 * ({@link RangeFilteredCursor}) rather than around each input.
 *
 * <p><b>Peak heap and descriptors, both divided across the ranges.</b> Each range's merge budget is
 * {@code mergeBudgetBytes / R} and its {@link KWayMerge} pass width is that divided by the format's
 * per-open-stream price ({@link #perStreamBytes}); the process fd budget is divided by {@code R} too,
 * since the ranges hold their streams open at the same time. Terms the budget does NOT cover, so
 * realized peak still carries an {@code R}× component: the {@code max(2, …)} floor (each range opens
 * at least 2 streams), the {@code R} concurrent writers' buffers, and — the largest term measured in
 * practice — the ALLOCATION FLOAT of decoding and of stepping over skipped pages, which scales with
 * {@code R} and is reclaimable rather than live. Measured on the columnar path: ~1.6 MB live per open
 * stream against ~480 MB/range of peak, i.e. most of the R-linear peak is float, not retained data.
 *
 * <p><b>Degenerate cases.</b> Fewer than two distinct sample keys ⇒ {@link #boundaries} returns
 * {@code null} and {@link SortTransform} falls back to the untouched serial path (so a keyspace that
 * cannot be split is byte-identical to today). Fewer distinct keys than {@code R} ⇒ fewer ranges.
 * Empty ranges produce zero parts. {@code R == 1} never reaches this class — the serial path handles it.
 *
 * <p><b>Row-group skip.</b> Each range reads only the Parquet row groups that
 * overlap its key range: for a sorted segment, physical row group {@code i} holds keys in
 * {@code [firstKey_i, firstKey_{i+1}]} — inclusive upper, since a duplicate key can straddle the
 * boundary and leave a key equal to {@code firstKey_{i+1}} in group {@code i} — so a range
 * {@code [lo, hi)} prunes every group whose span is
 * wholly outside it and decodes only the covered inclusive physical span (see {@link
 * #selectRowGroups} — the skip is CONSERVATIVE: a straddling boundary group is read in full and
 * {@link RangeFilteredStream} still trims per-row, so correctness never depends on boundary
 * precision, only on never skipping a group that could hold an in-range key). First keys are the
 * ACTUAL decoded keys from {@link SortedFileIndex#rowGroupSpans}, never footer stats (§9.1). The
 * skip is this path's decode-parallelism win over reading every segment whole; it is byte-identical
 * to reading every group (RangeFilteredStream produces the same rows either way).
 *
 * <p><b>Cascading ranges are unreachable in normal operation, and probably unnecessary at all.</b>
 * A cascade is a multi-pass merge: when a range's fan-in is narrower than the staged-segment count it
 * merges in several passes, rewriting every one of its rows each time. {@link
 * SortTransform} clamps {@code R} so that cannot happen ({@link #effectiveRanges}) and falls back to
 * the serial merge when not even one range fits, so no production run reaches the cascade branches
 * below.
 *
 * <p>We suspect they are not needed even as a fallback. Staged segments are produced by
 * work-stealing workers that own DISJOINT key ranges, so a well-formed run's segments barely overlap
 * in keyspace and the merge is closer to an ordered concatenation than to an interleave — which is
 * exactly what {@link PageAwareMerger}'s decode-free page-whole fast path already exploits
 * ({@code page_whole_emitted}). A merge that never has to interleave does not need many streams open
 * at once, and it is simultaneous open streams, not sortedness, that forces a cascade. Note the
 * weaker claim: segments still overlap at range boundaries, and after a resume or a backfill they can
 * overlap arbitrarily, so "barely" is not "never".
 *
 * <p>The branches are therefore kept as a correctness net against the clamp arithmetic being wrong
 * rather than deleted, and are exercised directly by the tests (which construct this class without
 * going through the clamp). Removing them is a reasonable follow-up once the disjointness argument is
 * measured rather than reasoned.
 *
 * <p><b>Completeness stamp.</b> The output carries the same self-describing proof the serial path
 * writes — {@code file_index} 1..N over the whole output with a single {@code file_final} on N — so a
 * reader can tell a complete file set from a truncated one without trusting a sidecar. It is assigned
 * late by necessity: a part's index is its position in the GLOBAL roll sequence, which depends on how
 * many parts every lower range produced, and no range knows that while it is writing. Each range
 * therefore hands its parts back OPEN ({@link RangeResult#writers}), and {@link SortTransform} — which
 * collects the results in range order — assigns the indices, marks the last part final, and closes.
 * Deferring the footer rather than the data keeps the cost small: a drained-but-unclosed writer has
 * already flushed its row groups and retains only their metadata plus at most one buffered row group.
 */
final class ParallelRangeMerge {

    private static final Logger log = LoggerFactory.getLogger(ParallelRangeMerge.class);

    /**
     * Boundary-sample cap per segment. The sample only has to resolve {@code R-1} evenly spaced split
     * points out of at most a few dozen ranges, so thousands of candidates per segment is already far
     * more resolution than the split can use; retaining one per PAGE would scale the sampler's heap
     * with the listing's row count, which is exactly what {@code I11} forbids elsewhere.
     */
    private static final int MAX_SAMPLES_PER_SEGMENT = 4_096;

    private static final AtomicLong MERGE_SEQUENCE = new AtomicLong();

    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final RangeMergeTimer rangeTimer;
    private final IntSupplier softFdLimitSupplier;
    private final String workerThreadPrefix;

    /**
     * Every final-output part this merge has opened, across all ranges — the failure path's handle on
     * them. Synchronized because range threads register concurrently. One instance per merge, so it
     * does not accumulate across runs.
     */
    private final List<SortedFileWriter> openParts = Collections.synchronizedList(new ArrayList<>());

    ParallelRangeMerge(SortRun run, RangeMergeTimer rangeTimer) {
        this(run, rangeTimer, MergeFdBudget::softOpenFileLimit);
    }

    ParallelRangeMerge(SortRun run, RangeMergeTimer rangeTimer, IntSupplier softFdLimitSupplier) {
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.rangeTimer = rangeTimer;
        this.softFdLimitSupplier = softFdLimitSupplier;
        this.workerThreadPrefix = "swath-sort-range-" + MERGE_SEQUENCE.incrementAndGet() + "-";
    }

    String workerThreadPrefix() {
        return workerThreadPrefix;
    }

    /**
     * One range's ordered output: the rolled tmp part files (in key order), the writers that produced
     * them — returned still OPEN — plus aggregate counts.
     *
     * <p>The writers are open because the completeness stamp cannot be written yet. A part's
     * {@code file_index} is its position in the output's GLOBAL roll sequence, which depends on how
     * many parts every lower range produced, and that is unknown until they all finish.
     * {@link SortTransform} collects these results in range order, assigns the indices, marks the very
     * last part final, and closes. {@code writers} is index-aligned with {@code tmpParts}.
     */
    record RangeResult(List<Path> tmpParts, List<SortedFileWriter> writers, long rows, long mergePasses,
                       long cascadedPasses, long fastPathEmissions) {
    }

    enum ClampReason {
        NONE("none"),
        BELOW_STAGED_FLOOR("below_staged_floor"),
        FD_EXHAUSTED("fd_exhausted"),
        FD_LIMITED("fd_limited"),
        WOULD_CASCADE("would_cascade");

        private final String logValue;

        ClampReason(String logValue) {
            this.logValue = logValue;
        }

        String logValue() {
            return logValue;
        }
    }

    record EffectiveRanges(int ranges, ClampReason reason) {
        EffectiveRanges {
            if (ranges < 1) {
                throw new IllegalArgumentException("ranges must be >= 1");
            }
        }
    }

    private record IndexedRangeResult(int range, RangeResult result) {
    }

    /**
     * Sample evenly-spaced key boundaries partitioning {@code segments} into up to {@code desiredRanges}
     * contiguous ranges. Returns the {@code R-1} boundary keys (so the range count is
     * {@code boundaries.size() + 1}), or {@code null} when the keyspace has fewer than two distinct
     * sample keys and so cannot be split (the caller then uses the serial path).
     */
    static List<byte[]> boundaries(List<Path> segments, int desiredRanges, SortMetrics metrics)
            throws IOException {
        TreeSet<byte[]> distinct = new TreeSet<>(KeyBytes::compareUnsigned);
        for (Path segment : segments) {
            sampleKeys(segment, distinct, metrics);
        }
        if (distinct.size() < 2) {
            return null;   // cannot split — degenerate, fall back to serial
        }
        List<byte[]> candidates = new ArrayList<>(distinct);
        int ranges = Math.min(desiredRanges, candidates.size());
        // Pick R-1 interior split points, evenly spaced through the sorted distinct sample. Range 0
        // starts at -inf (index 0 is never a boundary); dedup keeps boundaries strictly increasing.
        List<byte[]> boundaries = new ArrayList<>();
        byte[] last = null;
        for (int j = 1; j < ranges; j++) {
            int idx = (int) ((long) j * candidates.size() / ranges);
            byte[] key = candidates.get(idx);
            if (last == null || KeyBytes.compareUnsigned(key, last) > 0) {
                boundaries.add(key);
                last = key;
            }
        }
        return boundaries.isEmpty() ? null : boundaries;
    }

    /**
     * Add {@code segment}'s boundary-candidate keys to {@code distinct}, dispatching on the staging
     * format the producer stamped. Both samples are the same shape — the first key of each physical
     * unit of the segment — and both are read WITHOUT decoding any row:
     *
     * <ul>
     *   <li><b>Page-run</b> ({@code .pageseg}, the live listing lane's format): each page's
     *       {@code minKey}, walked with a {@link PageFrontierReader}. The frontier parses only the
     *       record body's leading fields, so this costs a pass over the segment's framing, not a
     *       decode. {@code minKey()} returns the reader's INTERNAL buffer (read-only, not
     *       defensively copied — the hot merge path relies on that), so it is cloned before being
     *       retained here.</li>
     *   <li><b>Columnar Parquet</b> (the {@link CaptureSorter} fixture path, and this class's own
     *       cascade intermediates): each row group's first key from
     *       {@link SortedFileIndex#firstKeysPerRowGroup}.</li>
     * </ul>
     *
     * <p>Sample granularity differs between the two — pages are typically finer than 1 MB row
     * groups — which affects only how evenly the ranges balance, never correctness: every row is
     * assigned to exactly one range by an exact per-row key compare in {@link RangeFilteredStream},
     * whatever the boundaries are.
     */
    private static void sampleKeys(Path segment, TreeSet<byte[]> distinct, SortMetrics metrics)
            throws IOException {
        if (SortTransform.isPageRunSegment(segment)) {
            // Stride: a page holds ~1000 entries, so an unsampled walk retains one key per ~1000 rows
            // -- ~1M cloned keys plus TreeSet overhead on a billion-row listing, held serially before
            // any range starts. Boundary choice affects BALANCE ONLY (see the class javadoc), so
            // thinning the sample cannot cost correctness; MAX_SAMPLES_PER_SEGMENT keeps far more
            // candidates than the R-1 splits ever consume.
            long totalPages = PageRunSegmentReader.readTrailer(segment).totalRecords();
            // Ceiling division: floor would leave stride == 1 up to 2 x the cap (retaining ~8k keys
            // for a 4097-page segment), so the "cap" would not be one.
            long stride = totalPages <= MAX_SAMPLES_PER_SEGMENT
                    ? 1
                    : (totalPages + MAX_SAMPLES_PER_SEGMENT - 1) / MAX_SAMPLES_PER_SEGMENT;
            if (stride > 1) {
                // Instrumentation (AGENTS.md "instrument every new algo path"): the cap changes which
                // boundaries get chosen, so a run whose ranges balanced badly needs to be able to tell
                // a thinned sample from a full one. Fires once per capped SEGMENT, so the run total is
                // how many segments were large enough to thin.
                metrics.recordStealReason("SORT", "merge_range_sample_capped");
            }
            try (PageFrontierReader frontier = new PageFrontierReader(segment, metrics)) {
                for (long page = 0; frontier.hasPage(); page++) {
                    if (page % stride == 0) {
                        // minKey() may hand back a buffer the reader owns; retain a copy.
                        distinct.add(frontier.minKey().clone());
                    }
                    // This walk reads and CRC-verifies EVERY page of EVERY segment before any range
                    // starts -- single-threaded, with nothing else running to advance the signal. It
                    // emitted nothing, so the liveness watchdog's 120 s total-freeze tripwire halted
                    // the JVM on any listing whose staging took longer than that to scan. The serial
                    // merge never walks a whole segment, which is why only the parallel path tripped.
                    metrics.markProgress();
                    frontier.advance();
                }
            }
            return;
        }
        for (SortedFileIndex.RowGroupKey rg : SortedFileIndex.firstKeysPerRowGroup(segment)) {
            distinct.add(rg.firstKey());
        }
    }

    /**
     * Merge each range concurrently, returning the ordered per-range results (range 0 first). Fails
     * the whole merge — after cancelling siblings and sweeping this run's own tmp/intermediate files —
     * if any range throws (never silently drops a range's rows). {@code progressCallback} and
     * {@code hook} are invoked from multiple range threads, so they are serialized here.
     *
     * <p><b>Failure is observed in completion order.</b> A later-submitted range's failure is surfaced
     * immediately even while an earlier range is still draining. Siblings are interrupted, joined to
     * proven quiescence, and only afterwards are writers closed and owned files swept.
     */
    List<RangeResult> run(List<Path> stagingSegments, Path stagingDir, List<byte[]> boundaries,
                          LongConsumer progressCallback) throws IOException {
        int ranges = boundaries.size() + 1;
        int perRangeFanIn = perRangeFanIn(ranges, stagingSegments);
        // Row-group skip: derive each original segment's (firstKey, physicalBlockIndex, rowCount)
        // spans ONCE up front (a short key-column pass per segment) and share the read-only result
        // across every range thread — every range prunes against the same per-segment spans, so this
        // avoids re-deriving them R times. Cascade intermediates (created later, per range) are not
        // here and are span-derived on the fly in rangeSegmentIo#open.
        // Which staging format this merge is over. Page-run is the live listing lane's format;
        // columnar Parquet is CaptureSorter's fixture path. The row-group span pre-derivation below is
        // Parquet-only -- page-run inputs carry their per-page frontier in the record framing itself,
        // so there is nothing to pre-derive and each range walks it directly.
        // ANY page-run input, not just the first. The top-level trim and the range-scoped duplicate
        // hook are what keep straddling-page rows out of the output and out of the hook; both are
        // idempotent when the inputs were already trimmed per stream (the columnar path), so turning
        // them on for a MIXED staging set is free. Sniffing segment 0 instead would leave them off
        // for a Parquet-first mixed set, and an all-page-run cascade GROUP inside that run would then
        // report out-of-range duplicate pairs that the neighbouring range reports too.
        boolean pageRunFormat = stagingSegments.stream().anyMatch(SortTransform::isPageRunSegment);
        Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans = new HashMap<>();
        if (!pageRunFormat) {
            for (Path segment : stagingSegments) {
                segmentSpans.put(segment, SortedFileIndex.rowGroupSpans(segment));
            }
        }
        Object progressLock = new Object();
        LongConsumer safeProgress = units -> {
            synchronized (progressLock) {
                progressCallback.accept(units);
            }
        };
        DuplicateHook safeHook = (prev, dup) -> {
            synchronized (progressLock) {
                hook.onDuplicate(prev, dup);
            }
        };

        int threads = Math.min(ranges, Math.max(1, Runtime.getRuntime().availableProcessors()));
        AtomicInteger threadSequence = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    workerThreadPrefix + threadSequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        });
        CompletionService<IndexedRangeResult> completions = new ExecutorCompletionService<>(pool);
        List<Future<?>> futures = new ArrayList<>(ranges);
        AtomicInteger openPartCount = new AtomicInteger();
        int openPartLimit = openOutputPartLimit(ranges,
                Math.min(perRangeFanIn, stagingSegments.size()));
        try {
            for (int r = 0; r < ranges; r++) {
                int range = r;
                byte[] lo = range == 0 ? null : boundaries.get(range - 1);
                byte[] hi = range == ranges - 1 ? null : boundaries.get(range);
                Callable<RangeResult> task = mergeRange(range, lo, hi, stagingSegments, stagingDir,
                        perRangeFanIn, safeProgress, safeHook, segmentSpans, pageRunFormat,
                        openPartCount, openPartLimit);
                futures.add(completions.submit(() -> new IndexedRangeResult(range, task.call())));
            }
            List<RangeResult> results = new ArrayList<>(Collections.nCopies(ranges, null));
            for (int completed = 0; completed < ranges; completed++) {
                IndexedRangeResult result = completions.take().get();
                results.set(result.range(), result.result());
            }
            if (shutdownAndAwait(pool, false)) {
                Thread.currentThread().interrupt();
            }
            log.info("sort_merge_range_parallel ranges={} threads={} per_range_fan_in={}",
                    ranges, threads, perRangeFanIn);
            return results;
        } catch (InterruptedException e) {
            futures.forEach(f -> f.cancel(true));
            shutdownAndAwait(pool, true);
            releaseOpenParts();
            sweepOwnFiles(stagingDir);
            Thread.currentThread().interrupt();
            throw new IOException("parallel range merge interrupted", e);
        } catch (ExecutionException e) {
            futures.forEach(f -> f.cancel(true));
            boolean interruptedWhileJoining = shutdownAndAwait(pool, true);
            releaseOpenParts();
            sweepOwnFiles(stagingDir);
            if (interruptedWhileJoining) {
                Thread.currentThread().interrupt();
            }
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof UncheckedIOException uio) {
                throw uio.getCause() == null ? new IOException(uio.getMessage(), uio) : uio.getCause();
            }
            if (cause instanceof RuntimeException runtime) {
                // Preserve policy exceptions such as CaptureSorter's DuplicateKeyException. The
                // parallel coordinator must not change the public failure type relative to serial.
                throw runtime;
            }
            throw new IOException("parallel range merge failed", cause);
        } catch (RuntimeException e) {
            // Anything the two checked paths above do not name -- a RejectedExecutionException from
            // submit() being the realistic one, since it fires mid-loop with some ranges already
            // running. Without this the open parts and their files would survive the failure.
            futures.forEach(f -> f.cancel(true));
            boolean interruptedWhileJoining = shutdownAndAwait(pool, true);
            releaseOpenParts();
            sweepOwnFiles(stagingDir);
            if (interruptedWhileJoining) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            pool.shutdownNow();
        }
    }

    private Callable<RangeResult> mergeRange(int range, byte[] lo, byte[] hi, List<Path> stagingSegments,
                                             Path stagingDir, int perRangeFanIn, LongConsumer safeProgress,
                                             DuplicateHook safeHook,
                                             Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans,
                                             boolean pageRunFormat, AtomicInteger openPartCount,
                                             int openPartLimit) {
        return () -> {
            SortedFileWriterFactory rangeWriterFactory =
                    finalWriterFactory.forOutputSequence();
            long startNanos = System.nanoTime();
            List<Path> intermediates = new ArrayList<>();
            // Row-group skip counters — accumulated (single-threaded within this one range) across
            // every segment this range opens, so the per-range log + skip signal reflect the whole range.
            long[] groupsRead = {0};
            long[] groupsSkipped = {0};
            // Page-skip counters live on the frontier wrappers this range opened (one per page-run
            // input); collected after the merge drains, for the same read-vs-skipped signal.
            List<RangeScopedPageFrontier> pageFrontiers = new ArrayList<>();
            // Page-run ranges see whole straddling boundary pages, so a duplicate pair lying OUTSIDE
            // this range would be reported by both adjacent ranges. Scope the hook to [lo, hi) so the
            // run's duplicate counts match the serial merge's exactly. (The Parquet path already trims
            // per input stream, so its hook never sees an out-of-range row.)
            DuplicateHook rangeHook = pageRunFormat ? (prev, dup) -> {
                if (inRange(dup.key().rawUnsafe(), lo, hi)) {
                    safeHook.onDuplicate(prev, dup);
                }
            } : safeHook;
            SegmentWriter segmentWriter =
                    new SegmentWriter(comparator, rangeHook, metrics, config.segmentRowGroupBytes());
            PageRunSegmentWriter pageRunWriter =
                    new PageRunSegmentWriter(comparator, rangeHook, metrics, config.segmentCodec());
            KWayMerge.SegmentIo<Path> io = rangeSegmentIo(segmentWriter, pageRunWriter, stagingDir, range,
                    lo, hi, intermediates, segmentSpans, groupsRead, groupsSkipped, pageFrontiers,
                    pageRunFormat);
            KWayMerge<Path> merge = new KWayMerge<>(comparator, perRangeFanIn, io, rangeHook, metrics);

            List<Path> tmpParts = new ArrayList<>();
            List<SortedFileWriter> parts = new ArrayList<>();
            long rows;
            // Page-run: the inputs are page frontiers, so the merged stream still carries the far side
            // of any straddling boundary page — trim it here (RangeFilteredCursor). Parquet: each input
            // was already entry-filtered on open, so the merged stream is in-range by construction.
            try (SortedCursor merged = rangeScoped(merge.merge(stagingSegments, safeProgress),
                    pageRunFormat, lo, hi)) {
                // drainOpen, not drain: the parts are left OPEN and unstamped. Their file_index is a
                // position in the GLOBAL roll sequence, which this range cannot know -- it depends on
                // how many parts the ranges below it produce. SortTransform assigns the indices and
                // closes, once every range has drained.
                rows = RolledPartWriter.drainOpen(merged, config.finalFileBytes(),
                        () -> openRangePart(stagingDir, range, tmpParts, rangeWriterFactory,
                                openPartCount, openPartLimit), safeProgress, parts, false);
            } catch (IOException | RuntimeException e) {
                // This range failed, so nothing it wrote will be published: release the open parts
                // rather than strand their descriptors until the sweep. Never stamped -- an aborted
                // range's files must not claim a position in a sequence that will not exist.
                try {
                    RolledPartWriter.closeQuietly(parts);
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
            // Reclaim this range's cascade intermediates (KWayMerge already deleted the ones it folded;
            // deleteIfExists is a no-op on those). Originals are shared across ranges and are NEVER
            // deleted here — SortTransform deletes them once, after the whole publish.
            for (Path p : intermediates) {
                Files.deleteIfExists(p);
            }
            // Instrumentation (metrics discipline, §5a): one increment per range engaged — the total
            // across a run equals the range count, the cheap keyspace-partition signal.
            metrics.recordStealReason("SORT", "merge_range_parallel");
            // Row-group skip signal (§5a): fire once per range that pruned ≥1 row group — the run
            // total is "how many ranges the skip engaged on", and read-vs-skipped in the log below is
            // the skip-fraction (did it help) signal. Absent when a range read every group (no skip).
            if (groupsSkipped[0] > 0) {
                metrics.recordStealReason("SORT", "merge_range_rowgroup_skipped");
            }
            long pagesKept = 0;
            long pagesSkipped = 0;
            long pagesUnread = 0;
            for (RangeScopedPageFrontier f : pageFrontiers) {
                pagesKept += f.pagesKept();
                pagesSkipped += f.pagesSkipped();
                pagesUnread += f.pagesUnread();
            }
            // Page skip signal (§5a), the page-run twin of the row-group one above: fire once per
            // range that stepped over >=1 page, so the run total is "how many ranges the page skip
            // engaged on", and kept-vs-skipped in the log is the skip-fraction (did it help) signal.
            if (pagesSkipped + pagesUnread > 0) {
                metrics.recordStealReason("SORT", "merge_range_page_skipped");
            }
            long rangeNanos = System.nanoTime() - startNanos;
            rangeTimer.recordRangeMerge(rangeNanos);
            log.info("sort_merge_range range={} rows={} row_groups_read={} row_groups_skipped={} "
                            + "pages_kept={} pages_skipped={} pages_unread={} duration_ms={}",
                    range, rows, groupsRead[0], groupsSkipped[0], pagesKept, pagesSkipped, pagesUnread,
                    rangeNanos / 1_000_000L);
            return new RangeResult(tmpParts, parts, rows, merge.mergePasses(), merge.cascadedPasses(),
                    merge.fastPathEmissions());
        };
    }

    private SortedFileWriter openRangePart(Path stagingDir, int range, List<Path> tmpParts,
            SortedFileWriterFactory rangeWriterFactory, AtomicInteger openPartCount,
            int openPartLimit) throws IOException {
        // Range-local ordinal: it names the tmp file, and is only a PLACEHOLDER index. The real
        // file_index is assigned by SortTransform once every range has drained and the global roll
        // sequence is known; the footer is not written until then (see setFileIndex).
        int localIndex = tmpParts.size() + 1;
        Path tmp = stagingDir.resolve("prange-" + range + "-" + localIndex + ".parquet.tmp");
        int open = openPartCount.incrementAndGet();
        if (open > openPartLimit) {
            openPartCount.decrementAndGet();
            throw new IOException("parallel range merge output-part fd budget exhausted: limit="
                    + openPartLimit + ", attempted=" + open
                    + "; lower swath.sort.merge-parallelism or raise the soft open-file limit");
        }
        SortedFileWriter writer;
        try {
            writer = rangeWriterFactory.create(tmp, localIndex);
        } catch (IOException | RuntimeException e) {
            openPartCount.decrementAndGet();
            throw e;
        }
        // Register on creation, not on return, so the failure path can release a part whatever
        // happens to the range that owns it. A cancelled range exits cooperatively at the next safe
        // point and closes its local list, while this registry lets the coordinator release every
        // already-open part again after quiescence without depending on a cancelled Future's result.
        // Closes are idempotent, so double-releasing is harmless.
        openParts.add(writer);
        tmpParts.add(tmp);
        return writer;
    }

    private KWayMerge.SegmentIo<Path> rangeSegmentIo(SegmentWriter segmentWriter,
                                                     PageRunSegmentWriter pageRunWriter, Path stagingDir,
                                                     int range, byte[] lo, byte[] hi,
                                                     List<Path> intermediates,
                                                     Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans,
                                                     long[] groupsRead, long[] groupsSkipped,
                                                     List<RangeScopedPageFrontier> pageFrontiers,
                                                     boolean pageRunFormat) {
        int[] seq = {0};
        return new KWayMerge.SegmentIo<>() {
            @Override
            public EntryStream open(Path segment) throws IOException {
                // Restrict every input to this range. Originals span the whole keyspace. Cascade
                // intermediates are in-range on the COLUMNAR path (trimmed per stream on the way in),
                // but NOT on the page-run one — there the trim happens above the merge, so a straddling
                // page's far-side rows persist through every cascade pass and are removed once, at the
                // top, by RangeFilteredCursor. Re-scoping an intermediate is harmless either way.
                //
                // The RangeFilteredStream wrap is the same on both formats and is UNCHANGED — it
                // trims straddling boundary units per-row, so the per-format skip below is a pure
                // performance layer that never affects which rows this range emits.
                if (SortTransform.isPageRunSegment(segment)) {
                    // Entry-typed fallback for a page-run input (a merge group that MIXES formats, so
                    // KWayMerge cannot take the frontier path). The frontier route below is the normal
                    // one; both produce the same rows.
                    return new RangeFilteredStream(
                            new PageRunSegmentReader(openScopedFrontier(segment), comparator, metrics),
                            lo, hi);
                }
                // Row-group skip: decode only the physical row groups whose key span overlaps
                // [lo, hi). Originals were span-derived up front (segmentSpans); cascade intermediates
                // are not in that map, so derive their spans on the fly.
                List<SortedFileIndex.RowGroupSpan> spans = segmentSpans.get(segment);
                if (spans == null) {
                    spans = SortedFileIndex.rowGroupSpans(segment);
                }
                RangeSelection selection = selectRowGroups(spans, lo, hi);
                groupsRead[0] += selection.groupsRead();
                groupsSkipped[0] += selection.groupsSkipped();
                return new RangeFilteredStream(new SegmentReader(segment, selection.blockIndices()), lo, hi);
            }

            @Override
            public boolean supportsPageFrontier(Path segment) {
                // Page-run inputs expose a decode-free frontier, so KWayMerge keeps PageAwareMerger's
                // page-whole fast path INSIDE each range. Without this the parallel path would quietly
                // fall back to the entry-typed StreamingMerger while the serial R=1 baseline kept the
                // fast path — every range would pay a per-entry heap the control arm does not, and an
                // A/B would be measuring a merger downgrade on top of the range parallelism.
                return SortTransform.isPageRunSegment(segment);
            }

            @Override
            public PageFrontierStream openFrontier(Path segment) throws IOException {
                return openScopedFrontier(segment);
            }

            /** The range-scoped page frontier for {@code segment}, registered for its skip counters. */
            private PageFrontierStream openScopedFrontier(Path segment) throws IOException {
                long totalPages = PageRunSegmentReader.readTrailer(segment).totalRecords();
                RangeScopedPageFrontier scoped = new RangeScopedPageFrontier(
                        new PageFrontierReader(segment, metrics), lo, hi, totalPages, metrics);
                pageFrontiers.add(scoped);
                return scoped;
            }

            @Override
            public Path writeIntermediate(SortedCursor sorted) throws IOException {
                // Cascade intermediates keep this range's INPUT format, so open() above dispatches
                // them back to the same reader: a page-run merge stays page-run end to end instead of
                // silently switching to columnar Parquet for its second pass.
                if (pageRunFormat) {
                    Path dest = stagingDir.resolve("merge-r" + range + "-" + (seq[0]++)
                            + SortTransform.SEGMENT_SUFFIX);
                    pageRunWriter.writeIntermediate(sorted, dest);
                    intermediates.add(dest);
                    return dest;
                }
                Path dest = stagingDir.resolve("merge-r" + range + "-" + (seq[0]++) + ".parquet");
                segmentWriter.writeIntermediate(sorted, dest);
                intermediates.add(dest);
                return dest;
            }

            @Override
            public void delete(Path segment) throws IOException {
                Files.deleteIfExists(segment);
            }
        };
    }

    /**
     * Row-group skip: the physical row-group block indices to decode for range {@code [lo, hi)},
     * plus how many non-empty groups this selection reads vs skips (the instrumentation signal).
     */
    record RangeSelection(int[] blockIndices, int groupsRead, int groupsSkipped) {
    }

    /**
     * Which row groups of a sorted segment overlap {@code [lo, hi)} ({@code lo} inclusive, {@code hi}
     * exclusive; either {@code null} = unbounded). For sorted non-empty groups with first keys
     * {@code k_0 < k_1 < … < k_{n-1}}, physical group {@code i} spans {@code [k_i, k_{i+1})} (the last
     * group's upper bound is {@code +inf}). Because duplicate/straddle keys can put a key equal to
     * {@code k_{i+1}} inside group {@code i}, the loss-free (conservative) overlap test is
     * {@code k_i < hi} and {@code upper_i = k_{i+1} >= lo} — a contiguous run
     * {@code [firstCovered, lastCovered]}:
     * <ul>
     *   <li>{@code firstCovered} = the smallest {@code i} whose span can still reach {@code lo} —
     *       smallest {@code i} with {@code upper_i = k_{i+1} >= lo} (the last group's {@code upper} is
     *       {@code +inf}, so it always qualifies), or {@code 0} when {@code lo} is unbounded. Pruning by
     *       {@code k_i} alone would DROP rows when a key spans several equal-first-key groups or
     *       straddles a boundary, so the prune is by the group's max possible key {@code upper_i};</li>
     *   <li>{@code lastCovered} = the largest {@code i} with {@code k_i < hi}, or {@code n-1} when
     *       {@code hi} is unbounded (and {@code -1} — nothing overlaps — when {@code hi <= k_0}).</li>
     * </ul>
     * The returned {@code blockIndices} is the INCLUSIVE physical span
     * {@code [blockIndex(firstCovered) .. blockIndex(lastCovered)]} — conservative by construction: it
     * reads every physical block in that span (including any empty groups omitted from {@code spans}),
     * so a maybe-covered block is never skipped, and the straddling boundary groups are still trimmed
     * per-row by {@link RangeFilteredStream}. An empty {@code spans} (segment with no rows) or a range
     * that overlaps no group yields an empty selection (reads nothing).
     */
    static RangeSelection selectRowGroups(List<SortedFileIndex.RowGroupSpan> spans, byte[] lo, byte[] hi) {
        int n = spans.size();
        if (n == 0) {
            return new RangeSelection(new int[0], 0, 0);
        }
        int firstCovered = 0;
        if (lo != null) {
            // Skip group i ONLY when its entire key-span is provably below lo. Group i's keys are all
            // <= upper_i, where upper_i = firstKey_{i+1} (or +inf for the last group): swath allows
            // multiple rows per key (versions / cross row_types), so a single key can overflow one row
            // group or straddle a boundary, leaving a key equal to firstKey_{i+1} inside group i.
            // Skipping by firstKey_i alone would then drop rows a key shares across consecutive
            // equal-first-key groups. firstCovered = the smallest i with upper_i >= lo (predecessor-
            // inclusive: it may read ~1 extra group per boundary, which RangeFilteredStream trims
            // per-row). firstKey — hence upper_i — is nondecreasing, so {i : upper_i >= lo} is a
            // suffix and the covered span stays contiguous.
            firstCovered = n - 1;   // last group's upper is +inf, so it always qualifies
            for (int i = 0; i < n - 1; i++) {
                if (KeyBytes.compareUnsigned(spans.get(i + 1).firstKey(), lo) >= 0) {
                    firstCovered = i;
                    break;
                }
            }
        }
        int lastCovered = n - 1;
        if (hi != null) {
            int t = -1;
            for (int i = 0; i < n; i++) {
                if (KeyBytes.compareUnsigned(spans.get(i).firstKey(), hi) < 0) {
                    t = i;   // k_i < hi — this group can hold keys below hi
                } else {
                    break;   // sorted: every later group starts at/above hi
                }
            }
            lastCovered = t;
        }
        if (firstCovered > lastCovered) {
            return new RangeSelection(new int[0], 0, n);   // no group overlaps [lo, hi)
        }
        int groupsRead = lastCovered - firstCovered + 1;
        int pFirst = spans.get(firstCovered).blockIndex();
        int pLast = spans.get(lastCovered).blockIndex();
        int[] blocks = new int[pLast - pFirst + 1];
        for (int b = pFirst; b <= pLast; b++) {
            blocks[b - pFirst] = b;
        }
        return new RangeSelection(blocks, groupsRead, n - groupsRead);
    }

    /**
     * Per-range merge budget = total / R, expressed as a {@link KWayMerge} pass width. Package-private
     * because {@link SortTransform} asks the same question before the merge starts: staged segments
     * beyond this width mean the ranges cascade, and a cascading merge has no completion denominator.
     */
    int perRangeFanIn(int ranges, List<Path> stagingSegments) {
        return perRangeFanIn(ranges, perStreamBytes(stagingSegments), ranges);
    }

    /**
     * As {@link #perRangeFanIn(int, List)}, over prices the caller already computed. The seam exists
     * for {@link #effectiveRanges}, which evaluates this at more than one candidate range count and
     * must not re-read every segment's trailer (or re-stat every segment) once per candidate.
     */
    private int perRangeFanIn(int ranges, long perStreamBytes, long openPartBudget) {
        long perRangeBudget = config.mergeBudgetBytes() / ranges;
        long budgetBound = perRangeBudget / perStreamBytes;
        // Descriptors are a WHOLE-PROCESS budget and the ranges hold their streams open at the same
        // time, so the fd bound divides across them too. The serial path clamps once against the full
        // budget (MergeFanInPlanner); a parallel merge that reused that bound unchanged would open R
        // times as many descriptors as the process is allowed.
        //
        // The max(2, ...) floor below is a floor, not a guarantee: a merge needs 2 streams to merge
        // anything, so at an extreme R (where a range's share falls below 2) the ranges together can
        // still exceed the process share -- 2 x R descriptors. That regime is unreachable through the
        // supported entry point, because effectiveRanges() clamps R long before a range's share falls
        // that far; it stays possible only for a direct caller of this class.
        long fdBound = streamFdBudget(openPartBudget) / (long) ranges;
        return (int) Math.min(config.fanIn(), Math.max(2L, Math.min(budgetBound, fdBound)));
    }

    /**
     * Descriptors left for merge INPUT streams once the output parts have taken theirs.
     *
     * <p>The parts are the term the budget used to ignore. Every range's parts stay open until the
     * whole merge finishes — that is what makes the global completeness stamp possible — so the
     * process holds one descriptor per OUTPUT part on top of {@code R × fanIn} input streams, and the
     * part count is set by {@code final-file-bytes} rather than by {@code R}. On a large listing with
     * a small roll threshold the parts alone can exhaust the budget, which would surface as an EMFILE
     * partway through a merge the clamp had already declared safe.
     */
    private long usableFdBudget() {
        int softLimit = softFdLimitSupplier.getAsInt();
        if (softLimit < 0) {
            return Long.MAX_VALUE;
        }
        return Math.max(0L, (long) softLimit - MergeFdBudget.FD_HEADROOM);
    }

    /**
     * Descriptors available to merge inputs after reserving {@code openPartBudget} output writers.
     * No staged-byte estimate appears here: page-run compression and Parquet encoding are unrelated,
     * so physical staging bytes cannot conservatively predict how many final files will roll.
     */
    private long streamFdBudget(long openPartBudget) {
        long usable = usableFdBudget();
        return usable == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, usable - openPartBudget);
    }

    /**
     * Maximum final-output writers the range fleet may actually open while all final-pass input
     * streams are live. The guard is enforced atomically in {@link #openRangePart}; exceeding it
     * fails and cancels the merge before another descriptor is opened.
     */
    private int openOutputPartLimit(int ranges, int perRangeFanIn) {
        long usable = usableFdBudget();
        if (usable == Long.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        long inputReservation = (long) ranges * perRangeFanIn;
        return (int) Math.max(0L, Math.min(Integer.MAX_VALUE, usable - inputReservation));
    }

    /** Total staged bytes, best-effort: an unreadable segment counts as zero and only shrinks R. */
    private static long stagedBytes(List<Path> stagingSegments) {
        long total = 0;
        for (Path segment : stagingSegments) {
            try {
                total += Files.size(segment);
            } catch (IOException e) {
                log.debug("could not size a staging segment for the parallel-merge floor", e);
            }
        }
        return total;
    }

    /**
     * The requested range count, reduced until no range would cascade over {@code stagingSegments}.
     * {@link SortTransform} applies this to {@code merge-parallelism} before it splits anything; the
     * result is the {@code R} the run actually uses.
     *
     * <p><b>Why the clamp is not optional.</b> {@link #perRangeFanIn} divides BOTH the merge memory
     * budget and the process descriptor budget by the range count. Once a range's share falls below
     * the staged-segment count, every range cascades — it merges in several passes, rewriting all of
     * its rows each time — and the knob goes backwards: measured 5.69× → 3.80× at {@code R=32} on a
     * 16 GB heap, and 41 % slower than the single-pass arm at IDENTICAL heap when only the budget was
     * pinned. Without this clamp the pessimisation is also SILENT: {@code merge_range_parallel} still
     * fires once per range, so a run that was made slower by its own tuning is indistinguishable in
     * the metrics from one that was made faster.
     *
     * <p>The bound tightens as a listing grows, which is exactly when an operator is least likely to
     * re-derive it by hand: segment count rises with the object count, so an {@code R} that is
     * single-pass on a 10 M-object bucket can cascade on a billion-object one at the same heap.
     *
     * <p>Returns at least 1 plus a typed reason whenever the requested count was not honoured. A 1
     * can mean the staged-size floor declined the work, descriptors were exhausted, or the memory
     * budget/configured fan-in would force a cascade; none means the keyspace was unsplittable (that
     * is known only after boundary sampling). With combined constraints, the reason names the one
     * that determines the final range count: descriptor exhaustion applies only when descriptors
     * alone force one range, while a binding descriptor reduction above one is descriptor-limited.
     * The search starts from a closed-form estimate and steps down only to absorb constraints not
     * represented in that estimate, so it evaluates {@link #perRangeFanIn} a couple of times, not
     * {@code R} times.
     */
    EffectiveRanges effectiveRanges(int requested, List<Path> stagingSegments) {
        int segments = stagingSegments.size();
        if (requested <= 1 || segments <= 0) {
            return new EffectiveRanges(Math.max(1, requested), ClampReason.NONE);
        }
        // Too small to be worth splitting: the speedup would be seconds and the cost is a permanent
        // change to the published file count. Checked before anything else, because it is the cheapest
        // test and the most common answer on ordinary runs.
        if (stagedBytes(stagingSegments) < config.minParallelStagedBytes()) {
            return new EffectiveRanges(1, ClampReason.BELOW_STAGED_FLOOR);
        }
        long perStream = perStreamBytes(stagingSegments);
        // Reserve one output writer per candidate range. Additional rolls are not estimated from
        // staging bytes (that is not a valid upper bound); openRangePart enforces the remaining
        // output-writer allowance dynamically. If even one output plus a two-way merge cannot fit,
        // this is specifically fd exhaustion, not an unsplittable keyspace.
        if (streamFdBudget(1) < 2) {
            return new EffectiveRanges(1, ClampReason.FD_EXHAUSTED);
        }
        // Closed form: the largest R with (budget/R)/perStream >= segments, and with the per-range
        // share of what the parts left over still spanning the segments. Both are the inequalities
        // perRangeFanIn tests.
        long byBudget = config.mergeBudgetBytes() / perStream / segments;
        long usableFds = usableFdBudget();
        long byFd = usableFds == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : usableFds / (segments + 1L);   // R*segments inputs + R initial output writers
        int candidate = (int) Math.max(1L, Math.min(requested, Math.min(byBudget, byFd)));
        // Floor division in several places means the closed form can land one step high; step down
        // until the predicate SortTransform reports on is actually true, so the clamp cannot be off by
        // one. Re-evaluate the exact predicate per candidate because the integer divisions floor.
        while (candidate > 1
                && perRangeFanIn(candidate, perStream, candidate)
                        < segments) {
            candidate--;
        }
        ClampReason reason = ClampReason.NONE;
        if (candidate < requested) {
            // Name the constraint that determines the final range count. In particular, a partial
            // descriptor reduction may expose an independent configured-fan-in or heap limit that
            // then forces the search all the way to one range. That is a cascade decline, not fd
            // exhaustion: descriptors alone would still have permitted parallel work.
            boolean nonFdForcesSerial = config.fanIn() < segments || byBudget < 2;
            boolean fdBinding = byFd < requested && byFd <= byBudget;
            reason = candidate == 1
                    ? (nonFdForcesSerial ? ClampReason.WOULD_CASCADE : ClampReason.FD_EXHAUSTED)
                    : (fdBinding ? ClampReason.FD_LIMITED : ClampReason.WOULD_CASCADE);
        }
        return new EffectiveRanges(candidate, reason);
    }

    /**
     * Estimated heap held by ONE open merge stream, which is what the per-range budget is divided by.
     * The two staging formats are not comparable here and using the wrong one is a real regression:
     *
     * <ul>
     *   <li><b>Page-run:</b> the EXACT {@code maxRecordLen} from each segment's trailer (an O(1) read)
     *       — the same quantity {@link MergeFanInPlanner#exactMemoryFanIn} clamps the serial merge
     *       with. Typically tens of KiB.</li>
     *   <li><b>Columnar Parquet:</b> {@code segmentRowGroupBytes}, since {@link SegmentReader} holds a
     *       row group per open stream.</li>
     * </ul>
     *
     * <p>Pricing a page-run stream at {@code segmentRowGroupBytes} (1 MB) instead of its real
     * {@code maxRecordLen} overstates it by one to two orders of magnitude, which drives
     * {@code budgetBound} far below the segment count and makes every range CASCADE — turning the
     * parallel merge's win into a loss on exactly the workload this path exists to serve. Falls back
     * to the Parquet estimate if a trailer cannot be read (correct, merely conservative).
     */
    private long perStreamBytes(List<Path> stagingSegments) {
        long pageRun = MergeFanInPlanner.maxPageRunRecordLen(stagingSegments);
        if (pageRun > 0) {
            return pageRun;   // all page-run: the exact per-stream heap from the trailers
        }
        // Not all page-run. maxPageRunRecordLen reports -1 for a MIXED set as well as for an
        // all-Parquet one, and falling straight back to segmentRowGroupBytes would then price a
        // page-run stream at a Parquet row-group size -- an independently configurable knob that a
        // page record can exceed, which would let a mixed merge open more streams than the budget
        // allows. Take the larger of the two prices so the bound holds for whichever input is worse.
        long columnar = config.segmentRowGroupBytes();
        long pageRunSubset = MergeFanInPlanner.maxPageRunRecordLen(
                stagingSegments.stream().filter(SortTransform::isPageRunSegment).toList());
        return Math.max(columnar, Math.max(pageRunSubset, 0));
    }

    /** {@code lo <= key < hi}, either bound {@code null} meaning unbounded — the range's ownership test. */
    private static boolean inRange(byte[] key, byte[] lo, byte[] hi) {
        return (lo == null || KeyBytes.compareUnsigned(key, lo) >= 0)
                && (hi == null || KeyBytes.compareUnsigned(key, hi) < 0);
    }

    /** Trim a merged range stream to {@code [lo, hi)} on the page-run path; identity on the Parquet one. */
    private static SortedCursor rangeScoped(SortedCursor merged, boolean pageRunFormat, byte[] lo,
                                            byte[] hi) {
        return pageRunFormat ? new RangeFilteredCursor(merged, lo, hi) : merged;
    }

    /**
     * Stop the pool and prove every range thread has exited before cleanup or return. Cancellation
     * is cooperative: the shared merge and drain loops poll the interrupt flag at row/page/pass safe
     * points, while try-with-resources closes readers and range writers on the way out. Interrupts of
     * the coordinator during this join are remembered and restored only after quiescence is proven.
     */
    private static boolean shutdownAndAwait(ExecutorService pool, boolean cancel) {
        if (cancel) {
            pool.shutdownNow();
        } else {
            pool.shutdown();
        }
        boolean interrupted = false;
        while (!pool.isTerminated()) {
            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                interrupted = true;
                pool.shutdownNow();
            }
        }
        return interrupted;
    }

    /**
     * Release the open, unstamped parts of ranges that SUCCEEDED before a sibling failed. Their
     * writers are handed back open by design (the global index is assigned later), so a failure that
     * skips {@link SortTransform}'s publish would otherwise strand descriptors until GC — and the
     * sweep below would unlink files still held open. Never stamps: an aborted merge's parts must not
     * claim a position in a sequence that will never be published.
     */
    private void releaseOpenParts() {
        List<SortedFileWriter> snapshot;
        synchronized (openParts) {
            snapshot = new ArrayList<>(openParts);
        }
        try {
            RolledPartWriter.closeQuietly(snapshot);
        } catch (IOException | RuntimeException ignored) {
            // Already failing, and the sweep unlinks these tmp files next; a close error here would
            // only mask the merge failure the caller is about to throw.
        }
    }

    /** Best-effort sweep of THIS run's own tmp parts and cascade intermediates on the failure path. */
    private static void sweepOwnFiles(Path stagingDir) {
        sweep(stagingDir, "prange-*.parquet.tmp");
        sweep(stagingDir, "merge-r*-*.parquet");
        sweep(stagingDir, "merge-r*-*" + SortTransform.SEGMENT_SUFFIX);   // page-run cascade intermediates
    }

    private static void sweep(Path stagingDir, String glob) {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(stagingDir, glob)) {
            for (Path stale : stream) {
                Files.deleteIfExists(stale);
            }
        } catch (IOException e) {
            log.debug("failed to sweep {} in {} after a parallel merge failure", glob, stagingDir, e);
        }
    }
}
