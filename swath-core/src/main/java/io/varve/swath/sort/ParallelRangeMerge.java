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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Off-by-default (behind {@code swath.sort.merge-parallelism}): partition the keyspace into
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
 * observed key distribution ({@link SortedFileIndex#firstKeysPerRowGroup} first-keys across all
 * segments), evenly spaced through the sorted distinct sample.
 *
 * <p><b>Peak heap (approximately bounded — see the caveats).</b> Each range's merge budget is
 * {@code mergeBudgetBytes / R}, so its {@link KWayMerge} pass width (read streams held open at once) is
 * {@code min(fanIn, max(2, (mergeBudgetBytes/R) / segmentRowGroupBytes))}. Summed over the {@code R}
 * concurrent ranges, the merge <i>read</i> streams stay near {@code mergeBudgetBytes} — <i>above</i> the
 * per-range floor. Two terms this budget does NOT cover, so realized peak can carry an {@code R}× term:
 * (1) the {@code max(2, …)} floor means at a tiny budget each range still opens 2 read streams
 * ({@code 2·R·segmentRowGroupBytes}); (2) the {@code R} concurrent {@code SortedFileWriter}s/cascade
 * {@code SegmentWriter}s each buffer up to {@code finalRowGroupBytes}/{@code segmentRowGroupBytes}, an
 * added {@code ~R·(finalRowGroupBytes + segmentRowGroupBytes)} the read budget ignores. Off-by-default;
 * this real profile must be measured before promoting the knob.
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
 * <p><b>Still not implemented:</b> correct global multi-file
 * completeness stamps ({@code file_index} = 1..N, single {@code file_final}) — each range writes with
 * a range-local {@code file_index} and no file is marked final, so the produced files are a correct
 * global sort by <em>filename</em> order but do NOT carry the self-describing completeness proof
 * {@link SortedParquetWriter} documents. The path remains off by default while that proof is absent.
 */
final class ParallelRangeMerge {

    private static final Logger log = LoggerFactory.getLogger(ParallelRangeMerge.class);

    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final RangeMergeTimer rangeTimer;

    ParallelRangeMerge(SortRun run, RangeMergeTimer rangeTimer) {
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.rangeTimer = rangeTimer;
    }

    /** One range's ordered output: the rolled tmp part files (in key order) plus aggregate counts. */
    record RangeResult(List<Path> tmpParts, long rows, long mergePasses, long cascadedPasses,
                       long fastPathEmissions) {
    }

    /**
     * Sample evenly-spaced key boundaries partitioning {@code segments} into up to {@code desiredRanges}
     * contiguous ranges. Returns the {@code R-1} boundary keys (so the range count is
     * {@code boundaries.size() + 1}), or {@code null} when the keyspace has fewer than two distinct
     * sample keys and so cannot be split (the caller then uses the serial path).
     */
    static List<byte[]> boundaries(List<Path> segments, int desiredRanges) throws IOException {
        TreeSet<byte[]> distinct = new TreeSet<>(KeyBytes::compareUnsigned);
        for (Path segment : segments) {
            for (SortedFileIndex.RowGroupKey rg : SortedFileIndex.firstKeysPerRowGroup(segment)) {
                distinct.add(rg.firstKey());
            }
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
     * Merge each range concurrently, returning the ordered per-range results (range 0 first). Fails
     * the whole merge — after cancelling siblings and sweeping this run's own tmp/intermediate files —
     * if any range throws (never silently drops a range's rows). {@code progressCallback} and
     * {@code hook} are invoked from multiple range threads, so they are serialized here.
     *
     * <p><b>Failure is observed in submission (range) order, not first-completed order.</b>
     * {@link Future#get()} is called on the futures in range order below, so a late range's
     * failure is not noticed — and its siblings are not
     * cancelled — until every earlier range's future has already completed (successfully or not).
     * Correctness still holds either way: nothing is published until every range succeeds, so a whole
     * merge that will fail never publishes a partial/gapped output regardless of which range's failure
     * is observed first (see {@code aFailingRangeFailsTheWholeMerge}). First-completed failure
     * detection (e.g. via {@code ExecutorCompletionService}, to cancel siblings sooner) is a
     * productionization follow-up, not required for correctness.
     */
    List<RangeResult> run(List<Path> stagingSegments, Path stagingDir, List<byte[]> boundaries,
                          LongConsumer progressCallback) throws IOException {
        int ranges = boundaries.size() + 1;
        int perRangeFanIn = perRangeFanIn(ranges);
        // Row-group skip: derive each original segment's (firstKey, physicalBlockIndex, rowCount)
        // spans ONCE up front (a short key-column pass per segment) and share the read-only result
        // across every range thread — every range prunes against the same per-segment spans, so this
        // avoids re-deriving them R times. Cascade intermediates (created later, per range) are not
        // here and are span-derived on the fly in rangeSegmentIo#open.
        Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans = new HashMap<>();
        for (Path segment : stagingSegments) {
            segmentSpans.put(segment, SortedFileIndex.rowGroupSpans(segment));
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
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<RangeResult>> futures = new ArrayList<>(ranges);
        try {
            for (int r = 0; r < ranges; r++) {
                int range = r;
                byte[] lo = range == 0 ? null : boundaries.get(range - 1);
                byte[] hi = range == ranges - 1 ? null : boundaries.get(range);
                futures.add(pool.submit(mergeRange(range, lo, hi, stagingSegments, stagingDir,
                        perRangeFanIn, safeProgress, safeHook, segmentSpans)));
            }
            List<RangeResult> results = new ArrayList<>(ranges);
            for (Future<RangeResult> f : futures) {
                results.add(f.get());
            }
            log.info("sort_merge_range_parallel ranges={} threads={} per_range_fan_in={}",
                    ranges, threads, perRangeFanIn);
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            futures.forEach(f -> f.cancel(true));
            sweepOwnFiles(stagingDir);
            throw new IOException("parallel range merge interrupted", e);
        } catch (ExecutionException e) {
            futures.forEach(f -> f.cancel(true));
            sweepOwnFiles(stagingDir);
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof UncheckedIOException uio) {
                throw uio.getCause() == null ? new IOException(uio.getMessage(), uio) : uio.getCause();
            }
            throw new IOException("parallel range merge failed", cause);
        } finally {
            pool.shutdownNow();
        }
    }

    private Callable<RangeResult> mergeRange(int range, byte[] lo, byte[] hi, List<Path> stagingSegments,
                                             Path stagingDir, int perRangeFanIn, LongConsumer safeProgress,
                                             DuplicateHook safeHook,
                                             Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans) {
        return () -> {
            long startNanos = System.nanoTime();
            List<Path> intermediates = new ArrayList<>();
            // Row-group skip counters — accumulated (single-threaded within this one range) across
            // every segment this range opens, so the per-range log + skip signal reflect the whole range.
            long[] groupsRead = {0};
            long[] groupsSkipped = {0};
            SegmentWriter segmentWriter =
                    new SegmentWriter(comparator, safeHook, metrics, config.segmentRowGroupBytes());
            KWayMerge.SegmentIo<Path> io = rangeSegmentIo(segmentWriter, stagingDir, range, lo, hi,
                    intermediates, segmentSpans, groupsRead, groupsSkipped);
            KWayMerge<Path> merge = new KWayMerge<>(comparator, perRangeFanIn, io, safeHook, metrics);

            List<Path> tmpParts = new ArrayList<>();
            long rows;
            try (SortedCursor merged = merge.merge(stagingSegments, safeProgress)) {
                // markFinalOnLast=false: range-local file_index, no file_final — the documented
                // parallel-path stamp gap (see this class's javadoc).
                rows = RolledPartWriter.drain(merged, config.finalFileBytes(),
                        () -> openRangePart(stagingDir, range, tmpParts), false, safeProgress);
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
            long rangeNanos = System.nanoTime() - startNanos;
            rangeTimer.recordRangeMerge(rangeNanos);
            log.info("sort_merge_range range={} rows={} row_groups_read={} row_groups_skipped={} "
                            + "duration_ms={}",
                    range, rows, groupsRead[0], groupsSkipped[0], rangeNanos / 1_000_000L);
            return new RangeResult(tmpParts, rows, merge.mergePasses(), merge.cascadedPasses(),
                    merge.fastPathEmissions());
        };
    }

    private SortedFileWriter openRangePart(Path stagingDir, int range, List<Path> tmpParts)
            throws IOException {
        int localIndex = tmpParts.size() + 1;   // range-LOCAL file index (documented stamp gap)
        Path tmp = stagingDir.resolve("prange-" + range + "-" + localIndex + ".parquet.tmp");
        SortedFileWriter writer = finalWriterFactory.create(tmp, localIndex);
        tmpParts.add(tmp);
        return writer;
    }

    private KWayMerge.SegmentIo<Path> rangeSegmentIo(SegmentWriter segmentWriter, Path stagingDir,
                                                     int range, byte[] lo, byte[] hi,
                                                     List<Path> intermediates,
                                                     Map<Path, List<SortedFileIndex.RowGroupSpan>> segmentSpans,
                                                     long[] groupsRead, long[] groupsSkipped) {
        int[] seq = {0};
        return new KWayMerge.SegmentIo<>() {
            @Override
            public EntryStream open(Path segment) throws IOException {
                // Restrict every input to this range. Originals span the whole keyspace; cascade
                // intermediates are already in-range, so re-filtering them is a harmless idempotent pass.
                //
                // Row-group skip: decode only the physical row groups whose key span overlaps
                // [lo, hi). Originals were span-derived up front (segmentSpans); cascade intermediates
                // are not in that map, so derive their spans on the fly. The RangeFilteredStream wrap
                // is UNCHANGED — it still trims the straddling boundary groups per-row, so the skip is
                // a pure performance layer and never affects which rows this range emits.
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
            public Path writeIntermediate(SortedCursor sorted) throws IOException {
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
    int perRangeFanIn(int ranges) {
        long perRangeBudget = config.mergeBudgetBytes() / ranges;
        long budgetBound = perRangeBudget / config.segmentRowGroupBytes();
        return (int) Math.min(config.fanIn(), Math.max(2L, budgetBound));
    }

    /** Best-effort sweep of THIS run's own tmp parts and cascade intermediates on the failure path. */
    private static void sweepOwnFiles(Path stagingDir) {
        sweep(stagingDir, "prange-*.parquet.tmp");
        sweep(stagingDir, "merge-r*-*.parquet");
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
