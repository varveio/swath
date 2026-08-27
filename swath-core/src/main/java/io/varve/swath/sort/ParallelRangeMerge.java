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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * {@link SortTransform} renames them into a single ascending {@code part-00000.parquet}… sequence.
 *
 * <p><b>Why this is correct.</b> Ranges split on the <b>key bytes</b> (the primary component of
 * {@link ListEntryComparator}); each row is assigned to exactly one range by an exact per-row key
 * compare ({@link RangeFilteredCursor}), so a key's versions/cross-type rows stay grouped in one
 * range and no row is dropped or duplicated regardless of how the boundary keys are chosen. Boundary
 * choice therefore only affects <em>balance</em>, never correctness — a badly chosen boundary just
 * yields uneven (or empty) ranges, still a total, gap-free partition. Boundaries are sampled from the
 * observed key distribution, evenly spaced through the sorted distinct sample (see {@link
 * #sampleKeys}: page min-keys from page-run staging).
 *
 * <p><b>Page-run staging.</b> Every input is a {@code .pageseg} segment. Its range-scoped page
 * frontier skips irrelevant pages while retaining {@link PageAwareMerger}'s
 * decode-free page-whole fast path inside each range, so a range runs the same merge algorithm the
 * serial path would; the {@code [lo, hi)} trim therefore sits ABOVE the merge
 * ({@link RangeFilteredCursor}) rather than around each input.
 *
 * <p><b>Peak heap and descriptors, both divided across the ranges.</b> Each range's merge budget is
 * {@code mergeBudgetBytes / R} and its {@link KWayMerge} pass width is that divided by the page-run
 * per-open-stream price ({@link #perStreamBytes}); the process fd budget is divided by {@code R} too,
 * since the ranges hold their streams open at the same time. Terms the budget does NOT cover, so
 * realized peak still carries an {@code R}× component: the {@code max(2, …)} floor (each range opens
 * at least 2 streams), the {@code R} concurrent writers' buffers, and — the largest term measured in
 * practice — the allocation float of decoding and of stepping over skipped pages, which scales with
 * {@code R} and is reclaimable rather than live.
 *
 * <p><b>Degenerate cases.</b> Fewer than two distinct sample keys ⇒ {@link #boundaries} returns
 * {@code null} and {@link SortTransform} falls back to the untouched serial path (so a keyspace that
 * cannot be split is byte-identical to today). Fewer distinct keys than {@code R} ⇒ fewer ranges.
 * Empty ranges produce zero parts. {@code R == 1} never reaches this class — the serial path handles it.
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

    private static final AtomicLong MERGE_SEQUENCE = new AtomicLong();
    /** At the supported 16-range maximum this retains 1,024 candidates per range. */
    static final int MAX_BOUNDARY_CANDIDATES = 16_384;

    private final SortRun run;
    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final EqualKeyPolicy equalKeyPolicy;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final RangeMergeTimer rangeTimer;
    private final IntSupplier softFdLimitSupplier;
    private final String workerThreadPrefix;

    private enum SampleSource {
        EMBEDDED,
        SCAN
    }

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
        this.run = run;
        this.config = run.config();
        this.comparator = run.comparator();
        this.hook = run.hook();
        this.equalKeyPolicy = run.equalKeyPolicy();
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
    static List<byte[]> boundaries(List<PageRunSegmentDescriptor> segments,
                                   int desiredRanges, SortMetrics metrics)
            throws IOException {
        BoundaryCandidates distinct = new BoundaryCandidates();
        boolean embedded = false;
        boolean scanned = false;
        for (PageRunSegmentDescriptor segment : segments) {
            SampleSource source = sampleKeys(segment, distinct, metrics);
            embedded |= source == SampleSource.EMBEDDED;
            scanned |= source == SampleSource.SCAN;
        }
        if (distinct.capped()) {
            metrics.recordStealReason("SORT", "merge_boundary_global_capped");
        }
        if (embedded && scanned) {
            metrics.recordStealReason("SORT", "merge_boundary_source_mixed");
        } else if (embedded) {
            metrics.recordStealReason("SORT", "merge_boundary_source_embedded");
        } else {
            metrics.recordStealReason("SORT", "merge_boundary_source_scan");
        }
        if (distinct.size() < 2) {
            return null;   // cannot split — degenerate, fall back to serial
        }
        List<byte[]> candidates = distinct.sortedKeys();
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
     * Add one page-run segment's page-minimum boundary candidates. The bounded trailer extension is
     * preferred; absent, unknown, or invalid extensions fall back transactionally to a frontier scan
     * without exposing provisional keys to the global set.
     */
    private static SampleSource sampleKeys(PageRunSegmentDescriptor descriptor,
                                           BoundaryCandidates distinct,
                                           SortMetrics metrics)
            throws IOException {
        PageRunBoundarySample.ReadResult embedded = descriptor.sample();
        if (embedded.valid()) {
            for (byte[] key : embedded.keys()) {
                distinct.add(key);
            }
            if (embedded.totalRecords() > PageRunBoundarySample.MAX_ENTRIES) {
                metrics.recordStealReason("SORT", "merge_range_sample_capped");
            }
            metrics.recordBoundaryIo(embedded.keys().size(), embedded.bytesRead(), 0);
            metrics.markProgress();
            return SampleSource.EMBEDDED;
        }
        recordFallback(embedded.status(), metrics);

        // Boundary choice affects balance only, so cap retained samples independently of row count.
        long stride = PageRunBoundarySample.stride(embedded.totalRecords());
        if (stride > 1) {
            metrics.recordStealReason("SORT", "merge_range_sample_capped");
        }
        try (PageFrontierReader frontier = new PageFrontierReader(descriptor.path(), metrics)) {
            for (long page = 0; frontier.hasPage(); page++) {
                if (page % stride == 0) {
                    distinct.add(frontier.minKey().clone());
                }
                metrics.markProgress();
                frontier.advance();
            }
        }
        long fixedTailStart = descriptor.fileSize()
                - PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES;
        long framedRecordBytes = descriptor.trailerStart() >= PageRunSegmentWriter.HEADER_BYTES
                        && descriptor.trailerStart() <= fixedTailStart
                ? descriptor.trailerStart() - PageRunSegmentWriter.HEADER_BYTES
                : 0;
        metrics.recordBoundaryIo(0, embedded.bytesRead(), framedRecordBytes);
        return SampleSource.SCAN;
    }

    /**
     * Deterministic bottom-hash sample over distinct page minima. The per-segment trailer cap alone
     * still allowed sampler heap to grow with segment count; this second cap keeps the whole boundary
     * phase bounded while retaining a uniform sample large enough for at most 16 output ranges.
     */
    static final class BoundaryCandidates {
        private static final Comparator<ScoredKey> BY_SCORE = (a, b) -> {
            int byHash = Long.compareUnsigned(a.score(), b.score());
            return byHash != 0 ? byHash : KeyBytes.compareUnsigned(a.key(), b.key());
        };

        private final TreeSet<byte[]> byKey = new TreeSet<>(KeyBytes::compareUnsigned);
        private final TreeSet<ScoredKey> byScore = new TreeSet<>(BY_SCORE);
        private final int maxCandidates;
        private boolean capped;

        BoundaryCandidates() {
            this(MAX_BOUNDARY_CANDIDATES);
        }

        /** Smaller cap seam for long-key and order-independence tests without production-size heap. */
        BoundaryCandidates(int maxCandidates) {
            if (maxCandidates < 1) {
                throw new IllegalArgumentException("maxCandidates must be >= 1");
            }
            this.maxCandidates = maxCandidates;
        }

        void add(byte[] key) {
            if (byKey.contains(key)) {
                return;
            }
            ScoredKey candidate = new ScoredKey(score(key), key);
            if (byScore.size() == maxCandidates
                    && BY_SCORE.compare(candidate, byScore.last()) >= 0) {
                capped = true;
                return;
            }

            byte[] retained = key.clone();
            byKey.add(retained);
            byScore.add(new ScoredKey(candidate.score(), retained));
            if (byScore.size() > maxCandidates) {
                ScoredKey removed = byScore.pollLast();
                byKey.remove(removed.key());
                capped = true;
            }
        }

        int size() {
            return byKey.size();
        }

        boolean capped() {
            return capped;
        }

        List<byte[]> sortedKeys() {
            return new ArrayList<>(byKey);
        }

        private static long score(byte[] key) {
            long hash = 0xcbf29ce484222325L;
            for (byte b : key) {
                hash = (hash ^ (b & 0xFFL)) * 0x100000001b3L;
            }
            // Avalanche the prefix-heavy FNV state before unsigned bottom-k selection.
            hash ^= hash >>> 33;
            hash *= 0xff51afd7ed558ccdL;
            hash ^= hash >>> 33;
            hash *= 0xc4ceb9fe1a85ec53L;
            return hash ^ (hash >>> 33);
        }

        private record ScoredKey(long score, byte[] key) {
        }
    }

    private static void recordFallback(PageRunBoundarySample.Status status, SortMetrics metrics) {
        switch (status) {
            case ABSENT -> metrics.recordStealReason("SORT", "merge_boundary_fallback_absent");
            case UNKNOWN -> metrics.recordStealReason("SORT", "merge_boundary_fallback_unknown");
            case INVALID_LENGTH ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_length");
            case INVALID_COUNT ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_count");
            case INVALID_CRC ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_crc");
            case INVALID_ORDER ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_order");
            case INVALID_BOUNDS ->
                    metrics.recordStealReason("SORT", "merge_boundary_fallback_invalid_bounds");
            case EMBEDDED -> throw new AssertionError("valid sample cannot fall back");
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
    List<RangeResult> run(List<PageRunSegmentDescriptor> segmentDescriptors, Path stagingDir,
                          List<byte[]> boundaries,
                          LongConsumer progressCallback) throws IOException {
        List<Path> stagingSegments = PageRunSegmentDescriptor.paths(segmentDescriptors);
        Map<Path, PageRunSegmentDescriptor> descriptorsByPath =
                PageRunSegmentDescriptor.byPath(segmentDescriptors);
        int ranges = boundaries.size() + 1;
        int perRangeFanIn = perRangeFanIn(ranges, segmentDescriptors);
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
                        perRangeFanIn, safeProgress, safeHook, openPartCount, openPartLimit,
                        descriptorsByPath);
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
                // Preserve policy exceptions such as DuplicateKeyException. The
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
                                             DuplicateHook safeHook, AtomicInteger openPartCount,
                                             int openPartLimit,
                                             Map<Path, PageRunSegmentDescriptor> descriptorsByPath) {
        return () -> {
            SortedFileWriterFactory rangeWriterFactory =
                    finalWriterFactory.forOutputSequence();
            long startNanos = System.nanoTime();
            // Page-skip counters live on the frontier wrappers this range opened (one per page-run
            // input); collected after the merge drains, for the same read-vs-skipped signal.
            List<RangeScopedPageFrontier> pageFrontiers = new ArrayList<>();
            // Page-run ranges see whole straddling boundary pages, so a duplicate pair lying OUTSIDE
            // this range would be reported by both adjacent ranges. Scope the hook to [lo, hi) so the
            // run's duplicate counts match the serial merge's exactly.
            DuplicateHook rangeHook = (prev, dup) -> {
                if (inRange(dup.key().rawUnsafe(), lo, hi)) {
                    safeHook.onDuplicate(prev, dup);
                }
            };
            PageRunSegmentWriter pageRunWriter =
                    new PageRunSegmentWriter(comparator, rangeHook, metrics, config.segmentCodec());
            PageRunMergeIo io = new PageRunMergeIo(run,
                    MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, pageRunWriter, stagingDir,
                    "merge-r" + range + "-", new KeyRange(lo, hi), descriptorsByPath,
                    pageFrontiers::add);
            KWayMerge<Path> merge = new KWayMerge<>(comparator, perRangeFanIn, io, rangeHook, metrics);

            List<Path> tmpParts = new ArrayList<>();
            List<SortedFileWriter> parts = new ArrayList<>();
            long rows;
            // Inputs are page frontiers, so the merged stream can still carry the far side of a
            // straddling boundary page. Trim once above the page-aware merge.
            try (SortedCursor merged = new RangeFilteredCursor(
                    merge.merge(stagingSegments, safeProgress), lo, hi)) {
                // drainOpen, not drain: the parts are left OPEN and unstamped. Their file_index is a
                // position in the GLOBAL roll sequence, which this range cannot know -- it depends on
                // how many parts the ranges below it produce. SortTransform assigns the indices and
                // closes, once every range has drained.
                rows = RolledPartWriter.drainOpen(merged, config.finalFileBytes(),
                        () -> openRangePart(stagingDir, range, tmpParts, rangeWriterFactory,
                                openPartCount, openPartLimit), safeProgress, metrics, equalKeyPolicy,
                        parts, false);
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
            for (Path p : io.intermediates()) {
                Files.deleteIfExists(p);
            }
            // Instrumentation (metrics discipline, §5a): one increment per range engaged — the total
            // across a run equals the range count, the cheap keyspace-partition signal.
            metrics.recordStealReason("SORT", "merge_range_parallel");
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
            log.info("sort_merge_range range={} rows={} pages_kept={} pages_skipped={} "
                            + "pages_unread={} duration_ms={}",
                    range, rows, pagesKept, pagesSkipped, pagesUnread,
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
        Path tmp = stagingDir.resolve(StagingNames.rangeTmp(range, localIndex));
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

    /**
     * Per-range merge budget = total / R, expressed as a {@link KWayMerge} pass width. Package-private
     * because {@link SortTransform} asks the same question before the merge starts: staged segments
     * beyond this width mean the ranges cascade, and a cascading merge has no completion denominator.
     */
    int perRangeFanIn(int ranges, List<PageRunSegmentDescriptor> segmentDescriptors) {
        return perRangeFanIn(ranges, perStreamBytes(segmentDescriptors), ranges);
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
    private static long stagedBytes(List<PageRunSegmentDescriptor> segmentDescriptors) {
        long total = 0;
        for (PageRunSegmentDescriptor descriptor : segmentDescriptors) {
            total += descriptor.fileSize();
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
    EffectiveRanges effectiveRanges(int requested,
                                    List<PageRunSegmentDescriptor> segmentDescriptors) {
        int segments = segmentDescriptors.size();
        if (requested <= 1 || segments <= 0) {
            return new EffectiveRanges(Math.max(1, requested), ClampReason.NONE);
        }
        // Too small to be worth splitting: the speedup would be seconds and the cost is a permanent
        // change to the published file count. Checked before anything else, because it is the cheapest
        // test and the most common answer on ordinary runs.
        if (stagedBytes(segmentDescriptors) < config.minParallelStagedBytes()) {
            return new EffectiveRanges(1, ClampReason.BELOW_STAGED_FLOOR);
        }
        long perStream = perStreamBytes(segmentDescriptors);
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

    private long perStreamBytes(List<PageRunSegmentDescriptor> segmentDescriptors) {
        long pageRun = PageRunSegmentDescriptor.maxRecordLen(segmentDescriptors);
        return pageRun > 0
                ? Math.max(config.mergePerStreamBytes(), pageRun)
                : config.mergePerStreamBytes();
    }

    /** {@code lo <= key < hi}, either bound {@code null} meaning unbounded — the range's ownership test. */
    private static boolean inRange(byte[] key, byte[] lo, byte[] hi) {
        return (lo == null || KeyBytes.compareUnsigned(key, lo) >= 0)
                && (hi == null || KeyBytes.compareUnsigned(key, hi) < 0);
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
        sweep(stagingDir, StagingNames.RANGE_TMP_GLOB);
        sweep(stagingDir, StagingNames.RANGE_LEGACY_CASCADE_PARQUET_GLOB);
        sweep(stagingDir, StagingNames.RANGE_CASCADE_PAGE_RUN_GLOB);
    }

    private static void sweep(Path stagingDir, String glob) {
        try {
            Sweeps.sweep(stagingDir, stale -> { }, glob);
        } catch (IOException e) {
            log.debug("failed to sweep {} in {} after a parallel merge failure", glob, stagingDir, e);
        }
    }
}
