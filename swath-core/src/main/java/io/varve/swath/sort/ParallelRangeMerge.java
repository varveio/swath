/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heap-, staged-size-, and fd-gated (configured by {@code swath.sort.merge-parallelism}): partition the keyspace into
 * {@code R} contiguous ordered ranges and merge each range independently on its own thread, each
 * producing its own ordered part file(s). The concatenation of the ranges' outputs, in range order,
 * is the global sort with no duplicate and no gap — given the input segments already had none — so
 * {@link DatasetPublisher} renames them into a single ascending {@code part-00000.parquet}… sequence.
 *
 * <p><b>Why this is correct.</b> Ranges split on the <b>key bytes</b> (the primary component of
 * {@link ListEntryComparator}); each row is assigned to exactly one range by an exact per-row key
 * compare ({@link RangeFilteredCursor}), so a key's versions/cross-type rows stay grouped in one
 * range and no row is dropped or duplicated regardless of how the boundary keys are chosen. Boundary
 * choice therefore only affects <em>balance</em>, never correctness — a badly chosen boundary just
 * yields uneven (or empty) ranges, still a total, gap-free partition. Boundaries always come from
 * the bounded distinct page-minimum candidate set. The default spaces them by candidate position;
 * the explicit rows policy weights those same candidates with validated type-2 entry mass.
 *
 * <p><b>Page-run staging.</b> Every input is a {@code .pageseg} segment. Its range-scoped page
 * frontier skips irrelevant pages while retaining {@link PageAwareMerger}'s
 * decode-free page-whole fast path inside each range, so a range runs the same merge algorithm the
 * serial path would; the {@code [lo, hi)} trim therefore sits ABOVE the merge
 * ({@link RangeFilteredCursor}) rather than around each input.
 * Type-2 page-index entries are positioning hints, not trusted metadata: planning streams them into
 * primitive per-range seams before worker launch, and the coordinator verifies every sampled claim
 * while chaining disjoint physical zones from the fixed header to each segment's trailer. Writers
 * remain open and unpublished until that whole-input proof succeeds.
 *
 * <p><b>Peak heap and descriptors, both divided across the ranges.</b> Each range's merge budget is
 * {@code mergeBudgetBytes / R} and its {@link KWayMerge} pass width is that divided by the page-run
 * per-open-stream price ({@link MergePlanner#perRangeFanIn}); the process fd budget is divided by {@code R} too,
 * since the ranges hold their streams open at the same time. Terms the budget does NOT cover, so
 * realized peak still carries an {@code R}× component: the {@code max(2, …)} floor (each range opens
 * at least 2 streams), the {@code R} concurrent writers' buffers, and — the largest term measured in
 * practice — the allocation float of decoding and of stepping over skipped pages, which scales with
 * {@code R} and is reclaimable rather than live.
 *
 * <p><b>Degenerate cases.</b> Fewer than two distinct sample keys ⇒ {@link MergePlanner#boundaries}
 * returns
 * {@code null} and {@link SortTransform} falls back to the untouched serial path (so a keyspace that
 * cannot be split is byte-identical to today). Fewer distinct keys than {@code R} ⇒ fewer ranges.
 * Empty ranges produce zero parts. {@code R == 1} never reaches this class — the serial path handles it.
 *
 * <p><b>Cascading ranges are unreachable in normal operation, and probably unnecessary at all.</b>
 * A cascade is a multi-pass merge: when a range's fan-in is narrower than the staged-segment count it
 * merges in several passes, rewriting every one of its rows each time. {@link
 * MergePlanner} clamps {@code R} so that cannot happen ({@link MergePlanner#effectiveRanges}) and falls back to
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
 * therefore hands its parts back OPEN ({@link ParallelRangeWorker.Result#writers}), and {@link
 * DatasetPublisher} — which collects the results in range order — assigns the indices, marks the
 * last part final, and closes.
 * Deferring the footer rather than the data keeps the cost small: a drained-but-unclosed writer has
 * already flushed its row groups and retains only their metadata plus at most one buffered row group.
 */
final class ParallelRangeMerge {

    private static final Logger log = LoggerFactory.getLogger(ParallelRangeMerge.class);

    private static final AtomicLong MERGE_SEQUENCE = new AtomicLong();
    private final DuplicateHook hook;
    private final SortMetrics metrics;
    private final MergePlanner planner;
    private final ParallelRangeWorker worker;
    private final String workerThreadPrefix;
    private final PageRunZoneVerifier.ProofReaderFactory proofReaderFactory;

    /**
     * Every final-output part this merge has opened, across all ranges — the failure path's handle on
     * them. Synchronized because range threads register concurrently. One instance per merge, so it
     * does not accumulate across runs.
     */
    private final List<SortedFileWriter> openParts = Collections.synchronizedList(new ArrayList<>());

    ParallelRangeMerge(SortRun run) {
        this(run, new MergePlanner(run), PageRunProofSpool.Reader::new);
    }

    ParallelRangeMerge(SortRun run, PageRunZoneVerifier.ProofReaderFactory proofReaderFactory) {
        this(run, new MergePlanner(run), proofReaderFactory);
    }

    ParallelRangeMerge(SortRun run, MergePlanner planner,
            PageRunZoneVerifier.ProofReaderFactory proofReaderFactory) {
        this.hook = run.hook();
        this.metrics = run.metrics();
        this.planner = planner;
        this.worker = new ParallelRangeWorker(run, log);
        this.workerThreadPrefix = "swath-sort-range-" + MERGE_SEQUENCE.incrementAndGet() + "-";
        this.proofReaderFactory = proofReaderFactory;
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
     * {@link DatasetPublisher} collects these results in range order, assigns the indices, marks the very
     * last part final, and closes. {@code writers} is index-aligned with {@code tmpParts}.
     */
    private record IndexedRangeResult(int range, ParallelRangeWorker.Result result) {
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
    List<ParallelRangeWorker.Result> run(PageRunCatalog catalog, Path stagingDir,
                          List<byte[]> boundaries,
                          LongConsumer progressCallback) throws IOException {
        List<PageRunSegmentDescriptor> segmentDescriptors = catalog.descriptors();
        List<Path> stagingSegments = catalog.paths();
        Map<Path, PageRunSegmentDescriptor> descriptorsByPath = catalog.byPath();
        int ranges = boundaries.size() + 1;
        int perRangeFanIn = planner.perRangeFanIn(ranges, catalog);
        // Position every range before worker launch. The plan retains O(segments*R) primitives,
        // never sampled-key lists; type-2 values remain hints until the post-worker physical-zone
        // proof below chains them from the fixed header to the trailer.
        PageRunSeekPlan seekPlan = PageRunSeekPlan.plan(segmentDescriptors, boundaries, metrics);
        Path proofSpoolPath = stagingDir.resolve(StagingNames.rangeProofTmp());
        PageRunProofSpool.Stats proofSpoolStats = new PageRunProofSpool.Stats(metrics);
        int proofSlots = Math.multiplyExact(ranges, seekPlan.segments().size());
        PageRunProofSpool.Writer proofSpool =
                new PageRunProofSpool.Writer(proofSpoolPath, proofSlots, proofSpoolStats);
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
        int openPartLimit = planner.openOutputPartLimit(ranges,
                Math.min(perRangeFanIn, stagingSegments.size()));
        try {
            for (int r = 0; r < ranges; r++) {
                int range = r;
                byte[] lo = range == 0 ? null : boundaries.get(range - 1);
                byte[] hi = range == ranges - 1 ? null : boundaries.get(range);
                Callable<ParallelRangeWorker.Result> task = worker.task(
                        range, lo, hi, stagingSegments, stagingDir,
                        perRangeFanIn, safeProgress, safeHook,
                        (ownedStaging, ownedRange, tmpParts, rangeWriterFactory) ->
                                openRangePart(ownedStaging, ownedRange, tmpParts,
                                        rangeWriterFactory, openPartCount, openPartLimit),
                        descriptorsByPath, seekPlan, proofSpool, proofSpoolPath);
                futures.add(completions.submit(() -> new IndexedRangeResult(range, task.call())));
            }
            List<ParallelRangeWorker.Result> results =
                    new ArrayList<>(Collections.nCopies(ranges, null));
            for (int completed = 0; completed < ranges; completed++) {
                IndexedRangeResult result = completions.take().get();
                results.set(result.range(), result.result());
            }
            if (shutdownAndAwait(pool, false)) {
                Thread.currentThread().interrupt();
            }
            proofSpool.close();
            List<PageRunZoneVerifier.RangeSummary> proof = results.stream()
                    .map(ParallelRangeWorker.Result::zoneSummary)
                    .toList();
            PageRunZoneVerifier.verify(
                    seekPlan, proof, metrics, proofReaderFactory, proofSpoolStats);
            PageRunProofSpool.Snapshot proofSpoolSnapshot = proofSpoolStats.snapshot();
            log.info("sort_merge_range_parallel ranges={} threads={} per_range_fan_in={} "
                            + "proof_spool_fds={} proof_spool_logical_extent_bytes={} "
                            + "proof_spool_preallocation_operations={} "
                            + "proof_spool_preallocation_attempted_bytes={} "
                            + "proof_spool_mapped_operations={} proof_spool_mapped_bytes={} "
                            + "proof_spool_ms={}",
                    ranges, threads, perRangeFanIn, MergePlanner.PROOF_SPOOL_FDS,
                    proofSpoolSnapshot.logicalExtentBytes(),
                    proofSpoolSnapshot.preallocationOperations(),
                    proofSpoolSnapshot.preallocationAttemptedBytes(),
                    proofSpoolSnapshot.mappedOperations(), proofSpoolSnapshot.mappedBytes(),
                    proofSpoolSnapshot.serviceNanos() / 1_000_000L);
            return results;
        } catch (InterruptedException e) {
            abortAndCleanUp(pool, futures, stagingDir, proofSpool);
            Thread.currentThread().interrupt();
            throw new IOException("parallel range merge interrupted", e);
        } catch (ExecutionException e) {
            abortAndCleanUp(pool, futures, stagingDir, proofSpool);
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
        } catch (IOException e) {
            // Coordinator-side zone proof failures happen after every worker returned its writers.
            // They are still pre-publication failures: close all writers and sweep owned debris.
            abortAndCleanUp(pool, futures, stagingDir, proofSpool);
            throw e;
        } catch (RuntimeException e) {
            // Anything the two checked paths above do not name -- a RejectedExecutionException from
            // submit() being the realistic one, since it fires mid-loop with some ranges already
            // running. Without this the open parts and their files would survive the failure.
            abortAndCleanUp(pool, futures, stagingDir, proofSpool);
            throw e;
        } finally {
            pool.shutdownNow();
            try {
                proofSpool.close();
            } catch (IOException ignored) {
                // Success closed it before verification; failure cleanup already preserves the cause.
            }
        }
    }


    private SortedFileWriter openRangePart(Path stagingDir, int range, List<Path> tmpParts,
            SortedFileWriterFactory rangeWriterFactory, AtomicInteger openPartCount,
            int openPartLimit) throws IOException {
        // Range-local ordinal: it names the tmp file, and is only a PLACEHOLDER index. The real
        // file_index is assigned by DatasetPublisher once every range has drained and the global roll
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

    /** Cancel, prove worker quiescence, release writers, then sweep owned files. */
    private void abortAndCleanUp(ExecutorService pool, List<Future<?>> futures, Path stagingDir,
                                 PageRunProofSpool.Writer proofSpool) {
        futures.forEach(future -> future.cancel(true));
        boolean interruptedWhileJoining = shutdownAndAwait(pool, true);
        try {
            proofSpool.close();
        } catch (IOException ignored) {
            // The initiating merge/proof failure remains authoritative.
        }
        releaseOpenParts();
        sweepOwnFiles(stagingDir);
        if (interruptedWhileJoining) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Release the open, unstamped parts of ranges that SUCCEEDED before a sibling failed. Their
     * writers are handed back open by design (the global index is assigned later), so a failure that
     * skips {@link DatasetPublisher}'s publish would otherwise strand descriptors until GC — and the
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
        sweep(stagingDir, StagingNames.RANGE_PROOF_TMP_GLOB);
    }

    private static void sweep(Path stagingDir, String glob) {
        try {
            Sweeps.sweep(stagingDir, stale -> { }, glob);
        } catch (IOException e) {
            log.debug("failed to sweep {} in {} after a parallel merge failure", glob, stagingDir, e);
        }
    }
}
