/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cascaded k-way merge of sorted segments, generic over the segment handle {@code S}
 * (a {@link java.nio.file.Path} in production; anything in tests) via {@link SegmentIo}.
 * {@link #merge} reduces the segment set to at most {@code fanIn} (the constructor parameter — the
 * caller passes {@link SortConfig#effectiveFanIn()} in production, not the raw {@code fan-in} knob;
 * see below) — merging in passes of {@code <= fanIn} segments into fresh intermediate segments when
 * there are more, so open readers (open file descriptors) never exceed {@code fanIn} plus the one
 * output being written (<b>fd bound ≤ F + 1</b>, I2) — then returns a streaming
 * {@link SortedCursor} over the surviving &le;{@code fanIn} segments.
 *
 * <p>Every cascade group uses {@link CascadePageMerger}, which forwards a globally disjoint page
 * without a row heap and uses {@link PageRowMerger} only for overlapping page clusters.
 *
 * <p><b>Merge-phase memory + fd bound (I11).</b> {@code fanIn} here is expected to already be the
 * RUNTIME-clamped fan-in {@link SortedDatasetCoordinator} computes — the MIN of the static
 * {@link SortConfig#effectiveFanIn()} budget derived from {@link SortConfig#mergePerStreamBytes()},
 * the fd clamp ({@link MergeFdBudget}), and the per-segment encoded-record refinement. Active merge
 * state remains bounded by configured segment/fan-in knobs rather than total object count. Passing the raw {@code
 * fan-in} knob straight through instead would violate I11. See contracts.md §6 / §7 for the full
 * fan-in/memory-budget derivation. {@code fanIn} alone remains only the fd/correctness bound (I2),
 * never a memory promise by itself.
 *
 * <p><b>Deletion policy.</b> {@code merge}'s ORIGINAL input segments are <b>never deleted by this
 * class</b>, at any pass — only an intermediate segment {@link SegmentIo#writeIntermediate created
 * by an earlier pass of this same merge} is deleted once a later pass has folded it in. Do not
 * delete an original the moment a pass folds it in: the checkpoint's {@code finalizedParts} still
 * points at it, and a crash mid-cascade must be able to redo the sort from those still-valid
 * segments (SORT-RESUME-2's cascade variant). Originals and any surviving intermediate are instead
 * reclaimed by the caller only after a successful publish ({@link SortedDatasetCoordinator} already tracks and
 * deletes both). See contracts.md §6 ("Cascade-scale resume semantics") for the full durability
 * boundary and the accepted ~2× transient staging footprint this policy trades for
 * crash-recoverability.
 *
 * <p>A cascaded pass emits {@code SORT.merge_pass_cascaded}. Each
 * cascade pass ({@link #onePass}) also logs {@code sort_merge_pass_cascaded} per-pass detail — see
 * {@link #onePass} for why that line stays log-only.
 *
 * <p><b>Disjoint-copyable measurement.</b> Every cascade group and the final page merge report their
 * input-segment classification through {@link #recordDisjoint}, which fires
 * {@code SORT.merge_disjoint_copyable} once per fully-disjoint input segment and
 * {@code SORT.merge_interleaved_segment} once per segment that shared its run with at least one
 * other input — so the disjoint fraction is directly computable from the counters before anyone
 * builds a byte-copy fast path on the assumption it is common.
 */
final class KWayMerge<S> {

    private static final Logger log = LoggerFactory.getLogger(KWayMerge.class);

    /**
     * §3.2: rows drained out of an intermediate cascade pass are reported to a merge's
     * progress callback in batches of this size (never per-row — hot-path overhead on a
     * billion-row cascade), the same batching finalization uses for the final
     * streaming pass. Shared here (rather than duplicated in {@link SortedDatasetCoordinator}) so both passes
     * advance {@code swath.progress.units} at the same granularity.
     */
    static final long PROGRESS_BATCH_ROWS = 1_000L;

    /** Storage seam: open a segment for reading, write an intermediate from a merged cursor, delete. */
    interface SegmentIo<S> {
        /** Write the fully-merged {@code sorted} to a new intermediate segment; do not close {@code sorted}. */
        S writeIntermediate(SortedCursor sorted) throws IOException;

        /**
         * Reclaim a segment. {@link KWayMerge} only ever calls this on a segment it created itself via
         * {@link #writeIntermediate} on an earlier pass — never on one of {@code merge}'s original
         * inputs (see the class javadoc's deletion policy).
         */
        void delete(S segment) throws IOException;

        /** Open one page stream for the only supported cascade representation. */
        PageStream openPages(S segment) throws IOException;

        /** Decoded-page residency available after the opened streams' frontiers are priced. */
        default long decodedPageBudgetBytes(List<PageStream> streams) throws IOException {
            return Long.MAX_VALUE;
        }
    }

    /** Minimal page frontier used only by cascade passes; no range/seek machinery survives here. */
    interface PageStream extends AutoCloseable {
        /** Complete any deferred first-page read after the group has passed frontier admission. */
        default void initialize() throws IOException {
        }

        /** Upper bound on heap the encoded current-page frontier can retain, computed before it exists. */
        default long frontierRetainedBytes() {
            return 0;
        }

        boolean hasPage();

        byte[] minKey();

        byte[] maxKey();

        PageBlock decodeCurrentPage();

        void advance() throws IOException;

        @Override
        void close() throws IOException;
    }

    private final Comparator<ListEntry> comparator;
    private final int fanIn;
    private final SegmentIo<S> io;
    private final DuplicateHook hook;
    private final SortMetrics metrics;

    private long mergePasses;
    private long cascadedPasses;

    KWayMerge(Comparator<ListEntry> comparator, int fanIn, SegmentIo<S> io, DuplicateHook hook,
              SortMetrics metrics) {
        if (fanIn < 2) {
            throw new IllegalArgumentException("fan-in must be >= 2, got " + fanIn);
        }
        this.comparator = comparator;
        this.fanIn = fanIn;
        this.io = io;
        this.hook = hook;
        this.metrics = metrics;
    }

    /**
     * Reduce {@code segments} to &le; {@code fanIn} via cascaded passes, then open a final streaming
     * merge over the survivors. {@code segments} themselves (the originals) are never deleted by
     * this call, even across multiple cascade passes — see the class javadoc. No
     * merge-progress callback — see the overload below for the {@code swath.progress.units} (§3.2)
     * feed, threaded through every intermediate cascade pass, not just the final streaming pass.
     */
    SortedCursor merge(List<S> segments) throws IOException {
        return merge(segments, units -> { });
    }

    /**
     * Same as {@link #merge(List)}, plus {@code progressCallback} (§3.2): invoked, batched at
     * {@link #PROGRESS_BATCH_ROWS}, as each intermediate cascade pass ({@link #onePass}) drains rows
     * into a fresh segment — so a cascade that outlives the OTLP push interval still advances
     * {@code swath.progress.units} (the blind spot this counter exists to close). The final
     * streaming pass returned here is NOT wrapped — its rows are the caller's (e.g.
     * finalization drain) to report as they are actually written.
     */
    SortedCursor merge(List<S> segments, LongConsumer progressCallback) throws IOException {
        List<S> current = reduceToFanIn(segments, progressCallback);
        recordFinalPass();
        return openMerger(current, this::recordDisjoint);
    }

    /** Run only the unchanged cascade passes, leaving at most {@code fanIn} survivors for another finalizer. */
    List<S> reduceToFanIn(List<S> segments, LongConsumer progressCallback) throws IOException {
        List<S> current = new ArrayList<>(segments);
        boolean currentAreOriginals = true;
        while (current.size() > fanIn) {
            MergeCancellation.check();
            current = onePass(current, currentAreOriginals, progressCallback);
            currentAreOriginals = false;   // every subsequent pass's input is our OWN intermediate
        }
        return List.copyOf(current);
    }

    /** Account for the final pass supplied either by this merger or by the pipeline finalizer. */
    void recordFinalPass() {
        mergePasses++;
    }

    /**
     * The merger factory shared by the final merge pass and every intermediate cascade pass.
     */
    private SortedCursor openMerger(List<S> group, MergeRunSink disjointSink)
            throws IOException {
        List<PageStream> pages = openPages(group);
        try {
            long decodedBudget = io.decodedPageBudgetBytes(pages);
            for (PageStream page : pages) {
                page.initialize();
            }
            return new DuplicateReporting(new CascadePageMerger(
                    pages, comparator, metrics, disjointSink,
                    decodedBudget), comparator, hook);
        } catch (IOException | RuntimeException failure) {
            closePagesAfterFailedOpen(pages, failure);
            throw failure;
        }
    }

    private List<PageStream> openPages(List<S> group) throws IOException {
        List<PageStream> streams = new ArrayList<>(group.size());
        try {
            for (S segment : group) {
                streams.add(io.openPages(segment));
            }
            return List.copyOf(streams);
        } catch (IOException | RuntimeException failure) {
            closePagesAfterFailedOpen(streams, failure);
            throw failure;
        }
    }

    private static void closePagesAfterFailedOpen(List<PageStream> streams, Throwable failure) {
        for (PageStream stream : streams) {
            try {
                stream.close();
            } catch (IOException | RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    long mergePasses() {
        return mergePasses;
    }

    long cascadedPasses() {
        return cascadedPasses;
    }

    /**
     * Merge {@code segments} in groups of {@code <= fanIn} into fresh intermediate segments.
     * {@code segmentsAreOriginals} is {@code true} only for the very first pass over the caller's
     * input: originals are never deleted here (crash-recoverability, see the class javadoc) — only a
     * segment this class itself created on an earlier pass ({@code segmentsAreOriginals == false}) is
     * reclaimed once the next pass has folded it in.
     */
    private List<S> onePass(List<S> segments, boolean segmentsAreOriginals, LongConsumer progressCallback)
            throws IOException {
        long startNanos = System.nanoTime();
        List<S> merged = new ArrayList<>();
        long[] passCopyable = new long[1];       // this onePass call's totals, for the log line only —
        long[] passInterleaved = new long[1];     // the metrics themselves fire per-segment, below
        for (int i = 0; i < segments.size(); i += fanIn) {
            MergeCancellation.check();
            List<S> group = segments.subList(i, Math.min(i + fanIn, segments.size()));
            S dest;
            // Use the same merger factory as the final pass and report source-run classification.
            try (SortedCursor m = openMerger(group,
                    (copyable, interleaved) -> {
                        passCopyable[0] += copyable;
                        passInterleaved[0] += interleaved;
                        recordDisjoint(copyable, interleaved);
                    })) {
                // Wrap the pass's merged cursor so io.writeIntermediate's full drain (whatever the
                // SegmentIo implementation does under the hood) still advances progress in batches.
                // The wrapper's close() is deliberately a no-op; this scope owns the real close.
                ProgressTrackingCursor tracked = new ProgressTrackingCursor(m, progressCallback);
                dest = io.writeIntermediate(tracked);
                tracked.flushRemainder();
            }
            if (!segmentsAreOriginals) {
                for (S consumed : group) {
                    io.delete(consumed);   // our own earlier-pass intermediate — safe to reclaim now
                }
            }
            merged.add(dest);
        }
        mergePasses++;
        cascadedPasses++;
        metrics.recordStealReason("SORT", "merge_pass_cascaded");
        // merge_ms in the summary is the TOTAL across every pass —
        // this per-pass line (log-only, no new summary field, schema kept stable) is what makes an
        // individual cascade pass's cost visible without dividing the total by a guessed pass count.
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        log.info("sort_merge_pass_cascaded pass={} inputs={} output_segments={} duration_ms={} "
                        + "copyable={} interleaved={}",
                cascadedPasses, segments.size(), merged.size(), durationMs, passCopyable[0], passInterleaved[0]);
        return merged;
    }

    /**
     * Called once per cascade group with that pass's disjoint-copyable classification. This fires the
     * metric once per <em>segment</em> (not once per pass with the total) so the fraction
     * copyable-vs-interleaved is directly countable post-hoc.
     */
    private void recordDisjoint(long copyableSegments, long interleavedSegments) {
        for (long i = 0; i < copyableSegments; i++) {
            metrics.recordStealReason("SORT", "merge_disjoint_copyable");
        }
        for (long i = 0; i < interleavedSegments; i++) {
            metrics.recordStealReason("SORT", "merge_interleaved_segment");
        }
    }

    /**
     * Wraps a {@link SortedCursor} so rows drained through it advance {@code progressCallback}
     * in batches of {@link #PROGRESS_BATCH_ROWS}, flushing any remainder once the delegate is fully
     * drained — finalization's batching, applied to an intermediate cascade
     * pass instead of the final streaming pass. {@link #close()} is deliberately a no-op: the call
     * site ({@link #onePass}) owns the delegate's lifecycle via its own try-with-resources, so this
     * wrapper must never double-close it.
     */
    private static final class ProgressTrackingCursor implements SortedCursor {
        private final SortedCursor delegate;
        private final LongConsumer progressCallback;
        private long batchRows;

        ProgressTrackingCursor(SortedCursor delegate, LongConsumer progressCallback) {
            this.delegate = delegate;
            this.progressCallback = progressCallback;
        }

        @Override
        public boolean hasNext() {
            MergeCancellation.check();
            return delegate.hasNext();
        }

        @Override
        public ListEntry next() {
            MergeCancellation.check();
            ListEntry entry = delegate.next();
            if (++batchRows >= PROGRESS_BATCH_ROWS) {
                progressCallback.accept(batchRows);
                batchRows = 0;
            }
            return entry;
        }

        /** Flush any partial batch (< {@link #PROGRESS_BATCH_ROWS}) once the delegate is drained. */
        void flushRemainder() {
            if (batchRows > 0) {
                progressCallback.accept(batchRows);
                batchRows = 0;
            }
        }

        @Override
        public void close() {
            // Deliberately a no-op: onePass's try-with-resources owns the real close.
        }
    }
}
