/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
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
 * <p>Every pass — each cascade pass ({@link #onePass}) and the final pass — selects its merger through
 * the one {@link #openMerger} factory: a {@link PageAwareMerger} when every segment in the group exposes
 * a {@link PageFrontierStream} (decode-free frontier + page-whole fast path + both overlap guards), else
 * the entry-typed {@link StreamingMerger} (heap merge + same-reader fast path), then wraps either
 * output in {@link DuplicateReporting}. This keeps the intra-segment/cross-segment overlap guards in
 * force and reports adjacent equals exactly once on EVERY pass, so a cascade intermediate is never
 * written from a mis-ordered cursor.
 *
 * <p><b>Merge-phase memory + fd bound (I11).</b> {@code fanIn} here is expected to already be the
 * RUNTIME-clamped fan-in {@link SortTransform} computes — the MIN of the static
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
 * reclaimed by the caller only after a successful publish ({@link SortTransform} already tracks and
 * deletes both). See contracts.md §6 ("Cascade-scale resume semantics") for the full durability
 * boundary and the accepted ~2× transient staging footprint this policy trades for
 * crash-recoverability.
 *
 * <p>A cascaded pass emits {@code SORT.merge_pass_cascaded}; {@code SORT.merge_fastpath} fires once
 * per pass that engaged the fast path (the accumulated total, not once per row), via
 * {@link StreamingMerger#close()}. Pass and fast-path counts are exposed for the JSON summary. Each
 * cascade pass ({@link #onePass}) also logs {@code sort_merge_pass_cascaded} per-pass detail — see
 * {@link #onePass} for why that line stays log-only.
 *
 * <p><b>Disjoint-copyable measurement.</b> Every {@link StreamingMerger} pass
 * (each cascade group, and the final streaming pass) classifies its input segments per
 * {@link StreamingMerger}'s class javadoc and reports here via {@link #recordDisjoint}, which fires
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
     * billion-row cascade), the same batching {@link RolledPartWriter#drain} uses for the final
     * streaming pass. Shared here (rather than duplicated in {@link SortTransform}) so both passes
     * advance {@code swath.progress.units} at the same granularity.
     */
    static final long PROGRESS_BATCH_ROWS = 1_000L;

    /** Storage seam: open a segment for reading, write an intermediate from a merged cursor, delete. */
    interface SegmentIo<S> {
        EntryStream open(S segment) throws IOException;

        /** Write the fully-merged {@code sorted} to a new intermediate segment; do not close {@code sorted}. */
        S writeIntermediate(SortedCursor sorted) throws IOException;

        /**
         * Reclaim a segment. {@link KWayMerge} only ever calls this on a segment it created itself via
         * {@link #writeIntermediate} on an earlier pass — never on one of {@code merge}'s original
         * inputs (see the class javadoc's deletion policy).
         */
        void delete(S segment) throws IOException;

        /**
         * Page-run capability: whether {@code segment} can be opened as a decode-free page
         * frontier ({@link #openFrontier}) so the FINAL merge pass can take {@link PageAwareMerger}'s
         * page-whole fast path. Default {@code false} — a non-page-run store keeps the entry-typed
         * {@link StreamingMerger}. The final pass uses {@link PageAwareMerger} only when EVERY survivor
         * reports {@code true}; generic non-page-frontier stores stay on {@link StreamingMerger}.
         */
        default boolean supportsPageFrontier(S segment) {
            return false;
        }

        /**
         * Open a decode-free page frontier over a page-run {@code segment}. Only valid when
         * {@link #supportsPageFrontier(Object)} is {@code true} for it.
         */
        default PageFrontierStream openFrontier(S segment) throws IOException {
            throw new UnsupportedOperationException("segment does not support a page frontier: " + segment);
        }

        /**
         * Aggregate decoded-page residency available to one merger instance. Called before any
         * frontier body is opened so a page-run implementation can preflight intermediate trailers.
         */
        default long decodedPageBudgetBytes(List<S> segments) throws IOException {
            return Long.MAX_VALUE;
        }
    }

    private final Comparator<ListEntry> comparator;
    private final int fanIn;
    private final SegmentIo<S> io;
    private final DuplicateHook hook;
    private final SortMetrics metrics;

    private long mergePasses;
    private long cascadedPasses;
    private long fastPathEmissions;

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
     * merge over the survivors. The returned cursor's fast-path emissions accumulate into
     * {@link #fastPathEmissions()} as it is consumed. {@code segments} themselves (the originals) are
     * never deleted by this call, even across multiple cascade passes — see the class javadoc. No
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
     * {@link RolledPartWriter#drain}) to report as they are actually written.
     */
    SortedCursor merge(List<S> segments, LongConsumer progressCallback) throws IOException {
        List<S> current = new ArrayList<>(segments);
        boolean currentAreOriginals = true;
        while (current.size() > fanIn) {
            MergeCancellation.check();
            current = onePass(current, currentAreOriginals, progressCallback);
            currentAreOriginals = false;   // every subsequent pass's input is our OWN intermediate
        }
        mergePasses++;   // the final streaming pass
        // The final pass selects its merger via the SAME openMerger factory every cascade
        // pass uses (below) — one place decides PageAware-vs-Streaming. If every survivor is a page-run
        // segment it runs the page-aware merger (decode-free frontier + page-whole fast path + both
        // overlap guards); a generic non-frontier store keeps the entry-typed StreamingMerger. The
        // two produce byte-identical output.
        return openMerger(current, this::recordDisjoint);
    }

    /**
     * The single merger-selection path shared by the final merge pass ({@link #merge}) AND every
     * intermediate cascade pass ({@link #onePass}). If every segment in
     * {@code group} exposes a {@link PageFrontierStream}, open a {@link PageAwareMerger} — so the
     * page-whole fast path, cross-segment overlap handling, and intra-segment ordering guards apply,
     * and even a cascade intermediate is written from a
     * correctly-guarded cursor. Otherwise a generic segment store falls back to the entry-typed
     * {@link StreamingMerger}. Routing every pass —
     * not only the final one — through this factory keeps an all-page-run cascade group on the
     * page-whole fast path (and {@link PageAwareMerger}'s cross-segment guard) rather than decoding
     * every page through the entry heap; it is not an ordering requirement. A page-run input handed
     * to the trusting {@link StreamingMerger} is opened as a {@link PageRunSegmentReader} — itself a
     * single-segment {@link PageAwareMerger} that rejects corrupt overlap and comparator-merges a legal
     * VERSIONS equality seam before the heap sees it — so a cascade intermediate is never mis-ordered,
     * whichever merger the group selects.
     */
    private SortedCursor openMerger(List<S> group, MergeRunSink disjointSink)
            throws IOException {
        SortedCursor merger;
        if (allSupportPageFrontier(group)) {
            long decodedBudget = io.decodedPageBudgetBytes(group);
            List<PageFrontierStream> frontiers = openFrontiers(group);
            merger = new PageAwareMerger(frontiers, comparator, MergeScope.CROSS_SEGMENT, metrics,
                    disjointSink, decodedBudget);
        } else {
            List<EntryStream> streams = open(group);
            merger = new StreamingMerger(streams, comparator, this::recordFastPath, disjointSink);
        }
        return new DuplicateReporting(merger, comparator, hook);
    }

    private boolean allSupportPageFrontier(List<S> segments) {
        if (segments.isEmpty()) {
            return false;   // empty listing: the StreamingMerger path publishes the empty file
        }
        for (S s : segments) {
            if (!io.supportsPageFrontier(s)) {
                return false;
            }
        }
        return true;
    }

    private List<PageFrontierStream> openFrontiers(List<S> segments) throws IOException {
        List<PageFrontierStream> streams = new ArrayList<>(segments.size());
        try {
            for (S s : segments) {
                streams.add(io.openFrontier(s));
            }
        } catch (IOException | RuntimeException e) {
            for (PageFrontierStream s : streams) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best effort on the error path
                }
            }
            throw e;
        }
        return streams;
    }

    long mergePasses() {
        return mergePasses;
    }

    long cascadedPasses() {
        return cascadedPasses;
    }

    long fastPathEmissions() {
        return fastPathEmissions;
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
            // Select the group's merger through the SAME openMerger factory the final
            // pass uses — so a cascade group of page-run segments is merged (and its intermediate
            // written) through the guarded PageAwareMerger, not the trusting StreamingMerger. Both
            // raw merger branches report source-run classification through the same sink.
            try (SortedCursor m = openMerger(group,
                    (copyable, interleaved) -> {
                        passCopyable[0] += copyable;
                        passInterleaved[0] += interleaved;
                        recordDisjoint(copyable, interleaved);
                    })) {
                // Wrap the pass's merged cursor so io.writeIntermediate's full drain (whatever the
                // SegmentIo implementation does under the hood) still advances progress in batches —
                // the wrapper's close() is deliberately a no-op; this try-with-resources on `m` (the
                // StreamingMerger, not the wrapper) owns the real close.
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
     * Called once by a {@link StreamingMerger} at {@link StreamingMerger#close()} with the total
     * fast-path emissions for that pass — never per row. {@link #fastPathEmissions} still
     * accumulates the exact total across all passes for the JSON summary; the metric
     * itself only fires (once per pass) when this pass actually took the fast path.
     */
    private void recordFastPath(long n) {
        fastPathEmissions += n;
        if (n > 0) {
            metrics.recordStealReason("SORT", "merge_fastpath");
        }
    }

    /**
     * Called once per raw merger (i.e. per cascade group, or once for the final pass) with that
     * pass's disjoint-copyable classification. Unlike {@link #recordFastPath}, this fires the
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

    private List<EntryStream> open(List<S> segments) throws IOException {
        List<EntryStream> streams = new ArrayList<>(segments.size());
        try {
            for (S s : segments) {
                streams.add(io.open(s));
            }
        } catch (IOException | RuntimeException e) {
            for (EntryStream s : streams) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best effort on the error path
                }
            }
            throw e;
        }
        return streams;
    }

    /**
     * Wraps a {@link SortedCursor} so rows drained through it advance {@code progressCallback}
     * in batches of {@link #PROGRESS_BATCH_ROWS}, flushing any remainder once the delegate is fully
     * drained — {@link RolledPartWriter#drain}'s batching, applied to an intermediate cascade
     * pass instead of the final streaming pass. {@link #close()} is deliberately a no-op: the call
     * site ({@link #onePass}) owns the delegate's lifecycle via its own try-with-resources on the
     * underlying {@link StreamingMerger}, so this wrapper must never double-close it.
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
            // Deliberately a no-op — see the class javadoc: onePass's try-with-resources on the
            // underlying StreamingMerger owns the real close.
        }
    }
}
