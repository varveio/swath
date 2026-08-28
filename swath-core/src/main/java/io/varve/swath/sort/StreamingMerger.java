/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.LongConsumer;

/**
 * Single-pass k-way merge of N sorted {@link EntryStream}s into one sorted {@link SortedCursor},
 * using a {@link PriorityQueue} keyed on each stream's head under the shared
 * {@link ListEntryComparator}.
 *
 * <p><b>Same-reader fast path.</b> After emitting from stream {@code R}, if {@code R}'s next head
 * is still {@code <=} the heap's current minimum (compared <em>before</em> reinsertion), the next
 * entry is emitted directly from {@code R} without touching the heap. The sorter's per-node runs
 * and disjoint-range segments make long monotone stretches from one stream the common case; each
 * such emission increments a local counter — {@code fastPathSink} is invoked <b>once</b>, in
 * {@link #close()}, with the accumulated total ({@code SORT.merge_fastpath}) — a big merge can take
 * the fast path millions of times, so the per-row metric call is avoided.
 *
 * <p>Read failures surface as {@link UncheckedIOException}. Adjacent duplicate reporting is applied
 * once around this merger's output by {@link DuplicateReporting}.
 *
 * <p><b>Disjoint-copyable classification.</b> Before building a byte-copy merge for
 * segments that are range-disjoint from every other input (so their rows could be copied straight
 * through instead of decoded), we need to measure how often that pattern actually occurs. The
 * cheapest sound signal is derived for free from the merge order itself, rather than an extra
 * footer/row-group min/max read (a second pass per segment, and subject to the exact statistics
 * truncation hazard {@link SortedFileIndex} already documents for why min/max may not equal the true
 * bound): a segment is "fully disjoint copyable" exactly when every row drawn from it in the merged
 * output forms <b>one uninterrupted run</b> — the merge never switches away to another stream and
 * back before that segment is exhausted. <b>Assuming distinct keys across segments</b> — true for
 * unique S3 object keys, the production case — any segment whose true key range overlaps another's
 * must produce at least one such interruption (the sorted order forces the overlapping rows to
 * interleave), so "one run" is then exactly "range-disjoint from every other input". Under
 * cross-segment <em>duplicate</em> keys (only reachable via the §0.5 sort-fixture duplicate-data
 * path, never live S3 listing) the equivalence can go slack at a shared boundary value — e.g. segment
 * {@code A=[m]} against {@code B=[a,m,z]} still emits {@code A} as one run even though {@code A}'s key
 * sits inside {@code B}'s range — so a duplicate-adjacent segment may be counted "copyable" here while
 * not being provably range-disjoint; this is a one-sided, rare slack (never the reverse: a genuine
 * interruption is never miscounted as one run), acceptable for a measurement-only counter. A segment
 * with more than one run is reported "interleaved". Each {@link #computeNext()} call that switches
 * the physical source stream increments that stream's run counter; at {@link #close()}, a stream with
 * exactly one run is "disjoint copyable", one with more than one run is "interleaved" (a stream that
 * contributed zero rows, e.g. an empty input, is reported as neither). Cost is O(1) extra per emitted
 * row (a reference compare plus, on a run change only, one map lookup) — no extra decode pass.
 */
final class StreamingMerger implements SortedCursor {

    /**
     * Reports, once at {@link #close()}, this merge's count of "fully disjoint copyable" input
     * segments (every emitted row formed one uninterrupted run) versus "interleaved" segments (more
     * than one run) — see the class javadoc's disjoint-copyable classification. {@link #NO_OP} is the
     * default for callers that don't need the counters (e.g. {@link SealedBuffer}'s in-memory
     * seal-path merge, out of scope for this measurement).
     */
    @FunctionalInterface
    interface DisjointSink {
        DisjointSink NO_OP = (copyableSegments, interleavedSegments) -> { };

        void accept(long copyableSegments, long interleavedSegments);
    }

    private final Comparator<ListEntry> comparator;
    private final LongConsumer fastPathSink;
    private final DisjointSink disjointSink;
    private final List<EntryStream> allStreams;
    private final PriorityQueue<EntryStream> heap;
    private final IdentityHashMap<EntryStream, Integer> streamIndex;
    private final int[] runCounts;   // per-input-stream count of runs it contributed to the output

    private EntryStream currentStream;   // stream held out of the heap (fast-path candidate)
    private ListEntry pending;           // next entry to return; null once fully drained
    private long fastPathCount;
    private boolean closed;

    StreamingMerger(List<EntryStream> streams, Comparator<ListEntry> comparator,
                    LongConsumer fastPathSink) {
        this(streams, comparator, fastPathSink, DisjointSink.NO_OP);
    }

    StreamingMerger(List<EntryStream> streams, Comparator<ListEntry> comparator,
                    LongConsumer fastPathSink, DisjointSink disjointSink) {
        this.comparator = comparator;
        this.fastPathSink = fastPathSink;
        this.disjointSink = disjointSink;
        this.allStreams = streams;
        this.heap = new PriorityQueue<>((a, b) -> comparator.compare(a.peek(), b.peek()));
        this.streamIndex = new IdentityHashMap<>(streams.size());
        this.runCounts = new int[streams.size()];
        for (int i = 0; i < streams.size(); i++) {
            streamIndex.put(streams.get(i), i);
        }
        for (EntryStream s : streams) {
            if (s.hasNext()) {
                heap.add(s);
            }
        }
        // computeNext() can observe cooperative range cancellation before construction completes.
        // In that case close() would otherwise be unreachable and every already-open input stream
        // would leak until GC, defeating ParallelRangeMerge's quiescent cleanup guarantee.
        try {
            this.pending = computeNext();
        } catch (RuntimeException e) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return pending != null;
    }

    @Override
    public ListEntry next() {
        if (pending == null) {
            throw new NoSuchElementException();
        }
        ListEntry result = pending;
        pending = computeNext();
        return result;
    }

    private ListEntry computeNext() {
        MergeCancellation.check();
        try {
            EntryStream src;
            if (currentStream != null && currentStream.hasNext()
                    && (heap.isEmpty() || comparator.compare(currentStream.peek(), heap.peek().peek()) <= 0)) {
                // Fast path: the last stream's head still beats the heap minimum.
                src = currentStream;
                fastPathCount++;   // accumulated locally; reported once in close()
            } else {
                if (currentStream != null && currentStream.hasNext()) {
                    heap.add(currentStream);   // it lost the lead — back into contention
                }
                if (heap.isEmpty()) {
                    currentStream = null;
                    return null;
                }
                src = heap.poll();
            }
            if (src != currentStream) {
                // A new run starts for src (the sorted-merge invariant makes this exact — see the
                // class javadoc's disjoint-copyable classification).
                runCounts[streamIndex.get(src)]++;
            }
            currentStream = src;
            return src.next();
        } catch (IOException e) {
            throw new UncheckedIOException("k-way merge read failed", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        fastPathSink.accept(fastPathCount);   // once per merge/pass, not once per row
        long copyableSegments = 0;
        long interleavedSegments = 0;
        for (int runCount : runCounts) {
            if (runCount == 1) {
                copyableSegments++;
            } else if (runCount > 1) {
                interleavedSegments++;
            }
            // runCount == 0: the input contributed no row (e.g. an empty segment) — counted as neither.
        }
        disjointSink.accept(copyableSegments, interleavedSegments);
        List<IOException> failures = new ArrayList<>();
        for (EntryStream s : allStreams) {
            try {
                s.close();
            } catch (IOException e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            UncheckedIOException wrapped = new UncheckedIOException("closing entry streams failed", failures.get(0));
            for (int i = 1; i < failures.size(); i++) {
                wrapped.addSuppressed(failures.get(i));
            }
            throw wrapped;
        }
    }
}
