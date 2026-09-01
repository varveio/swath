/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.ListEntry;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortedEntryCursor;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageBlockCursor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Page-whole cascade merger for page-run inputs. A page is streamed directly when its maximum is
 * below every successor frontier; only a transitively overlapping component enters the shared
 * {@link PageRowMerger}. Disjoint pages therefore stay off the per-row heap.
 */
final class CascadePageMerger implements SortedEntryCursor, LogicalMergeCompletion {

    private final List<CascadeReducer.PageStream> streams;
    private final PriorityQueue<Source> frontiers;
    private final PageRowMerger rows;
    private final DecodedPageBudget budget;
    private final SortMetrics metrics;
    private final MergeRunSink runSink;
    private final MergeRunTracker sourceRuns;

    private PageBlockCursor whole;
    private long wholeBytes;
    private byte[] overlapCeiling;
    private ListEntry pending;
    private boolean logicalMergeComplete;
    private boolean closed;

    CascadePageMerger(List<CascadeReducer.PageStream> streams,
            Comparator<ListEntry> comparator, SortMetrics metrics,
            MergeRunSink runSink, long decodedPageBudgetBytes) {
        this.streams = List.copyOf(streams);
        this.metrics = metrics;
        this.runSink = runSink;
        this.rows = new PageRowMerger(comparator);
        this.budget = new DecodedPageBudget(decodedPageBudgetBytes, metrics);
        this.sourceRuns = new MergeRunTracker(streams.size());
        this.frontiers = new PriorityQueue<>((left, right) ->
                Arrays.compareUnsigned(left.stream.minKey(), right.stream.minKey()));
        for (int source = 0; source < streams.size(); source++) {
            CascadeReducer.PageStream stream = streams.get(source);
            if (stream.hasPage()) {
                frontiers.add(new Source(source, stream));
            } else {
                metrics.recordStealReason("SORT", "cascade_page_empty_segment");
            }
        }
        try {
            pending = computeNext();
        } catch (RuntimeException failure) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
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
        try {
            while (true) {
                MergeCancellation.check();
                if (whole != null) {
                    if (whole.hasNext()) {
                        return whole.next();
                    }
                    whole = null;
                    budget.release(wholeBytes);
                    wholeBytes = 0;
                }
                if (rows.hasNext()) {
                    closeOverlapUnderFrontier();
                    ListEntry result = rows.next();
                    sourceRuns.emittedFrom(rows.lastSource());
                    budget.release(rows.releasedBytes());
                    if (!rows.hasNext()) {
                        overlapCeiling = null;
                    }
                    return result;
                }
                if (frontiers.isEmpty()) {
                    logicalMergeComplete = true;
                    return null;
                }
                planNextPage();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("cascade page merge read failed", failure);
        }
    }

    private void planNextPage() throws IOException {
        Source source = frontiers.poll();
        byte[] pageMax = source.stream.maxKey().clone();
        PageBlock page = source.stream.decodeCurrentPage();
        long retainedBytes = budget.reserve(page);
        try {
            source.stream.advance();
            if (source.stream.hasPage()) {
                frontiers.add(source);
            }
            boolean wholePage = frontiers.isEmpty()
                    || Arrays.compareUnsigned(frontiers.peek().stream.minKey(), pageMax) > 0;
            if (wholePage) {
                metrics.recordStealReason("SORT", "cascade_page_whole_merge");
                sourceRuns.emittedFrom(source.ordinal);
                whole = page.cursor();
                wholeBytes = retainedBytes;
                retainedBytes = 0;
            } else {
                metrics.recordStealReason("SORT", "cascade_page_overlap_merge");
                rows.add(source.ordinal, page, retainedBytes);
                retainedBytes = 0;
                overlapCeiling = pageMax;
            }
        } finally {
            budget.release(retainedBytes);
        }
    }

    private void closeOverlapUnderFrontier() throws IOException {
        while (!frontiers.isEmpty()
                && Arrays.compareUnsigned(
                        frontiers.peek().stream.minKey(), overlapCeiling) <= 0) {
            Source source = frontiers.poll();
            byte[] pageMax = source.stream.maxKey().clone();
            PageBlock page = source.stream.decodeCurrentPage();
            long retainedBytes = budget.reserve(page);
            try {
                source.stream.advance();
                if (source.stream.hasPage()) {
                    frontiers.add(source);
                }
                rows.add(source.ordinal, page, retainedBytes);
                retainedBytes = 0;
                if (Arrays.compareUnsigned(pageMax, overlapCeiling) > 0) {
                    overlapCeiling = pageMax;
                }
            } finally {
                budget.release(retainedBytes);
            }
        }
    }

    @Override
    public void completeLogicalMerge() {
        logicalMergeComplete = true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        if (whole != null) {
            try {
                whole.drainAndValidate();
            } catch (RuntimeException validationFailure) {
                failure = validationFailure;
            } finally {
                budget.release(wholeBytes);
                wholeBytes = 0;
            }
        }
        RuntimeException rowFailure = rows.drainAndValidate();
        if (rowFailure != null) {
            failure = append(failure, rowFailure);
        }
        budget.release(rows.releaseAllBytes());
        metrics.recordPipelineDecodedPagePeak(budget.peakResidentBytes());
        for (CascadeReducer.PageStream stream : streams) {
            try {
                stream.close();
            } catch (IOException closeFailure) {
                failure = append(failure,
                        new UncheckedIOException("closing cascade page stream failed", closeFailure));
            } catch (RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
        }
        if (logicalMergeComplete && failure == null) {
            long copyable = 0;
            long interleaved = 0;
            for (int source = 0; source < streams.size(); source++) {
                int runs = sourceRuns.count(source);
                if (runs == 1) {
                    copyable++;
                } else if (runs > 1) {
                    interleaved++;
                }
            }
            runSink.accept(copyable, interleaved);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException append(RuntimeException first, RuntimeException next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }

    private record Source(int ordinal, CascadeReducer.PageStream stream) {
    }
}
