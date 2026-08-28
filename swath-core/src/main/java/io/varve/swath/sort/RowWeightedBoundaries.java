/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Bounded row-mass boundary selection over the canonical distinct candidate set. */
final class RowWeightedBoundaries {

    private RowWeightedBoundaries() {
    }

    /**
     * Return row-quantile boundaries, or {@code null} after recording one exact fallback reason.
     * A mixed or legacy input is deliberately all-or-nothing: combining type-2 mass with unweighted
     * candidates would give the arm a biased denominator while looking successfully engaged.
     */
    static List<byte[]> select(List<PageRunSegmentDescriptor> segments, List<byte[]> candidates,
                               int desiredRanges, SortMetrics metrics) throws IOException {
        InputKind kind = classifyRowInputs(segments);
        if (kind != InputKind.TYPE2) {
            recordRowsFallback(kind, metrics);
            return null;
        }

        long[] weights = new long[candidates.size()];
        for (PageRunSegmentDescriptor descriptor : segments) {
            MergeCancellation.check();
            accumulate(descriptor, candidates, weights, metrics);
        }
        long total = 0;
        for (long weight : weights) {
            total = saturatedAdd(total, weight);
        }
        if (total == 0) {
            metrics.recordStealReason("SORT", "merge_boundary_rows_fallback_zero_mass");
            return null;
        }

        int ranges = Math.min(desiredRanges, candidates.size());
        List<byte[]> boundaries = quantiles(candidates, weights, total, ranges);
        metrics.recordStealReason("SORT", "merge_boundary_rows_on");
        return boundaries;
    }

    private static void accumulate(PageRunSegmentDescriptor descriptor, List<byte[]> candidates,
                                   long[] weights, SortMetrics metrics) throws IOException {
        try (PageRunSegmentIo io = PageRunSegmentIo.open(descriptor.path(), SortMetrics.NO_OP)) {
            if (io.fileSize != descriptor.fileSize() || io.trailerStart != descriptor.trailerStart()) {
                throw new SegmentCorruptionException(descriptor.path(),
                        SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH,
                        "segment changed after its row-boundary index was validated");
            }
            PageRunPageIndex.Cursor cursor = PageRunPageIndex.cursor(io, descriptor.extension());
            PageRunPageIndex.IndexEntry previous = null;
            while (cursor.hasNext()) {
                MergeCancellation.check();
                PageRunPageIndex.IndexEntry current = cursor.next().entry();
                if (previous != null) {
                    addWeight(candidates, weights, previous.minKey(),
                            current.cumulativeEntries() - previous.cumulativeEntries());
                }
                previous = current;
                metrics.markProgress();
            }
            if (previous != null) {
                addWeight(candidates, weights, previous.minKey(),
                        descriptor.trailer().totalEntries() - previous.cumulativeEntries());
            }
            metrics.recordRangeIndexBytes(cursor.bytesRead());
        }
    }

    /** Map a dropped bottom-hash candidate to its retained predecessor, preserving CDF order. */
    private static void addWeight(List<byte[]> candidates, long[] weights, byte[] key, long weight) {
        if (weight <= 0) {
            return;
        }
        int lo = 0;
        int hi = candidates.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (KeyBytes.compareUnsigned(candidates.get(mid), key) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        int bucket = Math.max(0, lo - 1);
        weights[bucket] = saturatedAdd(weights[bucket], weight);
    }

    /** Pick strictly increasing candidate indices whose prefix masses are nearest each quantile. */
    private static List<byte[]> quantiles(List<byte[]> candidates, long[] weights,
                                          long total, int ranges) {
        long[] prefixBefore = prefixBeforeInPlace(weights);

        List<byte[]> boundaries = new ArrayList<>(ranges - 1);
        int previous = 0;
        for (int j = 1; j < ranges; j++) {
            MergeCancellation.check();
            long target = quantile(total, j, ranges);
            int min = previous + 1;
            int max = candidates.size() - (ranges - j);
            int insertion = lowerBound(prefixBefore, target, min, max + 1);
            int upper = Math.min(max, insertion);
            int lower = Math.max(min, upper - 1);
            int chosen = distance(prefixBefore[lower], target)
                            <= distance(prefixBefore[upper], target)
                    ? lower : upper;
            boundaries.add(candidates.get(chosen));
            previous = chosen;
        }
        return List.copyOf(boundaries);
    }

    /** Convert histogram weights to prefix-before mass in the same policy-sized array. */
    static long[] prefixBeforeInPlace(long[] weights) {
        long prefix = 0;
        for (int i = 0; i < weights.length; i++) {
            long weight = weights[i];
            weights[i] = prefix;
            prefix = saturatedAdd(prefix, weight);
        }
        return weights;
    }

    private static int lowerBound(long[] values, long target, int from, int to) {
        int lo = from;
        int hi = to;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (values[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static long quantile(long total, int numerator, int denominator) {
        long quotient = total / denominator;
        long remainder = total % denominator;
        return quotient * numerator + remainder * numerator / denominator;
    }

    private static long distance(long value, long target) {
        return value >= target ? value - target : target - value;
    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static InputKind classifyRowInputs(List<PageRunSegmentDescriptor> segments) {
        InputKind found = null;
        for (PageRunSegmentDescriptor descriptor : segments) {
            InputKind current = switch (descriptor.extension().status()) {
                case EMBEDDED -> InputKind.TYPE2;
                case EMBEDDED_MINIMA_ONLY -> InputKind.TYPE1;
                case ABSENT -> InputKind.EXTENSIONLESS;
                default -> InputKind.INVALID;
            };
            if (found == null) {
                found = current;
            } else if (found != current) {
                return InputKind.MIXED;
            }
        }
        return found == null ? InputKind.INVALID : found;
    }

    private static void recordRowsFallback(InputKind kind, SortMetrics metrics) {
        switch (kind) {
            case EXTENSIONLESS -> metrics.recordStealReason(
                    "SORT", "merge_boundary_rows_fallback_extensionless");
            case TYPE1 -> metrics.recordStealReason(
                    "SORT", "merge_boundary_rows_fallback_type1");
            case INVALID -> metrics.recordStealReason(
                    "SORT", "merge_boundary_rows_fallback_invalid");
            case MIXED -> metrics.recordStealReason(
                    "SORT", "merge_boundary_rows_fallback_mixed");
            case TYPE2 -> throw new AssertionError("type-2 input does not fall back");
        }
    }

    private enum InputKind {
        TYPE2,
        EXTENSIONLESS,
        TYPE1,
        INVALID,
        MIXED
    }
}
