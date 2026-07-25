/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

/**
 * The flat {@code shape} feature-vector accumulators (end-of-run keyspace-classification signals;
 * §5): the run-level alphabet-cardinality union, the split-pivot divergence-depth histogram, and the
 * delimiter fan-out running stats. Each is folded once per completed node / split commit / probe
 * (never per key).
 *
 * <p>Extracted from {@code RunMetrics}. The {@code alphabetPositions}/{@code
 * divergenceDepthBuckets} dimensions ({@link #ALPHABET_POSITIONS}/{@link #DIVERGENCE_DEPTH_BUCKETS})
 * now live here (relocated); {@code RunMetrics} keeps delegating aliases for its own
 * pinned-surface callers. {@code massSkewGini}/latency percentiles are sourced from other subsystems
 * (the {@code CHILD_MASS} steal counters, the {@code swath.api.latency} timer) and passed into {@link
 * #buildSummary} rather than owned here.
 */
final class ShapeAccumulator {

    /** Number of relative code-point positions in the run-level alphabet cardinality AGGREGATE. */
    static final int ALPHABET_POSITIONS = 8;
    /** Number of divergence-depth histogram buckets; the last bucket is depth {@code >= 15}. */
    static final int DIVERGENCE_DEPTH_BUCKETS = 16;

    private final int alphabetPositions;
    private final int divergenceDepthBuckets;
    /** Union of printable-ASCII presence masks (2 words/position) OR-ed across every completed node. */
    private final AtomicLongArray alphabetMask;
    /** Split-pivot LCP-depth histogram — capped index from {@link #recordDivergenceDepth}. */
    private final AtomicLongArray divergenceDepthHistogram;
    /** Max/total delimiter=/ children seen at any single structure/seed probe, and the probe count. */
    private final AtomicLong delimiterFanoutMax = new AtomicLong();
    private final AtomicLong delimiterFanoutTotal = new AtomicLong();
    private final AtomicLong delimiterProbes = new AtomicLong();

    ShapeAccumulator(int alphabetPositions, int divergenceDepthBuckets) {
        this.alphabetPositions = alphabetPositions;
        this.divergenceDepthBuckets = divergenceDepthBuckets;
        this.alphabetMask = new AtomicLongArray(alphabetPositions * 2);
        this.divergenceDepthHistogram = new AtomicLongArray(divergenceDepthBuckets);
    }

    /**
     * Fold one completed node's per-position printable-ASCII presence masks (2 words per relative
     * position, from {@code AlphabetDigest.maskWords()}) into the run-level cardinality AGGREGATE by
     * OR-ing the observed-scalar sets. Called once per completed node (never per key); the per-position
     * union popcount is read out in {@link #buildSummary} as {@code alphabet_cardinality}.
     */
    void recordAlphabet(long[] maskWords) {
        if (maskWords == null) {
            return;
        }
        int n = Math.min(maskWords.length, alphabetMask.length());
        for (int i = 0; i < n; i++) {
            long w = maskWords[i];
            if (w != 0L) {
                alphabetMask.accumulateAndGet(i, w, (a, b) -> a | b);
            }
        }
    }

    /**
     * Fold one split commit's pivot-divergence LCP depth into the run-level divergence-depth histogram
     * (the byte position where this split's pivot diverges from its range's cursor), capped at the last
     * bucket. One bump per split commit, never per key.
     */
    void recordDivergenceDepth(int lcpDepth) {
        divergenceDepthHistogram.incrementAndGet(Math.min(lcpDepth, divergenceDepthBuckets - 1));
    }

    /**
     * Record the delimiter=/ fan-out (distinct child directories) observed at one structure/seed
     * probe. Tracks the widest fan-out seen, the running total, and the probe count so post-hoc
     * analysis reads a bucket's branching factor from the metrics alone. One update per probe.
     */
    void recordDelimiterFanout(int childCount) {
        if (childCount < 0) {
            return;
        }
        delimiterProbes.incrementAndGet();
        delimiterFanoutTotal.addAndGet(childCount);
        delimiterFanoutMax.accumulateAndGet(childCount, Math::max);
    }

    /**
     * The flat {@code shape} feature-vector, assembled once at end-of-run from the accumulators above
     * — a run-level AGGREGATE that (for a {@code --max-duration}/interrupted run) describes only the
     * LISTED SPAN, not the whole bucket. Region/worker-count/fingerprint are folded in by the JSON
     * writer (they live in the run config / process), so this carries only the engine-observed signals.
     * {@code massSkewGini} (over the {@code CHILD_MASS} steal distribution) and the {@code
     * apiLatencyP50Ms}/{@code apiLatencyP99Ms} client-side percentiles are sourced from other
     * subsystems (the facade's steal counters / api-latency timer) and passed as SUPPLIERS, invoked
     * at the same sequence points the facade's pre-extraction code read them: this method snapshots
     * its own accumulator atoms (alphabet cardinality, then divergence depth) FIRST, then invokes the
     * mass-skew supplier, then reads the delimiter-fanout tallies, then invokes the two latency
     * suppliers — preserving the exact end-of-run read order across the two subsystems.
     */
    RunSummary.ShapeSummary buildSummary(DoubleSupplier massSkewGini,
            Supplier<Double> apiLatencyP50Ms, Supplier<Double> apiLatencyP99Ms) {
        int[] cardinality = new int[alphabetPositions];
        int positionsObserved = 0;
        for (int pos = 0; pos < alphabetPositions; pos++) {
            int card = Long.bitCount(alphabetMask.get(2 * pos)) + Long.bitCount(alphabetMask.get(2 * pos + 1));
            cardinality[pos] = card;
            if (card > 0) {
                positionsObserved++;
            }
        }
        long[] depthHistogram = new long[divergenceDepthBuckets];
        for (int i = 0; i < divergenceDepthBuckets; i++) {
            depthHistogram[i] = divergenceDepthHistogram.get(i);
        }
        return new RunSummary.ShapeSummary(
                cardinality,
                positionsObserved,
                depthHistogram,
                massSkewGini.getAsDouble(),
                delimiterFanoutMax.get(),
                delimiterFanoutTotal.get(),
                delimiterProbes.get(),
                apiLatencyP50Ms.get(),
                apiLatencyP99Ms.get());
    }
}
