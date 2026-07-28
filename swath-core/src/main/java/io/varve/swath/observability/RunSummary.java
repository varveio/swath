/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.time.Duration;
import java.util.List;

/**
 * End-of-run metrics snapshot used for the one-line run summary and the JSON run-summary
 * artifact; see {@code docs/metrics-and-observability.md} §3 for the full field reference, the
 * JSON shape each field renders as, and how every derived ratio is computed.
 *
 * <p>The trailing efficiency/resource fields ({@code costUsd}, {@code keysPerSecond}, and the
 * ratios below) are derived summary/log fields, not Micrometer meters. Resource probes ({@code
 * peakRssBytes}, {@code peakHeapBytes}, {@code cpuSeconds}/{@code cpuEfficiency}) are {@code -1}
 * when unavailable (e.g. non-Linux); {@code overfetchRatio}, {@code pageFillRatio}, {@code
 * emptySplitRatio}, {@code wastedProbeRatio}, {@code stealSuccessRate}, and {@code
 * compressionRatio} instead render {@code 0.0} on a zero denominator, since they are always
 * computable from existing counters (no hot-path cost), just possibly vacuous on a tiny run.
 *
 * <p>{@code duration} is the LISTING clock — the same one {@code keysPerSecond} and every other
 * per-second/per-API-call figure divides by — {@code RunMetrics#markRunStarted()}'s zero point,
 * which a fresh run resets to AFTER seeding. {@code sessionDuration} is the whole CLI invocation's
 * own clock instead, seeding included — the same span the live progress line already reports. The
 * two agree exactly on a resumed or seed-skipped run; a fresh run's seed step is the gap between
 * them. Neither is wrong: {@code duration} is the honest throughput denominator (seeding fetches no
 * object), {@code sessionDuration} is what the operator actually waited on. {@code sessionDuration}
 * equals {@code duration} (never garbage) on any snapshot taken before the session-wide progress
 * reporter has claimed its start — a pre-seed early exit, or a caller that builds a summary directly
 * without ever starting one.
 *
 * <p>{@code timeToFirstStealMs} and {@code timeToPeakInFlightMs} are the run's ramp-up timings —
 * milliseconds from run start to the first work steal / to the instant peak concurrency was first
 * reached, {@code -1} when the event never happened (no steal, or a run that never started). They
 * are the same values {@code list_run_diagnostics} prints, carried here so the JSON report has them
 * too rather than leaving them log-only.
 *
 * <p>{@code avgInFlight} is the time-weighted average in-flight listing count over the run
 * (sampled on every in-flight transition, no polling thread), {@code 0.0} before the run starts.
 * {@code peakInFlight} saturates once the concurrency ceiling is hit, so {@code avgInFlight} is the
 * one that distinguishes sustained parallelism from a brief spike.
 *
 * <p>{@code seed}, {@code shape}, {@code trajectory}, and {@code demandGate} are {@code null}
 * when their mechanism never engaged this run — no seeding (resumed, or a sequential/
 * no-checkpoint run), no page ever fetched, or the demand gate never fired, respectively.
 * {@code slowRanges} and {@code callClassLatency} are empty (never {@code null}) in the
 * equivalent no-data case.
 */
public record RunSummary(
        long runId,
        long objects,
        Duration duration,
        Duration sessionDuration,
        String strategy,
        long apiCalls,
        double costUsd,
        long outputFiles,
        long compressedBytes,
        long keys,
        long pages,
        long peakInFlight,
        double avgInFlight,
        long timeToFirstStealMs,
        long timeToPeakInFlightMs,
        long steals,
        long splits,
        long errors,
        double keysPerSecond,
        double apiCallsPer1kObjects,
        long peakRssBytes,
        long peakHeapBytes,
        double cpuSeconds,
        double cpuEfficiency,
        double overfetchRatio,
        double pageFillRatio,
        double emptySplitRatio,
        double wastedProbeRatio,
        double stealSuccessRate,
        double compressionRatio,
        SeedSummary seed,
        ShapeSummary shape,
        TrajectorySummary trajectory,
        List<SlowRange> slowRanges,
        List<CallClassLatencySummary> callClassLatency,
        List<ClientCostSpan> clientCost,
        DemandGateSummary demandGate) {

    /**
     * One {@code call_class}/{@code phase} latency-percentile summary row — {@code callClass} is
     * one of {@code worker_page}/{@code pivot_probe}/{@code structure_probe} ({@code RunMetrics.CALL_CLASS_*}),
     * {@code phase} one of {@code connect_acquire}/{@code ttfb}/{@code total}/{@code response_parse}
     * ({@code RunMetrics.LATENCY_PHASE_*}). {@code p50Ms}/{@code p90Ms}/{@code p99Ms} come from the underlying
     * Micrometer Timer's {@code publishPercentiles} snapshot; {@code maxMs} and {@code count} are the
     * Timer's own max/count.
     */
    public record CallClassLatencySummary(String callClass, String phase, long count, Double p50Ms, Double p90Ms,
                                           Double p99Ms, double maxMs) {
    }

    /**
     * One client-service-cost span's percentile summary — what servicing a page costs the client
     * once the store has answered, decomposed into the parts that can contend independently.
     * {@code span} is one of {@code checkpoint_commit_wait}/{@code checkpoint_queue_wait}/{@code
     * checkpoint_commit}/{@code emit}/{@code writer_backpressure}/{@code parquet_write} ({@code
     * RunMetrics.CLIENT_COST_SPAN_*}); the remaining part of the decomposition, response parse, is
     * per-call-class and is carried by {@link CallClassLatencySummary} instead. Percentile/max/count
     * semantics are identical to {@link CallClassLatencySummary}'s.
     *
     * <p>The first five spans are PER PAGE. {@code parquet_write} is the deviation: it is one
     * observation per stretch of Parquet writer-lane work (per batch written, plus each idle-cadence
     * rotation and each lane's drain-time finalize/discard), measured on the pool's own threads —
     * so it is neither page-scoped nor part of a page's serial latency. See {@code
     * RunMetrics#recordParquetWrite} for the overlaps that follow from it.
     */
    public record ClientCostSpan(String span, long count, Double p50Ms, Double p90Ms, Double p99Ms,
                                  double maxMs) {
    }

    /**
     * The {@code OWNER_SPLIT.demand_gated} T-vs-Tmax snapshot — {@code events} is the total
     * count of demand-gate suppressions this run; {@code lastT}/{@code minT} are the effective
     * concurrency target observed at the most recent / lowest-T gate event; {@code tMax} is the run's
     * configured ceiling (repeated here so the block is self-contained without cross-referencing
     * {@code config.max_parallel_listings}).
     */
    public record DemandGateSummary(long events, int lastT, int minT, int tMax) {
    }

    /**
     * The seed-step's already-computed shape (§5, promoted from {@code SeedStep}'s log-only
     * {@code seed_shallow} line): {@code mode} is {@code "none"}/{@code "shallow"}/{@code "hints"};
     * {@code probes}/{@code cutPoints}/{@code synthesizedCuts}/{@code ranges} are {@code SeedStep}'s
     * exact accounting for a fresh run's worklist tiling. {@code decisions} is the per-probed-
     * level seed decision trace — one entry per structure probe the seed step issued (bounded by the
     * same probe cap SeedStep already enforces, ≤ ~256).
     */
    public record SeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts, long ranges,
                               List<SeedDecision> decisions) {

        /** Convenience constructor: no decision trace (an empty list, never {@code null}). */
        public SeedSummary(String mode, long probes, long cutPoints, long synthesizedCuts, long ranges) {
            this(mode, probes, cutPoints, synthesizedCuts, ranges, List.of());
        }

        /**
         * One probed {@code delimiter=/} level's decision: {@code prefix} is the probed
         * directory (display-escaped/truncated, same idiom as every other byte-key rendering here);
         * {@code fanout} is that level's raw {@code CommonPrefixes} count; {@code classification} is
         * one of {@code narrow} (kept descending), {@code tiny_leaf_explosion} (truncated WITH
         * common prefixes — a plain, non-{@code key=value/} directory explosion left whole),
         * {@code fanout_tiled} (truncated WITH common prefixes that are Hive/Spark
         * {@code key=value/} partition directories — tiled at seed time along a {@code W}-capped
         * subset of those prefixes), {@code flat_wide} (truncated, no
         * common prefixes, a dense-flat region — a radix-banding CANDIDATE), {@code
         * dense_root_radix_banded} (a {@code flat_wide} region that was actually banded), or {@code
         * delimiter_seeded} (the top level, when the run's overall shape is the generic plain-tiled
         * case — no dense-root/tiny-leaf/partition subtype applied). {@code cutsKept}/{@code
         * cutsDiscarded} are this level's OWN contribution to the global cut-point set (kept = new
         * distinct cut byte strings added; discarded = duplicates of a cut already present) — 0/0 for
         * a level whose common prefixes were never tiled (a {@code tiny_leaf_explosion}/{@code
         * flat_wide} level, left to work-stealing instead), positive for a {@code fanout_tiled} level.
         */
        public record SeedDecision(String prefix, int fanout, boolean truncated, String classification,
                                    int cutsKept, int cutsDiscarded) {
        }
    }

    /**
     * The bounded (fixed {@code TRAJECTORY_BINS}-count) time-bin rollup of in-flight
     * concurrency + progress rate over the run — see {@code RunMetrics}'s trajectory fields for the
     * ring-doubling bin-merge scheme that keeps this at constant memory regardless of run length.
     * {@code inFlight}/{@code progressRate} are parallel arrays, one entry per bin actually used
     * (never zero-padded to the full bin count). {@code collapseAtFrac} is {@code -1.0} when the run
     * never permanently collapsed to {@code <= 2} in-flight before ending (the good outcome).
     */
    public record TrajectorySummary(
            double[] inFlight,
            double[] progressRate,
            double serialFrac,
            double collapseAtFrac,
            int peakWorkers,
            int finalWorkers) {
    }

    /**
     * One live range's terminal/mid-run diagnostic snapshot — bounds are display-escaped/
     * truncated (same idiom as every other byte-key rendering here; never raw, unbounded key
     * bytes). {@code estRemaining}/{@code drainRate} are that range's own remaining-work estimate —
     * taken through the run's own {@code RemainingWorkEstimator}, so a dumped estimate is the one the
     * run's decisions were taken on — and observed keys/sec; the four tallies are cheap per-range
     * counters bumped alongside the existing global {@code swath.steal_reason} counters at the same
     * decision points (never a new hot-path check) — see {@code WorkerState}.
     */
    public record SlowRange(String lo, String hi, String cursor, double estRemaining, double drainRate,
                             long cursorPassedPivot, long noPivot, long structureSuppressed, long demandGated) {
    }

    /**
     * Shape feature-vector — the engine-observed post-hoc classification signals.
     * All are run-level AGGREGATES over the listed span. Region/worker-count/fingerprint
     * are added by the JSON writer from the run config/process, so are not carried here.
     *
     * <ul>
     *   <li>{@code alphabetCardinality} — the number of distinct printable-ASCII scalars
     *       observed at each of {@value RunMetrics#ALPHABET_POSITIONS} relative code-point positions
     *       past each range's divergence point, UNION-ed across every completed node. A single vector
     *       washes out per-range drift — a v1 corpus signal. {@code entropy} is NOT computed (only
     *       cardinality is tracked in {@code AlphabetDigest}).</li>
     *   <li>{@code alphabetPositionsObserved} — how many of those positions saw any signal.</li>
     *   <li>{@code divergenceDepthHistogram} — the LCP-depth distribution of split pivots
     *       (byte index where a pivot diverges from its range's cursor); the last bucket is depth
     *       {@code >= DIVERGENCE_DEPTH_BUCKETS-1}.</li>
     *   <li>{@code massSkewGini} — inequality of per-child emitted mass, a coarse 4-bucket
     *       approximation over the {@code CHILD_MASS} distribution.</li>
     *   <li>{@code delimiterFanoutMax}/{@code delimiterFanoutTotal}/{@code delimiterProbes} —
     *       the widest / total delimiter=/ child count observed and the number of structure/seed
     *       probes that observed it.</li>
     *   <li>{@code apiLatencyP50Ms}/{@code apiLatencyP99Ms} — client-side percentiles of
     *       {@code swath.api.latency}; {@code null} when no call was timed.</li>
     * </ul>
     */
    public record ShapeSummary(
            int[] alphabetCardinality,
            int alphabetPositionsObserved,
            long[] divergenceDepthHistogram,
            double massSkewGini,
            long delimiterFanoutMax,
            long delimiterFanoutTotal,
            long delimiterProbes,
            Double apiLatencyP50Ms,
            Double apiLatencyP99Ms) {
    }
}
