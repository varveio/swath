/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.time.Duration;

/**
 * One immutable sample of a run's live state, built once per {@link RunProgressReporter} tick and
 * fanned out to the installed {@link ProgressSink} — the whole payload a presentation layer gets,
 * so no renderer ever reaches back into {@link RunMetrics} (which would re-mutate the windowed-rate
 * baseline and produce cadence-dependent garbage rates).
 *
 * <p><b>Run-level fields</b> ({@code runId}..{@code retries}) are meaningful in every phase.
 * <b>Phase-shaped state</b> lives in {@link Seeding}/{@link Listing}/{@link Merging}: exactly one
 * of them is non-{@code null}, selected by {@link #phase()} — a seeding run has no {@code
 * in_flight}/{@code pages}, and a merging run has no worker concurrency, so neither is forced to
 * carry listing's shape. During seeding the "API attempts / retries" an operator watches for
 * liveness ARE the run-level {@link #apiCalls()}/{@link #retries()}: seeding is the only activity
 * in flight then, so they are not duplicated into {@link Seeding}.
 *
 * <p><b>Elapsed</b> is always the WHOLE session ({@link #sessionElapsed()}, from the first progress
 * start — before the seed probe, not per phase), with {@link #phaseElapsed()} alongside it; a phase
 * transition never silently resets the clock a renderer shows.
 *
 * <p><b>{@link #completion()} is {@code null} for listing</b>, and that is deliberate: an unsorted
 * scan has no honest denominator (no total object count exists before the run ends), so there is no
 * bar, no percentage and no ETA. It is populated only where numerator and denominator both have
 * exact, documented semantics — seeding against its bounded probe budget, and the merge against the
 * staged row count.
 */
public record ProgressEvent(
        Phase phase,
        long runId,
        String strategy,
        Duration sessionElapsed,
        Duration phaseElapsed,
        long apiCalls,
        double estimatedCostUsd,
        long retries,
        Completion completion,
        Seeding seeding,
        Listing listing,
        Merging merging) {

    /** What a {@link Completion} counts — never a bare string, so a renderer can label it itself. */
    public enum Unit {
        /** Structure probes issued by the seed step, against its bounded budget. */
        PROBES,
        /** Rows read out of the staged sort segments by the k-way merge. */
        ROWS
    }

    /**
     * A phase-specific completion figure with EXACT semantics on both sides: {@code done} of
     * {@code total} {@code unit}, both counted by the same code path. Only ever built where such a
     * denominator genuinely exists — see the record javadoc.
     */
    public record Completion(long done, long total, Unit unit) {

        /** {@code done/total}, clamped to {@code [0,1]}; {@code 0.0} on a zero/unknown total. */
        public double fraction() {
            return total > 0 ? Math.clamp((double) done / total, 0.0, 1.0) : 0.0;
        }
    }

    /**
     * Seed-phase state. {@code sinceLastProbe} is the age of the most recently COMPLETED probe —
     * the signal that distinguishes "seeding, healthy" (a young age, ticking over) from "hung" (an
     * age that keeps growing), which nothing could distinguish before: a seeding run reports zero
     * emitted objects, zero pages and zero workers by construction.
     */
    public record Seeding(long probesCompleted, long probeBudget, Duration sinceLastProbe) {
    }

    /**
     * Listing-phase state. {@code sessionObjects} is what THIS process emitted; {@code
     * recoveredObjects} is what a resume carried over from a previous attempt. They are separate so
     * a resumed run neither displays a zero it did not earn nor jumps by billions when the
     * recovered rows are folded in for the terminal summary.
     */
    public record Listing(long sessionObjects, long recoveredObjects, double liveObjectsPerSecond,
                          double averageObjectsPerSecond, long pages, long inFlight,
                          long concurrencyTarget, long steals, long splits) {
    }

    /**
     * Merge/publish-phase state. {@code sessionRowsMerged} counts rows merged since this phase
     * began (not the run's emitted objects, which stay flat through the merge and are 0 outright on
     * a merge-only resume), against {@code stagedRows} — the exact row total of the segments handed
     * to the merge. {@code passes} is only non-zero once the whole cascade finishes.
     */
    public record Merging(long sessionRowsMerged, long stagedRows, long segments, long passes) {
    }
}
