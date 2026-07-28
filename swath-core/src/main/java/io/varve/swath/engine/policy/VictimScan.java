/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * What one {@link StealPolicy#selectVictim} pass over the pool saw — the payload of the {@code
 * victim_scan} trace event (docs/internals/metrics-internals.md §7). Carried on the {@link
 * Selection} because selection is a PURE policy: it never holds the {@code TraceSink}, so the
 * executor ({@code Thief}) emits the event out of what the selection returns, exactly as it already
 * records the selection's {@link Engagement}s (contracts.md §2.1).
 *
 * <p><b>Aggregate per scan, never per candidate.</b> Selection runs constantly (once per steal
 * attempt, over the whole live pool), so a per-victim record would dominate the trace; the
 * per-scan tallies are enough to attribute a refusal once joined against the {@code claimed}/{@code
 * page_committed} events, which map {@code node_id} back to a range.
 *
 * @param seen                 candidates examined (the pool size)
 * @param skippedUnsplittable  candidates skipped as already marked unsplittable
 * @param skippedPaced         candidates skipped by the futility-pacing cooldown (before any estimate)
 * @param skippedNoSpan        candidates whose {@code estRemaining} read {@code <= 0}
 * @param bestEst              the winning candidate's {@code estRemaining} — {@link
 *                             Double#NEGATIVE_INFINITY} when nothing qualified (the argmax's own
 *                             "nothing seen yet" seed), {@link Double#POSITIVE_INFINITY} for an
 *                             open-frontier winner
 */
public record VictimScan(
        int seen,
        int skippedUnsplittable,
        int skippedPaced,
        int skippedNoSpan,
        double bestEst) {
}
