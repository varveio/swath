/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.AlphabetDigest;

/**
 * The owner's state for one page-commit's proactive self-split consideration (algorithms.md §3.3),
 * read while the caller still holds {@code WorkerState#lock()}. Distinct from {@link VictimView}/
 * {@link StealAttemptView} (the thief's own reads) — this is the DRAINING worker's own range, not a
 * candidate picked from a pool.
 *
 * <p>Source-agnostic (contracts.md §2.1): no {@code WorkerState}, no protocol/wire type. {@code
 * committed}/{@code lastSelfSplitPage} are the owner-split rate-limit's caller-owned bookkeeping
 * ({@code selfSplit[0]}/{@code selfSplit[1]} in the executor) — plain counts, not live state.
 *
 * @param hi                the owner's current upper bound ({@code null} = the open frontier)
 * @param lo                the owner's immutable range start ({@code null} = ⊥)
 * @param cursorTo          the cursor position this page-commit just advanced to
 * @param keysEmitted       keys emitted so far on this range
 * @param committed         non-empty pages committed so far on this range, including this one
 *                          ({@code WorkStealingScan}'s {@code selfSplit[0]} after this commit's
 *                          increment; meaningless when {@code hi == null} — the executor never
 *                          increments the counter on an open frontier, see
 *                          {@code OwnerSelfSplit#maybeOwnerSelfSplit})
 * @param lastSelfSplitPage the page index of this range's last published self-split ({@code
 *                          selfSplit[1]} before this commit)
 * @param outstanding       the live-node demand-gate count ({@code outstanding.get()}), read AT USE
 *                          TIME by the executor immediately before this call
 * @param densityFraction   the owner's far-ahead pivot fraction input ({@code
 *                          WorkerState#densityFraction()} — pure, zero-I/O)
 * @param observedDensityRatio the owner's observed local-vs-average density ratio ({@code
 *                          WorkerState#observedDensityRatio()} — pure, zero-I/O, raw/pre-toggle)
 * @param alphabetDigest    the owner's observed per-position alphabet, consumed by the
 *                          alphabet-aware pivot synthesis
 * @param confetti          a coherent read of the confetti feedback gate's counters at this
 *                          page-commit (issue #22: the executor snapshots {@code
 *                          ConfettiFeedbackGate} once when building this view, so the governor's
 *                          decision is a pure function of it — no live gate reference crosses in)
 */
public record OwnerSplitView(
        byte[] hi,
        byte[] lo,
        byte[] cursorTo,
        long keysEmitted,
        long committed,
        long lastSelfSplitPage,
        long outstanding,
        double densityFraction,
        double observedDensityRatio,
        AlphabetDigest alphabetDigest,
        ConfettiObservation confetti) {
}
