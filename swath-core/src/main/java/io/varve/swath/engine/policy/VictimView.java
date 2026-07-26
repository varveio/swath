/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * One candidate in the thief's victim pool (algorithms.md §3.2), read <b>speculatively</b> during
 * victim selection: {@code cursor}/{@code hi} are not a coherent snapshot — the coherent read
 * happens only after a victim is chosen, and is carried separately by {@link StealAttemptView}.
 *
 * <p>Source-agnostic (contracts.md §2.1): no {@code WorkerState}, no protocol/wire type — keys as raw
 * bytes, counts, and booleans only, so the same view shape serves the engine and a future
 * discrete-event simulator alike.
 *
 * @param nodeId              the candidate's {@code listing_node} id
 * @param lo                  the immutable lower bound of {@code (lo, hi]} ({@code null} = ⊥)
 * @param cursor              the speculative in-memory cursor ({@code null} = ⊥)
 * @param hi                  the speculative upper bound ({@code null} = the open frontier)
 * @param keysEmitted         keys emitted so far on this range
 * @param unsplittable        {@code true} iff a prior steal attempt cached this range as terminal
 * @param pacingSkipAvailable {@code true} iff this victim is currently in a per-victim futility
 *                            cooldown with at least one skip remaining — the fact
 *                            {@code WorkerState#stealPaced()} would observe and consume.
 *                            {@link Selection}'s returned mutations tell the executor exactly
 *                            which candidates to consume a skip on, preserving the mutate-on-read
 *                            semantics without the policy touching live worker state.
 */
public record VictimView(
        long nodeId,
        byte[] lo,
        byte[] cursor,
        byte[] hi,
        long keysEmitted,
        boolean unsplittable,
        boolean pacingSkipAvailable) {
}
