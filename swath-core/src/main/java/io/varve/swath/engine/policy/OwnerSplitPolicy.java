/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * <b>The policy seam</b> (contracts.md §2.1): the owner-side proactive
 * self-split's gate chain (algorithms.md §3.3) as a pure decision over one page-commit's {@link
 * OwnerSplitView} — every clock read, lock, durable-split CAS, and mutation of a shared
 * collaborator (the confetti feedback gate's probe sequence, issue #22) left to the executor
 * ({@code OwnerSelfSplit}). The engine and a future discrete-event simulator share this; each
 * supplies its own execution side.
 *
 * <p>One call per page-commit consideration, unlike {@link StealPolicy}'s two-call shape — there is
 * no multi-round probe cascade here (owner-split is zero-probe by construction, algorithms.md §3.3):
 * {@link #decide} runs the whole gate chain to a terminal {@link OwnerSplitDecision} in one call.
 *
 * <p>Source-agnostic (contracts.md §2.1, binding since before this interface's dispatch): no
 * S3/protocol type crosses into this package's views or decisions — keys as raw bytes, counts, and
 * policy-domain types only.
 */
public interface OwnerSplitPolicy {

    /**
     * Run the gate chain against one page-commit's {@link OwnerSplitView}: the remaining-work
     * floor, the page rate-limit, the demand gate, the observed-mass child-tail floor, the confetti
     * feedback gate, then pivot synthesis (rank-space {@code interpolate}), the reflection clamp,
     * and the reflect-lift.
     */
    OwnerSplitDecision decide(OwnerSplitView view);
}
