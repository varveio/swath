/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The owner-split governor produced a pivot to hand off (algorithms.md §3.3). Not yet a split — the
 * executor still narrows {@code hi}, runs the durable split CAS ({@code splitTxn}), and hands the
 * child off under the owner's own lock; a CAS rejection ({@code self_aborted}) is entirely an
 * executor-owned outcome, never a {@link Carve} vs {@link Skip} choice this decision makes (mirrors
 * {@link Commit}'s own late-loser note).
 *
 * @param pivot      the pivot {@code m} to split at ({@code cursorTo < m <= hi})
 * @param mutations  non-empty exactly when the confetti feedback gate's periodic probe let this
 *                   carve through past its over-threshold branch ({@code confetti_probe})
 * @param gateInputs what the chain read on its way to admitting this carve, for the {@code
 *                   owner_split_decision} trace event — see {@link OwnerSplitGateInputs}
 */
public record Carve(byte[] pivot, List<Engagement> engagements, List<OwnerSplitMutation> mutations,
                    OwnerSplitGateInputs gateInputs)
        implements OwnerSplitDecision {

    /** As the canonical constructor, for a policy that records no {@link OwnerSplitGateInputs}. */
    public Carve(byte[] pivot, List<Engagement> engagements, List<OwnerSplitMutation> mutations) {
        this(pivot, engagements, mutations, null);
    }
}
