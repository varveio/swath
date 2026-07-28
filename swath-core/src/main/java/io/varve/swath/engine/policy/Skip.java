/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * This page-commit's owner-side proactive self-split does not carve (algorithms.md §3.3): one gate
 * in the chain blocked it, or the range is unbounded. {@code engagements} may be empty even for a
 * genuine suppressed carve — some gates are deliberately silent; see
 * {@link OwnerSplitSkipReason}'s per-constant javadoc. {@code mutations} is non-empty only for
 * {@link OwnerSplitSkipReason#CONFETTI_SUPPRESSED}. {@code gateInputs} carries what the chain read
 * on its way to this gate (for the {@code owner_split_decision} trace event) — {@code null} only
 * for {@link OwnerSplitSkipReason#OPEN_FRONTIER}, which reads nothing.
 */
public record Skip(OwnerSplitSkipReason reason, List<Engagement> engagements,
                   List<OwnerSplitMutation> mutations, OwnerSplitGateInputs gateInputs)
        implements OwnerSplitDecision {

    /** As the canonical constructor, for a policy that records no {@link OwnerSplitGateInputs}. */
    public Skip(OwnerSplitSkipReason reason, List<Engagement> engagements, List<OwnerSplitMutation> mutations) {
        this(reason, engagements, mutations, null);
    }
}
