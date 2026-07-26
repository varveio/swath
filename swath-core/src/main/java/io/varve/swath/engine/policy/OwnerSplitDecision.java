/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The result of {@link OwnerSplitPolicy#decide}: either {@link Skip} (no carve this page-commit) or
 * {@link Carve} (a pivot to hand off) — algorithms.md §3.3.
 *
 * @see Skip
 * @see Carve
 */
public sealed interface OwnerSplitDecision permits Skip, Carve {

    /**
     * Engagement marks fired while deciding (e.g. {@code OWNER_SPLIT.demand_gated}, {@code
     * ALPHABET.alphabet_chosen}). Not every {@link Skip} carries one — some gates are, by design,
     * silent (no counter); see {@link OwnerSplitSkipReason}'s per-constant javadoc.
     */
    List<Engagement> engagements();
}
