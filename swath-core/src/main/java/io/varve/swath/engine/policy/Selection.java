/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The result of {@link StealPolicy#selectVictim(List)}: {@code argmax estRemaining} over the
 * curated pool (algorithms.md §3.2), or the discriminated reason no candidate qualified.
 *
 * @see Selected
 * @see NoVictim
 */
public sealed interface Selection permits Selected, NoVictim {

    /** Engagement marks fired while scanning the pool (e.g. {@code STEAL.futility_paced}). */
    List<Engagement> engagements();

    /** Per-victim durable-state mutations to apply, against possibly several pool candidates. */
    List<VictimMutation> mutations();

    /**
     * What this pass over the pool saw (candidates seen, per-cause skips, the winning estimate), for
     * the executor's {@code victim_scan} trace event (§7) — {@code null} for a selection built by a
     * caller that does not populate it. See {@link VictimScan}.
     */
    VictimScan scan();
}
