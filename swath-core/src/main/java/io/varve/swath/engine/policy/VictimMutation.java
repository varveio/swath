/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * A per-VICTIM durable-state change the policy decided but does not itself apply — the policy
 * never sees live {@code WorkerState} (seam-notes.md), so every mutation to it is returned as data
 * for the executor to apply to the real object identified by {@link #victimNodeId()}. Each variant
 * corresponds 1:1 to an existing {@code WorkerState} mutator; the extraction changes who calls it,
 * never what it does.
 *
 * <p>{@link Selection} may return mutations against <b>several</b> pool candidates in one call
 * (every paced candidate visited during selection consumes a cooldown skip, whether or not it is
 * ultimately chosen); a {@link StealAction} from a {@link StealAttempt} returns mutations against
 * only that attempt's single victim. Carrying {@link #victimNodeId()} on every variant lets both
 * call sites share one type instead of a selection-only wrapper.
 */
public sealed interface VictimMutation
        permits ConsumePacingSkip, MarkNonProductive, RecordFutileSteal, SetUnsplittable,
                RecordNoPivotTally, RecordZeroFanoutStructureProbe, ResetZeroFanoutStructureProbes {

    /** The {@code listing_node} id of the victim this mutation applies to. */
    long victimNodeId();
}
