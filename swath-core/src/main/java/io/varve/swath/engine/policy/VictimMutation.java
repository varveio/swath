/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * A per-VICTIM durable-state change the policy decided but does not itself apply — the policy
 * never sees live {@code WorkerState} (contracts.md §2.1), so every mutation to it is returned as data
 * for the executor to apply to the real object identified by {@link #victimNodeId()}. Every
 * {@link Kind} corresponds 1:1 to an existing {@code WorkerState} mutator; the extraction changes
 * who calls it, never what it does. A single record with a {@code Kind} tag, not a sealed
 * hierarchy of one-field cases: every case here carries the identical shape (just a victim id) and
 * differs only by name, which a tagged enum models more honestly than seven near-empty types would
 * (contrast {@link StealAction}/{@link Selection}/{@link ProbeOutcome}, whose cases carry genuinely
 * different payloads and so earn their own files).
 *
 * <p>{@link Selection} may return mutations against <b>several</b> pool candidates in one call
 * (every paced candidate visited during selection consumes a cooldown skip, whether or not it is
 * ultimately chosen); a {@link StealAction} from a {@link StealAttempt} returns mutations against
 * only that attempt's single victim. Carrying {@link #victimNodeId()} on every instance lets both
 * call sites share one type instead of a selection-only wrapper.
 */
public record VictimMutation(long victimNodeId, Kind kind) {

    /** Which existing {@code WorkerState} mutator to apply. */
    public enum Kind {
        /** Mirrors {@code WorkerState#stealPaced()}'s mutating decrement of the futility cooldown. */
        CONSUME_PACING_SKIP,
        /** Mirrors {@code WorkerState#markNonProductiveSteal(Snapshot)}. */
        MARK_NON_PRODUCTIVE,
        /** Mirrors {@code WorkerState#recordFutileSteal()}. */
        RECORD_FUTILE_STEAL,
        /** Mirrors {@code WorkerState#setUnsplittable(true)}. */
        SET_UNSPLITTABLE,
        /** Mirrors {@code WorkerState#recordNoPivot()} (the slow-range dump's per-range tally). */
        RECORD_NO_PIVOT_TALLY,
        /** Mirrors {@code WorkerState#recordZeroFanoutStructureProbe()}. */
        RECORD_ZERO_FANOUT_STRUCTURE_PROBE,
        /** Mirrors {@code WorkerState#resetZeroFanoutStructureProbes()}. */
        RESET_ZERO_FANOUT_STRUCTURE_PROBES,
        /** Mirrors {@code WorkerState#recordStructureSuppressed()} (the slow-range dump's per-range tally). */
        RECORD_STRUCTURE_SUPPRESSED_TALLY
    }
}
