/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/** Mirrors {@code WorkerState#recordNoPivot()} (the slow-range dump's per-range tally). */
public record RecordNoPivotTally(long victimNodeId) implements VictimMutation {
}
