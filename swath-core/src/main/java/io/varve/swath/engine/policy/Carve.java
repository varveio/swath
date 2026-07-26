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
 * @param pivot the pivot {@code m} to split at ({@code cursorTo < m <= hi})
 */
public record Carve(byte[] pivot, List<Engagement> engagements) implements OwnerSplitDecision {
}
