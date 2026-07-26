/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The cascade produced a pivot to hand off. Not yet a split — the executor still re-validates
 * {@code (cursor, hi)} under the victim's lock and runs the durable CAS ({@code splitTxn},
 * algorithms.md §3/§4.3) before this becomes {@code CHILD_CREATED}; a lock-loser downgrades this
 * into one of the executor's own lock-scoped {@code RETRY}s ({@code bound_moved}/
 * {@code cursor_passed_pivot}/{@code split_aborted}), never a policy-level {@link Retry}.
 *
 * @param pivot     the pivot {@code m} to split at ({@code c < m < H})
 * @param mechanism which named mechanism produced this pivot (§5 winning-mechanism engagement)
 */
public record Commit(byte[] pivot, PivotMechanism mechanism, List<Engagement> engagements,
                     List<VictimMutation> mutations) implements StealAction {
}
