/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.AlphabetDigest;

/**
 * The chosen victim's state for one steal attempt (algorithms.md §3), read under the
 * lock-guarded coherent {@code (cursor, hi)} snapshot the executor takes <b>after</b>
 * {@link Selection} picks a victim and <b>before</b> any pivot-cascade decision. Distinct from
 * {@link VictimView} (the speculative pool-wide selection read).
 *
 * <p>Source-agnostic (contracts.md §2.1): no {@code WorkerState}, no protocol/wire type.
 * {@link AlphabetDigest} carries no S3/wire dependency either — it is a pure per-worker observed
 * per-position alphabet, the same policy-domain status as {@code StealMath}/{@code ByteMidpoint}.
 *
 * @param victimNodeId                        the chosen victim's {@code listing_node} id
 * @param lo                                   the victim's immutable lower bound ({@code null} = ⊥)
 * @param cursor                               the coherent snapshot cursor ({@code null} = ⊥)
 * @param hi                                   the coherent snapshot bound ({@code null} = open frontier)
 * @param keysEmitted                          keys emitted so far on this range
 * @param densityFraction                      the victim's far-ahead pivot fraction input
 *                                              ({@code WorkerState#densityFraction()} — pure, zero-I/O)
 * @param alphabetDigest                       the victim's observed per-position alphabet, consumed
 *                                              by the alphabet-aware pivot synthesis
 * @param unchangedSinceNonProductiveSteal     {@code true} iff this exact {@code (cursor, hi)}
 *                                              snapshot already proved non-productive on a prior
 *                                              attempt against this victim
 * @param consecutiveZeroFanoutStructureProbes this victim's running consecutive zero-fan-out
 *                                              {@code delimiter=/} structure-probe streak
 * @param consecutiveTimedOutStructureProbes   this victim's running consecutive timed-out
 *                                              structure-probe streak
 */
public record StealAttemptView(
        long victimNodeId,
        byte[] lo,
        byte[] cursor,
        byte[] hi,
        long keysEmitted,
        double densityFraction,
        AlphabetDigest alphabetDigest,
        boolean unchangedSinceNonProductiveSteal,
        int consecutiveZeroFanoutStructureProbes,
        int consecutiveTimedOutStructureProbes) {
}
